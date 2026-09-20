"""
Holodyx Chat v3 — звонки, поиск, Telegram-фон.
Запуск: python chat.py
"""

import os
import re
import uuid
import sqlite3
import secrets
from flask import Flask, request, jsonify, session, send_from_directory, Response
from flask_socketio import SocketIO, emit, join_room
from werkzeug.security import generate_password_hash, check_password_hash

# ============================================================
# НАСТРОЙКИ
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, 'chat_uploads')
AVATAR_DIR = os.path.join(UPLOAD_DIR, 'avatars')
MEDIA_DIR = os.path.join(UPLOAD_DIR, 'media')
DB_PATH = os.path.join(BASE_DIR, 'chat.db')

os.makedirs(AVATAR_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

MAX_MEDIA = 50 * 1024 * 1024


# ============================================================
# БД
# ============================================================
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            avatar TEXT,
            bio TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room TEXT NOT NULL,
            sender_id INTEGER,
            sender_name TEXT,
            type TEXT DEFAULT 'text',
            text TEXT DEFAULT '',
            media_url TEXT,
            media_name TEXT,
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_msg_room ON messages(room, id)")
    c.execute("""
        CREATE TABLE IF NOT EXISTS reads (
            user_id INTEGER,
            room TEXT,
            last_read_id INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, room)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS dialogs (
            user_id INTEGER,
            peer_name TEXT,
            last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(user_id, peer_name)
        )
    """)
    conn.commit()
    conn.close()


def create_user(email, username, password):
    conn = db()
    try:
        conn.execute(
            "INSERT INTO users (email, username, password_hash) VALUES (?, ?, ?)",
            (email.lower(), username, generate_password_hash(password))
        )
        conn.commit()
        row = conn.execute("SELECT id FROM users WHERE email = ?", (email.lower(),)).fetchone()
        return row['id'] if row else None
    except sqlite3.IntegrityError as e:
        print(f"[create_user] {e}")
        return None
    finally:
        conn.close()


def user_by_email(email):
    conn = db(); r = conn.execute("SELECT * FROM users WHERE email = ?", (email.lower(),)).fetchone(); conn.close()
    return dict(r) if r else None


def user_by_username(username):
    conn = db(); r = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone(); conn.close()
    return dict(r) if r else None


def user_by_id(uid):
    conn = db(); r = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone(); conn.close()
    return dict(r) if r else None


def search_users(q, exclude_id, limit=20):
    conn = db()
    rows = conn.execute(
        """SELECT id, username, avatar, bio FROM users
           WHERE username LIKE ? AND id != ?
           ORDER BY username LIMIT ?""",
        (f'%{q}%', exclude_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_profile(uid, **fields):
    allowed = {'username', 'avatar', 'bio'}
    sets, vals = [], []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = ?"); vals.append(v)
    if not sets: return True
    vals.append(uid)
    conn = db()
    try:
        conn.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def add_msg(room, sender_id, sender_name, type_, text='', media_url=None, media_name=None):
    conn = db()
    conn.execute(
        "INSERT INTO messages (room, sender_id, sender_name, type, text, media_url, media_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (room, sender_id, sender_name, type_, text, media_url, media_name)
    )
    conn.commit()
    mid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()['id']
    r = conn.execute("SELECT * FROM messages WHERE id = ?", (mid,)).fetchone()
    conn.close()
    return dict(r)


def history(room, limit=200):
    conn = db()
    rows = conn.execute(
        "SELECT * FROM messages WHERE room = ? ORDER BY id DESC LIMIT ?",
        (room, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def mark_read(uid, room, last_id):
    conn = db()
    conn.execute(
        "INSERT INTO reads (user_id, room, last_read_id) VALUES (?, ?, ?) ON CONFLICT(user_id, room) DO UPDATE SET last_read_id = excluded.last_read_id",
        (uid, room, last_id)
    )
    conn.commit(); conn.close()


def reads_for_room(room):
    conn = db()
    rows = conn.execute("SELECT user_id, last_read_id FROM reads WHERE room = ?", (room,)).fetchall()
    conn.close()
    return {r['user_id']: r['last_read_id'] for r in rows}


def add_dialog(uid, peer_name):
    conn = db()
    conn.execute(
        """INSERT INTO dialogs (user_id, peer_name, last_activity) VALUES (?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(user_id, peer_name) DO UPDATE SET last_activity = CURRENT_TIMESTAMP""",
        (uid, peer_name)
    )
    conn.commit(); conn.close()


def list_dialogs(uid):
    conn = db()
    rows = conn.execute(
        "SELECT peer_name FROM dialogs WHERE user_id = ? ORDER BY last_activity DESC LIMIT 100",
        (uid,)
    ).fetchall()
    conn.close()
    return [r['peer_name'] for r in rows]


# ============================================================
# FLASK
# ============================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))
app.config['MAX_CONTENT_LENGTH'] = MAX_MEDIA
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('HTTPS', '0') == '1'

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading',
                    ping_timeout=60, ping_interval=25)

online = {}
name_to_sid = {}

ALLOWED_AVATAR = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
ALLOWED_IMAGE = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
ALLOWED_VIDEO = {'mp4', 'webm', 'mov', 'mkv'}
ALLOWED_AUDIO = {'webm', 'ogg', 'mp3', 'wav', 'm4a'}


def ext_of(fn):
    return fn.rsplit('.', 1)[-1].lower() if '.' in fn else ''


def dm_room(a, b):
    return 'dm:' + '|'.join(sorted([a.lower(), b.lower()]))


def serialize(m):
    return {
        'id': m['id'], 'room': m['room'], 'sender_id': m['sender_id'],
        'sender_name': m['sender_name'], 'type': m['type'], 'text': m['text'],
        'media_url': m['media_url'], 'media_name': m['media_name'], 'time': m['time'],
    }


def online_list():
    seen = {}
    for u in online.values():
        seen[u['id']] = {'id': u['id'], 'username': u['username'],
                         'avatar': u['avatar'], 'bio': u.get('bio', '')}
    return list(seen.values())


def broadcast_online():
    socketio.emit('online', {'users': online_list()}, to='general')


# ============================================================
# HTML
# ============================================================
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Holodyx Chat</title>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d0f12; --panel:#14171c; --panel-2:#1a1e24; --hover:#1f242b;
  --text:#e8eaed; --text-2:#8b929c; --text-3:#5f6772;
  --border:#232830; --accent:#3b82f6; --accent-hover:#2563eb;
  --danger:#ef4444; --success:#22c55e;
  --own:#2b3542; --other:#1a1e24;
}
html,body{height:100%;overflow:hidden}
body{font-family:-apple-system,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--text);font-size:14px;display:flex;align-items:center;justify-content:center}
button,input,textarea{font-family:inherit;color:inherit}
button{cursor:pointer;border:none;background:none}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:#2a3038;border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:#3a424c}

/* AUTH */
#auth{width:100%;max-width:380px;background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:32px;margin:20px}
#auth h1{font-size:22px;font-weight:600;text-align:center;margin-bottom:4px}
#auth .sub{text-align:center;color:var(--text-2);font-size:13px;margin-bottom:24px}
#auth .tabs{display:flex;background:var(--panel-2);border-radius:8px;padding:3px;margin-bottom:16px}
#auth .tabs button{flex:1;padding:8px;font-size:13px;font-weight:500;color:var(--text-2);border-radius:6px;transition:.15s}
#auth .tabs button.active{background:var(--panel);color:var(--text)}
#auth input{width:100%;padding:11px 13px;margin-bottom:10px;background:var(--panel-2);border:1px solid var(--border);border-radius:8px;font-size:14px;outline:none;transition:.15s}
#auth input:focus{border-color:var(--accent)}
#auth .submit{width:100%;padding:11px;background:var(--accent);border-radius:8px;font-weight:500;font-size:14px;transition:.15s}
#auth .submit:hover{background:var(--accent-hover)}
#auth .submit:disabled{opacity:.5;cursor:wait}
#auth .err{color:var(--danger);font-size:13px;text-align:center;margin-top:10px;min-height:18px}
#auth .ok{color:var(--success);font-size:13px;text-align:center;margin-top:10px;min-height:18px}

/* APP */
#app{display:none;width:100vw;height:100vh;grid-template-columns:300px 1fr}
#app.active{display:grid}

/* SIDEBAR */
#sidebar{background:var(--panel);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
#sidebarHeader{padding:12px 14px;display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--border)}
.avatar-sm{width:36px;height:36px;border-radius:50%;flex-shrink:0;overflow:hidden;background:#2a3038;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:13px;color:var(--text-2)}
.avatar-sm img{width:100%;height:100%;object-fit:cover}
#sidebarHeader .info{flex:1;min-width:0}
#sidebarHeader .name{font-weight:500;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#sidebarHeader .sub{font-size:12px;color:var(--success)}
.icon-btn{width:32px;height:32px;border-radius:6px;display:flex;align-items:center;justify-content:center;color:var(--text-2);transition:.15s}
.icon-btn:hover{background:var(--hover);color:var(--text)}
.icon-btn svg{width:18px;height:18px}
#search{padding:8px 12px;border-bottom:1px solid var(--border)}
#search input{width:100%;padding:8px 10px;background:var(--panel-2);border:1px solid transparent;border-radius:6px;font-size:13px;outline:none}
#search input:focus{border-color:var(--border);background:var(--bg)}
#chatList{flex:1;overflow-y:auto;padding:6px}
.chat-item{display:flex;gap:10px;align-items:center;padding:9px 10px;border-radius:8px;cursor:pointer;position:relative;transition:background .12s}
.chat-item:hover{background:var(--hover)}
.chat-item.active{background:var(--hover)}
.chat-item .avatar{width:40px;height:40px;border-radius:50%;flex-shrink:0;overflow:hidden;background:#2a3038;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;color:var(--text-2);position:relative}
.chat-item .avatar img{width:100%;height:100%;object-fit:cover}
.chat-item .avatar .online-dot{position:absolute;bottom:0;right:0;width:11px;height:11px;border-radius:50%;background:var(--success);border:2px solid var(--panel)}
.chat-item .body{flex:1;min-width:0}
.chat-item .row{display:flex;justify-content:space-between;align-items:baseline;gap:6px}
.chat-item .title{font-weight:500;font-size:13.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chat-item .time{font-size:11px;color:var(--text-3);flex-shrink:0}
.chat-item .preview{font-size:12.5px;color:var(--text-2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-top:1px}
.chat-item .badge{position:absolute;top:50%;transform:translateY(-50%);right:10px;background:var(--accent);color:#fff;font-size:11px;font-weight:600;padding:2px 6px;border-radius:10px;display:none}
.chat-item.unread .badge{display:block}

/* MAIN with Telegram-style background */
#main{
  display:flex;flex-direction:column;position:relative;
  overflow:hidden;
  background:#0d0f12;
}
/* мягкие цветные пятна */
#main::before{
  content:'';
  position:absolute;inset:0;
  background-image:
    radial-gradient(circle at 15% 25%, rgba(59,130,246,.10) 0%, transparent 35%),
    radial-gradient(circle at 85% 15%, rgba(124,92,255,.09) 0%, transparent 30%),
    radial-gradient(circle at 75% 75%, rgba(59,130,246,.07) 0%, transparent 40%),
    radial-gradient(circle at 25% 85%, rgba(124,92,255,.06) 0%, transparent 35%),
    radial-gradient(circle at 50% 50%, rgba(59,130,246,.04) 0%, transparent 50%);
  pointer-events:none;
  z-index:0;
}
/* паттерн из тонких фигур */
#main::after{
  content:'';
  position:absolute;inset:0;
  background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='280' height='280' viewBox='0 0 280 280'><g fill='none' stroke='rgba(255,255,255,0.04)' stroke-width='1.2' stroke-linecap='round' stroke-linejoin='round'><circle cx='40' cy='40' r='14'/><path d='M120 20 L140 40 L120 60 L100 40 Z'/><circle cx='230' cy='60' r='9'/><path d='M30 130 q15 -22 30 0 t30 0'/><rect x='100' y='115' width='22' height='22' rx='5' transform='rotate(15 111 126)'/><circle cx='190' cy='140' r='16'/><path d='M230 110 l10 15 l-10 15 l-10 -15 z'/><path d='M50 210 c15 -20 30 -20 45 0'/><circle cx='140' cy='220' r='10'/><rect x='200' y='205' width='26' height='16' rx='4'/><path d='M250 250 q-10 -15 -20 0'/><path d='M170 30 l8 8 l-8 8 l-8 -8 z'/><circle cx='90' cy='230' r='7'/><path d='M20 180 q10 10 20 0'/></g></svg>");
  background-repeat:repeat;
  opacity:.85;
  pointer-events:none;
  z-index:0;
}
#main > *{position:relative;z-index:1}

#chatHeader{padding:10px 16px;background:var(--panel);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px;flex-shrink:0;min-height:56px}
#chatHeader .avatar{width:36px;height:36px;border-radius:50%;overflow:hidden;background:#2a3038;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:13px;color:var(--text-2);flex-shrink:0;cursor:pointer}
#chatHeader .avatar img{width:100%;height:100%;object-fit:cover}
#chatHeader .info{flex:1;min-width:0}
#chatHeader .info .title{font-weight:500;font-size:14.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#chatHeader .info .sub{font-size:12px;color:var(--text-2)}
#chatHeader .info .sub.typing{color:var(--accent)}
#chatHeader .actions{display:flex;gap:4px}

#messagesWrap{flex:1;overflow-y:auto;padding:20px 8% 12px;display:flex;flex-direction:column;gap:2px}
.date-sep{align-self:center;font-size:12px;color:var(--text-3);background:var(--panel);padding:3px 10px;border-radius:10px;margin:12px 0 8px}
.bubble{max-width:60%;padding:8px 12px 6px;border-radius:12px;background:var(--other);border:1px solid var(--border);font-size:14px;line-height:1.4;word-wrap:break-word;margin-bottom:2px;align-self:flex-start}
.bubble .author{font-size:12px;font-weight:600;color:var(--accent);margin-bottom:2px}
.bubble .text{white-space:pre-wrap}
.bubble .meta{display:flex;align-items:center;justify-content:flex-end;gap:4px;font-size:10.5px;color:var(--text-3);margin-top:2px}
.bubble .meta .check{color:var(--accent);font-size:11px}
.bubble.me{align-self:flex-end;background:var(--own);border-color:#333d4c}
.bubble.me .author{display:none}
.bubble.me .meta{color:var(--text-3)}
.bubble img,.bubble video{max-width:100%;max-height:340px;border-radius:8px;display:block;margin-top:4px;cursor:pointer}
.bubble audio{margin-top:6px;width:240px;max-width:100%}

#inputBar{padding:10px 16px 16px;display:flex;gap:8px;align-items:flex-end;flex-shrink:0}
#inputBar .inputWrap{flex:1;background:var(--panel);border:1px solid var(--border);border-radius:10px;display:flex;align-items:center;padding:2px 2px 2px 4px;transition:.15s}
#inputBar .inputWrap:focus-within{border-color:#333d4c}
#inputBar input[type=text]{flex:1;background:transparent;border:none;outline:none;font-size:14px;padding:10px 8px}
#inputBar .send-btn{width:40px;height:40px;border-radius:8px;background:var(--accent);display:flex;align-items:center;justify-content:center;transition:.15s;flex-shrink:0}
#inputBar .send-btn:hover{background:var(--accent-hover)}
#inputBar .send-btn svg{width:18px;height:18px}
.recording{color:var(--danger) !important}

#empty{flex:1;display:flex;align-items:center;justify-content:center;flex-direction:column;color:var(--text-3);gap:12px;font-size:13px}
#empty svg{width:56px;height:56px;opacity:.4}

/* MODAL */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.7);display:none;align-items:center;justify-content:center;z-index:100;padding:20px}
.modal-overlay.visible{display:flex}
.modal{background:var(--panel);border:1px solid var(--border);border-radius:12px;width:100%;max-width:440px;padding:22px;max-height:85vh;overflow-y:auto}
.modal h2{font-size:17px;font-weight:600;margin-bottom:16px}
.modal label{display:block;font-size:12px;color:var(--text-2);margin-bottom:6px;margin-top:14px}
.modal input[type=text],.modal textarea{width:100%;padding:10px 12px;background:var(--panel-2);border:1px solid var(--border);border-radius:8px;font-size:14px;outline:none;resize:vertical}
.modal input:focus,.modal textarea:focus{border-color:#333d4c}
.modal textarea{min-height:70px}
.modal .avatar-preview{width:80px;height:80px;border-radius:50%;overflow:hidden;margin:0 auto 10px;background:#2a3038;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:26px;color:var(--text-2);cursor:pointer;border:2px dashed var(--border);transition:.15s}
.modal .avatar-preview:hover{border-color:var(--accent)}
.modal .avatar-preview img{width:100%;height:100%;object-fit:cover}
.modal .actions{display:flex;gap:8px;margin-top:18px}
.modal .actions button{flex:1;padding:10px;border-radius:8px;font-weight:500;font-size:13.5px;transition:.15s}
.modal .actions .primary{background:var(--accent)}
.modal .actions .primary:hover{background:var(--accent-hover)}
.modal .actions .secondary{background:var(--panel-2);border:1px solid var(--border)}
.modal .actions .secondary:hover{background:var(--hover)}
.modal .err{color:var(--danger);font-size:12.5px;margin-top:8px;min-height:14px}

.user-result{display:flex;gap:10px;align-items:center;padding:8px 10px;border-radius:8px;cursor:pointer;transition:.12s}
.user-result:hover{background:var(--hover)}
.user-result .avatar{width:36px;height:36px;border-radius:50%;background:#2a3038;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:13px;color:var(--text-2);overflow:hidden;flex-shrink:0}
.user-result .avatar img{width:100%;height:100%;object-fit:cover}
.user-result .info .name{font-weight:500;font-size:13.5px}
.user-result .info .bio{font-size:12px;color:var(--text-2);margin-top:1px}
.user-result .status{width:8px;height:8px;border-radius:50%;background:#3a424c;flex-shrink:0}
.user-result .status.online{background:var(--success)}

/* CALL */
#callScreen{position:fixed;inset:0;background:#000;z-index:200;display:none;flex-direction:column}
#callScreen.active{display:flex}
#callScreen .remoteVideo{flex:1;position:relative;overflow:hidden}
#callScreen .remoteVideo video{width:100%;height:100%;object-fit:cover;background:#000}
#callScreen .localVideo{position:absolute;bottom:16px;right:16px;width:180px;height:120px;border-radius:10px;overflow:hidden;border:2px solid #2a3038;background:#000;box-shadow:0 4px 20px rgba(0,0,0,.5);z-index:5}
#callScreen .localVideo video{width:100%;height:100%;object-fit:cover}
#callScreen .remoteVideo .placeholder{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:16px;color:#8b929c}
#callScreen .remoteVideo .placeholder .avatar{width:100px;height:100px;border-radius:50%;background:#2a3038;display:flex;align-items:center;justify-content:center;font-size:36px;font-weight:600;color:#e8eaed;overflow:hidden}
#callScreen .remoteVideo .placeholder .avatar img{width:100%;height:100%;object-fit:cover;border-radius:50%}
#callScreen .remoteVideo .placeholder .name{font-size:18px;font-weight:500;color:#e8eaed}
#callScreen .remoteVideo .placeholder .status{font-size:14px;color:#8b929c}
#callScreen .controls{position:absolute;bottom:24px;left:50%;transform:translateX(-50%);display:flex;gap:12px;z-index:10}
#callScreen .controls button{width:52px;height:52px;border-radius:50%;background:rgba(255,255,255,.15);color:#fff;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(10px);transition:.15s}
#callScreen .controls button:hover{background:rgba(255,255,255,.25)}
#callScreen .controls button.danger{background:var(--danger)}
#callScreen .controls button.danger:hover{background:#dc2626}
#callScreen .controls button.active{background:#fff;color:#000}
#callScreen .controls button svg{width:22px;height:22px}

#incomingModal{position:fixed;inset:0;background:rgba(0,0,0,.8);display:none;align-items:center;justify-content:center;z-index:300}
#incomingModal.visible{display:flex}
#incomingModal .box{background:var(--panel);border-radius:16px;padding:28px;text-align:center;min-width:280px}
#incomingModal .avatar{width:80px;height:80px;border-radius:50%;margin:0 auto 14px;background:#2a3038;display:flex;align-items:center;justify-content:center;font-size:28px;font-weight:600;overflow:hidden}
#incomingModal .avatar img{width:100%;height:100%;object-fit:cover}
#incomingModal h3{font-size:16px;margin-bottom:4px}
#incomingModal p{color:var(--text-2);font-size:13px;margin-bottom:20px}
#incomingModal .btns{display:flex;gap:12px;justify-content:center}
#incomingModal .btns button{width:56px;height:56px;border-radius:50%;display:flex;align-items:center;justify-content:center;transition:.15s}
#incomingModal .btns .accept{background:var(--success)}
#incomingModal .btns .accept:hover{background:#16a34a}
#incomingModal .btns .reject{background:var(--danger)}
#incomingModal .btns .reject:hover{background:#dc2626}
#incomingModal .btns svg{width:24px;height:24px;color:#fff}

#toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(20px);background:var(--panel);border:1px solid var(--border);padding:10px 16px;border-radius:8px;font-size:13px;opacity:0;transition:.25s;pointer-events:none;z-index:400}
#toast.visible{opacity:1;transform:translateX(-50%) translateY(0)}

#backBtn{display:none}
@media (max-width:780px){
  #app{grid-template-columns:1fr}
  #sidebar{position:absolute;inset:0;z-index:5;transition:transform .2s}
  #sidebar.hidden{transform:translateX(-100%)}
  #main{position:absolute;inset:0;z-index:4}
  #backBtn{display:flex}
  .bubble{max-width:80%}
}
</style>
</head>
<body>

<div id="auth">
  <h1>Holodyx Chat</h1>
  <div class="sub">Общайтесь, звоните, делитесь медиа</div>
  <div class="tabs">
    <button id="tabLogin" class="active">Вход</button>
    <button id="tabRegister">Регистрация</button>
  </div>
  <div id="loginForm">
    <input id="loginInput" type="text" placeholder="Email или ник" autocomplete="username">
    <input id="loginPass" type="password" placeholder="Пароль" autocomplete="current-password">
    <button class="submit" id="loginBtn">Войти</button>
  </div>
  <div id="registerForm" style="display:none">
    <input id="regEmail" type="email" placeholder="Email">
    <input id="regUser" type="text" placeholder="Ник (3–20 символов)">
    <input id="regPass" type="password" placeholder="Пароль (мин. 6)">
    <input id="regPass2" type="password" placeholder="Повторите пароль">
    <button class="submit" id="registerBtn">Зарегистрироваться</button>
  </div>
  <div class="err" id="authErr"></div>
  <div class="ok" id="authOk"></div>
</div>

<div id="app">
  <aside id="sidebar">
    <div id="sidebarHeader">
      <div class="avatar-sm" id="myAvatarSm">?</div>
      <div class="info">
        <div class="name" id="myNameSm">—</div>
        <div class="sub" id="myStatus">онлайн</div>
      </div>
      <button class="icon-btn" id="searchUsersBtn" title="Найти людей">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.35-4.35"/></svg>
      </button>
      <button class="icon-btn" id="settingsBtn" title="Настройки">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
      </button>
    </div>
    <div id="search"><input id="searchInput" type="text" placeholder="Поиск по чатам..."></div>
    <div id="chatList"></div>
  </aside>

  <main id="main">
    <div id="empty">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      <p>Выберите чат или найдите человека</p>
    </div>

    <div id="chatHeader" style="display:none">
      <button class="icon-btn" id="backBtn">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M15 18l-6-6 6-6"/></svg>
      </button>
      <div class="avatar" id="chatHeaderAvatar"></div>
      <div class="info">
        <div class="title" id="chatHeaderTitle">—</div>
        <div class="sub" id="chatHeaderSub">—</div>
      </div>
      <div class="actions" id="callActions" style="display:none">
        <button class="icon-btn" id="audioCallBtn" title="Аудиозвонок">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"/></svg>
        </button>
        <button class="icon-btn" id="videoCallBtn" title="Видеозвонок">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 7l-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>
        </button>
      </div>
    </div>

    <div id="messagesWrap" style="display:none"></div>

    <div id="inputBar" style="display:none">
      <div class="inputWrap">
        <input type="file" id="fileInput" accept="image/*,video/*" style="display:none">
        <button class="icon-btn" id="attachBtn" title="Фото или видео">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg>
        </button>
        <button class="icon-btn" id="micBtn" title="Голосовое">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="2" width="6" height="12" rx="3"/><path d="M5 10a7 7 0 0 0 14 0M12 19v3"/></svg>
        </button>
        <input id="msgInput" type="text" placeholder="Сообщение..." maxlength="4000" autocomplete="off">
      </div>
      <button class="send-btn" id="sendBtn">
        <svg viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2"><path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4 20-7z"/></svg>
      </button>
    </div>
  </main>
</div>

<div class="modal-overlay" id="settingsModal">
  <div class="modal">
    <h2>Настройки профиля</h2>
    <input type="file" id="avatarInput" accept="image/*" style="display:none">
    <div class="avatar-preview" id="avatarPreview">?</div>
    <label>Ник</label>
    <input type="text" id="setUsername" maxlength="20">
    <label>О себе</label>
    <textarea id="setBio" maxlength="300" placeholder="Пара слов о себе..."></textarea>
    <label>Email</label>
    <input type="text" id="setEmail" disabled style="opacity:.6">
    <div class="err" id="settingsErr"></div>
    <div class="actions">
      <button class="secondary" id="settingsCancel">Отмена</button>
      <button class="primary" id="settingsSave">Сохранить</button>
    </div>
    <div class="actions" style="margin-top:6px">
      <button class="secondary" id="logoutBtn" style="color:var(--danger)">Выйти из аккаунта</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="searchUsersModal">
  <div class="modal">
    <h2>Найти человека</h2>
    <input type="text" id="userSearchInput" placeholder="Введите ник..." autocomplete="off">
    <div id="userSearchResults" style="margin-top:12px;max-height:400px;overflow-y:auto"></div>
    <div class="actions">
      <button class="secondary" id="searchUsersClose">Закрыть</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="peerModal">
  <div class="modal">
    <h2>Профиль</h2>
    <div style="display:flex;gap:14px;align-items:center;padding:12px;background:var(--panel-2);border-radius:8px">
      <div class="avatar-sm" id="peerAvatar" style="width:60px;height:60px;font-size:20px"></div>
      <div>
        <div style="font-weight:600;font-size:15px" id="peerName">—</div>
        <div style="color:var(--text-2);font-size:13px;margin-top:3px" id="peerBio">—</div>
      </div>
    </div>
    <div class="actions">
      <button class="secondary" id="peerClose" style="flex:1">Закрыть</button>
    </div>
  </div>
</div>

<div id="incomingModal">
  <div class="box">
    <div class="avatar" id="incomingAvatar">?</div>
    <h3 id="incomingName">—</h3>
    <p id="incomingType">Входящий звонок</p>
    <div class="btns">
      <button class="reject" id="rejectCallBtn">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M23 1L1 23M1 1l22 22"/></svg>
      </button>
      <button class="accept" id="acceptCallBtn">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"/></svg>
      </button>
    </div>
  </div>
</div>

<div id="callScreen">
  <div class="remoteVideo">
    <video id="remoteVideo" autoplay playsinline></video>
    <div class="placeholder" id="callPlaceholder">
      <div class="avatar" id="callAvatar">?</div>
      <div class="name" id="callName">—</div>
      <div class="status" id="callStatus">Соединение...</div>
    </div>
  </div>
  <div class="localVideo">
    <video id="localVideo" autoplay muted playsinline></video>
  </div>
  <div class="controls">
    <button id="muteBtn" title="Микрофон">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="2" width="6" height="12" rx="3"/><path d="M5 10a7 7 0 0 0 14 0M12 19v3"/></svg>
    </button>
    <button id="camBtn" title="Камера">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 7l-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>
    </button>
    <button class="danger" id="endCallBtn" title="Завершить">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M23 1L1 23M1 1l22 22"/></svg>
    </button>
  </div>
</div>

<div id="toast"></div>

<script>
const $ = id => document.getElementById(id);
const socket = io({ autoConnect: false });
let me = null;
const chats = {};
let activeKey = null;
const onlineUsers = new Map();
const peerCache = {};

/* AUTH */
$('tabLogin').onclick = () => switchTab('login');
$('tabRegister').onclick = () => switchTab('register');
function switchTab(t) {
  const isLogin = t === 'login';
  $('tabLogin').classList.toggle('active', isLogin);
  $('tabRegister').classList.toggle('active', !isLogin);
  $('loginForm').style.display = isLogin ? '' : 'none';
  $('registerForm').style.display = isLogin ? 'none' : '';
  $('authErr').textContent = ''; $('authOk').textContent = '';
}
$('loginBtn').onclick = doLogin;
$('loginInput').addEventListener('keydown', e => e.key === 'Enter' && doLogin());
$('loginPass').addEventListener('keydown', e => e.key === 'Enter' && doLogin());
async function doLogin() {
  const login = $('loginInput').value.trim();
  const password = $('loginPass').value;
  if (!login || !password) { $('authErr').textContent = 'Заполните все поля'; return; }
  $('loginBtn').disabled = true;
  try {
    const r = await fetch('/api/login', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({login, password}) }).then(r => r.json());
    if (!r.ok) { $('authErr').textContent = r.error || 'Ошибка'; return; }
    me = r.user; enterApp();
  } catch (e) { $('authErr').textContent = 'Ошибка сети'; }
  finally { $('loginBtn').disabled = false; }
}
$('registerBtn').onclick = doRegister;
async function doRegister() {
  const email = $('regEmail').value.trim();
  const username = $('regUser').value.trim();
  const p1 = $('regPass').value, p2 = $('regPass2').value;
  $('authErr').textContent = ''; $('authOk').textContent = '';
  if (!email || !username || !p1) { $('authErr').textContent = 'Заполните поля'; return; }
  if (p1 !== p2) { $('authErr').textContent = 'Пароли не совпадают'; return; }
  if (p1.length < 6) { $('authErr').textContent = 'Пароль минимум 6 символов'; return; }
  $('registerBtn').disabled = true; $('registerBtn').textContent = 'Создаём...';
  try {
    const r = await fetch('/api/register', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({email, username, password: p1}) }).then(r => r.json());
    if (!r.ok) { $('authErr').textContent = r.error || 'Ошибка'; return; }
    $('authOk').textContent = 'Аккаунт создан';
    $('loginInput').value = email; $('loginPass').value = p1;
    await doLogin();
  } catch (e) { $('authErr').textContent = 'Ошибка сети'; }
  finally { $('registerBtn').disabled = false; $('registerBtn').textContent = 'Зарегистрироваться'; }
}
(async () => {
  try {
    const r = await fetch('/api/me').then(r => r.json()).catch(() => ({ok:false}));
    if (r.ok) { me = r.user; enterApp(); }
  } catch(e) {}
})();

function enterApp() {
  $('auth').style.display = 'none';
  $('app').classList.add('active');
  const av = $('myAvatarSm');
  av.innerHTML = me.avatar ? `<img src="${me.avatar}">` : initials(me.username);
  $('myNameSm').textContent = me.username;
  chats['general'] = { key:'general', type:'general', title:'Общий чат', messages:[], unread:0, typing:false };
  if (!socket.connected) socket.connect();
}

/* SOCKET */
socket.on('need_auth', () => socket.disconnect());
socket.on('connect_error', e => console.warn('Socket:', e.message));

socket.on('joined', data => {
  me = data.user;
  chats['general'].messages = data.history || [];
  data.online.forEach(u => {
    onlineUsers.set(u.id, u);
    peerCache[u.username.toLowerCase()] = u;
  });
  loadSavedDialogs();
  renderSidebar();
});
socket.on('online', data => {
  onlineUsers.clear();
  data.users.forEach(u => {
    onlineUsers.set(u.id, u);
    peerCache[u.username.toLowerCase()] = u;
  });
  renderSidebar();
  if (activeKey && activeKey !== 'general') updateHeaderSub();
});
socket.on('message', msg => {
  const key = msg.room;
  let chat = chats[key];
  if (!chat) {
    if (msg.room.startsWith('dm:')) {
      const parts = msg.room.slice(3).split('|');
      const peerName = parts.find(p => p !== me.username.toLowerCase());
      const peer = peerCache[peerName] || {username: peerName};
      chat = ensureDM(peer.username, peer);
      saveDialogs();
    } else return;
  }
  chat.messages.push(msg);
  const isActive = key === activeKey;
  const fromMe = msg.sender_id === me.id;
  if (!isActive && !fromMe && msg.type !== 'system') chat.unread = (chat.unread || 0) + 1;
  if (isActive) {
    renderMessages(); scrollBottom();
    if (!fromMe && chat.messages.length) {
      socket.emit('read', { room: msg.room, last_id: chat.messages[chat.messages.length-1].id });
    }
  }
  renderSidebar();
});
socket.on('typing', data => {
  const key = data.to === 'general' ? 'general' : dmKey(data.from, me.username);
  const chat = chats[key]; if (!chat) return;
  chat.typing = data.is_typing;
  if (activeKey === key) updateHeaderSub();
});
socket.on('read', data => {
  const chat = chats[data.room];
  if (chat) chat.reads = data.reads;
  if (activeKey === data.room) renderMessages();
});
socket.on('dm_history', data => {
  const key = data.room;
  let chat = chats[key];
  if (!chat) {
    const peer = peerCache[data.peer.toLowerCase()] || {username: data.peer};
    chat = ensureDM(data.peer, peer);
  }
  chat.messages = data.history || [];
  if (activeKey === key) { renderMessages(); scrollBottom(); }
});

function initials(n) { if (!n) return '?'; const p = n.trim().split(/\s+/); return (p.length === 1 ? p[0].slice(0,2) : p[0][0]+p[1][0]).toUpperCase(); }
function parseTime(iso) {
  if (!iso) return null;
  let s = String(iso);
  if (!s.includes('T')) s = s.replace(' ', 'T') + 'Z';
  else if (!s.endsWith('Z') && !s.includes('+')) s += 'Z';
  const d = new Date(s); return isNaN(d) ? null : d;
}
function fmtTime(iso) { const d = parseTime(iso); return d ? d.toLocaleTimeString('ru-RU', {hour:'2-digit', minute:'2-digit'}) : ''; }
function fmtDate(iso) {
  const d = parseTime(iso); if (!d) return '';
  const today = new Date(), yest = new Date(); yest.setDate(today.getDate()-1);
  const same = (a,b) => a.toDateString() === b.toDateString();
  if (same(d, today)) return 'Сегодня';
  if (same(d, yest)) return 'Вчера';
  return d.toLocaleDateString('ru-RU', {day:'numeric', month:'long'});
}
function dmKey(a, b) { return 'dm:' + [a.toLowerCase(), b.toLowerCase()].sort().join('|'); }
function ensureDM(peerName, peer) {
  const key = dmKey(me.username, peerName);
  if (!chats[key]) {
    chats[key] = { key, type:'dm', title: peerName, peer: peerName, avatar: peer.avatar || null, messages:[], unread:0, typing:false };
    saveDialogs();
  } else if (peer.avatar) chats[key].avatar = peer.avatar;
  return chats[key];
}
function preview(m) {
  if (m.type === 'image') return '📷 Фото';
  if (m.type === 'video') return '🎬 Видео';
  if (m.type === 'audio') return '🎤 Голосовое';
  return m.text || '';
}
function escapeHtml(s) { return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function avatarHtml(u) { if (u.avatar) return `<img src="${u.avatar}">`; return initials(u.username || u); }

function saveDialogs() {
  const list = Object.values(chats).filter(c => c.type === 'dm').map(c => c.peer);
  try { localStorage.setItem('holodyx_dialogs', JSON.stringify(list)); } catch(e) {}
}
function loadSavedDialogs() {
  try {
    const list = JSON.parse(localStorage.getItem('holodyx_dialogs') || '[]');
    list.forEach(peer => {
      if (!peer) return;
      const u = peerCache[peer.toLowerCase()] || {username: peer};
      ensureDM(peer, u);
    });
  } catch(e) {}
}

function renderSidebar() {
  const q = $('searchInput').value.trim().toLowerCase();
  const entries = Object.values(chats).filter(c => !q || c.title.toLowerCase().includes(q)).sort((a,b) => {
    if (a.type === 'general') return -1;
    if (b.type === 'general') return 1;
    const la = a.messages.length ? a.messages[a.messages.length-1].id : 0;
    const lb = b.messages.length ? b.messages[b.messages.length-1].id : 0;
    return lb - la;
  });
  $('chatList').innerHTML = '';
  entries.forEach(c => {
    const el = document.createElement('div');
    el.className = 'chat-item' + (c.key === activeKey ? ' active' : '') + (c.unread ? ' unread' : '');
    const last = c.messages[c.messages.length-1];
    let pv = 'Нет сообщений';
    if (last) {
      if (last.type === 'system') pv = last.text;
      else pv = (last.sender_id === me.id ? 'Вы: ' : (c.type==='general' ? last.sender_name+': ' : '')) + preview(last);
    }
    const pu = c.type === 'dm' ? {username: c.peer, avatar: c.avatar} : {username: c.title, avatar: null};
    const online = c.type === 'dm' && [...onlineUsers.values()].some(u => u.username.toLowerCase() === (c.peer||'').toLowerCase());
    el.innerHTML = `
      <div class="avatar">${c.type==='general'?'💬':avatarHtml(pu)}${online?'<div class="online-dot"></div>':''}</div>
      <div class="body">
        <div class="row"><div class="title">${escapeHtml(c.title)}</div><div class="time">${last?fmtTime(last.time):''}</div></div>
        <div class="preview">${escapeHtml(pv)}</div>
      </div>
      <div class="badge">${c.unread>99?'99+':c.unread}</div>
    `;
    el.onclick = () => openChat(c.key);
    $('chatList').appendChild(el);
  });
}

function openChat(key) {
  const c = chats[key]; if (!c) return;
  activeKey = key; c.unread = 0; c.typing = false;
  $('empty').style.display = 'none';
  $('chatHeader').style.display = 'flex';
  $('messagesWrap').style.display = 'flex';
  $('inputBar').style.display = 'flex';
  const pu = c.type === 'dm' ? {username: c.peer, avatar: c.avatar} : {username: c.title, avatar: null};
  $('chatHeaderAvatar').innerHTML = c.type === 'general' ? '💬' : avatarHtml(pu);
  $('chatHeaderTitle').textContent = c.title;
  $('callActions').style.display = c.type === 'dm' ? 'flex' : 'none';
  updateHeaderSub();
  $('sidebar').classList.add('hidden');
  renderMessages(); scrollBottom(); renderSidebar();
  if (c.type === 'dm') socket.emit('open_dm', { peer: c.peer });
  else {
    const lastId = c.messages.length ? c.messages[c.messages.length-1].id : 0;
    if (lastId) socket.emit('read', { room: 'general', last_id: lastId });
  }
  $('msgInput').focus();
}
$('backBtn').onclick = () => {
  $('sidebar').classList.remove('hidden');
  activeKey = null;
  $('empty').style.display = 'flex';
  $('chatHeader').style.display = 'none';
  $('messagesWrap').style.display = 'none';
  $('inputBar').style.display = 'none';
  renderSidebar();
};
function updateHeaderSub() {
  const c = chats[activeKey]; if (!c) return;
  const sub = $('chatHeaderSub');
  sub.classList.remove('typing');
  if (c.typing) { sub.textContent = 'печатает...'; sub.classList.add('typing'); return; }
  if (c.type === 'general') sub.textContent = onlineUsers.size + ' онлайн';
  else {
    const on = [...onlineUsers.values()].some(u => u.username.toLowerCase() === (c.peer||'').toLowerCase());
    sub.textContent = on ? 'онлайн' : 'не в сети';
  }
}
function renderMessages() {
  const c = chats[activeKey]; if (!c) return;
  $('messagesWrap').innerHTML = '';
  let lastDate = '';
  c.messages.forEach(m => {
    const d = fmtDate(m.time);
    if (d && d !== lastDate) { const s = document.createElement('div'); s.className = 'date-sep'; s.textContent = d; $('messagesWrap').appendChild(s); lastDate = d; }
    if (m.type === 'system') { const s = document.createElement('div'); s.className = 'date-sep'; s.textContent = m.text; $('messagesWrap').appendChild(s); return; }
    const mine = m.sender_id === me.id;
    const el = document.createElement('div');
    el.className = 'bubble ' + (mine ? 'me' : '');
    const showAuthor = c.type === 'general' && !mine;
    let bodyHtml = '';
    if (m.type === 'image') bodyHtml = `<img src="${m.media_url}" onclick="window.open('${m.media_url}','_blank')">`;
    else if (m.type === 'video') bodyHtml = `<video src="${m.media_url}" controls preload="metadata"></video>`;
    else if (m.type === 'audio') bodyHtml = `<audio src="${m.media_url}" controls preload="metadata"></audio>`;
    else bodyHtml = `<div class="text">${escapeHtml(m.text)}</div>`;
    let check = '';
    if (mine) {
      const rb = c.reads || {};
      const others = Object.entries(rb).filter(([uid]) => Number(uid) !== me.id);
      check = others.some(([_, lid]) => Number(lid) >= m.id) ? '✓✓' : '✓';
    }
    el.innerHTML = `${showAuthor?`<div class="author">${escapeHtml(m.sender_name)}</div>`:''}${bodyHtml}<div class="meta"><span>${fmtTime(m.time)}</span>${mine?`<span class="check">${check}</span>`:''}</div>`;
    $('messagesWrap').appendChild(el);
  });
}
function scrollBottom() { requestAnimationFrame(() => { const w = $('messagesWrap'); w.scrollTop = w.scrollHeight; }); }

function currentRoom() { const c = chats[activeKey]; if (!c) return null; return c.type === 'general' ? 'general' : dmKey(me.username, c.peer); }
function send() {
  const text = $('msgInput').value.trim();
  if (!text || !activeKey) return;
  const c = chats[activeKey];
  const to = c.type === 'general' ? 'general' : c.peer;
  socket.emit('send', { text, to });
  $('msgInput').value = '';
  socket.emit('typing', { is_typing: false, to });
}
$('sendBtn').onclick = send;
$('msgInput').addEventListener('keydown', e => { if (e.key === 'Enter') { e.preventDefault(); send(); } });
let typingTimer = null;
$('msgInput').addEventListener('input', () => {
  if (!activeKey) return;
  const c = chats[activeKey];
  const to = c.type === 'general' ? 'general' : c.peer;
  socket.emit('typing', { is_typing: true, to });
  clearTimeout(typingTimer);
  typingTimer = setTimeout(() => socket.emit('typing', { is_typing: false, to }), 1200);
});

$('attachBtn').onclick = () => $('fileInput').click();
$('fileInput').onchange = async e => {
  const f = e.target.files[0]; if (!f) return;
  e.target.value = '';
  if (!activeKey) return;
  const room = currentRoom();
  const kind = f.type.startsWith('video/') ? 'video' : 'image';
  await uploadFile(f, kind, room);
};
async function uploadFile(file, kind, room) {
  const fd = new FormData();
  fd.append('file', file); fd.append('kind', kind); fd.append('room', room);
  try {
    const r = await fetch('/api/upload', { method:'POST', body: fd }).then(r => r.json());
    if (!r.ok) toast(r.error || 'Ошибка загрузки');
  } catch(e) { toast('Ошибка сети'); }
}

let mediaRecorder = null, recordedChunks = [], recStart = 0, recTimer = null;
$('micBtn').onclick = async () => {
  if (mediaRecorder && mediaRecorder.state === 'recording') { mediaRecorder.stop(); return; }
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    recordedChunks = [];
    const mime = getAudioMime();
    mediaRecorder = mime ? new MediaRecorder(stream, { mimeType: mime }) : new MediaRecorder(stream);
    recStart = Date.now();
    mediaRecorder.ondataavailable = e => { if (e.data.size) recordedChunks.push(e.data); };
    mediaRecorder.onstop = async () => {
      clearInterval(recTimer);
      $('micBtn').classList.remove('recording');
      $('msgInput').placeholder = 'Сообщение...';
      const duration = Math.round((Date.now() - recStart) / 1000);
      stream.getTracks().forEach(t => t.stop());
      if (duration < 1) { toast('Слишком коротко'); return; }
      const type = mediaRecorder.mimeType || 'audio/webm';
      const blob = new Blob(recordedChunks, { type });
      const ext = type.includes('ogg') ? 'ogg' : 'webm';
      const f = new File([blob], `voice-${Date.now()}.${ext}`, { type });
      const room = currentRoom();
      if (room) await uploadFile(f, 'audio', room);
    };
    mediaRecorder.start();
    $('micBtn').classList.add('recording');
    let sec = 0;
    $('msgInput').placeholder = '⏺ 0:00';
    recTimer = setInterval(() => { sec++; $('msgInput').placeholder = `⏺ ${Math.floor(sec/60)}:${String(sec%60).padStart(2,'0')}`; }, 1000);
  } catch(e) { toast('Нет доступа к микрофону'); }
};
function getAudioMime() {
  const list = ['audio/webm;codecs=opus', 'audio/webm', 'audio/ogg;codecs=opus'];
  for (const m of list) if (window.MediaRecorder && MediaRecorder.isTypeSupported(m)) return m;
  return '';
}

$('settingsBtn').onclick = () => {
  $('setUsername').value = me.username || '';
  $('setBio').value = me.bio || '';
  $('setEmail').value = me.email || '';
  const prev = $('avatarPreview');
  prev.innerHTML = me.avatar ? `<img src="${me.avatar}">` : initials(me.username);
  $('settingsErr').textContent = '';
  $('settingsModal').classList.add('visible');
};
$('settingsCancel').onclick = () => $('settingsModal').classList.remove('visible');
$('avatarPreview').onclick = () => $('avatarInput').click();
$('avatarInput').onchange = e => {
  const f = e.target.files[0]; if (!f) return;
  $('avatarPreview').innerHTML = `<img src="${URL.createObjectURL(f)}">`;
};
$('settingsSave').onclick = async () => {
  const fd = new FormData();
  fd.append('username', $('setUsername').value.trim());
  fd.append('bio', $('setBio').value);
  const f = $('avatarInput').files[0];
  if (f) fd.append('avatar', f);
  try {
    const r = await fetch('/api/profile', { method:'POST', body: fd }).then(r => r.json());
    if (!r.ok) { $('settingsErr').textContent = r.error || 'Ошибка'; return; }
    me = r.user;
    const av = $('myAvatarSm');
    av.innerHTML = me.avatar ? `<img src="${me.avatar}">` : initials(me.username);
    $('myNameSm').textContent = me.username;
    $('settingsModal').classList.remove('visible');
    toast('Профиль обновлён');
  } catch(e) { $('settingsErr').textContent = 'Ошибка сети'; }
};
$('logoutBtn').onclick = async () => { await fetch('/api/logout', { method:'POST' }); location.reload(); };

$('searchUsersBtn').onclick = () => {
  $('searchUsersModal').classList.add('visible');
  $('userSearchInput').value = '';
  $('userSearchResults').innerHTML = '';
  setTimeout(() => $('userSearchInput').focus(), 100);
};
$('searchUsersClose').onclick = () => $('searchUsersModal').classList.remove('visible');
let searchTimer = null;
$('userSearchInput').addEventListener('input', () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(searchUsers, 250);
});
async function searchUsers() {
  const q = $('userSearchInput').value.trim();
  if (!q) { $('userSearchResults').innerHTML = ''; return; }
  try {
    const r = await fetch(`/api/users/search?q=${encodeURIComponent(q)}`).then(r => r.json());
    if (!r.ok) return;
    const box = $('userSearchResults');
    box.innerHTML = '';
    if (!r.users.length) {
      box.innerHTML = '<div style="color:var(--text-3);text-align:center;padding:20px;font-size:13px">Никого не найдено</div>';
      return;
    }
    r.users.forEach(u => {
      const online = [...onlineUsers.values()].some(x => x.id === u.id);
      const el = document.createElement('div');
      el.className = 'user-result';
      el.innerHTML = `
        <div class="avatar">${u.avatar ? `<img src="${u.avatar}">` : initials(u.username)}</div>
        <div class="info" style="flex:1;min-width:0">
          <div class="name">${escapeHtml(u.username)}</div>
          <div class="bio">${escapeHtml(u.bio || 'Без описания')}</div>
        </div>
        <div class="status ${online?'online':''}"></div>
      `;
      el.onclick = () => {
        peerCache[u.username.toLowerCase()] = u;
        ensureDM(u.username, u);
        $('searchUsersModal').classList.remove('visible');
        openChat(dmKey(me.username, u.username));
      };
      box.appendChild(el);
    });
  } catch(e) {}
}

document.querySelector('#chatHeader .avatar').onclick = () => {
  const c = chats[activeKey]; if (!c || c.type !== 'dm') return;
  const u = peerCache[c.peer.toLowerCase()] || {username: c.peer, avatar: c.avatar, bio: ''};
  $('peerAvatar').innerHTML = u.avatar ? `<img src="${u.avatar}">` : initials(u.username);
  $('peerName').textContent = u.username;
  $('peerBio').textContent = u.bio || 'Без описания';
  $('peerModal').classList.add('visible');
};
$('peerClose').onclick = () => $('peerModal').classList.remove('visible');

function toast(t) {
  const el = $('toast'); el.textContent = t; el.classList.add('visible');
  clearTimeout(toast._t); toast._t = setTimeout(() => el.classList.remove('visible'), 2200);
}
$('searchInput').addEventListener('input', renderSidebar);
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    document.querySelectorAll('.modal-overlay.visible').forEach(m => m.classList.remove('visible'));
  }
});

/* ============================================================
   WEBRTC
============================================================ */
let pc = null;
let localStream = null;
let currentCallPeer = null;
let currentCallType = null;
let isCaller = false;
let pendingCandidates = [];
let callTimeout = null;

const rtcConfig = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ]
};

function showCallScreen(peerName, type) {
  currentCallPeer = peerName;
  currentCallType = type;
  $('callName').textContent = peerName;
  $('callStatus').textContent = type === 'video' ? 'Видеозвонок' : 'Аудиозвонок';
  const u = peerCache[peerName.toLowerCase()] || {username: peerName};
  $('callAvatar').innerHTML = u.avatar ? `<img src="${u.avatar}">` : initials(peerName);
  $('callScreen').classList.add('active');
  $('callPlaceholder').style.display = 'flex';
  $('camBtn').style.display = type === 'video' ? 'flex' : 'none';
}

function hideCallScreen() {
  $('callScreen').classList.remove('active');
  $('remoteVideo').srcObject = null;
  $('localVideo').srcObject = null;
  if (callTimeout) { clearTimeout(callTimeout); callTimeout = null; }
}

async function startCall(type) {
  const c = chats[activeKey]; if (!c || c.type !== 'dm') return;
  isCaller = true;
  try {
    localStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: type === 'video'
    });
    $('localVideo').srcObject = localStream;
    showCallScreen(c.peer, type);
    $('callStatus').textContent = 'Вызов...';

    pc = new RTCPeerConnection(rtcConfig);
    localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
    setupPcHandlers();

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    socket.emit('call_offer', { to: c.peer, offer, type });

    callTimeout = setTimeout(() => {
      toast('Нет ответа');
      endCall(true);
    }, 30000);
  } catch(e) {
    toast('Нет доступа к микрофону/камере');
    hideCallScreen();
  }
}

function setupPcHandlers() {
  pc.ontrack = e => {
    $('remoteVideo').srcObject = e.streams[0];
    $('callPlaceholder').style.display = 'none';
    $('callStatus').textContent = 'Соединено';
    if (callTimeout) { clearTimeout(callTimeout); callTimeout = null; }
  };
  pc.onicecandidate = e => {
    if (e.candidate) {
      socket.emit('call_ice', { to: currentCallPeer, candidate: e.candidate });
    }
  };
  pc.onconnectionstatechange = () => {
    if (['failed','disconnected','closed'].includes(pc.connectionState)) {
      if ($('callScreen').classList.contains('active')) endCall(true);
    }
  };
}

$('audioCallBtn').onclick = () => startCall('audio');
$('videoCallBtn').onclick = () => startCall('video');

function endCall(silent) {
  if (currentCallPeer) socket.emit('call_end', { to: currentCallPeer });
  if (localStream) { localStream.getTracks().forEach(t => t.stop()); localStream = null; }
  if (pc) { try { pc.close(); } catch(e){} pc = null; }
  currentCallPeer = null;
  currentCallType = null;
  isCaller = false;
  pendingCandidates = [];
  hideCallScreen();
}
$('endCallBtn').onclick = () => endCall(false);

let micEnabled = true, camEnabled = true;
$('muteBtn').onclick = () => {
  if (!localStream) return;
  micEnabled = !micEnabled;
  localStream.getAudioTracks().forEach(t => t.enabled = micEnabled);
  $('muteBtn').classList.toggle('active', !micEnabled);
};
$('camBtn').onclick = () => {
  if (!localStream) return;
  camEnabled = !camEnabled;
  localStream.getVideoTracks().forEach(t => t.enabled = camEnabled);
  $('camBtn').classList.toggle('active', !camEnabled);
};

socket.on('call_offer', async data => {
  if (pc) {
    socket.emit('call_reject', { to: data.from, reason: 'busy' });
    return;
  }
  isCaller = false;
  pendingCandidates = [];
  const u = peerCache[data.from.toLowerCase()] || {username: data.from};
  $('incomingAvatar').innerHTML = u.avatar ? `<img src="${u.avatar}">` : initials(data.from);
  $('incomingName').textContent = data.from;
  $('incomingType').textContent = data.type === 'video' ? 'Входящий видеозвонок' : 'Входящий звонок';
  $('incomingModal').classList.add('visible');
  $('incomingModal').dataset.offer = JSON.stringify(data.offer);
  $('incomingModal').dataset.from = data.from;
  $('incomingModal').dataset.type = data.type;
});

$('acceptCallBtn').onclick = async () => {
  const offer = JSON.parse($('incomingModal').dataset.offer);
  const from = $('incomingModal').dataset.from;
  const type = $('incomingModal').dataset.type;
  $('incomingModal').classList.remove('visible');

  try {
    localStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: type === 'video'
    });
    $('localVideo').srcObject = localStream;
    currentCallPeer = from;
    currentCallType = type;
    showCallScreen(from, type);
    $('callStatus').textContent = 'Соединение...';

    pc = new RTCPeerConnection(rtcConfig);
    localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
    setupPcHandlers();

    await pc.setRemoteDescription(new RTCSessionDescription(offer));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);

    for (const c of pendingCandidates) {
      try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch(e){}
    }
    pendingCandidates = [];

    socket.emit('call_answer', { to: from, answer });
  } catch(e) {
    toast('Не удалось принять звонок');
    socket.emit('call_reject', { to: from });
    hideCallScreen();
  }
};

$('rejectCallBtn').onclick = () => {
  const from = $('incomingModal').dataset.from;
  socket.emit('call_reject', { to: from });
  $('incomingModal').classList.remove('visible');
};

socket.on('call_answer', async data => {
  if (!pc) return;
  try {
    await pc.setRemoteDescription(new RTCSessionDescription(data.answer));
    for (const c of pendingCandidates) {
      try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch(e){}
    }
    pendingCandidates = [];
    $('callStatus').textContent = 'Соединение...';
  } catch(e) { console.error(e); }
});

socket.on('call_ice', async data => {
  if (!pc) return;
  const cand = data.candidate;
  if (pc.remoteDescription && pc.remoteDescription.type) {
    try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch(e){}
  } else {
    pendingCandidates.push(cand);
  }
});

socket.on('call_reject', data => {
  toast(data.reason === 'busy' ? 'Абонент занят' : 'Звонок отклонён');
  endCall(true);
});

socket.on('call_end', data => {
  if (currentCallPeer && data.from === currentCallPeer) {
    toast('Звонок завершён');
    endCall(true);
  }
});
</script>
</body>
</html>
"""


# ============================================================
# ROUTES
# ============================================================
@app.route('/')
def index():
    return Response(INDEX_HTML, mimetype='text/html')


@app.route('/health')
def health():
    return jsonify(status='ok', online=len(online))


@app.route('/favicon.ico')
def favicon():
    return '', 204


@app.route('/chat_uploads/avatars/<path:fn>')
def serve_avatar(fn):
    return send_from_directory(AVATAR_DIR, fn)


@app.route('/chat_uploads/media/<path:fn>')
def serve_media(fn):
    return send_from_directory(MEDIA_DIR, fn)


@app.route('/api/register', methods=['POST'])
def api_register():
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get('email') or '').strip().lower()
        username = (data.get('username') or '').strip()
        password = data.get('password') or ''
        print(f"[REGISTER] {email!r} {username!r}")
        if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
            return jsonify(ok=False, error='Некорректный email'), 400
        if not re.match(r'^[A-Za-zА-Яа-я0-9_]{3,20}$', username):
            return jsonify(ok=False, error='Ник: 3–20 символов'), 400
        if len(password) < 6:
            return jsonify(ok=False, error='Пароль минимум 6 символов'), 400
        if user_by_email(email):
            return jsonify(ok=False, error='Email занят'), 400
        if user_by_username(username):
            return jsonify(ok=False, error='Ник занят'), 400
        uid = create_user(email, username, password)
        if not uid:
            return jsonify(ok=False, error='Ошибка создания'), 500
        return jsonify(ok=True, uid=uid)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify(ok=False, error=f'Ошибка: {e}'), 500


@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.get_json(silent=True) or {}
    login = (data.get('login') or '').strip()
    password = data.get('password') or ''
    user = user_by_email(login) if '@' in login else None
    if not user:
        user = user_by_username(login)
    if not user or not check_password_hash(user['password_hash'], password):
        return jsonify(ok=False, error='Неверный логин или пароль'), 401
    session['uid'] = user['id']
    session.permanent = True
    return jsonify(ok=True, user={
        'id': user['id'], 'username': user['username'],
        'avatar': user['avatar'], 'bio': user['bio'], 'email': user['email'],
    })


@app.route('/api/me')
def api_me():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    u = user_by_id(uid)
    if not u:
        session.clear()
        return jsonify(ok=False), 401
    return jsonify(ok=True, user={
        'id': u['id'], 'username': u['username'],
        'avatar': u['avatar'], 'bio': u['bio'], 'email': u['email'],
    })


@app.route('/api/logout', methods=['POST'])
def api_logout():
    session.clear()
    return jsonify(ok=True)


@app.route('/api/users/search')
def api_users_search():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    q = (request.args.get('q') or '').strip()
    if len(q) < 1:
        return jsonify(ok=True, users=[])
    users = search_users(q, uid, 20)
    return jsonify(ok=True, users=users)


@app.route('/api/dialogs')
def api_dialogs():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    return jsonify(ok=True, dialogs=list_dialogs(uid))


@app.route('/api/profile', methods=['POST'])
def api_profile():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False, error='Не авторизован'), 401
    u = user_by_id(uid)
    if not u:
        return jsonify(ok=False, error='Нет юзера'), 404
    username = request.form.get('username')
    bio = request.form.get('bio')
    updates = {}
    if username and username != u['username']:
        if not re.match(r'^[A-Za-zА-Яа-я0-9_]{3,20}$', username):
            return jsonify(ok=False, error='Некорректный ник'), 400
        if user_by_username(username):
            return jsonify(ok=False, error='Ник занят'), 400
        updates['username'] = username
    if bio is not None:
        updates['bio'] = bio.strip()[:300]
    file = request.files.get('avatar')
    if file and file.filename:
        if ext_of(file.filename) not in ALLOWED_AVATAR:
            return jsonify(ok=False, error='Формат не поддерживается'), 400
        fn = f"{uuid.uuid4().hex}.{ext_of(file.filename)}"
        file.save(os.path.join(AVATAR_DIR, fn))
        updates['avatar'] = f"/chat_uploads/avatars/{fn}"
    if updates:
        update_profile(uid, **updates)
    nu = user_by_id(uid)
    return jsonify(ok=True, user={
        'id': nu['id'], 'username': nu['username'],
        'avatar': nu['avatar'], 'bio': nu['bio'], 'email': nu['email'],
    })


@app.route('/api/upload', methods=['POST'])
def api_upload():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False, error='Не авторизован'), 401
    file = request.files.get('file')
    kind = (request.form.get('kind') or 'file').lower()
    room = request.form.get('room') or 'general'
    if not file or not file.filename:
        return jsonify(ok=False, error='Файл не выбран'), 400
    ext = ext_of(file.filename)
    if kind == 'image':
        if ext not in ALLOWED_IMAGE:
            return jsonify(ok=False, error='Формат изображения не поддерживается'), 400
        msg_type = 'image'
    elif kind == 'video':
        if ext not in ALLOWED_VIDEO:
            return jsonify(ok=False, error='Формат видео не поддерживается'), 400
        msg_type = 'video'
    elif kind == 'audio':
        if ext not in ALLOWED_AUDIO:
            return jsonify(ok=False, error='Формат аудио не поддерживается'), 400
        msg_type = 'audio'
    else:
        return jsonify(ok=False, error='Неизвестный тип'), 400
    fn = f"{uuid.uuid4().hex}.{ext}"
    file.save(os.path.join(MEDIA_DIR, fn))
    url = f"/chat_uploads/media/{fn}"
    u = user_by_id(uid)
    msg = add_msg(room=room, sender_id=u['id'], sender_name=u['username'],
                  type_=msg_type, text='', media_url=url, media_name=file.filename)
    payload = serialize(msg)
    if room == 'general':
        socketio.emit('message', payload, to='general', include_self=False)
        socketio.emit('message', payload, to=request.sid)
    else:
        socketio.emit('message', payload, to=request.sid)
        parts = room[3:].split('|')
        for name in parts:
            sid = name_to_sid.get(name)
            if sid and sid != request.sid:
                socketio.emit('message', payload, to=sid)
    return jsonify(ok=True, message=payload)


# ============================================================
# SOCKET
# ============================================================
@socketio.on('connect')
def on_connect():
    uid = session.get('uid')
    if not uid:
        emit('need_auth'); return
    u = user_by_id(uid)
    if not u:
        emit('need_auth'); return
    online[request.sid] = {'id': u['id'], 'username': u['username'],
                            'avatar': u['avatar'], 'bio': u['bio']}
    name_to_sid[u['username'].lower()] = request.sid
    join_room('general')
    join_room(f"user:{u['id']}")
    emit('joined', {
        'user': {'id': u['id'], 'username': u['username'],
                 'avatar': u['avatar'], 'bio': u['bio'], 'email': u['email']},
        'history': history('general', 200),
        'online': online_list(),
    })
    sys_msg = add_msg('general', None, u['username'], 'system',
                      text=f"{u['username']} присоединился к чату")
    emit('message', serialize(sys_msg), to='general')
    broadcast_online()


@socketio.on('open_dm')
def on_open_dm(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    peer = (data.get('peer') or '').strip()
    if not peer: return
    add_dialog(me_user['id'], peer)
    room = dm_room(me_user['username'], peer)
    join_room(room)
    emit('dm_history', {'room': room, 'peer': peer, 'history': history(room, 200)})


@socketio.on('send')
def on_send(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    if not me_user: return
    text = (data.get('text') or '').strip()[:4000]
    to = (data.get('to') or 'general').strip()
    if not text: return
    room = 'general' if to == 'general' else dm_room(me_user['username'], to)
    msg = add_msg(room=room, sender_id=me_user['id'],
                  sender_name=me_user['username'], type_='text', text=text)
    payload = serialize(msg)
    if room == 'general':
        emit('message', payload, to='general')
    else:
        add_dialog(me_user['id'], to)
        emit('message', payload)
        sid = name_to_sid.get(to.lower())
        if sid:
            emit('message', payload, to=sid)


@socketio.on('typing')
def on_typing(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    if not me_user: return
    to = (data.get('to') or 'general').strip()
    is_typing = bool(data.get('is_typing'))
    room = 'general' if to == 'general' else dm_room(me_user['username'], to)
    emit('typing', {'from': me_user['username'], 'is_typing': is_typing, 'to': to},
         to=room, include_self=False)


@socketio.on('read')
def on_read(data):
    uid = session.get('uid')
    if not uid: return
    room = data.get('room')
    last_id = int(data.get('last_id') or 0)
    if not room: return
    mark_read(uid, room, last_id)
    reads = reads_for_room(room)
    emit('read', {'room': room, 'reads': {str(k): v for k, v in reads.items()}}, to=room)


@socketio.on('call_offer')
def on_call_offer(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    to = (data.get('to') or '').strip()
    offer = data.get('offer')
    call_type = data.get('type', 'audio')
    if not to or not offer: return
    sid = name_to_sid.get(to.lower())
    if sid:
        emit('call_offer', {
            'from': me_user['username'],
            'offer': offer,
            'type': call_type,
        }, to=sid)
    else:
        emit('call_reject', {'from': to, 'reason': 'offline'})


@socketio.on('call_answer')
def on_call_answer(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    to = (data.get('to') or '').strip()
    answer = data.get('answer')
    if not to or not answer: return
    sid = name_to_sid.get(to.lower())
    if sid:
        emit('call_answer', {'from': me_user['username'], 'answer': answer}, to=sid)


@socketio.on('call_ice')
def on_call_ice(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    to = (data.get('to') or '').strip()
    candidate = data.get('candidate')
    if not to or not candidate: return
    sid = name_to_sid.get(to.lower())
    if sid:
        emit('call_ice', {'from': me_user['username'], 'candidate': candidate}, to=sid)


@socketio.on('call_reject')
def on_call_reject(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    to = (data.get('to') or '').strip()
    reason = data.get('reason', 'rejected')
    if not to: return
    sid = name_to_sid.get(to.lower())
    if sid:
        emit('call_reject', {'from': me_user['username'], 'reason': reason}, to=sid)


@socketio.on('call_end')
def on_call_end(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    to = (data.get('to') or '').strip()
    if not to: return
    sid = name_to_sid.get(to.lower())
    if sid:
        emit('call_end', {'from': me_user['username']}, to=sid)


@socketio.on('disconnect')
def on_disc():
    u = online.pop(request.sid, None)
    if not u: return
    if name_to_sid.get(u['username'].lower()) == request.sid:
        name_to_sid.pop(u['username'].lower(), None)
    sys_msg = add_msg('general', None, u['username'], 'system',
                      text=f"{u['username']} покинул чат")
    socketio.emit('message', serialize(sys_msg), to='general')
    broadcast_online()


# ============================================================
init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("=" * 55)
    print("🚀 Holodyx Chat v3 — с фоном в стиле Telegram")
    print(f"   http://127.0.0.1:{port}")
    print(f"   БД: {DB_PATH}")
    print("=" * 55)
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
