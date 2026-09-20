"""
Holodyx Chat — всё в одном файле.
Запуск: python chat.py
Открыть: http://127.0.0.1:5000
"""

import os
import re
import uuid
import sqlite3
import secrets
from datetime import datetime
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

MAX_MEDIA = 50 * 1024 * 1024  # 50 МБ

# ============================================================
# БАЗА ДАННЫХ
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
    c.execute("""
        CREATE TABLE IF NOT EXISTS reads (
            user_id INTEGER,
            room TEXT,
            last_read_id INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, room)
        )
    """)
    conn.commit()
    conn.close()


# ---------- USERS ----------
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
    conn = db()
    r = conn.execute("SELECT * FROM users WHERE email = ?", (email.lower(),)).fetchone()
    conn.close()
    return dict(r) if r else None


def user_by_username(username):
    conn = db()
    r = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()
    return dict(r) if r else None


def user_by_id(uid):
    conn = db()
    r = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
    conn.close()
    return dict(r) if r else None


def update_profile(uid, **fields):
    allowed = {'username', 'avatar', 'bio'}
    sets, vals = [], []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = ?")
            vals.append(v)
    if not sets:
        return True
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


# ---------- MESSAGES ----------
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
    conn.commit()
    conn.close()


def reads_for_room(room):
    conn = db()
    rows = conn.execute("SELECT user_id, last_read_id FROM reads WHERE room = ?", (room,)).fetchall()
    conn.close()
    return {r['user_id']: r['last_read_id'] for r in rows}


# ============================================================
# FLASK APP
# ============================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))
app.config['MAX_CONTENT_LENGTH'] = MAX_MEDIA
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

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
# HTML (встроен)
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
:root{--bg:#0b0f17;--sidebar:#11161f;--chat-bg:#0e1420;--panel:#161c28;--panel-2:#1d2534;--accent:#4f8cff;--accent-2:#7c5cff;--text:#e6edf3;--muted:#7b879b;--border:#1f2734}
html,body{height:100%;overflow:hidden}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);display:flex;align-items:center;justify-content:center}
button,input,textarea{font-family:inherit}
#auth{width:100%;max-width:440px;background:var(--panel);border:1px solid var(--border);border-radius:20px;padding:34px;box-shadow:0 30px 80px rgba(0,0,0,.6);margin:20px}
#auth h1{font-size:30px;text-align:center;margin-bottom:6px;background:linear-gradient(90deg,var(--accent),var(--accent-2));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
#auth .sub{text-align:center;color:var(--muted);font-size:13px;margin-bottom:24px}
#auth .tabs{display:flex;background:var(--panel-2);border-radius:12px;padding:4px;margin-bottom:18px}
#auth .tabs button{flex:1;padding:9px;border:none;background:transparent;color:var(--muted);font-weight:600;cursor:pointer;border-radius:9px;font-size:14px}
#auth .tabs button.active{background:linear-gradient(90deg,var(--accent),var(--accent-2));color:#fff}
#auth input{width:100%;padding:13px 15px;margin-bottom:11px;background:var(--panel-2);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:15px;outline:none}
#auth input:focus{border-color:var(--accent)}
#auth .submit{width:100%;padding:13px;margin-top:6px;background:linear-gradient(90deg,var(--accent),var(--accent-2));color:#fff;border:none;border-radius:12px;font-weight:600;font-size:15px;cursor:pointer}
#auth .submit:disabled{opacity:.6;cursor:wait}
#auth .err{color:#ff6b6b;font-size:13px;text-align:center;margin-top:10px;min-height:18px}
#auth .ok{color:#2ecc71;font-size:13px;text-align:center;margin-top:10px;min-height:18px}
#app{display:none;width:100vw;height:100vh;grid-template-columns:340px 1fr}
#app.active{display:grid}
#sidebar{background:var(--sidebar);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
#sidebarHeader{padding:14px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px}
.avatar-sm{width:42px;height:42px;border-radius:50%;overflow:hidden;flex-shrink:0;background:linear-gradient(135deg,var(--accent),var(--accent-2));display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff}
.avatar-sm img{width:100%;height:100%;object-fit:cover}
#sidebarHeader .info{flex:1;min-width:0}
#sidebarHeader .name{font-weight:600;font-size:15px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#sidebarHeader .sub{font-size:12px;color:#2ecc71}
#sidebarHeader button{background:transparent;border:none;color:var(--muted);font-size:20px;cursor:pointer;padding:6px;border-radius:8px}
#sidebarHeader button:hover{background:rgba(255,255,255,.05);color:var(--text)}
#search{padding:10px 14px;border-bottom:1px solid var(--border)}
#search input{width:100%;padding:9px 12px;background:var(--panel-2);border:1px solid transparent;border-radius:10px;color:var(--text);font-size:14px;outline:none}
#search input:focus{border-color:var(--accent)}
#chatList{flex:1;overflow-y:auto;padding:6px}
.chat-item{display:flex;gap:12px;align-items:center;padding:10px 12px;border-radius:12px;cursor:pointer;position:relative}
.chat-item:hover{background:rgba(255,255,255,.03)}
.chat-item.active{background:linear-gradient(90deg,rgba(79,140,255,.15),rgba(124,92,255,.08))}
.chat-item .avatar{width:46px;height:46px;border-radius:50%;overflow:hidden;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:16px;color:#fff;flex-shrink:0;position:relative}
.chat-item .avatar img{width:100%;height:100%;object-fit:cover}
.chat-item .avatar.group{background:linear-gradient(135deg,var(--accent),var(--accent-2))}
.chat-item .avatar .online-dot{position:absolute;bottom:0;right:0;width:12px;height:12px;border-radius:50%;background:#2ecc71;border:2px solid var(--sidebar)}
.chat-item .body{flex:1;min-width:0}
.chat-item .row{display:flex;justify-content:space-between;align-items:baseline;gap:8px}
.chat-item .title{font-weight:600;font-size:14.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chat-item .time{font-size:11.5px;color:var(--muted);flex-shrink:0}
.chat-item .preview{font-size:13px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-top:2px}
.chat-item .badge{position:absolute;top:50%;transform:translateY(-50%);right:12px;background:var(--accent);color:#fff;font-size:11px;font-weight:700;padding:2px 7px;border-radius:10px;display:none}
.chat-item.unread .badge{display:block}
#main{background:var(--chat-bg);display:flex;flex-direction:column;position:relative}
#chatHeader{padding:10px 22px;background:var(--panel);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:14px;flex-shrink:0}
#chatHeader .avatar{width:42px;height:42px;border-radius:50%;overflow:hidden;background:linear-gradient(135deg,var(--accent),var(--accent-2));display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff}
#chatHeader .avatar img{width:100%;height:100%;object-fit:cover}
#chatHeader .info{flex:1;min-width:0}
#chatHeader .info .title{font-weight:600;font-size:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#chatHeader .info .sub{font-size:12.5px;color:var(--muted)}
#chatHeader .info .sub.typing{color:var(--accent);font-style:italic}
#messagesWrap{flex:1;overflow-y:auto;padding:22px 6% 12px;display:flex;flex-direction:column;gap:4px}
#messagesWrap::-webkit-scrollbar{width:8px}
#messagesWrap::-webkit-scrollbar-thumb{background:#232b38;border-radius:4px}
.date-sep{align-self:center;font-size:12px;color:var(--muted);background:var(--panel);padding:4px 12px;border-radius:12px;margin:14px 0 8px}
.bubble{max-width:65%;padding:8px 12px 6px;border-radius:14px;background:var(--panel-2);border:1px solid var(--border);font-size:14.5px;line-height:1.4;word-wrap:break-word;position:relative;margin-bottom:2px}
.bubble .author{font-size:12px;font-weight:700;color:var(--accent);margin-bottom:2px}
.bubble .text{white-space:pre-wrap}
.bubble .meta{display:flex;align-items:center;justify-content:flex-end;gap:4px;font-size:10.5px;color:var(--muted);margin-top:3px}
.bubble .meta .check{color:#4f8cff;font-size:11px}
.bubble.me{align-self:flex-end;background:linear-gradient(135deg,var(--accent),var(--accent-2));border:none;color:#fff;border-bottom-right-radius:4px}
.bubble.me .author{display:none}
.bubble.me .meta{color:rgba(255,255,255,.75)}
.bubble.me .meta .check{color:#fff}
.bubble.other{border-bottom-left-radius:4px}
.bubble img,.bubble video{max-width:100%;max-height:340px;border-radius:10px;display:block;margin-top:4px}
.bubble audio{margin-top:6px;width:240px;max-width:100%}
#inputBar{padding:12px 22px 18px;display:flex;gap:10px;align-items:flex-end;flex-shrink:0}
#inputBar .inputWrap{flex:1;background:var(--panel-2);border:1px solid var(--border);border-radius:22px;display:flex;align-items:center;padding:4px 4px 4px 6px;transition:border-color .2s}
#inputBar .inputWrap:focus-within{border-color:var(--accent)}
#inputBar input[type=text]{flex:1;background:transparent;border:none;outline:none;color:var(--text);font-size:15px;padding:10px 4px}
.icon-btn{width:38px;height:38px;border-radius:50%;background:transparent;border:none;color:var(--muted);cursor:pointer;display:flex;align-items:center;justify-content:center}
.icon-btn:hover{background:rgba(255,255,255,.06);color:var(--text)}
.icon-btn svg{width:20px;height:20px}
.icon-btn.recording{color:#ff6b6b;animation:pulse 1s infinite}
@keyframes pulse{50%{opacity:.5}}
#sendBtn{width:44px;height:44px;border-radius:50%;background:linear-gradient(135deg,var(--accent),var(--accent-2));color:#fff;border:none;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0}
#sendBtn:hover{filter:brightness(1.1);transform:scale(1.05)}
#sendBtn svg{width:20px;height:20px}
#empty{flex:1;display:flex;align-items:center;justify-content:center;flex-direction:column;color:var(--muted);gap:10px}
#empty svg{width:80px;height:80px;opacity:.3}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);display:none;align-items:center;justify-content:center;z-index:100}
.modal-overlay.visible{display:flex}
.modal{background:var(--panel);border:1px solid var(--border);border-radius:16px;width:100%;max-width:480px;padding:26px;margin:20px;max-height:90vh;overflow-y:auto;box-shadow:0 30px 80px rgba(0,0,0,.7)}
.modal h2{font-size:20px;margin-bottom:18px}
.modal label{display:block;font-size:12px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:6px;margin-top:14px}
.modal input[type=text],.modal textarea{width:100%;padding:11px 13px;background:var(--panel-2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:14.5px;outline:none;resize:vertical}
.modal textarea{min-height:80px}
.modal .avatar-preview{width:96px;height:96px;border-radius:50%;overflow:hidden;margin:0 auto 12px;background:linear-gradient(135deg,var(--accent),var(--accent-2));display:flex;align-items:center;justify-content:center;font-weight:700;font-size:32px;color:#fff;cursor:pointer;border:2px dashed transparent}
.modal .avatar-preview:hover{border-color:var(--accent)}
.modal .avatar-preview img{width:100%;height:100%;object-fit:cover}
.modal .actions{display:flex;gap:10px;margin-top:22px}
.modal .actions button{flex:1;padding:12px;border:none;border-radius:10px;font-weight:600;font-size:14.5px;cursor:pointer}
.modal .actions .primary{background:linear-gradient(90deg,var(--accent),var(--accent-2));color:#fff}
.modal .actions .secondary{background:var(--panel-2);color:var(--text);border:1px solid var(--border)}
.modal .err{color:#ff6b6b;font-size:13px;margin-top:10px;min-height:16px}
.modal .user-info-card{background:var(--panel-2);padding:14px;border-radius:12px;display:flex;gap:14px;align-items:center;margin-bottom:14px}
.modal .user-info-card .avatar{width:64px;height:64px;border-radius:50%;overflow:hidden;flex-shrink:0;background:linear-gradient(135deg,var(--accent),var(--accent-2));display:flex;align-items:center;justify-content:center;font-weight:700;font-size:22px;color:#fff}
.modal .user-info-card .avatar img{width:100%;height:100%;object-fit:cover}
.modal .user-info-card .info .name{font-weight:600;font-size:16px}
.modal .user-info-card .info .bio{color:var(--muted);font-size:13px;margin-top:3px}
#toast{position:fixed;bottom:20px;right:20px;background:var(--panel);border:1px solid var(--border);color:var(--text);padding:12px 16px;border-radius:12px;font-size:13px;opacity:0;transform:translateY(20px);transition:opacity .25s,transform .25s;pointer-events:none;box-shadow:0 10px 30px rgba(0,0,0,.4);max-width:300px;z-index:200}
#toast.visible{opacity:1;transform:none}
#backBtn{display:none;width:34px;height:34px;border-radius:50%;background:transparent;border:none;color:var(--text);cursor:pointer;align-items:center;justify-content:center}
@media (max-width:780px){#app{grid-template-columns:1fr}#sidebar{position:absolute;inset:0;z-index:5;transition:transform .25s}#sidebar.hidden{transform:translateX(-100%)}#main{position:absolute;inset:0;z-index:4}#backBtn{display:flex}}
</style>
</head>
<body>

<div id="auth">
  <h1>Holodyx Chat</h1>
  <div class="sub">Общайтесь, делитесь фото, видео и голосовыми</div>
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
    <input id="regEmail" type="email" placeholder="Email" autocomplete="email">
    <input id="regUser" type="text" placeholder="Ник (3–20 символов)" autocomplete="username">
    <input id="regPass" type="password" placeholder="Пароль (мин. 6)" autocomplete="new-password">
    <input id="regPass2" type="password" placeholder="Повторите пароль" autocomplete="new-password">
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
        <div class="sub">онлайн</div>
      </div>
      <button id="settingsBtn" title="Настройки">⚙</button>
    </div>
    <div id="search"><input id="searchInput" type="text" placeholder="Поиск..."></div>
    <div id="chatList"></div>
  </aside>
  <main id="main">
    <div id="empty">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      <p>Выберите чат, чтобы начать общение</p>
    </div>
    <div id="chatHeader" style="display:none">
      <button id="backBtn"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="20" height="20"><path d="M15 18l-6-6 6-6"/></svg></button>
      <div class="avatar" id="chatHeaderAvatar">?</div>
      <div class="info">
        <div class="title" id="chatHeaderTitle">—</div>
        <div class="sub" id="chatHeaderSub">—</div>
      </div>
      <button class="icon-btn" id="peerInfoBtn" title="Профиль" style="display:none"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg></button>
    </div>
    <div id="messagesWrap" style="display:none"></div>
    <div id="inputBar" style="display:none">
      <div class="inputWrap">
        <input type="file" id="fileInput" accept="image/*,video/*" style="display:none">
        <button class="icon-btn" id="attachBtn" title="Фото / Видео"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg></button>
        <button class="icon-btn" id="micBtn" title="Голосовое"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="2" width="6" height="12" rx="3"/><path d="M5 10a7 7 0 0 0 14 0M12 19v3"/></svg></button>
        <input id="msgInput" type="text" placeholder="Напишите сообщение..." maxlength="4000" autocomplete="off">
      </div>
      <button id="sendBtn"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4 20-7z"/></svg></button>
    </div>
  </main>
</div>

<div class="modal-overlay" id="settingsModal">
  <div class="modal">
    <h2>Настройки профиля</h2>
    <input type="file" id="avatarInput" accept="image/*" style="display:none">
    <div class="avatar-preview" id="avatarPreview">?</div>
    <label>Ник</label><input type="text" id="setUsername" maxlength="20">
    <label>Описание профиля</label><textarea id="setBio" maxlength="300" placeholder="Расскажите о себе..."></textarea>
    <label>Email</label><input type="text" id="setEmail" disabled>
    <div class="err" id="settingsErr"></div>
    <div class="actions">
      <button class="secondary" id="settingsCancel">Отмена</button>
      <button class="primary" id="settingsSave">Сохранить</button>
    </div>
    <div class="actions" style="margin-top:8px">
      <button class="secondary" id="logoutBtn" style="color:#ff6b6b">Выйти из аккаунта</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="peerModal">
  <div class="modal">
    <h2>Профиль</h2>
    <div class="user-info-card">
      <div class="avatar" id="peerAvatar">?</div>
      <div class="info"><div class="name" id="peerName">—</div><div class="bio" id="peerBio">—</div></div>
    </div>
    <div class="actions"><button class="secondary" id="peerClose" style="flex:1">Закрыть</button></div>
  </div>
</div>

<div id="toast"></div>

<script>
const $ = id => document.getElementById(id);
const socket = io({ autoConnect: false, transports: ['websocket', 'polling'] });
let me = null;
const chats = {};
let activeKey = null;
const onlineUsers = new Map();
const peerCache = {};

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
  $('authErr').textContent = '';
  if (!login || !password) { $('authErr').textContent = 'Заполните все поля'; return; }
  $('loginBtn').disabled = true;
  try {
    const r = await fetch('/api/login', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({login, password}) }).then(r => r.json());
    if (!r.ok) { $('authErr').textContent = r.error || 'Ошибка входа'; return; }
    me = r.user; enterApp();
  } catch (e) { $('authErr').textContent = 'Сеть: ' + e.message; }
  finally { $('loginBtn').disabled = false; }
}
$('registerBtn').onclick = doRegister;
$('regPass2').addEventListener('keydown', e => e.key === 'Enter' && doRegister());
async function doRegister() {
  const email = $('regEmail').value.trim();
  const username = $('regUser').value.trim();
  const p1 = $('regPass').value, p2 = $('regPass2').value;
  $('authErr').textContent = ''; $('authOk').textContent = '';
  if (!email || !username || !p1) { $('authErr').textContent = 'Заполните все поля'; return; }
  if (p1 !== p2) { $('authErr').textContent = 'Пароли не совпадают'; return; }
  if (p1.length < 6) { $('authErr').textContent = 'Пароль минимум 6 символов'; return; }
  $('registerBtn').disabled = true; $('registerBtn').textContent = 'Регистрация...';
  try {
    const r = await fetch('/api/register', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({email, username, password: p1}) }).then(r => r.json());
    if (!r.ok) { $('authErr').textContent = r.error || 'Ошибка регистрации'; return; }
    $('authOk').textContent = '✅ Аккаунт создан! Входим...';
    $('loginInput').value = email; $('loginPass').value = p1;
    await doLogin();
  } catch (e) { $('authErr').textContent = 'Сеть: ' + e.message; }
  finally { $('registerBtn').disabled = false; $('registerBtn').textContent = 'Зарегистрироваться'; }
}
(async () => {
  try {
    const r = await fetch('/api/me').then(r => r.json()).catch(() => ({ok:false}));
    if (r.ok) { me = r.user; enterApp(); }
  } catch (e) {}
})();

function enterApp() {
  $('auth').style.display = 'none'; $('app').classList.add('active');
  const av = $('myAvatarSm');
  av.innerHTML = me.avatar ? `<img src="${me.avatar}">` : initials(me.username);
  av.style.background = me.avatar ? 'transparent' : colorFromName(me.username);
  $('myNameSm').textContent = me.username;
  chats['general'] = { key:'general', type:'general', title:'Общий чат', messages:[], unread:0, typing:false };
  if (!socket.connected) socket.connect();
}

socket.on('need_auth', () => socket.disconnect());
socket.on('connect_error', e => console.warn('Socket:', e.message));
socket.on('joined', data => {
  me = data.user;
  chats['general'].messages = data.history || [];
  data.online.forEach(u => {
    onlineUsers.set(u.id, u);
    peerCache[u.username.toLowerCase()] = u;
    if (u.username.toLowerCase() !== me.username.toLowerCase()) ensureDM(u.username, u);
  });
  renderSidebar();
  if (activeKey === 'general') { renderMessages(); scrollBottom(); }
});
socket.on('online', data => {
  onlineUsers.clear();
  data.users.forEach(u => {
    onlineUsers.set(u.id, u);
    peerCache[u.username.toLowerCase()] = u;
    if (u.username.toLowerCase() !== me.username.toLowerCase()) ensureDM(u.username, u);
  });
  renderSidebar(); updateHeaderSub();
});
socket.on('message', msg => {
  const key = msg.room;
  let chat = chats[key];
  if (!chat) {
    if (msg.room.startsWith('dm:')) {
      const parts = msg.room.slice(3).split('|');
      const peerName = parts.find(p => p !== me.username.toLowerCase());
      const peer = peerCache[peerName] || {username: peerName, avatar: null};
      chat = ensureDM(peer.username, peer);
    } else return;
  }
  chat.messages.push(msg);
  const isActive = key === activeKey;
  const fromMe = msg.sender_id === me.id;
  if (!isActive && !fromMe && msg.type !== 'system') {
    chat.unread = (chat.unread || 0) + 1;
    const who = chat.type === 'general' ? msg.sender_name : chat.title;
    toastMsg(`💬 ${who}: ${preview(msg)}`);
  }
  if (isActive) {
    renderMessages(); scrollBottom();
    if (!fromMe && chat.messages.length) {
      const lastId = chat.messages[chat.messages.length - 1].id;
      socket.emit('read', { room: msg.room, last_id: lastId });
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
function colorFromName(n) { let h = 0; for (let i = 0; i < n.length; i++) h = (h*31 + n.charCodeAt(i)) % 360; return `linear-gradient(135deg,hsl(${h},65%,55%),hsl(${(h+40)%360},65%,45%))`; }
function parseTime(iso) { if (!iso) return null; let s = String(iso); if (!s.includes('T')) s = s.replace(' ', 'T') + 'Z'; else if (!s.endsWith('Z') && !s.includes('+')) s += 'Z'; const d = new Date(s); return isNaN(d) ? null : d; }
function fmtTime(iso) { const d = parseTime(iso); return d ? d.toLocaleTimeString('ru-RU', {hour:'2-digit', minute:'2-digit'}) : ''; }
function fmtDate(iso) { const d = parseTime(iso); if (!d) return ''; const today = new Date(), yest = new Date(); yest.setDate(today.getDate()-1); const same = (a,b) => a.toDateString() === b.toDateString(); if (same(d, today)) return 'Сегодня'; if (same(d, yest)) return 'Вчера'; return d.toLocaleDateString('ru-RU', {day:'numeric', month:'long'}); }
function dmKey(a, b) { return 'dm:' + [a.toLowerCase(), b.toLowerCase()].sort().join('|'); }
function ensureDM(peerName, peer) {
  const key = dmKey(me.username, peerName);
  if (!chats[key]) chats[key] = { key, type:'dm', title: peerName, peer: peerName, avatar: peer.avatar || null, messages:[], unread:0, typing:false };
  else if (peer.avatar) chats[key].avatar = peer.avatar;
  return chats[key];
}
function preview(m) { if (m.type === 'image') return '📷 Фото'; if (m.type === 'video') return '🎬 Видео'; if (m.type === 'audio') return '🎤 Голосовое'; return m.text || ''; }
function escapeHtml(s) { return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function avatarHtml(user) { if (user.avatar) return `<img src="${user.avatar}" alt="">`; return initials(user.username || user); }
function bgForAvatar(user) { return user.avatar ? 'transparent' : colorFromName(user.username || '?'); }

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
    const last = c.messages[c.messages.length - 1];
    let pv = 'Нет сообщений';
    if (last) {
      if (last.type === 'system') pv = last.text;
      else pv = (last.sender_id === me.id ? 'Вы: ' : (c.type==='general' ? last.sender_name+': ' : '')) + preview(last);
    }
    const peerUser = c.type === 'dm' ? {username: c.peer, avatar: c.avatar} : {username: c.title, avatar: null};
    const online = c.type === 'dm' && [...onlineUsers.values()].some(u => u.username.toLowerCase() === (c.peer||'').toLowerCase());
    el.innerHTML = `
      <div class="avatar ${c.type==='general'?'group':''}" style="background:${c.type==='general'?'':bgForAvatar(peerUser)}">
        ${c.type === 'general' ? '💬' : avatarHtml(peerUser)}
        ${online ? '<div class="online-dot"></div>' : ''}
      </div>
      <div class="body">
        <div class="row"><div class="title">${escapeHtml(c.title)}</div><div class="time">${last ? fmtTime(last.time) : ''}</div></div>
        <div class="preview">${escapeHtml(pv)}</div>
      </div>
      <div class="badge">${c.unread > 99 ? '99+' : c.unread}</div>
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
  const peerUser = c.type === 'dm' ? {username: c.peer, avatar: c.avatar} : {username: c.title, avatar: null};
  $('chatHeaderAvatar').innerHTML = c.type === 'general' ? '💬' : avatarHtml(peerUser);
  $('chatHeaderAvatar').style.background = c.type === 'general' ? 'linear-gradient(135deg,#4f8cff,#7c5cff)' : bgForAvatar(peerUser);
  $('chatHeaderTitle').textContent = c.title;
  updateHeaderSub();
  $('peerInfoBtn').style.display = c.type === 'dm' ? '' : 'none';
  $('sidebar').classList.add('hidden');
  renderMessages(); scrollBottom(); renderSidebar();
  if (c.type === 'dm') socket.emit('open_dm', { peer: c.peer });
  else { const lastId = c.messages.length ? c.messages[c.messages.length-1].id : 0; if (lastId) socket.emit('read', { room: 'general', last_id: lastId }); }
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
  if (c.typing) { sub.textContent = 'печатает…'; sub.classList.add('typing'); return; }
  if (c.type === 'general') sub.textContent = onlineUsers.size + ' онлайн';
  else { const on = [...onlineUsers.values()].some(u => u.username.toLowerCase() === (c.peer||'').toLowerCase()); sub.textContent = on ? 'онлайн' : 'не в сети'; }
}
function renderMessages() {
  const c = chats[activeKey]; if (!c) return;
  $('messagesWrap').innerHTML = '';
  let lastDate = '';
  c.messages.forEach(m => {
    const d = fmtDate(m.time);
    if (d && d !== lastDate) { const sep = document.createElement('div'); sep.className = 'date-sep'; sep.textContent = d; $('messagesWrap').appendChild(sep); lastDate = d; }
    if (m.type === 'system') { const s = document.createElement('div'); s.className = 'date-sep'; s.textContent = m.text; $('messagesWrap').appendChild(s); return; }
    const mine = m.sender_id === me.id;
    const el = document.createElement('div');
    el.className = 'bubble ' + (mine ? 'me' : 'other');
    const showAuthor = c.type === 'general' && !mine;
    let bodyHtml = '';
    if (m.type === 'image') bodyHtml = `<img src="${m.media_url}" alt="" onclick="window.open('${m.media_url}','_blank')">`;
    else if (m.type === 'video') bodyHtml = `<video src="${m.media_url}" controls preload="metadata"></video>`;
    else if (m.type === 'audio') bodyHtml = `<audio src="${m.media_url}" controls preload="metadata"></audio>`;
    else bodyHtml = `<div class="text">${escapeHtml(m.text)}</div>`;
    let check = '';
    if (mine) {
      const readBy = c.reads || {};
      const others = Object.entries(readBy).filter(([uid]) => Number(uid) !== me.id);
      const isRead = others.some(([_, lastId]) => Number(lastId) >= m.id);
      check = isRead ? '✓✓' : '✓';
    }
    el.innerHTML = `
      ${showAuthor ? `<div class="author">${escapeHtml(m.sender_name)}</div>` : ''}
      ${bodyHtml}
      <div class="meta"><span>${fmtTime(m.time)}</span>${mine ? `<span class="check">${check}</span>` : ''}</div>
    `;
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
    if (!r.ok) toastMsg('⚠ ' + (r.error||'Ошибка загрузки'));
  } catch (e) { toastMsg('⚠ Сеть: ' + e.message); }
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
      $('msgInput').placeholder = 'Напишите сообщение...';
      const duration = Math.round((Date.now() - recStart) / 1000);
      stream.getTracks().forEach(t => t.stop());
      if (duration < 1) { toastMsg('Слишком короткая запись'); return; }
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
    $('msgInput').placeholder = '⏺ Идёт запись... 0:00';
    recTimer = setInterval(() => { sec++; $('msgInput').placeholder = `⏺ Идёт запись... ${Math.floor(sec/60)}:${String(sec%60).padStart(2,'0')}`; }, 1000);
  } catch (err) { toastMsg('Нет доступа к микрофону'); }
};
function getAudioMime() { const list = ['audio/webm;codecs=opus', 'audio/webm', 'audio/ogg;codecs=opus']; for (const m of list) if (window.MediaRecorder && MediaRecorder.isTypeSupported(m)) return m; return ''; }
$('settingsBtn').onclick = () => {
  $('setUsername').value = me.username || '';
  $('setBio').value = me.bio || '';
  $('setEmail').value = me.email || '';
  const prev = $('avatarPreview');
  prev.innerHTML = me.avatar ? `<img src="${me.avatar}">` : initials(me.username);
  prev.style.background = me.avatar ? 'transparent' : colorFromName(me.username);
  $('settingsErr').textContent = '';
  $('settingsModal').classList.add('visible');
};
$('settingsCancel').onclick = () => $('settingsModal').classList.remove('visible');
$('avatarPreview').onclick = () => $('avatarInput').click();
$('avatarInput').onchange = e => {
  const f = e.target.files[0]; if (!f) return;
  const url = URL.createObjectURL(f);
  $('avatarPreview').innerHTML = `<img src="${url}">`;
  $('avatarPreview').style.background = 'transparent';
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
    av.style.background = me.avatar ? 'transparent' : colorFromName(me.username);
    $('myNameSm').textContent = me.username;
    $('settingsModal').classList.remove('visible');
    toastMsg('✅ Профиль обновлён');
  } catch (e) { $('settingsErr').textContent = 'Сеть: ' + e.message; }
};
$('logoutBtn').onclick = async () => { await fetch('/api/logout', { method:'POST' }); location.reload(); };
$('peerInfoBtn').onclick = () => {
  const c = chats[activeKey]; if (!c || c.type !== 'dm') return;
  const u = peerCache[c.peer.toLowerCase()] || {username: c.peer, avatar: c.avatar, bio: ''};
  $('peerAvatar').innerHTML = u.avatar ? `<img src="${u.avatar}">` : initials(u.username);
  $('peerAvatar').style.background = u.avatar ? 'transparent' : colorFromName(u.username);
  $('peerName').textContent = u.username;
  $('peerBio').textContent = u.bio || 'Нет описания';
  $('peerModal').classList.add('visible');
};
$('peerClose').onclick = () => $('peerModal').classList.remove('visible');
$('searchInput').addEventListener('input', renderSidebar);
function toastMsg(t) { const el = $('toast'); el.textContent = t; el.classList.add('visible'); clearTimeout(toastMsg._t); toastMsg._t = setTimeout(() => el.classList.remove('visible'), 2500); }
document.addEventListener('keydown', e => { if (e.key === 'Escape') { $('settingsModal').classList.remove('visible'); $('peerModal').classList.remove('visible'); } });
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
            return jsonify(ok=False, error='Ник: 3–20 символов (буквы, цифры, _)'), 400
        if len(password) < 6:
            return jsonify(ok=False, error='Пароль минимум 6 символов'), 400
        if user_by_email(email):
            return jsonify(ok=False, error='Email уже занят'), 400
        if user_by_username(username):
            return jsonify(ok=False, error='Ник уже занят'), 400
        uid = create_user(email, username, password)
        if not uid:
            return jsonify(ok=False, error='Не удалось создать'), 500
        print(f"[REGISTER] OK uid={uid}")
        return jsonify(ok=True, uid=uid)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify(ok=False, error=f'Ошибка: {e}'), 500


@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.get_json(silent=True) or {}
    login = (data.get('login') or '').strip()
    password = data.get('password') or ''
    user = None
    if '@' in login:
        user = user_by_email(login)
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
            return jsonify(ok=False, error='Формат аватара не поддерживается'), 400
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
    socketio.emit('message', payload, to=room)
    if room.startswith('dm:'):
        for name in room[3:].split('|'):
            sid = name_to_sid.get(name)
            if sid:
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
    online[request.sid] = {'id': u['id'], 'username': u['username'], 'avatar': u['avatar'], 'bio': u['bio']}
    name_to_sid[u['username'].lower()] = request.sid
    join_room('general')
    join_room(f"user:{u['id']}")
    emit('joined', {
        'user': {'id': u['id'], 'username': u['username'], 'avatar': u['avatar'], 'bio': u['bio'], 'email': u['email']},
        'history': history('general', 200),
        'online': online_list(),
    })
    sys_msg = add_msg('general', None, u['username'], 'system', text=f"{u['username']} присоединился к чату")
    emit('message', serialize(sys_msg), to='general')
    broadcast_online()


@socketio.on('open_dm')
def on_open_dm(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    peer = (data.get('peer') or '').strip()
    if not peer: return
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
    msg = add_msg(room=room, sender_id=me_user['id'], sender_name=me_user['username'], type_='text', text=text)
    payload = serialize(msg)
    if room == 'general':
        emit('message', payload, to='general')
    else:
        emit('message', payload)
        sid = name_to_sid.get(to.lower())
        if sid: emit('message', payload, to=sid)


@socketio.on('typing')
def on_typing(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    if not me_user: return
    to = (data.get('to') or 'general').strip()
    is_typing = bool(data.get('is_typing'))
    room = 'general' if to == 'general' else dm_room(me_user['username'], to)
    emit('typing', {'from': me_user['username'], 'is_typing': is_typing, 'to': to}, to=room, include_self=False)


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


@socketio.on('disconnect')
def on_disc():
    u = online.pop(request.sid, None)
    if not u: return
    if name_to_sid.get(u['username'].lower()) == request.sid:
        name_to_sid.pop(u['username'].lower(), None)
    sys_msg = add_msg('general', None, u['username'], 'system', text=f"{u['username']} покинул чат")
    socketio.emit('message', serialize(sys_msg), to='general')
    broadcast_online()


# ============================================================
# ЗАПУСК
# ============================================================
init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("=" * 55)
    print("🚀 Holodyx Chat запущен")
    print(f"   Открой: http://127.0.0.1:{port}")
    print(f"   БД:     {DB_PATH}")
    print(f"   Файлы:  {UPLOAD_DIR}")
    print("=" * 55)
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)