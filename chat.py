# ========== ЧАСТЬ 1/5 ==========
"""
Holodyx Chat v6
Запуск: python chat.py
"""

import os, re, uuid, json, sqlite3, secrets
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, session, send_from_directory, Response
from flask_socketio import SocketIO, emit, join_room
from werkzeug.security import generate_password_hash, check_password_hash

APP_VERSION = "6.0"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UP = os.path.join(BASE_DIR, 'chat_uploads')
AV = os.path.join(UP, 'avatars')
MD = os.path.join(UP, 'media')
ST = os.path.join(UP, 'stories')
for d in (AV, MD, ST):
    os.makedirs(d, exist_ok=True)
DB_PATH = os.path.join(BASE_DIR, 'chat.db')
MAX_MEDIA = 50 * 1024 * 1024

DONATE_URL = os.environ.get('DONATE_URL', 'https://www.donationalerts.com/r/твой_ник')


# ============================================================
# DATABASE
# ============================================================
def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    c = db(); k = c.cursor()
    k.execute("""CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE, username TEXT UNIQUE,
        password_hash TEXT, avatar TEXT,
        bio TEXT DEFAULT '', phone TEXT DEFAULT '',
        real_name TEXT DEFAULT '',
        premium INTEGER DEFAULT 0,
        is_admin INTEGER DEFAULT 0,
        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    k.execute("""CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        room TEXT, sender_id INTEGER, sender_name TEXT,
        type TEXT DEFAULT 'text', text TEXT DEFAULT '',
        media_url TEXT, media_name TEXT,
        time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    k.execute("CREATE INDEX IF NOT EXISTS i_mr ON messages(room,id)")

    k.execute("""CREATE TABLE IF NOT EXISTS reads(
        user_id INTEGER, room TEXT, last_read_id INTEGER DEFAULT 0,
        PRIMARY KEY(user_id,room))""")

    k.execute("""CREATE TABLE IF NOT EXISTS groups(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT, type TEXT DEFAULT 'group',
        about TEXT DEFAULT '', owner_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    k.execute("""CREATE TABLE IF NOT EXISTS group_members(
        group_id INTEGER, user_id INTEGER, role TEXT DEFAULT 'member',
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(group_id,user_id))""")

    k.execute("""CREATE TABLE IF NOT EXISTS stories(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, type TEXT DEFAULT 'image',
        media_url TEXT, text TEXT DEFAULT '', bg_color TEXT DEFAULT '#3b82f6',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, expires_at TIMESTAMP)""")

    k.execute("""CREATE TABLE IF NOT EXISTS story_views(
        story_id INTEGER, user_id INTEGER,
        viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(story_id,user_id))""")

    k.execute("""CREATE TABLE IF NOT EXISTS settings(
        user_id INTEGER PRIMARY KEY, data TEXT DEFAULT '{}')""")

    k.execute("""CREATE TABLE IF NOT EXISTS premium_requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        message TEXT DEFAULT '',
        donate_name TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        reviewed_at TIMESTAMP,
        reviewed_by INTEGER)""")

    c.commit(); c.close()


# ============================================================
# USERS
# ============================================================
def create_user(email, username, pwd, real_name=''):
    c = db()
    try:
        first = c.execute("SELECT COUNT(*) AS n FROM users").fetchone()['n'] == 0
        c.execute(
            "INSERT INTO users(email,username,password_hash,real_name,is_admin) VALUES(?,?,?,?,?)",
            (email.lower(), username, generate_password_hash(pwd), real_name.strip(), 1 if first else 0)
        )
        c.commit()
        r = c.execute("SELECT id FROM users WHERE email=?", (email.lower(),)).fetchone()
        return r['id'] if r else None
    except sqlite3.IntegrityError as e:
        print(f"[create_user] {e}"); return None
    finally:
        c.close()


def u_email(e):
    c = db(); r = c.execute("SELECT * FROM users WHERE email=?", (e.lower(),)).fetchone(); c.close()
    return dict(r) if r else None


def u_name(n):
    c = db(); r = c.execute("SELECT * FROM users WHERE username=?", (n,)).fetchone(); c.close()
    return dict(r) if r else None


def u_id(i):
    c = db(); r = c.execute("SELECT * FROM users WHERE id=?", (i,)).fetchone(); c.close()
    return dict(r) if r else None


def upd_seen(uid):
    c = db()
    c.execute("UPDATE users SET last_seen=CURRENT_TIMESTAMP WHERE id=?", (uid,))
    c.commit(); c.close()


def search_u(q, ex, lim=20):
    c = db()
    r = c.execute("""SELECT id,username,avatar,bio,last_seen,premium FROM users
        WHERE username LIKE ? AND id!=? ORDER BY username LIMIT ?""",
        (f'%{q}%', ex, lim)).fetchall()
    c.close()
    return [dict(x) for x in r]


def upd_prof(uid, **f):
    a = {'username','avatar','bio','phone'}
    s, v = [], []
    for k, x in f.items():
        if k in a:
            s.append(f"{k}=?"); v.append(x)
    if not s: return True
    v.append(uid)
    c = db()
    try:
        c.execute(f"UPDATE users SET {','.join(s)} WHERE id=?", v)
        c.commit(); return True
    except sqlite3.IntegrityError:
        return False
    finally:
        c.close()


# ============================================================
# MESSAGES
# ============================================================
def add_msg(room, sid, sname, typ, text='', murl=None, mname=None):
    c = db()
    c.execute("""INSERT INTO messages(room,sender_id,sender_name,type,text,media_url,media_name)
        VALUES(?,?,?,?,?,?,?)""", (room, sid, sname, typ, text, murl, mname))
    c.commit()
    mid = c.execute("SELECT last_insert_rowid() AS id").fetchone()['id']
    r = c.execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
    c.close()
    return dict(r)


def hist(room, lim=200):
    c = db()
    r = c.execute("SELECT * FROM messages WHERE room=? ORDER BY id DESC LIMIT ?", (room, lim)).fetchall()
    c.close()
    return [dict(x) for x in reversed(r)]


def mark_read(uid, room, lid):
    c = db()
    c.execute("""INSERT INTO reads(user_id,room,last_read_id) VALUES(?,?,?)
        ON CONFLICT(user_id,room) DO UPDATE SET last_read_id=excluded.last_read_id""", (uid, room, lid))
    c.commit(); c.close()


def reads_room(room):
    c = db()
    r = c.execute("SELECT user_id,last_read_id FROM reads WHERE room=?", (room,)).fetchall()
    c.close()
    return {x['user_id']: x['last_read_id'] for x in r}


# ============================================================
# GROUPS
# ============================================================
def create_group(name, typ, owner, about=''):
    c = db()
    c.execute("INSERT INTO groups(name,type,owner_id,about) VALUES(?,?,?,?)", (name, typ, owner, about))
    c.commit()
    gid = c.execute("SELECT last_insert_rowid() AS id").fetchone()['id']
    c.execute("INSERT INTO group_members(group_id,user_id,role) VALUES(?,?,'owner')", (gid, owner))
    c.commit(); c.close()
    return gid


def get_group(gid):
    c = db(); r = c.execute("SELECT * FROM groups WHERE id=?", (gid,)).fetchone(); c.close()
    return dict(r) if r else None


def user_groups(uid):
    c = db()
    r = c.execute("""SELECT g.*,gm.role FROM groups g
        JOIN group_members gm ON gm.group_id=g.id
        WHERE gm.user_id=? ORDER BY g.id DESC""", (uid,)).fetchall()
    c.close()
    return [dict(x) for x in r]


def group_members(gid):
    c = db()
    r = c.execute("""SELECT u.id,u.username,u.avatar,u.bio,u.premium,gm.role
        FROM group_members gm JOIN users u ON u.id=gm.user_id
        WHERE gm.group_id=? ORDER BY gm.role DESC,u.username""", (gid,)).fetchall()
    c.close()
    return [dict(x) for x in r]


def add_mem(gid, uid, role='member'):
    c = db()
    try:
        c.execute("INSERT OR IGNORE INTO group_members(group_id,user_id,role) VALUES(?,?,?)", (gid, uid, role))
        c.commit()
    finally:
        c.close()


def rm_mem(gid, uid):
    c = db()
    c.execute("DELETE FROM group_members WHERE group_id=? AND user_id=?", (gid, uid))
    c.commit(); c.close()


def is_mem(gid, uid):
    c = db()
    r = c.execute("SELECT 1 FROM group_members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    c.close()
    return bool(r)


def user_gids(uid):
    c = db()
    r = c.execute("SELECT group_id FROM group_members WHERE user_id=?", (uid,)).fetchall()
    c.close()
    return [x['group_id'] for x in r]


def g_room(gid):
    return f"group:{gid}"


def my_role(gid, uid):
    c = db()
    r = c.execute("SELECT role FROM group_members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    c.close()
    return r['role'] if r else None


# ============================================================
# STORIES
# ============================================================
def add_story(uid, typ, murl=None, text='', bg='#3b82f6'):
    c = db()
    exp = (datetime.utcnow() + timedelta(hours=24)).isoformat()
    c.execute("INSERT INTO stories(user_id,type,media_url,text,bg_color,expires_at) VALUES(?,?,?,?,?,?)",
              (uid, typ, murl, text, bg, exp))
    c.commit()
    sid = c.execute("SELECT last_insert_rowid() AS id").fetchone()['id']
    c.close()
    return sid


def active_stories(ex=None):
    c = db()
    r = c.execute("""SELECT s.*,u.username,u.avatar FROM stories s
        JOIN users u ON u.id=s.user_id
        WHERE datetime(s.expires_at)>datetime('now')
        ORDER BY s.created_at ASC""").fetchall()
    c.close()
    bu = {}
    for x in r:
        uid = x['user_id']
        if uid not in bu:
            bu[uid] = {'user_id': uid, 'username': x['username'], 'avatar': x['avatar'],
                       'stories': [], 'has_unviewed': False}
        bu[uid]['stories'].append({
            'id': x['id'], 'type': x['type'], 'media_url': x['media_url'],
            'text': x['text'], 'bg_color': x['bg_color'], 'created_at': x['created_at']
        })
    if ex:
        c = db()
        v = c.execute("SELECT story_id FROM story_views WHERE user_id=?", (ex,)).fetchall()
        c.close()
        vids = {x['story_id'] for x in v}
        for u in bu.values():
            u['has_unviewed'] = any(s['id'] not in vids for s in u['stories'])
    return list(bu.values())


def view_story(sid, uid):
    c = db()
    try:
        c.execute("INSERT OR IGNORE INTO story_views(story_id,user_id) VALUES(?,?)", (sid, uid))
        c.commit()
    finally:
        c.close()


def my_stories(uid):
    c = db()
    r = c.execute("""SELECT * FROM stories WHERE user_id=?
        AND datetime(expires_at)>datetime('now')
        ORDER BY created_at DESC""", (uid,)).fetchall()
    c.close()
    return [dict(x) for x in r]


# ============================================================
# SETTINGS
# ============================================================
def get_settings(uid):
    c = db()
    r = c.execute("SELECT data FROM settings WHERE user_id=?", (uid,)).fetchone()
    c.close()
    if r:
        try: return json.loads(r['data'])
        except: return {}
    return {}


def save_settings(uid, data):
    c = db()
    c.execute("""INSERT INTO settings(user_id,data) VALUES(?,?)
        ON CONFLICT(user_id) DO UPDATE SET data=excluded.data""", (uid, json.dumps(data)))
    c.commit(); c.close()


# ============================================================
# PREMIUM
# ============================================================
def activate_premium_forever(uid):
    c = db()
    c.execute("UPDATE users SET premium=1 WHERE id=?", (uid,))
    c.commit(); c.close()


def create_premium_request(uid, message='', donate_name=''):
    c = db()
    r = c.execute("SELECT 1 FROM premium_requests WHERE user_id=? AND status='pending'", (uid,)).fetchone()
    if r:
        c.close(); return None, "У вас уже есть активная заявка"

    u = c.execute("SELECT username FROM users WHERE id=?", (uid,)).fetchone()
    chat_name = u['username'] if u else ''
    donate_name = (donate_name or '').strip()

    if not donate_name:
        c.close(); return None, "Укажите имя, с которого сделан донат"

    if donate_name.lower() != chat_name.lower():
        c.close()
        return None, f'Имя «{donate_name}» не совпадает с вашим ником «{chat_name}». Premium не будет выдан.'

    c.execute("INSERT INTO premium_requests(user_id,message,donate_name) VALUES(?,?,?)",
              (uid, message[:500], donate_name))
    c.commit()
    rid = c.execute("SELECT last_insert_rowid() AS id").fetchone()['id']
    c.close()
    return rid, None


def list_premium_requests(status=None):
    c = db()
    if status:
        r = c.execute("""SELECT pr.*,u.username,u.avatar,u.premium,u.real_name
            FROM premium_requests pr JOIN users u ON u.id=pr.user_id
            WHERE pr.status=? ORDER BY pr.id DESC LIMIT 100""", (status,)).fetchall()
    else:
        r = c.execute("""SELECT pr.*,u.username,u.avatar,u.premium,u.real_name
            FROM premium_requests pr JOIN users u ON u.id=pr.user_id
            ORDER BY pr.id DESC LIMIT 100""").fetchall()
    c.close()
    return [dict(x) for x in r]


def approve_request(rid, admin_uid):
    c = db()
    r = c.execute("SELECT * FROM premium_requests WHERE id=? AND status='pending'", (rid,)).fetchone()
    if not r:
        c.close(); return None
    c.execute("""UPDATE premium_requests SET status='approved',
        reviewed_at=CURRENT_TIMESTAMP, reviewed_by=? WHERE id=?""", (admin_uid, rid))
    c.commit()
    target_uid = r['user_id']
    c.close()
    activate_premium_forever(target_uid)
    return target_uid


def reject_request(rid, admin_uid):
    c = db()
    c.execute("""UPDATE premium_requests SET status='rejected',
        reviewed_at=CURRENT_TIMESTAMP, reviewed_by=? WHERE id=? AND status='pending'""",
        (admin_uid, rid))
    c.commit(); c.close()


def my_request(uid):
    c = db()
    r = c.execute("SELECT * FROM premium_requests WHERE user_id=? ORDER BY id DESC LIMIT 1", (uid,)).fetchone()
    c.close()
    return dict(r) if r else None


def admin_stats():
    c = db()
    users = c.execute("SELECT COUNT(*) AS n FROM users").fetchone()['n']
    premium = c.execute("SELECT COUNT(*) AS n FROM users WHERE premium=1").fetchone()['n']
    pending = c.execute("SELECT COUNT(*) AS n FROM premium_requests WHERE status='pending'").fetchone()['n']
    msgs = c.execute("SELECT COUNT(*) AS n FROM messages").fetchone()['n']
    c.close()
    return {'users': users, 'premium': premium, 'pending_requests': pending, 'messages': msgs}

# ========== КОНЕЦ ЧАСТИ 1 ==========# ========== ЧАСТЬ 2/5 ==========
# ============================================================
# FLASK APP
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

ALLOWED_AV = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
ALLOWED_IMG = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
ALLOWED_VID = {'mp4', 'webm', 'mov', 'mkv'}
ALLOWED_AUD = {'webm', 'ogg', 'mp3', 'wav', 'm4a'}


def ext_of(f):
    return f.rsplit('.', 1)[-1].lower() if '.' in f else ''


def dm_room(a, b):
    return 'dm:' + '|'.join(sorted([a.lower(), b.lower()]))


def ser(m):
    return {'id': m['id'], 'room': m['room'], 'sender_id': m['sender_id'],
            'sender_name': m['sender_name'], 'type': m['type'], 'text': m['text'],
            'media_url': m['media_url'], 'media_name': m['media_name'], 'time': m['time']}


def online_list():
    s = {}
    for u in online.values():
        s[u['id']] = {'id': u['id'], 'username': u['username'], 'avatar': u['avatar'],
                      'bio': u.get('bio', ''), 'premium': u.get('premium', 0)}
    return list(s.values())


def bcast_online():
    socketio.emit('online', {'users': online_list()}, to='general')


def user_public(u):
    return {
        'id': u['id'], 'username': u['username'], 'avatar': u['avatar'],
        'bio': u['bio'], 'email': u['email'], 'phone': u.get('phone', ''),
        'real_name': u.get('real_name', ''),
        'premium': u.get('premium', 0), 'is_admin': u.get('is_admin', 0),
    }


# ============================================================
# HTML
# ============================================================
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Holodyx Chat</title>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d0f12;--panel:#14171c;--p2:#1a1e24;--hv:#1f242b;
  --t:#e8eaed;--t2:#8b929c;--t3:#5f6772;
  --bd:#232830;--ac:#3b82f6;--ach:#2563eb;--dg:#ef4444;--ok:#22c55e;--pm:#f5b942;
  --own:#2b3542;--oth:#1a1e24;
}
html,body{height:100%;overflow:hidden}
body{font-family:-apple-system,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--t);
  font-size:14px;display:flex;align-items:center;justify-content:center}
button,input,textarea{font-family:inherit;color:inherit}
button{cursor:pointer;border:none;background:none}
::-webkit-scrollbar{width:6px}::-webkit-scrollbar-thumb{background:#2a3038;border-radius:3px}

/* AUTH */
#auth{width:100%;max-width:400px;background:var(--panel);border:1px solid var(--bd);
  border-radius:12px;padding:32px;margin:20px}
#auth h1{font-size:22px;font-weight:600;text-align:center;margin-bottom:4px}
#auth .sub{text-align:center;color:var(--t2);font-size:13px;margin-bottom:22px}
#auth .tabs{display:flex;background:var(--p2);border-radius:8px;padding:3px;margin-bottom:18px}
#auth .tabs button{flex:1;padding:8px;font-size:13px;font-weight:500;color:var(--t2);border-radius:6px}
#auth .tabs button.active{background:var(--panel);color:var(--t)}
#auth input{width:100%;padding:11px 13px;margin-bottom:10px;background:var(--p2);
  border:1px solid var(--bd);border-radius:8px;font-size:14px;outline:none}
#auth input:focus{border-color:var(--ac)}
#auth .submit{width:100%;padding:11px;background:var(--ac);border-radius:8px;font-weight:500;font-size:14px}
#auth .submit:hover{background:var(--ach)}
#auth .submit:disabled{opacity:.5;cursor:wait}
#auth .err,#auth .ok{font-size:13px;text-align:center;margin-top:10px;min-height:18px}
#auth .err{color:var(--dg)}#auth .ok{color:var(--ok)}

#regProgress{display:flex;gap:6px;margin-bottom:18px}
#regProgress .step{flex:1;height:3px;background:var(--p2);border-radius:2px;transition:.3s}
#regProgress .step.done{background:var(--ac)}
.step-block{display:none}
.step-block.active{display:block;animation:fade .2s}
@keyframes fade{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
.step-nav{display:flex;gap:8px;margin-top:14px}
.step-nav button{flex:1;padding:10px;border-radius:8px;font-weight:500;font-size:14px}
.step-nav .back{background:var(--p2);border:1px solid var(--bd);color:var(--t2)}
.step-nav .next{background:var(--ac);color:#fff}
.step-nav .next:hover{background:var(--ach)}
.pw-strength{height:3px;background:var(--p2);border-radius:2px;margin:-4px 0 10px;overflow:hidden}
.pw-strength .fill{height:100%;width:0;transition:.3s}
.pw-strength .fill.weak{width:33%;background:var(--dg)}
.pw-strength .fill.medium{width:66%;background:var(--pm)}
.pw-strength .fill.strong{width:100%;background:var(--ok)}
.skip-link{display:block;text-align:center;color:var(--t2);font-size:12.5px;
  margin-top:12px;cursor:pointer;text-decoration:underline}
.skip-link:hover{color:var(--ac)}
.hint{font-size:11.5px;color:var(--t3);margin:-4px 0 10px;line-height:1.4}
.hint b{color:var(--t2)}

/* APP */
#app{display:none;width:100vw;height:100vh;grid-template-columns:300px 1fr}
#app.active{display:grid}
#sidebar{background:var(--panel);border-right:1px solid var(--bd);
  display:flex;flex-direction:column;overflow:hidden}
#sh{padding:12px 14px;display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--bd)}
.av-sm{width:36px;height:36px;border-radius:50%;flex-shrink:0;overflow:hidden;background:#2a3038;
  display:flex;align-items:center;justify-content:center;font-weight:600;font-size:13px;color:var(--t2)}
.av-sm img{width:100%;height:100%;object-fit:cover}
#sh .info{flex:1;min-width:0}
#sh .nm{font-weight:500;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#sh .sb{font-size:12px;color:var(--ok)}
.ib{width:32px;height:32px;border-radius:6px;display:flex;align-items:center;
  justify-content:center;color:var(--t2)}
.ib:hover{background:var(--hv);color:var(--t)}.ib svg{width:18px;height:18px}
#search{padding:8px 12px;border-bottom:1px solid var(--bd)}
#search input{width:100%;padding:8px 10px;background:var(--p2);border:1px solid transparent;
  border-radius:6px;font-size:13px;outline:none}
#search input:focus{background:var(--bg)}
#storiesBar{display:flex;gap:8px;padding:10px 12px;overflow-x:auto;border-bottom:1px solid var(--bd)}
#storiesBar::-webkit-scrollbar{height:0}
.story-circle{display:flex;flex-direction:column;align-items:center;gap:4px;cursor:pointer;
  flex-shrink:0;width:56px}
.story-circle .ring{width:52px;height:52px;border-radius:50%;padding:2px;
  background:linear-gradient(135deg,#f5b942,#e08b26);display:flex;align-items:center;
  justify-content:center;position:relative}
.story-circle.viewed .ring{background:#3a424c}
.story-circle .ring>div{width:100%;height:100%;border-radius:50%;overflow:hidden;background:#2a3038;
  border:2px solid var(--panel);display:flex;align-items:center;justify-content:center;
  font-weight:600;color:var(--t2);font-size:14px}
.story-circle .ring>div img{width:100%;height:100%;object-fit:cover}
.story-circle .ring.plus::after{content:'+';position:absolute;right:-2px;bottom:-2px;
  width:18px;height:18px;border-radius:50%;background:var(--ac);color:#fff;
  display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;
  border:2px solid var(--panel)}
.story-circle .lbl{font-size:11px;color:var(--t2);white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis;max-width:56px}
#chatList{flex:1;overflow-y:auto;padding:6px}
.ci{display:flex;gap:10px;align-items:center;padding:9px 10px;border-radius:8px;
  cursor:pointer;position:relative}
.ci:hover,.ci.active{background:var(--hv)}
.ci .av{width:40px;height:40px;border-radius:50%;flex-shrink:0;overflow:hidden;background:#2a3038;
  display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;
  color:var(--t2);position:relative}
.ci .av img{width:100%;height:100%;object-fit:cover}
.ci .av .on{position:absolute;bottom:0;right:0;width:11px;height:11px;border-radius:50%;
  background:var(--ok);border:2px solid var(--panel)}
.ci .bd{flex:1;min-width:0}
.ci .rw{display:flex;justify-content:space-between;gap:6px}
.ci .ttl{font-weight:500;font-size:13.5px;white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis;display:flex;align-items:center;gap:4px}
.ci .tm{font-size:11px;color:var(--t3)}
.ci .pv{font-size:12.5px;color:var(--t2);overflow:hidden;text-overflow:ellipsis;
  white-space:nowrap;margin-top:1px}
.ci .bdg{position:absolute;top:50%;transform:translateY(-50%);right:10px;background:var(--ac);
  color:#fff;font-size:11px;font-weight:600;padding:2px 6px;border-radius:10px;display:none}
.ci.unread .bdg{display:block}
#main{display:flex;flex-direction:column;position:relative;overflow:hidden;background:var(--bg)}
#main::before{content:'';position:absolute;inset:0;background-image:
  radial-gradient(circle at 15% 25%,rgba(59,130,246,.06) 0%,transparent 35%),
  radial-gradient(circle at 85% 15%,rgba(124,92,255,.05) 0%,transparent 30%),
  radial-gradient(circle at 75% 75%,rgba(59,130,246,.04) 0%,transparent 40%);
  pointer-events:none;z-index:0}
#main>*{position:relative;z-index:1}
#ch{padding:10px 16px;background:var(--panel);border-bottom:1px solid var(--bd);
  display:flex;align-items:center;gap:12px;flex-shrink:0;min-height:56px}
#ch .av{width:36px;height:36px;border-radius:50%;overflow:hidden;background:#2a3038;
  display:flex;align-items:center;justify-content:center;font-weight:600;font-size:13px;
  color:var(--t2);flex-shrink:0;cursor:pointer}
#ch .av img{width:100%;height:100%;object-fit:cover}
#ch .info{flex:1;min-width:0}
#ch .info .ttl{font-weight:500;font-size:14.5px;white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis;display:flex;align-items:center;gap:4px}
#ch .info .sb{font-size:12px;color:var(--t2)}
#ch .info .sb.typing{color:var(--ac)}
#msgs{flex:1;overflow-y:auto;padding:20px 8% 12px;display:flex;flex-direction:column;gap:2px}
.dsep{align-self:center;font-size:12px;color:var(--t3);background:var(--panel);
  padding:3px 10px;border-radius:10px;margin:12px 0 8px}
.bub{max-width:60%;padding:8px 12px 6px;border-radius:12px;background:var(--oth);
  border:1px solid var(--bd);font-size:14px;line-height:1.4;word-wrap:break-word;
  margin-bottom:2px;align-self:flex-start}
.bub .au{font-size:12px;font-weight:600;color:var(--ac);margin-bottom:2px}
.bub .tx{white-space:pre-wrap}
.bub .mt{display:flex;align-items:center;justify-content:flex-end;gap:4px;
  font-size:10.5px;color:var(--t3);margin-top:2px}
.bub .mt .chk{color:var(--ac);font-size:11px}
.bub.me{align-self:flex-end;background:var(--own);border-color:#333d4c}
.bub.me .au{display:none}
.bub img,.bub video{max-width:100%;max-height:340px;border-radius:8px;display:block;
  margin-top:4px;cursor:pointer}
.bub audio{margin-top:6px;width:240px;max-width:100%}
#ib{padding:10px 16px 16px;display:flex;gap:8px;align-items:flex-end;flex-shrink:0}
#ib .iw{flex:1;background:var(--panel);border:1px solid var(--bd);border-radius:10px;
  display:flex;align-items:center;padding:2px 2px 2px 4px}
#ib .iw:focus-within{border-color:#333d4c}
#ib input[type=text]{flex:1;background:transparent;border:none;outline:none;
  font-size:14px;padding:10px 8px}
#ib .snd{width:40px;height:40px;border-radius:8px;background:var(--ac);
  display:flex;align-items:center;justify-content:center;flex-shrink:0}
#ib .snd:hover{background:var(--ach)}
#ib .snd svg{width:18px;height:18px}
.rec{color:var(--dg)!important}
#empty{flex:1;display:flex;align-items:center;justify-content:center;flex-direction:column;
  color:var(--t3);gap:12px;font-size:13px}
#empty svg{width:56px;height:56px;opacity:.4}

/* MODALS */
.mo{position:fixed;inset:0;background:rgba(0,0,0,.7);display:none;align-items:center;
  justify-content:center;z-index:100;padding:20px}
.mo.visible{display:flex}
.md{background:var(--panel);border:1px solid var(--bd);border-radius:12px;
  width:100%;max-width:440px;padding:22px;max-height:85vh;overflow-y:auto}
.md h2{font-size:17px;font-weight:600;margin-bottom:16px}
.md label{display:block;font-size:12px;color:var(--t2);margin-bottom:6px;margin-top:14px}
.md input[type=text],.md input[type=email],.md input[type=tel],.md textarea{
  width:100%;padding:10px 12px;background:var(--p2);border:1px solid var(--bd);
  border-radius:8px;font-size:14px;outline:none;resize:vertical}
.md input:focus,.md textarea:focus{border-color:#333d4c}
.md textarea{min-height:70px}
.md .avp{width:80px;height:80px;border-radius:50%;overflow:hidden;margin:0 auto 10px;
  background:#2a3038;display:flex;align-items:center;justify-content:center;
  font-weight:600;font-size:26px;color:var(--t2);cursor:pointer;border:2px dashed var(--bd)}
.md .avp img{width:100%;height:100%;object-fit:cover}
.md .acts{display:flex;gap:8px;margin-top:18px}
.md .acts button{flex:1;padding:10px;border-radius:8px;font-weight:500;font-size:13.5px}
.md .acts .pr{background:var(--ac);color:#fff}
.md .acts .pr:hover{background:var(--ach)}
.md .acts .sc{background:var(--p2);border:1px solid var(--bd)}
.md .acts .sc:hover{background:var(--hv)}
.md .err{color:var(--dg);font-size:12.5px;margin-top:8px;min-height:14px}
.ur{display:flex;gap:10px;align-items:center;padding:8px 10px;border-radius:8px;cursor:pointer}
.ur:hover{background:var(--hv)}
.ur .av{width:36px;height:36px;border-radius:50%;background:#2a3038;display:flex;
  align-items:center;justify-content:center;font-weight:600;font-size:13px;
  color:var(--t2);overflow:hidden;flex-shrink:0}
.ur .av img{width:100%;height:100%;object-fit:cover}
.ur .inf .n{font-weight:500;font-size:13.5px;display:flex;align-items:center;gap:4px}
.ur .inf .b{font-size:12px;color:var(--t2);margin-top:1px}
.ur .st{width:8px;height:8px;border-radius:50%;background:#3a424c;flex-shrink:0}
.ur .st.on{background:var(--ok)}
.ur input[type=checkbox]{width:18px;height:18px;accent-color:var(--ac);cursor:pointer}
#storiesBar .add-story{width:52px;height:52px;border-radius:50%;background:var(--p2);
  display:flex;align-items:center;justify-content:center;color:var(--ac);font-size:24px;
  font-weight:300;flex-shrink:0}
/* Story viewer */
#storyView{position:fixed;inset:0;background:#000;z-index:500;display:none;flex-direction:column}
#storyView.active{display:flex}
#storyView .progress{display:flex;gap:2px;padding:8px 12px}
#storyView .progress .bar{flex:1;height:2px;background:rgba(255,255,255,.3);
  border-radius:1px;overflow:hidden}
#storyView .progress .bar .fill{height:100%;background:#fff;width:0;transition:width .1s linear}
#storyView .head{display:flex;align-items:center;gap:10px;padding:8px 14px;color:#fff}
#storyView .head .av{width:36px;height:36px;border-radius:50%;overflow:hidden;
  background:#2a3038;display:flex;align-items:center;justify-content:center;
  font-weight:600;font-size:13px}
#storyView .head .av img{width:100%;height:100%;object-fit:cover}
#storyView .head .nm{font-weight:500;font-size:14px}
#storyView .head .tm{font-size:12px;opacity:.7}
#storyView .head .cls{margin-left:auto;width:32px;height:32px;border-radius:50%;
  background:rgba(255,255,255,.15);color:#fff;display:flex;align-items:center;justify-content:center}
#storyView .content{flex:1;position:relative;display:flex;align-items:center;
  justify-content:center;overflow:hidden}
#storyView .content img,#storyView .content video{max-width:100%;max-height:100%;object-fit:contain}
#storyView .content .txt{font-size:28px;font-weight:600;color:#fff;padding:40px;
  text-align:center;line-height:1.3}
#storyView .navL,#storyView .navR{position:absolute;top:0;bottom:0;width:33%;z-index:2;cursor:pointer}
#storyView .navL{left:0}#storyView .navR{right:0}
#toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(20px);
  background:var(--panel);border:1px solid var(--bd);padding:10px 16px;border-radius:8px;
  font-size:13px;opacity:0;transition:.25s;pointer-events:none;z-index:400}
#toast.visible{opacity:1;transform:translateX(-50%) translateY(0)}
.sw{position:relative;width:40px;height:22px;background:#3a424c;border-radius:11px;
  cursor:pointer;flex-shrink:0}
.sw::after{content:'';position:absolute;top:2px;left:2px;width:18px;height:18px;
  background:#fff;border-radius:50%;transition:.2s}
.sw.on{background:var(--ac)}
.sw.on::after{transform:translateX(18px)}
.st-tabs{display:flex;background:var(--p2);padding:4px;gap:2px;margin:0 0 14px;
  border-radius:10px;overflow-x:auto}
.st-tabs button{flex:1;padding:8px;font-size:12.5px;font-weight:500;color:var(--t2);
  border-radius:7px;white-space:nowrap;min-width:70px}
.st-tabs button.active{background:var(--panel);color:var(--t)}
.st-sec{padding:12px 0;border-bottom:1px solid var(--bd)}
.st-sec:last-child{border-bottom:none}
.st-sec .ttl{font-size:12px;color:var(--t2);text-transform:uppercase;margin-bottom:8px;padding:0 4px}
.st-row{display:flex;align-items:center;justify-content:space-between;padding:9px 4px;gap:12px}
.st-row .lbl{flex:1}
.st-row .lbl .n{font-size:13.5px;font-weight:500}
.st-row .lbl .d{font-size:12px;color:var(--t2);margin-top:2px}
.st-row .val{font-size:13px;color:var(--t2)}
.rg{display:flex;gap:6px;flex-wrap:wrap}
.rg button{padding:6px 12px;background:var(--p2);border:1px solid var(--bd);
  border-radius:6px;font-size:12.5px;color:var(--t2)}
.rg button.active{background:var(--ac);border-color:var(--ac);color:#fff}
.prem{background:linear-gradient(135deg,#f5b942,#e08b26);border-radius:12px;
  padding:20px;color:#fff;text-align:center;margin-bottom:14px}
.prem h3{font-size:18px;margin-bottom:6px}
.prem p{font-size:13px;opacity:.9;margin-bottom:14px}
.pf{display:flex;gap:10px;align-items:flex-start;padding:9px 4px;border-bottom:1px solid var(--bd)}
.pf:last-child{border-bottom:none}
.pf .ic{color:var(--pm);font-size:18px;flex-shrink:0}
.pf .n{font-size:13.5px;font-weight:500}
.pf .d{font-size:12px;color:var(--t2);margin-top:2px}
.star{color:var(--pm);font-size:12px}
#backBtn{display:none}
@media(max-width:780px){
#app{grid-template-columns:1fr}
#sidebar{position:absolute;inset:0;z-index:5;transition:transform .2s}
#sidebar.hidden{transform:translateX(-100%)}
#main{position:absolute;inset:0;z-index:4}
#backBtn{display:flex}
.bub{max-width:80%}
}
</style>
</head>
<body>

<div id="auth">
  <h1>Holodyx Chat</h1>
  <div class="sub">Общайтесь, делитесь медиа, звоните</div>

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
    <div id="regProgress">
      <div class="step" data-step="1"></div>
      <div class="step" data-step="2"></div>
      <div class="step" data-step="3"></div>
      <div class="step" data-step="4"></div>
    </div>

    <!-- Шаг 1 -->
    <div class="step-block active" data-step="1">
      <label style="font-size:12px;color:var(--t2);display:block;margin-bottom:6px">Email</label>
      <input id="regEmail" type="email" placeholder="you@example.com" autocomplete="email">
      <div class="hint">Мы не отправляем письма. Email нужен только для входа.</div>
      <div class="step-nav">
        <button class="next" id="next1">Далее →</button>
      </div>
    </div>

    <!-- Шаг 2 -->
    <div class="step-block" data-step="2">
      <label style="font-size:12px;color:var(--t2);display:block;margin-bottom:6px">Ник в чате</label>
      <input id="regUser" type="text" placeholder="Ваш ник (3–20 символов)" autocomplete="username">
      <div class="hint">Это имя увидят другие. Изменить потом можно, но не сразу.</div>
      <div class="step-nav">
        <button class="back" id="back2">← Назад</button>
        <button class="next" id="next2">Далее →</button>
      </div>
    </div>

    <!-- Шаг 3 -->
    <div class="step-block" data-step="3">
      <label style="font-size:12px;color:var2;display:block;margin-bottom:6px">Пароль</label>
      <input id="regPass" type="password" placeholder="Минимум 6 символов" autocomplete="new-password">
      <div class="pw-strength"><div class="fill" id="pwFill"></div></div>
      <input id="regPass2" type="password" placeholder="Повторите пароль" autocomplete="new-password">
      <div class="hint">Пароль нигде не сохраняется в открытом виде.</div>
      <div class="step-nav">
        <button class="back" id="back3">← Назад</button>
        <button class="next" id="next3">Далее →</button>
      </div>
    </div>

    <!-- Шаг 4 -->
    <div class="step-block" data-step="4">
      <label style="font-size:12px;color:var(--t2);display:block;margin-bottom:6px">Настоящее имя</label>
      <input id="regRealName" type="text" placeholder="Иван Петров" autocomplete="name">
      <div class="hint">
        <b>Обязательно.</b> Укажите имя, с которого будет сделан донат на DonationAlerts —
        иначе Premium не будет выдан.
      </div>
      <div class="step-nav">
        <button class="back" id="back4">← Назад</button>
        <button class="next" id="finishReg">Создать аккаунт</button>
      </div>
    </div>
  </div>

  <div class="err" id="authErr"></div>
  <div class="ok" id="authOk"></div>
</div>

<div id="app">
  <aside id="sidebar">
    <div id="sh">
      <div class="av-sm" id="myAvatarSm">?</div>
      <div class="info">
        <div class="nm" id="myNameSm">—</div>
        <div class="sb" id="myStatus">онлайн</div>
      </div>
      <button class="ib" id="newChatBtn" title="Создать">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14M5 12h14"/></svg>
      </button>
      <button class="ib" id="settingsBtn" title="Настройки">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
      </button>
    </div>
    <div id="storiesBar"></div>
    <div id="search"><input id="searchInput" type="text" placeholder="Поиск по чатам..."></div>
    <div id="chatList"></div>
  </aside>

  <main id="main">
    <div id="empty">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      <p>Выберите чат или создайте новый</p>
    </div>
    <div id="ch" style="display:none">
      <button class="ib" id="backBtn"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M15 18l-6-6 6-6"/></svg></button>
      <div class="av" id="chAvatar"></div>
      <div class="info">
        <div class="ttl" id="chTitle">—</div>
        <div class="sb" id="chSub">—</div>
      </div>
      <button class="ib" id="chInfoBtn" title="Инфо" style="display:none">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>
      </button>
    </div>
    <div id="msgs" style="display:none"></div>
    <div id="ib" style="display:none">
      <div class="iw">
        <input type="file" id="fileInput" accept="image/*,video/*" style="display:none">
        <button class="ib" id="attachBtn" title="Фото или видео">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg>
        </button>
        <button class="ib" id="micBtn" title="Голосовое">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="2" width="6" height="12" rx="3"/><path d="M5 10a7 7 0 0 0 14 0M12 19v3"/></svg>
        </button>
        <input id="msgInput" type="text" placeholder="Сообщение..." maxlength="4000">
      </div>
      <button class="snd" id="sendBtn">
        <svg viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2"><path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4 20-7z"/></svg>
      </button>
    </div>
  </main>
</div>

<!-- Модалки и JS будут в частях 3-5 -->

# ========== КОНЕЦ ЧАСТИ 2 ==========<!-- ========== ЧАСТЬ 3/5 (продолжение HTML) ========== -->

<!-- NEW CHAT MODAL -->
<div class="mo" id="newModal"><div class="md">
  <h2>Создать</h2>
  <div style="display:flex;flex-direction:column;gap:8px">
    <button class="sc" id="newDM" style="padding:12px;background:var(--p2);border:1px solid var(--bd);border-radius:8px;text-align:left;font-size:14px;cursor:pointer;color:var(--t)">👤 Личный чат</button>
    <button class="sc" id="newGroup" style="padding:12px;background:var(--p2);border:1px solid var(--bd);border-radius:8px;text-align:left;font-size:14px;cursor:pointer;color:var(--t)">👥 Группа</button>
    <button class="sc" id="newChannel" style="padding:12px;background:var(--p2);border:1px solid var(--bd);border-radius:8px;text-align:left;font-size:14px;cursor:pointer;color:var(--t)">📢 Канал</button>
    <button class="sc" id="newStory" style="padding:12px;background:var(--p2);border:1px solid var(--bd);border-radius:8px;text-align:left;font-size:14px;cursor:pointer;color:var(--t)">📷 История (24ч)</button>
  </div>
  <div class="acts"><button class="sc" id="newCancel">Отмена</button></div>
</div></div>

<!-- CREATE GROUP MODAL -->
<div class="mo" id="createGroupModal"><div class="md">
  <h2 id="cgTitle">Новая группа</h2>
  <label>Название</label>
  <input type="text" id="cgName" maxlength="60" placeholder="Моя группа">
  <label>Описание</label>
  <textarea id="cgAbout" maxlength="200" placeholder="О чём это?"></textarea>
  <label>Участники (начните вводить ник)</label>
  <input type="text" id="cgSearch" placeholder="Поиск пользователей..." autocomplete="off">
  <div id="cgResults" style="max-height:200px;overflow-y:auto;margin-top:8px"></div>
  <div id="cgSelected" style="margin-top:10px;display:flex;flex-wrap:wrap;gap:6px"></div>
  <div class="err" id="cgErr"></div>
  <div class="acts"><button class="sc" id="cgCancel">Отмена</button><button class="pr" id="cgCreate">Создать</button></div>
</div></div>

<!-- SEARCH USERS MODAL -->
<div class="mo" id="searchModal"><div class="md">
  <h2>Найти человека</h2>
  <input type="text" id="userSearchInput" placeholder="Введите ник..." autocomplete="off">
  <div id="userSearchResults" style="margin-top:12px;max-height:400px;overflow-y:auto"></div>
  <div class="acts"><button class="sc" id="searchClose">Закрыть</button></div>
</div></div>

<!-- PEER MODAL -->
<div class="mo" id="peerModal"><div class="md">
  <h2>Профиль</h2>
  <div style="display:flex;gap:14px;align-items:center;padding:12px;background:var(--p2);border-radius:8px">
    <div class="av-sm" id="peerAvatar" style="width:60px;height:60px;font-size:20px"></div>
    <div>
      <div style="font-weight:600;font-size:15px" id="peerName">—</div>
      <div style="color:var(--t2);font-size:13px;margin-top:3px" id="peerBio">—</div>
      <div style="color:var(--t3);font-size:12px;margin-top:4px" id="peerSeen"></div>
    </div>
  </div>
  <div class="acts"><button class="sc" id="peerClose" style="flex:1">Закрыть</button></div>
</div></div>

<!-- GROUP INFO MODAL -->
<div class="mo" id="groupInfoModal"><div class="md">
  <h2 id="giName">Группа</h2>
  <div id="giAbout" style="color:var(--t2);font-size:13px;margin-bottom:14px"></div>
  <div style="font-size:12px;color:var(--t2);text-transform:uppercase;margin-bottom:8px">Участники (<span id="giCount">0</span>)</div>
  <div id="giMembers" style="max-height:300px;overflow-y:auto"></div>
  <div style="margin-top:14px">
    <label>Добавить участников</label>
    <input type="text" id="giSearch" placeholder="Ник..." autocomplete="off">
    <div id="giResults" style="margin-top:8px"></div>
  </div>
  <div class="acts">
    <button class="sc" id="giLeave" style="color:var(--dg)">Покинуть</button>
    <button class="sc" id="giClose" style="flex:1">Закрыть</button>
  </div>
</div></div>

<!-- SETTINGS MODAL -->
<div class="mo" id="settingsModal"><div class="md">
  <h2>Настройки</h2>
  <input type="file" id="avatarInput" accept="image/*" style="display:none">
  <div class="st-tabs">
    <button data-t="acc" class="active">Аккаунт</button>
    <button data-t="chat">Чаты</button>
    <button data-t="priv">Приватность</button>
    <button data-t="notif">Уведомления</button>
    <button data-t="prem">Premium</button>
    <button data-t="admin" id="adminTab" style="display:none">👑 Админ</button>
  </div>

  <!-- АККАУНТ -->
  <div class="stab" data-t="acc">
    <div class="avp" id="avatarPreview">?</div>
    <label>Ник в чате</label>
    <input type="text" id="setUsername" maxlength="20">
    <label>Настоящее имя (изменить нельзя)</label>
    <input type="text" id="setRealName" disabled style="opacity:.6">
    <div class="hint" style="margin-top:-4px;margin-bottom:10px">
      Это имя сверяется с донатом. Если не совпадает — Premium не выдадут.
    </div>
    <label>О себе</label>
    <textarea id="setBio" maxlength="300" placeholder="Пара слов о себе"></textarea>
    <label>Телефон</label>
    <input type="tel" id="setPhone" maxlength="30" placeholder="+7 ...">
    <label>Email</label>
    <input type="email" id="setEmail" disabled style="opacity:.6">
    <div class="err" id="settingsErr"></div>
    <div class="acts">
      <button class="sc" id="logoutBtn" style="color:var(--dg)">Выйти</button>
      <button class="pr" id="settingsSave">Сохранить</button>
    </div>
  </div>

  <!-- ЧАТЫ -->
  <div class="stab" data-t="chat" style="display:none">
    <div class="st-sec"><div class="ttl">Тема</div>
      <div class="rg" id="themeGroup">
        <button data-v="dark" class="active">Тёмная</button>
        <button data-v="light">Светлая</button>
      </div>
    </div>
    <div class="st-sec"><div class="ttl">Анимации</div>
      <div class="st-row">
        <div class="lbl"><div class="n">Плавные переходы</div><div class="d">Отключите, если тормозит</div></div>
        <div class="sw on" data-k="animations"></div>
      </div>
    </div>
    <div class="st-sec"><div class="ttl">Обои чата</div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">
        <div class="wp" data-v="default" style="aspect-ratio:1;border-radius:8px;cursor:pointer;border:2px solid var(--ac);background:linear-gradient(135deg,#0d0f12,#1a1e24)"></div>
        <div class="wp" data-v="dark" style="aspect-ratio:1;border-radius:8px;cursor:pointer;border:2px solid transparent;background:#0a0a0a"></div>
        <div class="wp" data-v="blue" style="aspect-ratio:1;border-radius:8px;cursor:pointer;border:2px solid transparent;background:linear-gradient(135deg,#1e3a5f,#0d1b2a)"></div>
        <div class="wp" data-v="purple" style="aspect-ratio:1;border-radius:8px;cursor:pointer;border:2px solid transparent;background:linear-gradient(135deg,#3b2352,#1a1024)"></div>
        <div class="wp" data-v="green" style="aspect-ratio:1;border-radius:8px;cursor:pointer;border:2px solid transparent;background:linear-gradient(135deg,#1e3a2f,#0d1f18)"></div>
        <div class="wp" data-v="orange" style="aspect-ratio:1;border-radius:8px;cursor:pointer;border:2px solid transparent;background:linear-gradient(135deg,#3a2818,#1f1208)"></div>
      </div>
    </div>
  </div>

  <!-- ПРИВАТНОСТЬ -->
  <div class="stab" data-t="priv" style="display:none">
    <div class="st-sec"><div class="ttl">Кто видит время захода</div>
      <div class="rg" id="privLastSeen">
        <button data-v="all" class="active">Все</button>
        <button data-v="nobody">Никто</button>
      </div>
    </div>
    <div class="st-sec"><div class="ttl">Кто может писать</div>
      <div class="rg" id="privMessages">
        <button data-v="all" class="active">Все</button>
        <button data-v="nobody">Никто</button>
      </div>
    </div>
    <div class="st-sec">
      <div class="st-row"><div class="lbl"><div class="n">Скрыть телефон</div></div><div class="sw" data-k="hidePhone"></div></div>
      <div class="st-row"><div class="lbl"><div class="n">Скрыть аватар</div></div><div class="sw" data-k="hideAvatar"></div></div>
    </div>
  </div>

  <!-- УВЕДОМЛЕНИЯ -->
  <div class="stab" data-t="notif" style="display:none">
    <div class="st-sec">
      <div class="st-row"><div class="lbl"><div class="n">Звук сообщений</div></div><div class="sw on" data-k="soundMessages"></div></div>
      <div class="st-row"><div class="lbl"><div class="n">Звук звонков</div></div><div class="sw on" data-k="soundCalls"></div></div>
      <div class="st-row"><div class="lbl"><div class="n">Уведомления</div></div><div class="sw on" data-k="notifications"></div></div>
    </div>
  </div>

  <!-- PREMIUM -->
  <div class="stab" data-t="prem" style="display:none">
    <div class="prem">
      <h3>⭐ Holodyx Premium</h3>
      <p>Поддержите проект — получите Premium навсегда</p>
      <a href="#" id="donateLink" target="_blank"
         style="display:block;background:#fff;color:#b8730f;padding:12px;border-radius:8px;font-weight:600;text-decoration:none;text-align:center">
        💛 Поддержать проект
      </a>
      <div style="margin-top:12px;text-align:left">
        <label style="display:block;font-size:12px;color:rgba(255,255,255,.85);margin-bottom:6px">
          Имя, с которого сделан донат <b>(= ваш ник в чате)</b>
        </label>
        <input id="donateNameInput" type="text" placeholder="Ваш ник в чате"
               style="width:100%;padding:10px 12px;border-radius:8px;border:1px solid rgba(255,255,255,.3);background:rgba(0,0,0,.2);color:#fff;font-size:14px;outline:none">
        <div id="donateNameHint" style="font-size:11px;color:rgba(255,255,255,.75);margin-top:6px;line-height:1.4">
          Premium выдаётся только если имя в донате совпадает с ником в чате.
        </div>
      </div>
      <button id="iDonatedBtn" style="margin-top:10px;background:transparent;color:#fff;border:1px solid rgba(255,255,255,.5);padding:10px 16px;border-radius:8px;font-size:13px;cursor:pointer;width:100%">
        Я поддержал → отправить заявку
      </button>
      <div id="premRequestStatus" style="margin-top:12px;font-size:12px;color:rgba(255,255,255,.9)"></div>
    </div>
    <div class="pf"><div class="ic">⭐</div><div><div class="n">Значок Premium</div><div class="d">Звёздочка рядом с ником</div></div></div>
    <div class="pf"><div class="ic">🎨</div><div><div class="n">Эксклюзивные обои</div><div class="d">Специальные темы</div></div></div>
    <div class="pf"><div class="ic">📁</div><div><div class="n">Файлы до 500 МБ</div><div class="d">Вместо 50 МБ</div></div></div>
    <div class="pf"><div class="ic">⚡</div><div><div class="n">Приоритет</div><div class="d">Быстрее загрузка</div></div></div>
  </div>

  <!-- АДМИН -->
  <div class="stab" data-t="admin" style="display:none">
    <div class="st-sec">
      <div class="ttl">Статистика</div>
      <div id="adminStats" style="display:grid;grid-template-columns:repeat(2,1fr);gap:8px"></div>
    </div>
    <div class="st-sec">
      <div class="ttl">Заявки на Premium</div>
      <div id="adminRequests" style="max-height:400px;overflow-y:auto"></div>
    </div>
  </div>

  <div class="acts"><button class="sc" id="settingsCancel" style="flex:1">Закрыть</button></div>
</div></div>

<!-- CREATE STORY MODAL -->
<div class="mo" id="storyModal"><div class="md">
  <h2>Новая история</h2>
  <label>Тип</label>
  <div class="rg" id="stType">
    <button data-v="image" class="active">📷 Фото</button>
    <button data-v="video">🎬 Видео</button>
    <button data-v="text">📝 Текст</button>
  </div>
  <label>Текст (необязательно)</label>
  <input type="text" id="stText" maxlength="200" placeholder="Подпись...">
  <div id="stFileWrap" style="margin-top:12px">
    <input type="file" id="stFile" accept="image/*" style="display:none">
    <button class="sc" id="stPickFile" style="width:100%;padding:10px;background:var(--p2);border:1px dashed var(--bd);border-radius:8px;color:var(--t);cursor:pointer">Выбрать файл</button>
    <div id="stFileName" style="font-size:12px;color:var(--t2);margin-top:6px"></div>
  </div>
  <div class="err" id="stErr"></div>
  <div class="acts"><button class="sc" id="stCancel">Отмена</button><button class="pr" id="stCreate">Опубликовать</button></div>
</div></div>

<!-- STORY VIEWER -->
<div id="storyView">
  <div class="progress" id="storyProgress"></div>
  <div class="head">
    <div class="av" id="svAvatar"></div>
    <div><div class="nm" id="svName">—</div><div class="tm" id="svTime">—</div></div>
    <button class="cls" id="svClose">✕</button>
  </div>
  <div class="content" id="svContent"></div>
  <div class="navL" id="svPrev"></div>
  <div class="navR" id="svNext"></div>
</div>

<div id="toast"></div>

<script>
// ========== ЧАСТЬ 4 начнётся здесь ==========
</script>
</body>
</html>
"""

# ========== КОНЕЦ ЧАСТИ 3 ==========// ========== ЧАСТЬ 4/5 (JavaScript) ==========

const $ = id => document.getElementById(id);
const socket = io({ autoConnect: false, transports: ['websocket','polling'] });
let me = null;
const chats = {};
let activeKey = null;
const onlineUsers = new Map();
const peerCache = {};
let settings = {
  theme:'dark', animations:true, wallpaper:'default',
  privLastSeen:'all', privMessages:'all', hidePhone:false, hideAvatar:false,
  soundMessages:true, soundCalls:true, notifications:true
};

/* ============ AUTH ============ */
$('tabLogin').onclick = () => switchTab('login');
$('tabRegister').onclick = () => switchTab('register');
function switchTab(t){
  const l = t==='login';
  $('tabLogin').classList.toggle('active', l);
  $('tabRegister').classList.toggle('active', !l);
  $('loginForm').style.display = l?'':'none';
  $('registerForm').style.display = l?'none':'';
  $('authErr').textContent=''; $('authOk').textContent='';
  if (!l) gotoStep(1);
}
$('loginBtn').onclick = doLogin;
$('loginInput').addEventListener('keydown', e => e.key==='Enter' && doLogin());
$('loginPass').addEventListener('keydown', e => e.key==='Enter' && doLogin());

async function doLogin(){
  const login = $('loginInput').value.trim();
  const password = $('loginPass').value;
  if (!login || !password){ $('authErr').textContent='Заполните поля'; return; }
  $('loginBtn').disabled = true;
  try {
    const r = await fetch('/api/login',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({login,password})}).then(r=>r.json());
    if (!r.ok){ $('authErr').textContent = r.error||'Ошибка'; return; }
    me = r.user; enterApp();
  } catch(e){ $('authErr').textContent='Ошибка сети'; }
  finally { $('loginBtn').disabled = false; }
}

/* ============ REGISTRATION STEPS ============ */
let regStep = 1;
const regData = { email:'', username:'', password:'', real_name:'' };

function gotoStep(n){
  regStep = n;
  document.querySelectorAll('.step-block').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.step) === n);
  });
  document.querySelectorAll('#regProgress .step').forEach(s => {
    s.classList.toggle('done', parseInt(s.dataset.step) <= n);
  });
  $('authErr').textContent=''; $('authOk').textContent='';
}

$('next1').onclick = async () => {
  const email = $('regEmail').value.trim();
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
    $('authErr').textContent = 'Некорректный email'; return;
  }
  // проверка занятости email
  const r = await fetch(`/api/check-email?email=${encodeURIComponent(email)}`).then(r=>r.json());
  if (r.ok && !r.free) { $('authErr').textContent = 'Email уже занят'; return; }
  regData.email = email;
  gotoStep(2);
};

$('next2').onclick = async () => {
  const u = $('regUser').value.trim();
  if (!/^[A-Za-zА-Яа-я0-9_]{3,20}$/.test(u)) {
    $('authErr').textContent = 'Ник: 3–20 символов (буквы, цифры, _)'; return;
  }
  const r = await fetch(`/api/check-username?username=${encodeURIComponent(u)}`).then(r=>r.json());
  if (r.ok && !r.free) { $('authErr').textContent = 'Ник уже занят'; return; }
  regData.username = u;
  gotoStep(3);
};

$('regPass').addEventListener('input', () => {
  const p = $('regPass').value;
  const f = $('pwFill');
  f.className = 'fill';
  if (p.length === 0) return;
  if (p.length < 6) f.classList.add('weak');
  else if (p.length < 10) f.classList.add('medium');
  else f.classList.add('strong');
});

$('next3').onclick = () => {
  const p1 = $('regPass').value, p2 = $('regPass2').value;
  if (p1.length < 6) { $('authErr').textContent = 'Пароль минимум 6 символов'; return; }
  if (p1 !== p2) { $('authErr').textContent = 'Пароли не совпадают'; return; }
  regData.password = p1;
  gotoStep(4);
};

$('finishReg').onclick = async () => {
  const rn = $('regRealName').value.trim();
  if (!/^[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё\s\-]{1,38}$/.test(rn)) {
    $('authErr').textContent = 'Настоящее имя: 2–40 букв, пробел или дефис'; return;
  }
  regData.real_name = rn;
  $('finishReg').disabled = true;
  $('finishReg').textContent = 'Создаём...';
  try {
    const r = await fetch('/api/register',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(regData)}).then(r=>r.json());
    if (!r.ok){ $('authErr').textContent = r.error||'Ошибка'; return; }
    $('authOk').textContent = 'Аккаунт создан!';
    $('loginInput').value = regData.email;
    $('loginPass').value = regData.password;
    await doLogin();
  } catch(e){ $('authErr').textContent = 'Ошибка сети'; }
  finally {
    $('finishReg').disabled = false;
    $('finishReg').textContent = 'Создать аккаунт';
  }
};

$('back2').onclick = () => gotoStep(1);
$('back3').onclick = () => gotoStep(2);
$('back4').onclick = () => gotoStep(3);

/* ============ SETTINGS APPLY ============ */
function applySettings(){
  document.body.classList.toggle('light', settings.theme==='light');
  document.body.classList.toggle('no-anim', !settings.animations);
  const wps = {
    default:'', dark:'#0a0a0a',
    blue:'linear-gradient(135deg,#1e3a5f,#0d1b2a)',
    purple:'linear-gradient(135deg,#3b2352,#1a1024)',
    green:'linear-gradient(135deg,#1e3a2f,#0d1f18)',
    orange:'linear-gradient(135deg,#3a2818,#1f1208)'
  };
  const main = $('main');
  if (settings.wallpaper && settings.wallpaper!=='default'){
    main.style.background = wps[settings.wallpaper] || '';
    document.body.classList.add('wallpaper');
  } else {
    main.style.background = '';
    document.body.classList.remove('wallpaper');
  }
}
function saveSettings(){
  try { localStorage.setItem('holodyx_settings', JSON.stringify(settings)); } catch(e){}
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify(settings)}).catch(()=>{});
}

/* ============ ENTER APP ============ */
function enterApp(){
  $('auth').style.display='none';
  $('app').classList.add('active');
  const av = $('myAvatarSm');
  av.innerHTML = me.avatar ? `<img src="${me.avatar}">` : initials(me.username);
  $('myNameSm').textContent = me.username + (me.premium ? ' ⭐' : '');
  chats['general'] = {key:'general',type:'general',title:'Общий чат',messages:[],unread:0};
  try {
    const s = JSON.parse(localStorage.getItem('holodyx_settings')||'null');
    if (s) Object.assign(settings, s);
  } catch(e){}
  applySettings();
  if (me.is_admin) $('adminTab').style.display = '';
  if (!socket.connected) socket.connect();
  startVersionCheck();
}

/* ============ SOCKET ============ */
socket.on('need_auth', () => socket.disconnect());
socket.on('connect_error', e => console.warn('Socket:', e.message));

socket.on('joined', data => {
  me = data.user;
  chats['general'].messages = data.history||[];
  data.online.forEach(u => { onlineUsers.set(u.id, u); peerCache[u.username.toLowerCase()] = u; });
  (data.groups||[]).forEach(g => {
    const key = 'group:'+g.id;
    chats[key] = {key,type:g.type,title:g.name,gid:g.id,about:g.about||'',owner_id:g.owner_id,role:g.role,messages:[],unread:0};
  });
  if (data.settings) { Object.assign(settings, data.settings); applySettings(); }
  loadSavedDialogs();
  loadStories();
  renderSidebar();
  saveDialogs();
  if (me.is_admin) loadAdminPanel();
});

socket.on('online', data => {
  onlineUsers.clear();
  data.users.forEach(u => { onlineUsers.set(u.id, u); peerCache[u.username.toLowerCase()] = u; });
  renderSidebar();
  if (activeKey && activeKey!=='general') updateHeaderSub();
});

socket.on('message', msg => {
  const key = msg.room;
  let chat = chats[key];
  if (!chat){
    if (msg.room.startsWith('dm:')){
      const parts = msg.room.slice(3).split('|');
      const pn = parts.find(p => p !== me.username.toLowerCase());
      const peer = peerCache[pn]||{username:pn};
      chat = ensureDM(peer.username, peer);
      saveDialogs();
    } else if (msg.room.startsWith('group:')){
      const gid = parseInt(msg.room.slice(6));
      chat = chats[msg.room] = {key:msg.room,type:'group',title:'Группа #'+gid,gid,messages:[],unread:0};
    } else return;
  }
  chat.messages.push(msg);
  const isActive = key===activeKey;
  const fromMe = msg.sender_id===me.id;
  if (!isActive && !fromMe && msg.type!=='system') chat.unread = (chat.unread||0)+1;
  if (isActive){
    renderMessages(); scrollBottom();
    if (!fromMe && chat.messages.length){
      socket.emit('read',{room:msg.room,last_id:chat.messages[chat.messages.length-1].id});
    }
  }
  renderSidebar();
  if (settings.soundMessages && !fromMe) beep();
});

socket.on('typing', data => {
  const key = data.to.startsWith('group:')||data.to==='general' ? data.to : dmKey(data.from, me.username);
  const chat = chats[key]; if (!chat) return;
  chat.typing = data.is_typing;
  if (activeKey===key) updateHeaderSub();
});
socket.on('read', data => {
  const chat = chats[data.room]; if (chat) chat.reads = data.reads;
  if (activeKey===data.room) renderMessages();
});
socket.on('dm_history', data => {
  const key = data.room;
  let chat = chats[key];
  if (!chat){
    const peer = peerCache[data.peer.toLowerCase()]||{username:data.peer};
    chat = ensureDM(data.peer, peer);
  }
  chat.messages = data.history||[];
  if (activeKey===key){ renderMessages(); scrollBottom(); }
});
socket.on('group_history', data => {
  const key = data.room;
  let chat = chats[key];
  if (!chat){
    chat = chats[key] = {key, type:data.group.type, title:data.group.name, gid:data.group.id, messages:[], unread:0};
  }
  chat.messages = data.history||[];
  chat.members = data.members||[];
  chat.about = data.group.about;
  chat.owner_id = data.group.owner_id;
  if (activeKey===key){ renderMessages(); scrollBottom(); updateHeaderSub(); renderGroupInfo(); }
  renderSidebar();
});
socket.on('group_added', data => {
  const g = data.group;
  const key = 'group:'+g.id;
  if (!chats[key]){
    chats[key] = {key,type:g.type,title:g.name,gid:g.id,about:g.about,owner_id:g.owner_id,messages:[],unread:0};
  }
  renderSidebar();
  toast(`Добавлены в ${g.type==='channel'?'канал':'группу'}: ${g.name}`);
});
socket.on('join_group_room', data => socket.emit('join_group_room',{gid:data.gid}));
socket.on('story_created', () => loadStories());
socket.on('error_msg', data => toast(data.msg));
socket.on('premium_request_new', data => {
  if (me.is_admin) {
    toast(`🔔 Новая заявка от ${data.username}`);
    loadAdminPanel();
  }
});
socket.on('premium_granted', data => {
  me.premium = 1;
  toast(data.message || 'Вам выдан Premium!');
  setTimeout(() => location.reload(), 1500);
});

/* ============ HELPERS ============ */
function initials(n){ if (!n) return '?'; const p = n.trim().split(/\s+/);
  return (p.length===1?p[0].slice(0,2):p[0][0]+p[1][0]).toUpperCase(); }
function parseT(iso){ if (!iso) return null; let s=String(iso);
  if (!s.includes('T')) s=s.replace(' ','T')+'Z';
  else if (!s.endsWith('Z')&&!s.includes('+')) s+='Z';
  const d=new Date(s); return isNaN(d)?null:d; }
function fmtT(iso){ const d=parseT(iso);
  return d?d.toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'}):''; }
function fmtD(iso){ const d=parseT(iso); if (!d) return '';
  const t=new Date(), y=new Date(); y.setDate(t.getDate()-1);
  const s=(a,b)=>a.toDateString()===b.toDateString();
  if (s(d,t)) return 'Сегодня'; if (s(d,y)) return 'Вчера';
  return d.toLocaleDateString('ru-RU',{day:'numeric',month:'long'}); }
function fmtSeen(iso){ const d=parseT(iso); if (!d) return '';
  const diff=(Date.now()-d)/1000;
  if (diff<60) return 'был(а) только что';
  if (diff<3600) return `был(а) ${Math.floor(diff/60)} мин назад`;
  if (diff<86400) return `был(а) ${Math.floor(diff/3600)} ч назад`;
  return 'был(а) '+d.toLocaleDateString('ru-RU'); }
function dmKey(a,b){ return 'dm:'+[a.toLowerCase(),b.toLowerCase()].sort().join('|'); }
function ensureDM(pn, peer){
  const key = dmKey(me.username, pn);
  if (!chats[key]) chats[key] = {key,type:'dm',title:pn,peer:pn,avatar:peer.avatar||null,messages:[],unread:0};
  else if (peer.avatar) chats[key].avatar = peer.avatar;
  return chats[key];
}
function prev(m){ if (m.type==='image') return '📷 Фото'; if (m.type==='video') return '🎬 Видео';
  if (m.type==='audio') return '🎤 Голосовое'; return m.text||''; }
function esc(s){ return String(s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function avHtml(u){ if (u.avatar) return `<img src="${u.avatar}">`; return initials(u.username||u); }
function saveDialogs(){ try { localStorage.setItem('holodyx_dialogs',
  JSON.stringify(Object.values(chats).filter(c=>c.type==='dm').map(c=>c.peer))); } catch(e){} }
function loadSavedDialogs(){ try {
  const l=JSON.parse(localStorage.getItem('holodyx_dialogs')||'[]');
  l.forEach(p=>{ if (!p) return;
    const u=peerCache[p.toLowerCase()]||{username:p};
    ensureDM(p,u); }); } catch(e){} }

function beep(){
  try {
    const ctx = new (window.AudioContext||window.webkitAudioContext)();
    const o = ctx.createOscillator(); const g = ctx.createGain();
    o.connect(g); g.connect(ctx.destination);
    o.frequency.value = 880; o.type = 'sine';
    g.gain.setValueAtTime(0.0001, ctx.currentTime);
    g.gain.exponentialRampToValueAtTime(0.15, ctx.currentTime + 0.01);
    g.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.15);
    o.start(); o.stop(ctx.currentTime + 0.16);
  } catch(e){}
}

/* ============ RENDER SIDEBAR ============ */
function renderSidebar(){
  const q = $('searchInput').value.trim().toLowerCase();
  const entries = Object.values(chats).filter(c => !q || c.title.toLowerCase().includes(q)).sort((a,b)=>{
    if (a.type==='general') return -1;
    if (b.type==='general') return 1;
    const la = a.messages.length?a.messages[a.messages.length-1].id:0;
    const lb = b.messages.length?b.messages[b.messages.length-1].id:0;
    return lb-la;
  });
  $('chatList').innerHTML = '';
  entries.forEach(c => {
    const el = document.createElement('div');
    el.className = 'ci'+(c.key===activeKey?' active':'')+(c.unread?' unread':'');
    const last = c.messages[c.messages.length-1];
    let pv = 'Нет сообщений';
    if (last){
      if (last.type==='system') pv = last.text;
      else pv = (last.sender_id===me.id?'Вы: ':(c.type!=='dm'?last.sender_name+': ':''))+prev(last);
    }
    const pu = c.type==='dm'?{username:c.peer,avatar:c.avatar}:{username:c.title,avatar:null};
    const isOn = c.type==='dm'&&[...onlineUsers.values()].some(u=>u.username.toLowerCase()===(c.peer||'').toLowerCase());
    const icon = c.type==='general'?'💬':c.type==='channel'?'📢':c.type==='group'?'👥':avHtml(pu);
    el.innerHTML = `
      <div class="av">${icon}${isOn?'<div class="on"></div>':''}</div>
      <div class="bd">
        <div class="rw"><div class="ttl">${esc(c.title)}</div><div class="tm">${last?fmtT(last.time):''}</div></div>
        <div class="pv">${esc(pv)}</div>
      </div>
      <div class="bdg">${c.unread>99?'99+':c.unread}</div>`;
    el.onclick = () => openChat(c.key);
    $('chatList').appendChild(el);
  });
}

/* ============ OPEN CHAT ============ */
function openChat(key){
  const c = chats[key]; if (!c) return;
  activeKey = key; c.unread=0; c.typing=false;
  $('empty').style.display='none';
  $('ch').style.display='flex';
  $('msgs').style.display='flex';
  $('ib').style.display='flex';
  const pu = c.type==='dm'?{username:c.peer,avatar:c.avatar}:{username:c.title,avatar:null};
  const icon = c.type==='general'?'💬':c.type==='channel'?'📢':c.type==='group'?'👥':avHtml(pu);
  $('chAvatar').innerHTML = icon;
  $('chTitle').textContent = c.title;
  $('chInfoBtn').style.display = (c.type==='dm'||c.type==='group'||c.type==='channel')?'flex':'none';
  updateHeaderSub();
  $('sidebar').classList.add('hidden');
  renderMessages(); scrollBottom(); renderSidebar();
  if (c.type==='dm') socket.emit('open_dm',{peer:c.peer});
  else if (c.type==='group'||c.type==='channel') socket.emit('open_group',{gid:c.gid});
  else { const lid=c.messages.length?c.messages[c.messages.length-1].id:0;
    if (lid) socket.emit('read',{room:'general',last_id:lid}); }
  $('msgInput').focus();
  const isChannel = c.type==='channel';
  const canWrite = !isChannel || c.role==='owner' || c.role==='admin';
  $('msgInput').disabled = !canWrite;
  $('msgInput').placeholder = isChannel && !canWrite ? 'Только владелец может писать' : 'Сообщение...';
  $('sendBtn').style.display = canWrite?'flex':'none';
}
$('backBtn').onclick = () => {
  $('sidebar').classList.remove('hidden');
  activeKey = null;
  $('empty').style.display='flex';
  $('ch').style.display='none';
  $('msgs').style.display='none';
  $('ib').style.display='none';
  renderSidebar();
};

function updateHeaderSub(){
  const c = chats[activeKey]; if (!c) return;
  const sb = $('chSub');
  sb.classList.remove('typing');
  if (c.typing){ sb.textContent='печатает...'; sb.classList.add('typing'); return; }
  if (c.type==='general') sb.textContent = onlineUsers.size+' онлайн';
  else if (c.type==='group') sb.textContent = (c.members?c.members.length:'?')+' участников';
  else if (c.type==='channel') sb.textContent = (c.members?c.members.length:'?')+' подписчиков';
  else {
    const on = [...onlineUsers.values()].some(u=>u.username.toLowerCase()===(c.peer||'').toLowerCase());
    const p = peerCache[(c.peer||'').toLowerCase()];
    sb.textContent = on?'онлайн':(p?fmtSeen(p.last_seen):'не в сети');
  }
}

function renderMessages(){
  const c = chats[activeKey]; if (!c) return;
  $('msgs').innerHTML = '';
  let lastDate = '';
  c.messages.forEach(m => {
    const d = fmtD(m.time);
    if (d && d!==lastDate){ const s=document.createElement('div');
      s.className='dsep'; s.textContent=d; $('msgs').appendChild(s); lastDate=d; }
    if (m.type==='system'){ const s=document.createElement('div');
      s.className='dsep'; s.textContent=m.text; $('msgs').appendChild(s); return; }
    const mine = m.sender_id===me.id;
    const el = document.createElement('div');
    el.className = 'bub '+(mine?'me':'');
    const showAuthor = (c.type!=='dm') && !mine;
    let body = '';
    if (m.type==='image') body = `<img src="${m.media_url}" onclick="window.open('${m.media_url}','_blank')">`;
    else if (m.type==='video') body = `<video src="${m.media_url}" controls preload="metadata"></video>`;
    else if (m.type==='audio') body = `<audio src="${m.media_url}" controls preload="metadata"></audio>`;
    else body = `<div class="tx">${esc(m.text)}</div>`;
    let chk = '';
    if (mine && c.type==='dm'){
      const rb = c.reads||{};
      const others = Object.entries(rb).filter(([uid])=>Number(uid)!==me.id);
      chk = others.some(([_,lid])=>Number(lid)>=m.id)?'✓✓':'✓';
    } else if (mine) { chk = '✓'; }
    el.innerHTML = `${showAuthor?`<div class="au">${esc(m.sender_name)}</div>`:''}${body}
      <div class="mt"><span>${fmtT(m.time)}</span>${mine?`<span class="chk">${chk}</span>`:''}</div>`;
    $('msgs').appendChild(el);
  });
}
function scrollBottom(){ requestAnimationFrame(()=>{ const w=$('msgs'); w.scrollTop=w.scrollHeight; }); }
function currentRoom(){ const c=chats[activeKey]; if (!c) return null;
  if (c.type==='general') return 'general';
  if (c.type==='group'||c.type==='channel') return 'group:'+c.gid;
  return dmKey(me.username, c.peer); }

/* ============ SEND ============ */
function send(){
  const text = $('msgInput').value.trim();
  if (!text || !activeKey) return;
  const c = chats[activeKey];
  const to = c.type==='general'?'general':(c.type==='group'||c.type==='channel')?'group:'+c.gid:c.peer;
  socket.emit('send',{text,to});
  $('msgInput').value='';
  socket.emit('typing',{is_typing:false,to});
}
$('sendBtn').onclick = send;
$('msgInput').addEventListener('keydown', e => { if (e.key==='Enter'){ e.preventDefault(); send(); } });
let typingT = null;
$('msgInput').addEventListener('input', () => {
  if (!activeKey) return;
  const c = chats[activeKey];
  const to = c.type==='general'?'general':(c.type==='group'||c.type==='channel')?'group:'+c.gid:c.peer;
  socket.emit('typing',{is_typing:true,to});
  clearTimeout(typingT);
  typingT = setTimeout(()=>socket.emit('typing',{is_typing:false,to}),1200);
});

/* ============ MEDIA ============ */
$('attachBtn').onclick = () => $('fileInput').click();
$('fileInput').onchange = async e => {
  const f = e.target.files[0]; if (!f) return;
  e.target.value='';
  if (!activeKey) return;
  const kind = f.type.startsWith('video/')?'video':'image';
  await uploadFile(f, kind, currentRoom());
};
async function uploadFile(file, kind, room){
  const MAX_MB = 50;
  if (file.size > MAX_MB*1024*1024){ toast(`Файл больше ${MAX_MB} МБ`); return; }
  const ext = (file.name.split('.').pop()||'').toLowerCase();
  const ok = {image:['jpg','jpeg','png','gif','webp'], video:['mp4','webm','mov'], audio:['webm','ogg','mp3','wav','m4a']};
  if (ok[kind] && !ok[kind].includes(ext)) { toast(`Формат .${ext} не поддерживается`); return; }
  const btn = $('attachBtn');
  btn.style.opacity = '0.5';
  try {
    const fd = new FormData();
    fd.append('file', file); fd.append('kind', kind); fd.append('room', room);
    const ctrl = new AbortController();
    const tm = setTimeout(()=>ctrl.abort(), 90000);
    const r = await fetch('/api/upload',{method:'POST',body:fd,signal:ctrl.signal}).then(r=>r.json());
    clearTimeout(tm);
    if (!r.ok) toast(r.error||'Не удалось загрузить');
  } catch(e){
    toast(e.name==='AbortError'?'Превышено время':'Ошибка сети: '+e.message);
  } finally { btn.style.opacity = '1'; }
}

/* ============ VOICE ============ */
let mr=null, chunks=[], recStart=0, recT=null;
$('micBtn').onclick = async () => {
  if (mr && mr.state==='recording'){ mr.stop(); return; }
  try {
    const stream = await navigator.mediaDevices.getUserMedia({audio:true});
    chunks = [];
    const mime = getAudioMime();
    mr = mime ? new MediaRecorder(stream,{mimeType:mime}) : new MediaRecorder(stream);
    recStart = Date.now();
    mr.ondataavailable = e => { if (e.data.size) chunks.push(e.data); };
    mr.onstop = async () => {
      clearInterval(recT);
      $('micBtn').classList.remove('rec');
      $('msgInput').placeholder='Сообщение...';
      const dur = Math.round((Date.now()-recStart)/1000);
      stream.getTracks().forEach(t=>t.stop());
      if (dur<1){ toast('Слишком коротко'); return; }
      const type = mr.mimeType||'audio/webm';
      const blob = new Blob(chunks,{type});
      const ext = type.includes('ogg')?'ogg':'webm';
      const f = new File([blob],`voice-${Date.now()}.${ext}`,{type});
      const room = currentRoom();
      if (room) await uploadFile(f,'audio',room);
    };
    mr.start();
    $('micBtn').classList.add('rec');
    let sec=0;
    $('msgInput').placeholder='⏺ 0:00';
    recT = setInterval(()=>{ sec++; $('msgInput').placeholder=`⏺ ${Math.floor(sec/60)}:${String(sec%60).padStart(2,'0')}`; },1000);
  } catch(e){ toast('Нет доступа к микрофону'); }
};
function getAudioMime(){
  const l=['audio/webm;codecs=opus','audio/webm','audio/ogg;codecs=opus'];
  for (const m of l) if (window.MediaRecorder && MediaRecorder.isTypeSupported(m)) return m;
  return '';
}

/* ============ STORIES ============ */
async function loadStories(){
  try {
    const r = await fetch('/api/stories/feed').then(r=>r.json());
    if (!r.ok) return;
    renderStories(r.my, r.others||[]);
  } catch(e){}
}
function renderStories(my, others){
  const bar = $('storiesBar');
  bar.innerHTML = '';
  const addBtn = document.createElement('div');
  addBtn.className = 'story-circle';
  addBtn.innerHTML = `<div class="ring plus" style="background:var(--p2)"><div style="background:var(--p2);color:var(--ac);font-size:24px;font-weight:300">+</div></div><div class="lbl">Моя</div>`;
  addBtn.onclick = () => $('storyModal').classList.add('visible');
  bar.appendChild(addBtn);
  const all = [];
  if (my) all.push(my);
  others.forEach(o => all.push(o));
  all.forEach(u => {
    const el = document.createElement('div');
    el.className = 'story-circle'+(u.has_unviewed?'':' viewed');
    el.innerHTML = `
      <div class="ring"><div>${u.avatar?`<img src="${u.avatar}">`:initials(u.username)}</div></div>
      <div class="lbl">${u.is_me?'Вы':esc(u.username)}</div>`;
    el.onclick = () => openStory(u, 0);
    bar.appendChild(el);
  });
}
let storyState = { user:null, idx:0, timer:null };
function openStory(user, idx){
  storyState = {user, idx, timer:null};
  $('storyView').classList.add('active');
  $('svAvatar').innerHTML = user.avatar?`<img src="${user.avatar}">`:initials(user.username);
  $('svName').textContent = user.is_me?'Ваша история':user.username;
  renderStory();
}
function renderStory(){
  const u = storyState.user; if (!u) return;
  const s = u.stories[storyState.idx]; if (!s) return;
  $('svTime').textContent = fmtSeen(s.created_at);
  const c = $('svContent'); c.innerHTML = '';
  if (s.type==='image'&&s.media_url){ const img=document.createElement('img'); img.src=s.media_url; c.appendChild(img); }
  else if (s.type==='video'&&s.media_url){ const v=document.createElement('video'); v.src=s.media_url; v.autoplay=true; v.playsInline=true; c.appendChild(v); }
  else { const t=document.createElement('div'); t.className='txt'; t.textContent=s.text||''; c.appendChild(t); }
  const pr = $('storyProgress'); pr.innerHTML='';
  u.stories.forEach((_,i)=>{
    const b=document.createElement('div'); b.className='bar';
    const f=document.createElement('div'); f.className='fill';
    if (i<storyState.idx) f.style.width='100%';
    b.appendChild(f); pr.appendChild(b);
  });
  if (!u.is_me) fetch(`/api/stories/${s.id}/view`,{method:'POST'}).catch(()=>{});
  clearTimeout(storyState.timer);
  const fill = pr.children[storyState.idx].firstChild;
  fill.style.width='0'; setTimeout(()=>fill.style.width='100%',50);
  storyState.timer = setTimeout(()=>{
    if (storyState.idx < u.stories.length-1){ storyState.idx++; renderStory(); }
    else closeStory();
  }, 5000);
}
function closeStory(){
  clearTimeout(storyState.timer);
  $('storyView').classList.remove('active');
  loadStories();
}
$('svClose').onclick = closeStory;
$('svPrev').onclick = () => { if (storyState.idx>0){ storyState.idx--; renderStory(); } };
$('svNext').onclick = () => { if (storyState.idx<storyState.user.stories.length-1){ storyState.idx++; renderStory(); } else closeStory(); };

/* ============ CREATE STORY ============ */
let stType='image', stFile=null;
$('stType').querySelectorAll('button').forEach(b=>{
  b.onclick = () => {
    $('stType').querySelectorAll('button').forEach(x=>x.classList.remove('active'));
    b.classList.add('active'); stType = b.dataset.v;
    $('stFileWrap').style.display = stType==='text'?'none':'block';
    $('stFile').accept = stType==='video'?'video/*':'image/*';
    $('stFileName').textContent=''; stFile = null;
  };
});
$('stPickFile').onclick = () => $('stFile').click();
$('stFile').onchange = e => { stFile = e.target.files[0]; $('stFileName').textContent = stFile?stFile.name:''; };
$('stCancel').onclick = () => $('storyModal').classList.remove('visible');
$('stCreate').onclick = async () => {
  $('stErr').textContent='';
  const fd = new FormData();
  fd.append('type', stType); fd.append('text', $('stText').value);
  if (stType!=='text'){
    if (!stFile){ $('stErr').textContent='Выберите файл'; return; }
    fd.append('file', stFile);
  }
  try {
    const r = await fetch('/api/stories',{method:'POST',body:fd}).then(r=>r.json());
    if (!r.ok){ $('stErr').textContent = r.error||'Ошибка'; return; }
    $('storyModal').classList.remove('visible');
    $('stText').value=''; stFile=null; $('stFileName').textContent='';
    loadStories(); toast('История опубликована');
  } catch(e){ $('stErr').textContent='Ошибка сети'; }
};

/* ============ SETTINGS ============ */
$('settingsBtn').onclick = () => {
  $('setUsername').value = me.username||'';
  $('setRealName').value = me.real_name||'';
  $('setBio').value = me.bio||'';
  $('setPhone').value = me.phone||'';
  $('setEmail').value = me.email||'';
  const prev = $('avatarPreview');
  prev.innerHTML = me.avatar?`<img src="${me.avatar}">`:initials(me.username);
  $('settingsErr').textContent='';
  applySettingsToUI();
  $('settingsModal').classList.add('visible');
  if (me.is_admin) loadAdminPanel();
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
  fd.append('phone', $('setPhone').value);
  const f = $('avatarInput').files[0];
  if (f) fd.append('avatar', f);
  try {
    const r = await fetch('/api/profile',{method:'POST',body:fd}).then(r=>r.json());
    if (!r.ok){ $('settingsErr').textContent = r.error||'Ошибка'; return; }
    me = r.user;
    const av = $('myAvatarSm');
    av.innerHTML = me.avatar?`<img src="${me.avatar}">`:initials(me.username);
    $('myNameSm').textContent = me.username + (me.premium?' ⭐':'');
    $('settingsModal').classList.remove('visible');
    saveSettings(); toast('Профиль обновлён');
  } catch(e){ $('settingsErr').textContent='Ошибка сети'; }
};
$('logoutBtn').onclick = async () => { await fetch('/api/logout',{method:'POST'}); location.reload(); };

document.querySelectorAll('.st-tabs button').forEach(b=>{
  b.onclick = () => {
    document.querySelectorAll('.st-tabs button').forEach(x=>x.classList.remove('active'));
    b.classList.add('active');
    document.querySelectorAll('.stab').forEach(s=>s.style.display = s.dataset.t===b.dataset.t?'':'none');
  };
});
document.querySelectorAll('.sw').forEach(s=>{
  s.onclick = () => { s.classList.toggle('on');
    settings[s.dataset.k] = s.classList.contains('on');
    applySettings(); saveSettings(); };
});
document.querySelectorAll('.rg button').forEach(b=>{
  b.onclick = () => {
    const grp = b.parentElement;
    grp.querySelectorAll('button').forEach(x=>x.classList.remove('active'));
    b.classList.add('active');
    if (grp.id==='themeGroup'){ settings.theme = b.dataset.v; applySettings(); }
    if (grp.id==='privLastSeen'){ settings.privLastSeen = b.dataset.v; }
    if (grp.id==='privMessages'){ settings.privMessages = b.dataset.v; }
    saveSettings();
  };
});
document.querySelectorAll('.wp').forEach(w=>{
  w.onclick = () => {
    document.querySelectorAll('.wp').forEach(x=>x.style.borderColor='transparent');
    w.style.borderColor = 'var(--ac)';
    settings.wallpaper = w.dataset.v;
    applySettings(); saveSettings();
  };
});
function applySettingsToUI(){
  document.querySelectorAll('.rg button').forEach(b=>{
    const grp = b.parentElement;
    if (grp.id==='themeGroup') b.classList.toggle('active', b.dataset.v===settings.theme);
    if (grp.id==='privLastSeen') b.classList.toggle('active', b.dataset.v===settings.privLastSeen);
    if (grp.id==='privMessages') b.classList.toggle('active', b.dataset.v===settings.privMessages);
  });
  document.querySelectorAll('.sw').forEach(s=>{ s.classList.toggle('on', !!settings[s.dataset.k]); });
  document.querySelectorAll('.wp').forEach(w=>{
    w.style.borderColor = w.dataset.v===settings.wallpaper ? 'var(--ac)' : 'transparent';
  });
}

/* ============ PREMIUM ============ */
async function loadPremiumStatus(){
  const statusEl = $('premRequestStatus');
  if (!statusEl) return;
  if (me.premium){
    statusEl.innerHTML = '✅ <b>Premium активен</b>';
    $('iDonatedBtn').style.display='none';
    return;
  }
  try {
    const r = await fetch('/api/premium/my-request').then(r=>r.json());
    if (r.ok && r.request){
      const s = r.request.status;
      if (s==='pending'){
        statusEl.innerHTML = '⏳ Заявка отправлена. Админ проверит в течение недели.';
        $('iDonatedBtn').style.display='none';
      } else if (s==='rejected'){
        statusEl.innerHTML = '❌ Заявка отклонена. Можно отправить новую.';
      }
    }
  } catch(e){}
}
document.addEventListener('click', e => {
  if (e.target.closest('.st-tabs button[data-t="prem"]')) loadPremiumStatus();
});
$('iDonatedBtn').addEventListener('click', async () => {
  const donateName = ($('donateNameInput').value||'').trim();
  const hint = $('donateNameHint');
  if (!donateName){ hint.textContent='⚠ Укажите имя, с которого сделан донат'; hint.style.color='#fecaca'; return; }
  if (donateName.toLowerCase() !== me.username.toLowerCase()){
    hint.textContent = `⚠ «${donateName}» не совпадает с ником «${me.username}»`;
    hint.style.color='#fecaca'; return;
  }
  hint.textContent = 'Premium выдаётся только если имя в донате совпадает с ником в чате.';
  hint.style.color='rgba(255,255,255,.75)';
  const msg = prompt('Комментарий для админа (необязательно):','')||'';
  const r = await fetch('/api/premium/request',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({message:msg, donate_name:donateName})}).then(r=>r.json());
  if (!r.ok){ toast(r.error||'Ошибка'); return; }
  toast('Заявка отправлена!');
  loadPremiumStatus();
});

/* ============ ADMIN ============ */
async function loadAdminPanel(){
  if (!me.is_admin) return;
  const st = await fetch('/api/admin/stats').then(r=>r.json());
  if (st.ok){
    $('adminStats').innerHTML = `
      <div style="padding:10px;background:var(--p2);border-radius:8px">
        <div style="font-size:11px;color:var(--t2)">Пользователей</div>
        <div style="font-size:20px;font-weight:600">${st.stats.users}</div>
      </div>
      <div style="padding:10px;background:var(--p2);border-radius:8px">
        <div style="font-size:11px;color:var(--t2)">Premium</div>
        <div style="font-size:20px;font-weight:600">${st.stats.premium}</div>
      </div>
      <div style="padding:10px;background:var(--p2);border-radius:8px">
        <div style="font-size:11px;color:var(--t2)">Онлайн</div>
        <div style="font-size:20px;font-weight:600">${st.stats.online||0}</div>
      </div>
      <div style="padding:10px;background:var(--p2);border-radius:8px">
        <div style="font-size:11px;color:var(--t2)">Заявок ждёт</div>
        <div style="font-size:20px;font-weight:600;color:#f5b942">${st.stats.pending_requests}</div>
      </div>`;
  }
  const rq = await fetch('/api/admin/premium-requests?status=pending').then(r=>r.json());
  if (rq.ok){
    const box = $('adminRequests');
    if (!rq.requests.length){
      box.innerHTML = '<div style="color:var(--t3);text-align:center;padding:20px;font-size:13px">Нет заявок</div>';
      return;
    }
    box.innerHTML = rq.requests.map(r => `
      <div style="padding:12px;border:1px solid var(--bd);border-radius:8px;margin-bottom:8px">
        <div style="display:flex;gap:10px;align-items:center;margin-bottom:8px">
          <div class="av-sm" style="width:36px;height:36px;font-size:13px">${r.avatar?`<img src="${r.avatar}">`:initials(r.username)}</div>
          <div style="flex:1">
            <div style="font-weight:600;font-size:13.5px">${esc(r.username)}</div>
            <div style="font-size:11.5px;color:var(--t3)">${r.real_name?`Настоящее имя: ${esc(r.real_name)}`:''}</div>
          </div>
        </div>
        <div style="font-size:12px;color:#f5b942;margin-bottom:6px">
          💰 Имя в донате: <b>${esc(r.donate_name||'—')}</b>
        </div>
        ${r.message?`<div style="font-size:12.5px;color:var(--t2);padding:8px;background:var(--p2);border-radius:6px;margin-bottom:8px">${esc(r.message)}</div>`:''}
        <div style="display:flex;gap:6px">
          <button onclick="approvePremium(${r.id})" style="flex:1;padding:8px;background:var(--ok);color:#fff;border:none;border-radius:6px;font-size:12.5px;cursor:pointer">✅ Выдать Premium</button>
          <button onclick="rejectPremium(${r.id})" style="flex:1;padding:8px;background:var(--dg);color:#fff;border:none;border-radius:6px;font-size:12.5px;cursor:pointer">❌ Отклонить</button>
        </div>
      </div>`).join('');
  }
}
window.approvePremium = async rid => {
  const r = await fetch(`/api/admin/premium-requests/${rid}/approve`,{method:'POST'}).then(r=>r.json());
  if (r.ok){ toast('Premium выдан'); loadAdminPanel(); } else toast(r.error||'Ошибка');
};
window.rejectPremium = async rid => {
  if (!confirm('Отклонить заявку?')) return;
  const r = await fetch(`/api/admin/premium-requests/${rid}/reject`,{method:'POST'}).then(r=>r.json());
  if (r.ok){ toast('Отклонено'); loadAdminPanel(); }
};

/* ============ NEW CHAT ============ */
$('newChatBtn').onclick = () => $('newModal').classList.add('visible');
$('newCancel').onclick = () => $('newModal').classList.remove('visible');
$('newDM').onclick = () => { $('newModal').classList.remove('visible');
  $('searchModal').classList.add('visible'); setTimeout(()=>$('userSearchInput').focus(),100); };
$('newGroup').onclick = () => { $('newModal').classList.remove('visible'); openCreateGroup('group'); };
$('newChannel').onclick = () => { $('newModal').classList.remove('visible'); openCreateGroup('channel'); };
$('newStory').onclick = () => { $('newModal').classList.remove('visible'); $('storyModal').classList.add('visible'); };

let cgType='group', cgSelected=[];
function openCreateGroup(type){
  cgType = type; cgSelected=[];
  $('cgTitle').textContent = type==='channel'?'Новый канал':'Новая группа';
  $('cgName').value=''; $('cgAbout').value=''; $('cgSearch').value='';
  $('cgResults').innerHTML=''; $('cgSelected').innerHTML=''; $('cgErr').textContent='';
  $('createGroupModal').classList.add('visible');
}
$('cgCancel').onclick = () => $('createGroupModal').classList.remove('visible');
$('cgSearch').oninput = async () => {
  const q = $('cgSearch').value.trim();
  if (!q){ $('cgResults').innerHTML=''; return; }
  clearTimeout(window.cgT);
  window.cgT = setTimeout(async ()=>{
    const r = await fetch(`/api/users/search?q=${encodeURIComponent(q)}`).then(r=>r.json());
    if (!r.ok) return;
    const box = $('cgResults'); box.innerHTML='';
    r.users.forEach(u=>{
      if (cgSelected.includes(u.username)) return;
      const el = document.createElement('div');
      el.className='ur';
      el.innerHTML = `<div class="av">${u.avatar?`<img src="${u.avatar}">`:initials(u.username)}</div>
        <div class="inf" style="flex:1"><div class="n">${esc(u.username)}</div></div>`;
      el.onclick = () => { cgSelected.push(u.username); renderSelected();
        box.innerHTML=''; $('cgSearch').value=''; };
      box.appendChild(el);
    });
  }, 250);
};
function renderSelected(){
  $('cgSelected').innerHTML = cgSelected.map(u=>
    `<span style="background:var(--p2);padding:4px 10px;border-radius:12px;font-size:12px">${esc(u)}</span>`).join('');
}
$('cgCreate').onclick = async () => {
  const name = $('cgName').value.trim();
  if (!name){ $('cgErr').textContent='Введите название'; return; }
  const r = await fetch('/api/groups',{method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({name, type:cgType, about:$('cgAbout').value, members:cgSelected})}).then(r=>r.json());
  if (!r.ok){ $('cgErr').textContent = r.error||'Ошибка'; return; }
  $('createGroupModal').classList.remove('visible');
  const g = r.group;
  const key = 'group:'+g.id;
  chats[key] = {key,type:g.type,title:g.name,gid:g.id,about:g.about,owner_id:g.owner_id,role:'owner',messages:[],unread:0};
  socket.emit('join_group_room',{gid:g.id});
  socket.emit('group_created',{gid:g.id});
  renderSidebar(); openChat(key);
  toast(cgType==='channel'?'Канал создан':'Группа создана');
};

/* ============ SEARCH USERS ============ */
$('searchClose').onclick = () => $('searchModal').classList.remove('visible');
let sT = null;
$('userSearchInput').oninput = () => { clearTimeout(sT); sT = setTimeout(searchUsers, 250); };
async function searchUsers(){
  const q = $('userSearchInput').value.trim();
  if (!q){ $('userSearchResults').innerHTML=''; return; }
  const r = await fetch(`/api/users/search?q=${encodeURIComponent(q)}`).then(r=>r.json());
  if (!r.ok) return;
  const box = $('userSearchResults'); box.innerHTML='';
  if (!r.users.length){ box.innerHTML='<div style="color:var(--t3);text-align:center;padding:20px">Не найдено</div>'; return; }
  r.users.forEach(u=>{
    const on = [...onlineUsers.values()].some(x=>x.id===u.id);
    const el = document.createElement('div');
    el.className='ur';
    el.innerHTML = `<div class="av">${u.avatar?`<img src="${u.avatar}">`:initials(u.username)}</div>
      <div class="inf" style="flex:1"><div class="n">${esc(u.username)}${u.premium?' <span class="star">⭐</span>':''}</div>
      <div class="b">${esc(u.bio||'Без описания')}</div></div>
      <div class="st ${on?'on':''}"></div>`;
    el.onclick = () => {
      peerCache[u.username.toLowerCase()] = u;
      ensureDM(u.username, u);
      $('searchModal').classList.remove('visible');
      openChat(dmKey(me.username, u.username));
    };
    box.appendChild(el);
  });
}

/* ============ INFO ============ */
$('chInfoBtn').onclick = () => {
  const c = chats[activeKey]; if (!c) return;
  if (c.type==='dm'){
    const u = peerCache[c.peer.toLowerCase()]||{username:c.peer,avatar:c.avatar};
    $('peerAvatar').innerHTML = u.avatar?`<img src="${u.avatar}">`:initials(u.username);
    $('peerName').textContent = u.username+(u.premium?' ⭐':'');
    $('peerBio').textContent = u.bio||'Без описания';
    $('peerSeen').textContent = u.last_seen?fmtSeen(u.last_seen):'';
    $('peerModal').classList.add('visible');
  } else {
    $('giName').textContent = c.title;
    $('giAbout').textContent = c.about||'Без описания';
    socket.emit('open_group',{gid:c.gid});
    $('groupInfoModal').classList.add('visible');
  }
};
function renderGroupInfo(){
  const c = chats[activeKey]; if (!c) return;
  $('giCount').textContent = (c.members||[]).length;
  $('giMembers').innerHTML = (c.members||[]).map(m=>`
    <div class="ur"><div class="av">${m.avatar?`<img src="${m.avatar}">`:initials(m.username)}</div>
    <div class="inf" style="flex:1"><div class="n">${esc(m.username)}${m.premium?' <span class="star">⭐</span>':''}</div>
    <div class="b">${m.role==='owner'?'Владелец':m.role==='admin'?'Админ':'Участник'}</div></div></div>`).join('');
}
$('giClose').onclick = () => $('groupInfoModal').classList.remove('visible');
$('giLeave').onclick = async () => {
  const c = chats[activeKey]; if (!c||!c.gid) return;
  if (!confirm('Покинуть?')) return;
  await fetch(`/api/groups/${c.gid}/leave`,{method:'POST'});
  delete chats[activeKey];
  activeKey=null;
  $('groupInfoModal').classList.remove('visible');
  $('empty').style.display='flex';
  $('ch').style.display='none'; $('msgs').style.display='none'; $('ib').style.display='none';
  $('sidebar').classList.remove('hidden');
  renderSidebar();
};
let giT=null;
$('giSearch').oninput = () => {
  clearTimeout(giT);
  giT = setTimeout(async ()=>{
    const q = $('giSearch').value.trim();
    if (!q){ $('giResults').innerHTML=''; return; }
    const c = chats[activeKey];
    const r = await fetch(`/api/users/search?q=${encodeURIComponent(q)}`).then(r=>r.json());
    if (!r.ok) return;
    const box = $('giResults'); box.innerHTML='';
    r.users.forEach(u=>{
      if (c.members&&c.members.some(m=>m.id===u.id)) return;
      const el = document.createElement('div');
      el.className='ur';
      el.innerHTML = `<div class="av">${u.avatar?`<img src="${u.avatar}">`:initials(u.username)}</div>
        <div class="inf" style="flex:1"><div class="n">${esc(u.username)}</div></div>`;
      el.onclick = async () => {
        await fetch(`/api/groups/${c.gid}/members`,{method:'POST',headers:{'Content-Type':'application/json'},
          body: JSON.stringify({usernames:[u.username]})});
        toast('Добавлен: '+u.username);
        $('giResults').innerHTML=''; $('giSearch').value='';
        socket.emit('open_group',{gid:c.gid});
      };
      box.appendChild(el);
    });
  }, 250);
};
$('peerClose').onclick = () => $('peerModal').classList.remove('visible');

/* ============ SEARCH + TOAST ============ */
$('searchInput').oninput = renderSidebar;
function toast(t){
  const el = $('toast');
  el.textContent = t;
  el.classList.add('visible');
  clearTimeout(toast._t);
  toast._t = setTimeout(()=>el.classList.remove('visible'),2200);
}
document.addEventListener('keydown', e => {
  if (e.key==='Escape'){
    document.querySelectorAll('.mo.visible').forEach(m=>m.classList.remove('visible'));
    closeStory();
  }
});

/* ============ AUTO VERSION CHECK ============ */
let currentVersion = null;
async function startVersionCheck(){
  try {
    const r = await fetch('/api/version').then(r=>r.json());
    if (r.ok) currentVersion = r.version;
  } catch(e){}
  setInterval(async () => {
    try {
      const r = await fetch('/api/version').then(r=>r.json());
      if (r.ok && currentVersion && r.version !== currentVersion){
        toast('🔄 Доступно обновление. Обновить страницу?');
      }
    } catch(e){}
  }, 30000);
}

/* ============ DONATE LINK ============ */
fetch('/api/version').then(r=>r.json()).then(r=>{
  if (r.ok && r.donate_url) $('donateLink').href = r.donate_url;
}).catch(()=>{});# ========== ЧАСТЬ 5/5 ==========
# ============================================================
# HTTP ROUTES
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


@app.route('/api/version')
def api_version():
    return jsonify(ok=True, version=APP_VERSION, donate_url=DONATE_URL)


@app.route('/chat_uploads/avatars/<path:fn>')
def serve_avatar(fn):
    return send_from_directory(AV, fn)


@app.route('/chat_uploads/media/<path:fn>')
def serve_media(fn):
    return send_from_directory(MD, fn)


@app.route('/chat_uploads/stories/<path:fn>')
def serve_story(fn):
    return send_from_directory(ST, fn)


@app.errorhandler(413)
def too_large(e):
    return jsonify(ok=False, error='Файл слишком большой (макс. 50 МБ)'), 413


# ---------- CHECKS ----------
@app.route('/api/check-email')
def api_check_email():
    e = (request.args.get('email') or '').strip().lower()
    return jsonify(ok=True, free=(u_email(e) is None))


@app.route('/api/check-username')
def api_check_username():
    n = (request.args.get('username') or '').strip()
    return jsonify(ok=True, free=(u_name(n) is None))


# ---------- AUTH ----------
@app.route('/api/register', methods=['POST'])
def api_register():
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get('email') or '').strip().lower()
        username = (data.get('username') or '').strip()
        password = data.get('password') or ''
        real_name = (data.get('real_name') or '').strip()

        if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
            return jsonify(ok=False, error='Некорректный email'), 400
        if not re.match(r'^[A-Za-zА-Яа-я0-9_]{3,20}$', username):
            return jsonify(ok=False, error='Ник: 3–20 символов'), 400
        if len(password) < 6:
            return jsonify(ok=False, error='Пароль минимум 6'), 400
        if not re.match(r'^[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё\s\-]{1,38}$', real_name):
            return jsonify(ok=False, error='Настоящее имя: 2–40 букв'), 400
        if u_email(email):
            return jsonify(ok=False, error='Email занят'), 400
        if u_name(username):
            return jsonify(ok=False, error='Ник занят'), 400

        uid = create_user(email, username, password, real_name)
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
    user = u_email(login) if '@' in login else None
    if not user:
        user = u_name(login)
    if not user or not check_password_hash(user['password_hash'], password):
        return jsonify(ok=False, error='Неверный логин или пароль'), 401
    session['uid'] = user['id']
    session.permanent = True
    upd_seen(user['id'])
    return jsonify(ok=True, user=user_public(user))


@app.route('/api/me')
def api_me():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    u = u_id(uid)
    if not u:
        session.clear()
        return jsonify(ok=False), 401
    return jsonify(ok=True, user=user_public(u))


@app.route('/api/logout', methods=['POST'])
def api_logout():
    session.clear()
    return jsonify(ok=True)


# ---------- USERS ----------
@app.route('/api/users/search')
def api_users_search():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    q = (request.args.get('q') or '').strip()
    if len(q) < 1:
        return jsonify(ok=True, users=[])
    return jsonify(ok=True, users=search_u(q, uid, 20))


# ---------- SETTINGS ----------
@app.route('/api/settings', methods=['GET'])
def api_get_settings():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    return jsonify(ok=True, settings=get_settings(uid))


@app.route('/api/settings', methods=['POST'])
def api_save_settings():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    save_settings(uid, request.get_json(silent=True) or {})
    return jsonify(ok=True)


# ---------- PROFILE ----------
@app.route('/api/profile', methods=['POST'])
def api_profile():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False, error='Не авторизован'), 401
    u = u_id(uid)
    if not u:
        return jsonify(ok=False), 404

    username = request.form.get('username')
    bio = request.form.get('bio')
    phone = request.form.get('phone')
    upd = {}

    if username and username != u['username']:
        if not re.match(r'^[A-Za-zА-Яа-я0-9_]{3,20}$', username):
            return jsonify(ok=False, error='Некорректный ник'), 400
        if u_name(username):
            return jsonify(ok=False, error='Ник занят'), 400
        upd['username'] = username
    if bio is not None:
        upd['bio'] = bio.strip()[:300]
    if phone is not None:
        upd['phone'] = phone.strip()[:30]

    file = request.files.get('avatar')
    if file and file.filename:
        if ext_of(file.filename) not in ALLOWED_AV:
            return jsonify(ok=False, error='Формат не поддерживается'), 400
        fn = f"{uuid.uuid4().hex}.{ext_of(file.filename)}"
        file.save(os.path.join(AV, fn))
        upd['avatar'] = f"/chat_uploads/avatars/{fn}"

    if upd:
        upd_prof(uid, **upd)
    nu = u_id(uid)
    return jsonify(ok=True, user=user_public(nu))


# ---------- UPLOAD ----------
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
        if ext not in ALLOWED_IMG:
            return jsonify(ok=False, error='Формат изображения'), 400
        mt = 'image'
    elif kind == 'video':
        if ext not in ALLOWED_VID:
            return jsonify(ok=False, error='Формат видео'), 400
        mt = 'video'
    elif kind == 'audio':
        if ext not in ALLOWED_AUD:
            return jsonify(ok=False, error='Формат аудио'), 400
        mt = 'audio'
    else:
        return jsonify(ok=False, error='Неизвестный тип'), 400

    fn = f"{uuid.uuid4().hex}.{ext}"
    file.save(os.path.join(MD, fn))
    url = f"/chat_uploads/media/{fn}"
    u = u_id(uid)
    msg = add_msg(room=room, sid=u['id'], sname=u['username'],
                  typ=mt, text='', murl=url, mname=file.filename)
    payload = ser(msg)

    if room == 'general':
        socketio.emit('message', payload, to='general', include_self=False)
        socketio.emit('message', payload, to=request.sid)
    elif room.startswith('group:'):
        socketio.emit('message', payload, to=room, include_self=False)
        socketio.emit('message', payload, to=request.sid)
    else:
        socketio.emit('message', payload, to=request.sid)
        for name in room[3:].split('|'):
            sid = name_to_sid.get(name)
            if sid and sid != request.sid:
                socketio.emit('message', payload, to=sid)

    return jsonify(ok=True, message=payload)


# ---------- GROUPS ----------
@app.route('/api/groups', methods=['POST'])
def api_create_group():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()[:60]
    type_ = data.get('type', 'group')
    about = (data.get('about') or '').strip()[:200]
    members = data.get('members') or []

    if not name:
        return jsonify(ok=False, error='Введите название'), 400
    if type_ not in ('group', 'channel'):
        return jsonify(ok=False, error='Неверный тип'), 400

    gid = create_group(name, type_, uid, about)
    for un in members:
        u = u_name(un)
        if u and u['id'] != uid:
            add_mem(gid, u['id'])
    return jsonify(ok=True, group=get_group(gid))


@app.route('/api/groups', methods=['GET'])
def api_list_groups():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    return jsonify(ok=True, groups=user_groups(uid))


@app.route('/api/groups/<int:gid>')
def api_get_group(gid):
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    if not is_mem(gid, uid):
        return jsonify(ok=False, error='Нет доступа'), 403
    g = get_group(gid)
    if not g:
        return jsonify(ok=False), 404
    return jsonify(ok=True, group=g, members=group_members(gid))


@app.route('/api/groups/<int:gid>/members', methods=['POST'])
def api_add_members(gid):
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    if not is_mem(gid, uid):
        return jsonify(ok=False), 403
    data = request.get_json(silent=True) or {}
    added = []
    for un in data.get('usernames') or []:
        u = u_name(un)
        if u:
            add_mem(gid, u['id'])
            added.append(u['username'])
    if added:
        msg = add_msg(g_room(gid), None, 'system', 'system',
                      text=f"Добавлены: {', '.join(added)}")
        socketio.emit('message', ser(msg), to=g_room(gid))
    return jsonify(ok=True, added=added)


@app.route('/api/groups/<int:gid>/leave', methods=['POST'])
def api_leave_group(gid):
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    u = u_id(uid)
    rm_mem(gid, uid)
    msg = add_msg(g_room(gid), None, 'system', 'system',
                  text=f"{u['username']} покинул чат")
    socketio.emit('message', ser(msg), to=g_room(gid))
    return jsonify(ok=True)


# ---------- STORIES ----------
@app.route('/api/stories/feed')
def api_stories_feed():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    feed = active_stories(ex=uid)
    mine = my_stories(uid)
    my = None
    if mine:
        u = u_id(uid)
        my = {'user_id': uid, 'username': u['username'], 'avatar': u['avatar'],
              'stories': [{'id': s['id'], 'type': s['type'], 'media_url': s['media_url'],
                            'text': s['text'], 'bg_color': s['bg_color'],
                            'created_at': s['created_at']} for s in mine],
              'has_unviewed': False, 'is_me': True}
    feed = [f for f in feed if f['user_id'] != uid]
    return jsonify(ok=True, my=my, others=feed)


@app.route('/api/stories', methods=['POST'])
def api_create_story():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    stype = request.form.get('type', 'image')
    text = (request.form.get('text') or '').strip()[:200]
    bg_color = request.form.get('bg_color', '#3b82f6')
    if stype not in ('image', 'video', 'text'):
        return jsonify(ok=False, error='Неверный тип'), 400
    murl = None
    file = request.files.get('file')
    if file and file.filename:
        ext = ext_of(file.filename)
        if stype == 'image' and ext not in ALLOWED_IMG:
            return jsonify(ok=False, error='Формат'), 400
        if stype == 'video' and ext not in ALLOWED_VID:
            return jsonify(ok=False, error='Формат'), 400
        fn = f"{uuid.uuid4().hex}.{ext}"
        file.save(os.path.join(ST, fn))
        murl = f"/chat_uploads/stories/{fn}"
    sid = add_story(uid, stype, murl, text, bg_color)
    socketio.emit('story_created', {'user_id': uid}, to='general')
    return jsonify(ok=True, story_id=sid)


@app.route('/api/stories/<int:sid>/view', methods=['POST'])
def api_view_story(sid):
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    view_story(sid, uid)
    return jsonify(ok=True)


# ---------- PREMIUM ----------
@app.route('/api/premium/my-request')
def api_my_premium_request():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    return jsonify(ok=True, request=my_request(uid))


@app.route('/api/premium/request', methods=['POST'])
def api_premium_request():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    u = u_id(uid)
    if u.get('premium'):
        return jsonify(ok=False, error='У вас уже есть Premium'), 400
    data = request.get_json(silent=True) or {}
    msg = (data.get('message') or '').strip()[:500]
    donate_name = (data.get('donate_name') or '').strip()[:50]
    rid, err = create_premium_request(uid, msg, donate_name)
    if err:
        return jsonify(ok=False, error=err), 400
    socketio.emit('premium_request_new',
                  {'user_id': uid, 'username': u['username'], 'donate_name': donate_name},
                  to='admins')
    return jsonify(ok=True, request_id=rid)


# ---------- ADMIN ----------
@app.route('/api/admin/stats')
def api_admin_stats():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    u = u_id(uid)
    if not u.get('is_admin'):
        return jsonify(ok=False, error='Нет доступа'), 403
    s = admin_stats()
    s['online'] = len(online)
    return jsonify(ok=True, stats=s)


@app.route('/api/admin/premium-requests')
def api_admin_requests():
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    u = u_id(uid)
    if not u.get('is_admin'):
        return jsonify(ok=False, error='Нет доступа'), 403
    status = request.args.get('status') or 'pending'
    return jsonify(ok=True, requests=list_premium_requests(status))


@app.route('/api/admin/premium-requests/<int:rid>/approve', methods=['POST'])
def api_admin_approve(rid):
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    u = u_id(uid)
    if not u.get('is_admin'):
        return jsonify(ok=False, error='Нет доступа'), 403
    target = approve_request(rid, uid)
    if not target:
        return jsonify(ok=False, error='Заявка не найдена'), 404
    tu = u_id(target)
    sid = name_to_sid.get(tu['username'].lower()) if tu else None
    if sid:
        socketio.emit('premium_granted',
                      {'message': 'Вам выдан Premium навсегда!'}, to=sid)
    return jsonify(ok=True)


@app.route('/api/admin/premium-requests/<int:rid>/reject', methods=['POST'])
def api_admin_reject(rid):
    uid = session.get('uid')
    if not uid:
        return jsonify(ok=False), 401
    u = u_id(uid)
    if not u.get('is_admin'):
        return jsonify(ok=False, error='Нет доступа'), 403
    reject_request(rid, uid)
    return jsonify(ok=True)


# ============================================================
# SOCKET.IO
# ============================================================
@socketio.on('connect')
def on_connect():
    uid = session.get('uid')
    if not uid:
        emit('need_auth'); return
    u = u_id(uid)
    if not u:
        emit('need_auth'); return
    upd_seen(uid)
    online[request.sid] = {
        'id': u['id'], 'username': u['username'], 'avatar': u['avatar'],
        'bio': u['bio'], 'premium': u.get('premium', 0),
    }
    name_to_sid[u['username'].lower()] = request.sid
    join_room('general')
    join_room(f"user:{u['id']}")
    if u.get('is_admin'):
        join_room('admins')
    for gid in user_gids(u['id']):
        join_room(g_room(gid))

    emit('joined', {
        'user': user_public(u),
        'history': hist('general', 200),
        'online': online_list(),
        'groups': user_groups(u['id']),
        'settings': get_settings(u['id']),
    })
    sys_msg = add_msg('general', None, u['username'], 'system',
                      text=f"{u['username']} присоединился к чату")
    emit('message', ser(sys_msg), to='general')
    bcast_online()


@socketio.on('open_dm')
def on_open_dm(data):
    uid = session.get('uid')
    if not uid: return
    me_user = u_id(uid)
    peer = (data.get('peer') or '').strip()
    if not peer: return
    room = dm_room(me_user['username'], peer)
    join_room(room)
    emit('dm_history', {'room': room, 'peer': peer, 'history': hist(room, 200)})


@socketio.on('open_group')
def on_open_group(data):
    uid = session.get('uid')
    if not uid: return
    gid = int(data.get('gid') or 0)
    if not gid or not is_mem(gid, uid): return
    room = g_room(gid)
    join_room(room)
    emit('group_history', {
        'room': room, 'group': get_group(gid),
        'members': group_members(gid), 'history': hist(room, 200),
    })


@socketio.on('send')
def on_send(data):
    uid = session.get('uid')
    if not uid: return
    mu = u_id(uid)
    if not mu: return
    text = (data.get('text') or '').strip()[:4000]
    to = (data.get('to') or 'general').strip()
    if not text: return

    if to.startswith('group:'):
        gid = int(to.split(':', 1)[1])
        g = get_group(gid)
        if not g or not is_mem(gid, uid): return
        if g['type'] == 'channel':
            role = my_role(gid, uid)
            if role not in ('owner', 'admin'):
                emit('error_msg', {'msg': 'В канале пишет только владелец'})
                return
        room = g_room(gid)
        msg = add_msg(room, mu['id'], mu['username'], 'text', text=text)
        emit('message', ser(msg), to=room)
        return

    if to == 'general':
        room = 'general'
    else:
        room = dm_room(mu['username'], to)

    msg = add_msg(room, mu['id'], mu['username'], 'text', text=text)
    payload = ser(msg)
    if room == 'general':
        emit('message', payload, to='general')
    else:
        emit('message', payload)
        sid = name_to_sid.get(to.lower())
        if sid:
            emit('message', payload, to=sid)


@socketio.on('typing')
def on_typing(data):
    uid = session.get('uid')
    if not uid: return
    mu = u_id(uid)
    if not mu: return
    to = (data.get('to') or 'general').strip()
    is_t = bool(data.get('is_typing'))
    if to.startswith('group:'):
        room = to
    elif to == 'general':
        room = 'general'
    else:
        room = dm_room(mu['username'], to)
    emit('typing', {'from': mu['username'], 'is_typing': is_t, 'to': to},
         to=room, include_self=False)


@socketio.on('read')
def on_read(data):
    uid = session.get('uid')
    if not uid: return
    room = data.get('room')
    lid = int(data.get('last_id') or 0)
    if not room: return
    mark_read(uid, room, lid)
    emit('read', {'room': room, 'reads': {str(k): v for k, v in reads_room(room).items()}}, to=room)


@socketio.on('group_created')
def on_group_created(data):
    gid = int(data.get('gid') or 0)
    if not gid: return
    g = get_group(gid)
    if not g: return
    for m in group_members(gid):
        sid = name_to_sid.get(m['username'].lower())
        if sid:
            emit('group_added', {'group': g}, to=sid)
            emit('join_group_room', {'gid': gid}, to=sid)


@socketio.on('join_group_room')
def on_join_group_room(data):
    gid = int(data.get('gid') or 0)
    if not gid: return
    join_room(g_room(gid))


@socketio.on('disconnect')
def on_disc():
    u = online.pop(request.sid, None)
    if not u: return
    if name_to_sid.get(u['username'].lower()) == request.sid:
        name_to_sid.pop(u['username'].lower(), None)
    try:
        upd_seen(u['id'])
    except:
        pass
    sys_msg = add_msg('general', None, u['username'], 'system',
                      text=f"{u['username']} покинул чат")
    socketio.emit('message', ser(sys_msg), to='general')
    bcast_online()


# ============================================================
# STARTUP
# ============================================================
init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("=" * 55)
    print(f"🚀 Holodyx Chat v{APP_VERSION}")
    print(f"   http://127.0.0.1:{port}")
    print(f"   БД: {DB_PATH}")
    print("=" * 55)
    socketio.run(app, host='0.0.0.0', port=port, debug=False,
                 allow_unsafe_werkzeug=True)

# ========== КОНЕЦ ФАЙЛА ==========
