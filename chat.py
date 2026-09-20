"""
Holodyx Chat v5 — каналы, группы, истории.
Запуск: python chat.py
"""

import os
import re
import uuid
import json
import sqlite3
import secrets
from datetime import datetime, timedelta
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
STORY_DIR = os.path.join(UPLOAD_DIR, 'stories')
DB_PATH = os.path.join(BASE_DIR, 'chat.db')

for d in (AVATAR_DIR, MEDIA_DIR, STORY_DIR):
    os.makedirs(d, exist_ok=True)

MAX_MEDIA = 50 * 1024 * 1024
STORY_TTL_HOURS = 24


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
            phone TEXT DEFAULT '',
            premium INTEGER DEFAULT 0,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
            user_id INTEGER, room TEXT, last_read_id INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, room)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS dialogs (
            user_id INTEGER, peer_name TEXT,
            last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(user_id, peer_name)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            user_id INTEGER PRIMARY KEY,
            data TEXT DEFAULT '{}'
        )
    """)

    # КАНАЛЫ И ГРУППЫ
    c.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'group',  -- 'group' | 'channel'
            about TEXT DEFAULT '',
            avatar TEXT,
            owner_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS group_members (
            group_id INTEGER,
            user_id INTEGER,
            role TEXT DEFAULT 'member',  -- 'owner' | 'admin' | 'member'
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(group_id, user_id)
        )
    """)

    # ИСТОРИИ
    c.execute("""
        CREATE TABLE IF NOT EXISTS stories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            type TEXT DEFAULT 'image',  -- 'image' | 'video' | 'text'
            media_url TEXT,
            text TEXT DEFAULT '',
            bg_color TEXT DEFAULT '#3b82f6',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS story_views (
            story_id INTEGER,
            user_id INTEGER,
            viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(story_id, user_id)
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
        print(f"[create_user] {e}"); return None
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


def update_last_seen(uid):
    conn = db()
    conn.execute("UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", (uid,))
    conn.commit(); conn.close()


def search_users(q, exclude_id, limit=20):
    conn = db()
    rows = conn.execute(
        """SELECT id, username, avatar, bio, last_seen, premium FROM users
           WHERE username LIKE ? AND id != ?
           ORDER BY username LIMIT ?""",
        (f'%{q}%', exclude_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_profile(uid, **fields):
    allowed = {'username', 'avatar', 'bio', 'phone'}
    sets, vals = [], []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = ?"); vals.append(v)
    if not sets: return True
    vals.append(uid)
    conn = db()
    try:
        conn.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit(); return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def set_premium(uid, value=True):
    conn = db()
    conn.execute("UPDATE users SET premium = ? WHERE id = ?", (1 if value else 0, uid))
    conn.commit(); conn.close()


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


# ---------- SETTINGS ----------
def get_settings(uid):
    conn = db()
    r = conn.execute("SELECT data FROM settings WHERE user_id = ?", (uid,)).fetchone()
    conn.close()
    if r:
        try: return json.loads(r['data'])
        except: return {}
    return {}


def save_settings(uid, data):
    conn = db()
    conn.execute(
        "INSERT INTO settings (user_id, data) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET data = excluded.data",
        (uid, json.dumps(data))
    )
    conn.commit(); conn.close()


# ---------- GROUPS / CHANNELS ----------
def create_group(name, type_, owner_id, about='', avatar=None):
    conn = db()
    conn.execute(
        "INSERT INTO groups (name, type, owner_id, about, avatar) VALUES (?, ?, ?, ?, ?)",
        (name, type_, owner_id, about, avatar)
    )
    conn.commit()
    gid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()['id']
    conn.execute(
        "INSERT INTO group_members (group_id, user_id, role) VALUES (?, ?, 'owner')",
        (gid, owner_id)
    )
    conn.commit(); conn.close()
    return gid


def get_group(gid):
    conn = db()
    r = conn.execute("SELECT * FROM groups WHERE id = ?", (gid,)).fetchone()
    conn.close()
    return dict(r) if r else None


def user_groups(uid):
    conn = db()
    rows = conn.execute("""
        SELECT g.*, gm.role FROM groups g
        JOIN group_members gm ON gm.group_id = g.id
        WHERE gm.user_id = ?
        ORDER BY g.id DESC
    """, (uid,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def group_members(gid):
    conn = db()
    rows = conn.execute("""
        SELECT u.id, u.username, u.avatar, u.bio, u.premium, gm.role
        FROM group_members gm
        JOIN users u ON u.id = gm.user_id
        WHERE gm.group_id = ?
        ORDER BY gm.role DESC, u.username
    """, (gid,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_group_member(gid, uid, role='member'):
    conn = db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO group_members (group_id, user_id, role) VALUES (?, ?, ?)",
            (gid, uid, role)
        )
        conn.commit(); return True
    finally:
        conn.close()


def remove_group_member(gid, uid):
    conn = db()
    conn.execute("DELETE FROM group_members WHERE group_id = ? AND user_id = ?", (gid, uid))
    conn.commit(); conn.close()


def is_group_member(gid, uid):
    conn = db()
    r = conn.execute("SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?", (gid, uid)).fetchone()
    conn.close()
    return bool(r)


def group_room(gid):
    return f"group:{gid}"


def user_groups_ids(uid):
    conn = db()
    rows = conn.execute("SELECT group_id FROM group_members WHERE user_id = ?", (uid,)).fetchall()
    conn.close()
    return [r['group_id'] for r in rows]


# ---------- STORIES ----------
def create_story(uid, type_, media_url=None, text='', bg_color='#3b82f6'):
    conn = db()
    expires = (datetime.utcnow() + timedelta(hours=STORY_TTL_HOURS)).isoformat()
    conn.execute(
        "INSERT INTO stories (user_id, type, media_url, text, bg_color, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
        (uid, type_, media_url, text, bg_color, expires)
    )
    conn.commit()
    sid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()['id']
    conn.close()
    return sid


def active_stories(exclude_uid=None):
    """Список активных историй (не старше 24ч), сгруппированных по юзеру."""
    conn = db()
    rows = conn.execute("""
        SELECT s.*, u.username, u.avatar
        FROM stories s JOIN users u ON u.id = s.user_id
        WHERE datetime(s.expires_at) > datetime('now')
        ORDER BY s.created_at ASC
    """).fetchall()
    conn.close()

    by_user = {}
    for r in rows:
        uid = r['user_id']
        if uid not in by_user:
            by_user[uid] = {
                'user_id': uid,
                'username': r['username'],
                'avatar': r['avatar'],
                'stories': [],
                'has_unviewed': False,
            }
        by_user[uid]['stories'].append({
            'id': r['id'],
            'type': r['type'],
            'media_url': r['media_url'],
            'text': r['text'],
            'bg_color': r['bg_color'],
            'created_at': r['created_at'],
        })

    # Проверка "просмотрено"
    if exclude_uid:
        conn = db()
        viewed = conn.execute(
            "SELECT story_id FROM story_views WHERE user_id = ?", (exclude_uid,)
        ).fetchall()
        conn.close()
        viewed_ids = {v['story_id'] for v in viewed}
        for u in by_user.values():
            u['has_unviewed'] = any(s['id'] not in viewed_ids for s in u['stories'])

    return list(by_user.values())


def view_story(sid, uid):
    conn = db()
    try:
        conn.execute("INSERT OR IGNORE INTO story_views (story_id, user_id) VALUES (?, ?)", (sid, uid))
        conn.commit()
    finally:
        conn.close()


def story_viewers(sid):
    conn = db()
    rows = conn.execute("""
        SELECT u.id, u.username, u.avatar FROM story_views sv
        JOIN users u ON u.id = sv.user_id WHERE sv.story_id = ?
    """, (sid,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def my_stories(uid):
    conn = db()
    rows = conn.execute("""
        SELECT * FROM stories WHERE user_id = ?
        AND datetime(expires_at) > datetime('now')
        ORDER BY created_at DESC
    """, (uid,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


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
                         'avatar': u['avatar'], 'bio': u.get('bio', ''),
                         'premium': u.get('premium', 0)}
    return list(seen.values())


def broadcast_online():
    socketio.emit('online', {'users': online_list()}, to='general')
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


@app.route('/chat_uploads/avatars/<path:fn>')
def serve_avatar(fn):
    return send_from_directory(AVATAR_DIR, fn)


@app.route('/chat_uploads/media/<path:fn>')
def serve_media(fn):
    return send_from_directory(MEDIA_DIR, fn)


@app.route('/chat_uploads/stories/<path:fn>')
def serve_story(fn):
    return send_from_directory(STORY_DIR, fn)


@app.route('/api/register', methods=['POST'])
def api_register():
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get('email') or '').strip().lower()
        username = (data.get('username') or '').strip()
        password = data.get('password') or ''
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
    update_last_seen(user['id'])
    return jsonify(ok=True, user={
        'id': user['id'], 'username': user['username'],
        'avatar': user['avatar'], 'bio': user['bio'], 'email': user['email'],
        'phone': user.get('phone', ''), 'premium': user.get('premium', 0),
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
        'phone': u.get('phone', ''), 'premium': u.get('premium', 0),
    })


@app.route('/api/logout', methods=['POST'])
def api_logout():
    session.clear()
    return jsonify(ok=True)


@app.route('/api/users/search')
def api_users_search():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    q = (request.args.get('q') or '').strip()
    if len(q) < 1: return jsonify(ok=True, users=[])
    users = search_users(q, uid, 20)
    return jsonify(ok=True, users=users)


@app.route('/api/dialogs')
def api_dialogs():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    return jsonify(ok=True, dialogs=list_dialogs(uid))


@app.route('/api/settings', methods=['GET'])
def api_get_settings():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    return jsonify(ok=True, settings=get_settings(uid))


@app.route('/api/settings', methods=['POST'])
def api_save_settings():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    data = request.get_json(silent=True) or {}
    save_settings(uid, data)
    return jsonify(ok=True)


@app.route('/api/premium', methods=['POST'])
def api_premium():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    data = request.get_json(silent=True) or {}
    action = data.get('action')
    if action == 'activate':
        set_premium(uid, True)
    elif action == 'deactivate':
        set_premium(uid, False)
    u = user_by_id(uid)
    return jsonify(ok=True, premium=u.get('premium', 0))


@app.route('/api/profile', methods=['POST'])
def api_profile():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False, error='Не авторизован'), 401
    u = user_by_id(uid)
    if not u: return jsonify(ok=False, error='Нет юзера'), 404

    username = request.form.get('username')
    bio = request.form.get('bio')
    phone = request.form.get('phone')
    updates = {}

    if username and username != u['username']:
        if not re.match(r'^[A-Za-zА-Яа-я0-9_]{3,20}$', username):
            return jsonify(ok=False, error='Некорректный ник'), 400
        if user_by_username(username):
            return jsonify(ok=False, error='Ник занят'), 400
        updates['username'] = username
    if bio is not None:
        updates['bio'] = bio.strip()[:300]
    if phone is not None:
        updates['phone'] = phone.strip()[:30]

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
        'phone': nu.get('phone', ''), 'premium': nu.get('premium', 0),
    })


@app.route('/api/upload', methods=['POST'])
def api_upload():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False, error='Не авторизован'), 401
    file = request.files.get('file')
    kind = (request.form.get('kind') or 'file').lower()
    room = request.form.get('room') or 'general'
    if not file or not file.filename:
        return jsonify(ok=False, error='Файл не выбран'), 400
    ext = ext_of(file.filename)
    if kind == 'image':
        if ext not in ALLOWED_IMAGE: return jsonify(ok=False, error='Не поддерживается'), 400
        msg_type = 'image'
    elif kind == 'video':
        if ext not in ALLOWED_VIDEO: return jsonify(ok=False, error='Не поддерживается'), 400
        msg_type = 'video'
    elif kind == 'audio':
        if ext not in ALLOWED_AUDIO: return jsonify(ok=False, error='Не поддерживается'), 400
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
    elif room.startswith('group:'):
        socketio.emit('message', payload, to=room, include_self=False)
        socketio.emit('message', payload, to=request.sid)
    else:
        socketio.emit('message', payload, to=request.sid)
        parts = room[3:].split('|')
        for name in parts:
            sid = name_to_sid.get(name)
            if sid and sid != request.sid:
                socketio.emit('message', payload, to=sid)
    return jsonify(ok=True, message=payload)


# ============ GROUPS / CHANNELS API ============

@app.route('/api/groups', methods=['POST'])
def api_create_group():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False, error='Не авторизован'), 401
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
    # добавить участников
    for username in members:
        u = user_by_username(username)
        if u and u['id'] != uid:
            add_group_member(gid, u['id'])

    # системное сообщение
    g = get_group(gid)
    room = group_room(gid)
    sys_msg = add_msg(room, None, 'system', 'system',
                      text=f"{g['type'] == 'channel' and 'Канал' or 'Группа'} «{g['name']}» создан")
    socketio.emit('message', serialize(sys_msg), to=room)

    return jsonify(ok=True, group=dict(g))


@app.route('/api/groups', methods=['GET'])
def api_list_groups():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    groups = user_groups(uid)
    return jsonify(ok=True, groups=groups)


@app.route('/api/groups/<int:gid>')
def api_get_group(gid):
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    if not is_group_member(gid, uid):
        return jsonify(ok=False, error='Нет доступа'), 403
    g = get_group(gid)
    if not g: return jsonify(ok=False, error='Не найдено'), 404
    members = group_members(gid)
    return jsonify(ok=True, group=g, members=members)


@app.route('/api/groups/<int:gid>/members', methods=['POST'])
def api_add_members(gid):
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    if not is_group_member(gid, uid): return jsonify(ok=False), 403
    data = request.get_json(silent=True) or {}
    usernames = data.get('usernames') or []
    added = []
    for username in usernames:
        u = user_by_username(username)
        if u:
            add_group_member(gid, u['id'])
            added.append(u['username'])
    # системное сообщение
    if added:
        room = group_room(gid)
        msg = add_msg(room, None, 'system', 'system',
                      text=f"Добавлены: {', '.join(added)}")
        socketio.emit('message', serialize(msg), to=room)
    return jsonify(ok=True, added=added)


@app.route('/api/groups/<int:gid>/leave', methods=['POST'])
def api_leave_group(gid):
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    remove_group_member(gid, uid)
    room = group_room(gid)
    u = user_by_id(uid)
    msg = add_msg(room, None, 'system', 'system', text=f"{u['username']} покинул чат")
    socketio.emit('message', serialize(msg), to=room)
    return jsonify(ok=True)


# ============ STORIES API ============

@app.route('/api/stories/feed')
def api_stories_feed():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    feed = active_stories(exclude_uid=uid)
    mine = my_stories(uid)
    my = None
    if mine:
        u = user_by_id(uid)
        my = {
            'user_id': uid,
            'username': u['username'],
            'avatar': u['avatar'],
            'stories': [{
                'id': s['id'], 'type': s['type'], 'media_url': s['media_url'],
                'text': s['text'], 'bg_color': s['bg_color'],
                'created_at': s['created_at'],
            } for s in mine],
            'has_unviewed': False,
            'is_me': True,
        }
    feed = [f for f in feed if f['user_id'] != uid]
    return jsonify(ok=True, my=my, others=feed)


@app.route('/api/stories', methods=['POST'])
def api_create_story():
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    stype = request.form.get('type', 'image')
    text = (request.form.get('text') or '').strip()[:200]
    bg_color = request.form.get('bg_color', '#3b82f6')

    if stype not in ('image', 'video', 'text'):
        return jsonify(ok=False, error='Неверный тип'), 400

    media_url = None
    file = request.files.get('file')
    if file and file.filename:
        ext = ext_of(file.filename)
        if stype == 'image' and ext not in ALLOWED_IMAGE:
            return jsonify(ok=False, error='Неверный формат'), 400
        if stype == 'video' and ext not in ALLOWED_VIDEO:
            return jsonify(ok=False, error='Неверный формат'), 400
        fn = f"{uuid.uuid4().hex}.{ext}"
        file.save(os.path.join(STORY_DIR, fn))
        media_url = f"/chat_uploads/stories/{fn}"

    sid = create_story(uid, stype, media_url, text, bg_color)
    socketio.emit('story_created', {'user_id': uid}, to='general')
    return jsonify(ok=True, story_id=sid)


@app.route('/api/stories/<int:sid>/view', methods=['POST'])
def api_view_story(sid):
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    view_story(sid, uid)
    return jsonify(ok=True)


@app.route('/api/stories/<int:sid>/viewers')
def api_story_viewers(sid):
    uid = session.get('uid')
    if not uid: return jsonify(ok=False), 401
    return jsonify(ok=True, viewers=story_viewers(sid))# ============================================================
# SOCKET
# ============================================================
@socketio.on('connect')
def on_connect():
    uid = session.get('uid')
    if not uid: emit('need_auth'); return
    u = user_by_id(uid)
    if not u: emit('need_auth'); return

    update_last_seen(uid)
    online[request.sid] = {
        'id': u['id'], 'username': u['username'],
        'avatar': u['avatar'], 'bio': u['bio'], 'premium': u.get('premium', 0),
    }
    name_to_sid[u['username'].lower()] = request.sid
    join_room('general')
    join_room(f"user:{u['id']}")

    # подписаться на комнаты всех групп пользователя
    for gid in user_groups_ids(u['id']):
        join_room(group_room(gid))

    emit('joined', {
        'user': {
            'id': u['id'], 'username': u['username'],
            'avatar': u['avatar'], 'bio': u['bio'], 'email': u['email'],
            'phone': u.get('phone', ''), 'premium': u.get('premium', 0),
        },
        'history': history('general', 200),
        'online': online_list(),
        'groups': user_groups(u['id']),
        'settings': get_settings(u['id']),
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


@socketio.on('open_group')
def on_open_group(data):
    uid = session.get('uid')
    if not uid: return
    gid = int(data.get('gid') or 0)
    if not gid: return
    if not is_group_member(gid, uid): return
    room = group_room(gid)
    join_room(room)
    g = get_group(gid)
    emit('group_history', {
        'room': room,
        'group': g,
        'members': group_members(gid),
        'history': history(room, 200),
    })


@socketio.on('send')
def on_send(data):
    uid = session.get('uid')
    if not uid: return
    me_user = user_by_id(uid)
    if not me_user: return
    text = (data.get('text') or '').strip()[:4000]
    to = (data.get('to') or 'general').strip()
    if not text: return

    # группа / канал
    if to.startswith('group:'):
        gid = int(to.split(':', 1)[1])
        g = get_group(gid)
        if not g: return
        if not is_group_member(gid, uid): return
        # в канале писать может только владелец/админ
        if g['type'] == 'channel':
            conn = db()
            role = conn.execute(
                "SELECT role FROM group_members WHERE group_id = ? AND user_id = ?",
                (gid, uid)
            ).fetchone()
            conn.close()
            if not role or role['role'] not in ('owner', 'admin'):
                emit('error_msg', {'msg': 'В канале пишет только владелец'})
                return
        room = group_room(gid)
        msg = add_msg(room=room, sender_id=me_user['id'],
                      sender_name=me_user['username'], type_='text', text=text)
        emit('message', serialize(msg), to=room)
        return

    # личка / общий
    if to == 'general':
        room = 'general'
    else:
        room = dm_room(me_user['username'], to)

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

    if to.startswith('group:'):
        room = to
    elif to == 'general':
        room = 'general'
    else:
        room = dm_room(me_user['username'], to)

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


# ---------- Группы через сокет ----------
@socketio.on('group_created')
def on_group_created(data):
    """Клиент сообщает, что создал группу — сервер подписывает всех участников."""
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
    join_room(group_room(gid))


# ---------- WebRTC ----------
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
        emit('call_offer', {'from': me_user['username'], 'offer': offer, 'type': call_type}, to=sid)
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
    try:
        update_last_seen(u['id'])
    except: pass
    sys_msg = add_msg('general', None, u['username'], 'system',
                      text=f"{u['username']} покинул чат")
    socketio.emit('message', serialize(sys_msg), to='general')
    broadcast_online()


# ============================================================
init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("=" * 55)
    print("🚀 Holodyx Chat v5 — каналы, группы, истории")
    print(f"   http://127.0.0.1:{port}")
    print(f"   БД: {DB_PATH}")
    print("=" * 55)
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
