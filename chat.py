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
