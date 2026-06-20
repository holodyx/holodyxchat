from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import hashlib
import os

app = FastAPI()

# Разрешаем запросы со всех адресов
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class UserAuth(BaseModel):
    username: str
    password: str

class MessageSend(BaseModel):
    username: str
    text: str

def init_db():
    conn = sqlite3.connect("holodyxchat.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password_hash TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT, text TEXT)")
    conn.commit()
    conn.close()

init_db()

@app.post("/register")
def register(user: UserAuth):
    conn = sqlite3.connect("holodyxchat.db")
    cursor = conn.cursor()
    pwd_hash = hashlib.sha256(user.password.encode()).hexdigest()
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (user.username, pwd_hash))
        conn.commit()
        return {"status": "success"}
    except:
        raise HTTPException(status_code=400, detail="Имя уже занято")
    finally:
        conn.close()

@app.post("/login")
def login(user: UserAuth):
    conn = sqlite3.connect("holodyxchat.db")
    cursor = conn.cursor()
    pwd_hash = hashlib.sha256(user.password.encode()).hexdigest()
    cursor.execute("SELECT * FROM users WHERE username=? AND password_hash=?", (user.username, pwd_hash))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")
    return {"status": "success"}

@app.post("/send")
def send_message(msg: MessageSend):
    conn = sqlite3.connect("holodyxchat.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO messages (sender, text) VALUES (?, ?)", (msg.username, msg.text))
    conn.commit()
    conn.close()
    return {"status": "success"}

@app.get("/messages")
def get_messages():
    conn = sqlite3.connect("holodyxchat.db")
    cursor = conn.cursor()
    cursor.execute("SELECT sender, text FROM messages ORDER BY id ASC LIMIT 50")
    messages = [{"sender": r[0], "text": r[1]} for r in cursor.fetchall()]
    conn.close()
    return messages

# Отдаем визуальный сайт прямо из корня
@app.get("/", response_class=HTMLResponse)
def get_site():
    # Сюда вставляется весь HTML-код, который мы сделали на прошлом шаге
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()
