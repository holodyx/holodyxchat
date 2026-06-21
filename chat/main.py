from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from typing import Dict
import json
import sqlite3
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def init_db():
    conn = sqlite3.connect("chat.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            token TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# НОВАЯ ФУНКЦИЯ: Сервер сам отдает файл index.html при заходе на сайт
@app.get("/", response_class=HTMLResponse)
async def get_index():
    # Ищем файл index.html в той же папочке, где лежит этот скрипт
    file_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, username: str, websocket: WebSocket):
        await websocket.accept()
        if username in self.active_connections:
            try: await self.active_connections[username].close(code=4000)
            except: pass
        self.active_connections[username] = websocket
        await self.broadcast_users_list()

    def disconnect(self, username: str, websocket: WebSocket):
        if username in self.active_connections and self.active_connections[username] == websocket:
            del self.active_connections[username]
            
    async def send_private_message(self, sender: str, recipient: str, text: str):
        packet = json.dumps({"type": "msg", "sender": sender, "text": text})
        if recipient in self.active_connections:
            await self.active_connections[recipient].send_text(packet)
        if sender in self.active_connections:
            await self.active_connections[sender].send_text(packet)

    async def broadcast_users_list(self):
        conn = sqlite3.connect("chat.db")
        cursor = conn.cursor()
        cursor.execute("SELECT username FROM users")
        all_registered = [row for row in cursor.fetchall()]
        conn.close()

        users_with_status = []
        for user in all_registered:
            users_with_status.append({
                "username": user,
                "is_online": user in self.active_connections
            })
        
        packet = json.dumps({"type": "users", "users": users_with_status})
        for connection in self.active_connections.values():
            await connection.send_text(packet)

manager = ConnectionManager()

@app.post("/auth")
async def auth_user(data: dict):
    username = data.get("username", "").strip()
    user_token = data.get("token", "").strip()
    
    if not username or not user_token:
        return {"success": False, "error": "Введите ваш никнейм!"}
        
    conn = sqlite3.connect("chat.db")
    cursor = conn.cursor()
    cursor.execute("SELECT token FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    
    if row is None:
        cursor.execute("INSERT INTO users (username, token) VALUES (?, ?)", (username, user_token))
        conn.commit()
        conn.close()
        return {"success": True}
    else:
        conn.close()
        if row == user_token:
            return {"success": True}
        else:
            return {"success": False, "error": "Этот никнейм уже занят другим устройством!"}

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await manager.connect(username, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            message_data = json.loads(data)
            recipient = message_data.get("recipient")
            text = message_data.get("text")
            if recipient and text:
                await manager.send_private_message(username, recipient, text)
    except WebSocketDisconnect:
        manager.disconnect(username, websocket)
        await manager.broadcast_users_list()
