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

@app.get("/", response_class=HTMLResponse)
async def get_index():
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
            
    async def route_message(self, sender: str, packet_data: dict):
        recipient = packet_data.get("recipient")
        if not recipient:
            return
            
        # Формируем правильный пакет для пересылки
        forward_packet = json.dumps({
            "type": "msg",
            "sender": sender,
            "text": packet_data.get("text", ""),
            "time": packet_data.get("time", ""),
            "fileType": packet_data.get("fileType", "text"),
            "fileName": packet_data.get("fileName", "")
        })
        
        # Отправляем получателю
        if recipient in self.active_connections:
            await self.active_connections[recipient].send_text(forward_packet)
        # Отправляем копию отправителю (кроме чата с самим собой, чтобы не дублировать)
        if sender in self.active_connections and sender != recipient:
            await self.active_connections[sender].send_text(forward_packet)

    async def broadcast_users_list(self):
        conn = sqlite3.connect("chat.db")
        cursor = conn.cursor()
        cursor.execute("SELECT username FROM users")
        all_registered = [row[0] for row in cursor.fetchall()]
        conn.close()

        users_with_status = []
        for user in all_registered:
            users_with_status.append({
                "username": user,
                "is_online": user in self.active_connections
            })
        
        packet = json.dumps({"type": "users", "users": users_with_status})
        for connection in self.active_connections.values():
            try:
                await connection.send_text(packet)
            except:
                pass

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
        if row[0] == user_token:
            return {"success": True}
        else:
            return {"success": False, "error": "Этот никнейм уже занят другим устройством!"}

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await manager.connect(username, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                # Читаем прилетевший JSON-объект вместо обычного текста
                message_data = json.loads(data)
                await manager.route_message(username, message_data)
            except json.JSONDecodeError:
                # Если прилетел не JSON (на всякий случай)
                pass
    except WebSocketDisconnect:
        manager.disconnect(username, websocket)
        await manager.broadcast_users_list()
