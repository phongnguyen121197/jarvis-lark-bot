import os
import json
import base64
import hashlib
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.backends import default_backend
from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import httpx
from dotenv import load_dotenv

load_dotenv()

# ============ CONFIG ============
LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")
LARK_ENCRYPT_KEY = os.getenv("LARK_ENCRYPT_KEY")
LARK_VERIFICATION_TOKEN = os.getenv("LARK_VERIFICATION_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Lark API endpoints
LARK_API_BASE = "https://open.larksuite.com/open-apis"
TENANT_ACCESS_TOKEN_URL = f"{LARK_API_BASE}/auth/v3/tenant_access_token/internal"
SEND_MESSAGE_URL = f"{LARK_API_BASE}/im/v1/messages"

# ============ APP ============
app = FastAPI(title="Jarvis - Lark AI Report Assistant")

# ============ DECRYPT LARK MESSAGE ============
class LarkDecryptor:
    def __init__(self, encrypt_key: str):
        key = hashlib.sha256(encrypt_key.encode()).digest()
        self.key = key
    
    def decrypt(self, encrypted_data: str) -> str:
        encrypted_bytes = base64.b64decode(encrypted_data)
        iv = encrypted_bytes[:16]
        encrypted_content = encrypted_bytes[16:]
        
        cipher = Cipher(AES(self.key), CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(encrypted_content) + decryptor.finalize()
        
        # Remove PKCS7 padding
        padding_len = decrypted[-1]
        decrypted = decrypted[:-padding_len]
        
        return decrypted.decode('utf-8')

decryptor = LarkDecryptor(LARK_ENCRYPT_KEY) if LARK_ENCRYPT_KEY else None

# ============ LARK AUTH ============
async def get_tenant_access_token() -> str:
    """Lấy tenant access token từ Lark"""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            TENANT_ACCESS_TOKEN_URL,
            json={
                "app_id": LARK_APP_ID,
                "app_secret": LARK_APP_SECRET
            }
        )
        data = response.json()
        if data.get("code") == 0:
            return data.get("tenant_access_token")
        else:
            raise Exception(f"Failed to get token: {data}")

# ============ SEND MESSAGE ============
async def send_lark_message(chat_id: str, text: str):
    """Gửi tin nhắn text về Lark chat"""
    token = await get_tenant_access_token()
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            SEND_MESSAGE_URL,
            params={"receive_id_type": "chat_id"},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json={
                "receive_id": chat_id,
                "msg_type": "text",
                "content": json.dumps({"text": text})
            }
        )
        return response.json()

# ============ WEBHOOK HANDLER ============
@app.post("/lark/events")
async def handle_lark_events(request: Request):
    """Xử lý webhook events từ Lark"""
    body = await request.json()
    
    # Debug log
    print(f"📩 Received raw event: {json.dumps(body, indent=2, ensure_ascii=False)}")
    
    # Decrypt if encrypted
    if "encrypt" in body and decryptor:
        try:
            decrypted_str = decryptor.decrypt(body["encrypt"])
            body = json.loads(decrypted_str)
            print(f"🔓 Decrypted event: {json.dumps(body, indent=2, ensure_ascii=False)}")
        except Exception as e:
            print(f"❌ Decrypt failed: {e}")
            raise HTTPException(status_code=400, detail="Decrypt failed")
    
    # 1. URL Verification (Lark gửi khi setup webhook)
    if "challenge" in body:
        print("✅ URL Verification challenge received")
        return JSONResponse(content={"challenge": body["challenge"]})
    
    # 2. Xử lý event schema 2.0
    header = body.get("header", {})
    event = body.get("event", {})
    
    # Verify token
    token = header.get("token")
    if token and token != LARK_VERIFICATION_TOKEN:
        print(f"❌ Token verification failed: got {token}")
        raise HTTPException(status_code=401, detail="Invalid token")
    
    event_type = header.get("event_type")
    
    # 3. Xử lý tin nhắn mới
    if event_type == "im.message.receive_v1":
        await handle_message_event(event)
    
    return JSONResponse(content={"code": 0, "msg": "success"})

async def handle_message_event(event: dict):
    """Xử lý khi có tin nhắn mới"""
    message = event.get("message", {})
    
    # Lấy thông tin cơ bản
    chat_id = message.get("chat_id")
    message_type = message.get("message_type")
    content_str = message.get("content", "{}")
    
    # Chỉ xử lý tin nhắn text
    if message_type != "text":
        return
    
    # Parse content
    try:
        content = json.loads(content_str)
        text = content.get("text", "")
    except:
        text = content_str
    
    print(f"💬 Message received: {text}")
    
    # Kiểm tra mention (tạm thời check keyword @Jarvis hoặc Jarvis)
    mentions = message.get("mentions", [])
    is_mentioned = len(mentions) > 0 or "jarvis" in text.lower()
    
    if not is_mentioned:
        print("⏭️ Not mentioned, skipping...")
        return
    
    # Xử lý câu hỏi
    # Tạm thời echo lại để test
    response_text = f"🤖 Jarvis đã nhận được tin nhắn: \"{text}\"\n\n(Đây là phản hồi test - Phase 1 đang được xây dựng)"
    
    await send_lark_message(chat_id, response_text)
    print(f"✅ Sent response to chat: {chat_id}")

# ============ HEALTH CHECK ============
@app.get("/")
async def root():
    return {"status": "ok", "message": "Jarvis is running 🤖"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

# ============ RUN ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
