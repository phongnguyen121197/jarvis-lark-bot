"""
Jarvis - Lark AI Report Assistant
Main application with all modules integrated
"""
import os
import json
import base64
import hashlib
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.backends import default_backend
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import modules
from intent_classifier import classify_intent, INTENT_KOC_REPORT, INTENT_CONTENT_CALENDAR, INTENT_GENERAL_SUMMARY, INTENT_UNKNOWN
from lark_base import generate_koc_summary, generate_content_calendar, test_connection
from report_generator import generate_koc_report_text, generate_content_calendar_text, generate_general_summary_text

# ============ CONFIG ============
LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")
LARK_ENCRYPT_KEY = os.getenv("LARK_ENCRYPT_KEY")
LARK_VERIFICATION_TOKEN = os.getenv("LARK_VERIFICATION_TOKEN")

LARK_API_BASE = "https://open.larksuite.com/open-apis"
TENANT_ACCESS_TOKEN_URL = f"{LARK_API_BASE}/auth/v3/tenant_access_token/internal"
SEND_MESSAGE_URL = f"{LARK_API_BASE}/im/v1/messages"

# ============ APP ============
app = FastAPI(title="Jarvis - Lark AI Report Assistant")

# ============ DECRYPT ============
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
        
        padding_len = decrypted[-1]
        decrypted = decrypted[:-padding_len]
        
        return decrypted.decode('utf-8')

decryptor = LarkDecryptor(LARK_ENCRYPT_KEY) if LARK_ENCRYPT_KEY else None

# ============ LARK AUTH ============
async def get_tenant_access_token() -> str:
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
            },
            timeout=30.0
        )
        return response.json()

# ============ MESSAGE HANDLER ============
async def process_jarvis_query(text: str) -> str:
    """
    Xử lý câu hỏi và trả về response
    """
    print(f"🔍 Processing query: {text}")
    
    # 1. Phân loại intent
    intent_result = classify_intent(text)
    intent = intent_result.get("intent")
    
    print(f"🎯 Intent: {intent}")
    print(f"📊 Params: {intent_result}")
    
    try:
        # 2. Xử lý theo intent
        if intent == INTENT_KOC_REPORT:
            month = intent_result.get("month")
            week = intent_result.get("week")
            
            # Lấy dữ liệu từ Lark Base
            summary_data = await generate_koc_summary(month=month, week=week)
            
            # Sinh báo cáo
            report = await generate_koc_report_text(summary_data)
            return report
        
        elif intent == INTENT_CONTENT_CALENDAR:
            start_date = intent_result.get("start_date")
            end_date = intent_result.get("end_date")
            team = intent_result.get("team_filter")
            
            # Lấy dữ liệu
            calendar_data = await generate_content_calendar(
                start_date=start_date,
                end_date=end_date,
                team=team
            )
            
            # Sinh báo cáo
            report = await generate_content_calendar_text(calendar_data)
            return report
        
        elif intent == INTENT_GENERAL_SUMMARY:
            month = intent_result.get("month")
            week = intent_result.get("week")
            start_date = intent_result.get("start_date")
            end_date = intent_result.get("end_date")
            
            # Lấy cả 2 loại dữ liệu
            from intent_classifier import get_current_week_range
            if not start_date or not end_date:
                start_date, end_date = get_current_week_range()
            
            koc_data = await generate_koc_summary(month=month, week=week)
            content_data = await generate_content_calendar(
                start_date=start_date,
                end_date=end_date
            )
            
            # Sinh báo cáo tổng hợp
            report = await generate_general_summary_text(koc_data, content_data)
            return report
        
        else:
            # Unknown intent
            return intent_result.get("suggestion", 
                "🤖 Xin chào! Tôi là Jarvis.\n\n"
                "Bạn có thể hỏi tôi về:\n"
                "• Báo cáo KOC: \"Tóm tắt KOC tháng 12\"\n"
                "• Lịch content: \"Lịch content tuần này\"\n"
                "• Tổng hợp: \"Summary tuần này\"\n\n"
                "Hãy thử hỏi tôi nhé! 😊"
            )
    
    except Exception as e:
        print(f"❌ Error processing query: {e}")
        import traceback
        traceback.print_exc()
        return f"❌ Có lỗi xảy ra khi xử lý yêu cầu: {str(e)}\n\nVui lòng thử lại sau."

# ============ WEBHOOK HANDLER ============
@app.post("/lark/events")
async def handle_lark_events(request: Request):
    body = await request.json()
    
    print(f"📩 Received raw event")
    
    # Decrypt if encrypted
    if "encrypt" in body and decryptor:
        try:
            decrypted_str = decryptor.decrypt(body["encrypt"])
            body = json.loads(decrypted_str)
            print(f"🔓 Decrypted event type: {body.get('header', {}).get('event_type', body.get('type'))}")
        except Exception as e:
            print(f"❌ Decrypt failed: {e}")
            raise HTTPException(status_code=400, detail="Decrypt failed")
    
    # URL Verification
    if "challenge" in body:
        print("✅ URL Verification challenge received")
        return JSONResponse(content={"challenge": body["challenge"]})
    
    # Event handling
    header = body.get("header", {})
    event = body.get("event", {})
    
    # Verify token
    token = header.get("token")
    if token and token != LARK_VERIFICATION_TOKEN:
        print(f"❌ Token verification failed")
        raise HTTPException(status_code=401, detail="Invalid token")
    
    event_type = header.get("event_type")
    
    # Handle message
    if event_type == "im.message.receive_v1":
        await handle_message_event(event)
    
    return JSONResponse(content={"code": 0, "msg": "success"})

async def handle_message_event(event: dict):
    message = event.get("message", {})
    
    chat_id = message.get("chat_id")
    message_type = message.get("message_type")
    content_str = message.get("content", "{}")
    
    if message_type != "text":
        return
    
    try:
        content = json.loads(content_str)
        text = content.get("text", "")
    except:
        text = content_str
    
    print(f"💬 Message: {text}")
    
    # Check mention
    mentions = message.get("mentions", [])
    is_mentioned = len(mentions) > 0 or "jarvis" in text.lower()
    
    if not is_mentioned:
        print("⏭️ Not mentioned, skipping")
        return
    
    # Remove @mention from text
    clean_text = text
    for mention in mentions:
        mention_key = mention.get("key", "")
        clean_text = clean_text.replace(mention_key, "").strip()
    
    # Process query
    response_text = await process_jarvis_query(clean_text or text)
    
    # Send response
    await send_lark_message(chat_id, response_text)
    print(f"✅ Response sent")

# ============ HEALTH & TEST ============
@app.get("/")
async def root():
    return {"status": "ok", "message": "Jarvis is running 🤖", "version": "3.0"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/test/base")
async def test_base():
    """Test kết nối Lark Base"""
    success = await test_connection()
    return {"success": success}

@app.get("/test/intent")
async def test_intent(q: str = "tóm tắt KOC tháng 12"):
    """Test intent classifier"""
    result = classify_intent(q)
    return result

# ============ RUN ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
