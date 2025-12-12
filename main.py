"""
Jarvis - Lark AI Report Assistant
Main application with all modules integrated
"""
import os
import json
import base64
import hashlib
import time
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
from intent_classifier import classify_intent, INTENT_KOC_REPORT, INTENT_CONTENT_CALENDAR, INTENT_TASK_SUMMARY, INTENT_GENERAL_SUMMARY, INTENT_UNKNOWN
from lark_base import generate_koc_summary, generate_content_calendar, generate_task_summary, test_connection
from report_generator import generate_koc_report_text, generate_content_calendar_text, generate_task_summary_text, generate_general_summary_text

# ============ CONFIG ============
LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")
LARK_ENCRYPT_KEY = os.getenv("LARK_ENCRYPT_KEY")
LARK_VERIFICATION_TOKEN = os.getenv("LARK_VERIFICATION_TOKEN")

LARK_API_BASE = "https://open.larksuite.com/open-apis"
TENANT_ACCESS_TOKEN_URL = f"{LARK_API_BASE}/auth/v3/tenant_access_token/internal"
SEND_MESSAGE_URL = f"{LARK_API_BASE}/im/v1/messages"

# Message deduplication cache
_processed_messages = {}
MESSAGE_CACHE_TTL = 300  # 5 minutes

def is_message_processed(message_id: str) -> bool:
    """Check if message was already processed"""
    now = time.time()
    
    # Clean up old entries
    expired = [mid for mid, ts in _processed_messages.items() if now - ts > MESSAGE_CACHE_TTL]
    for mid in expired:
        del _processed_messages[mid]
    
    # Check if already processed
    if message_id in _processed_messages:
        return True
    
    # Mark as processed
    _processed_messages[message_id] = now
    return False

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
            vi_tri = intent_result.get("vi_tri_filter")
            month = intent_result.get("month")
            
            # Lấy dữ liệu
            calendar_data = await generate_content_calendar(
                start_date=start_date,
                end_date=end_date,
                month=month,
                team=team,
                vi_tri=vi_tri
            )
            
            # Sinh báo cáo
            report = await generate_content_calendar_text(calendar_data)
            return report
        
        elif intent == INTENT_TASK_SUMMARY:
            month = intent_result.get("month")
            vi_tri = intent_result.get("vi_tri")
            
            # Lấy dữ liệu phân tích task
            task_data = await generate_task_summary(month=month, vi_tri=vi_tri)
            
            # Sinh báo cáo
            report = await generate_task_summary_text(task_data)
            return report
        
        elif intent == INTENT_GENERAL_SUMMARY:
            month = intent_result.get("month")
            week = intent_result.get("week")
            
            # Lấy cả 2 loại dữ liệu, filter theo tháng
            koc_data = await generate_koc_summary(month=month, week=week)
            content_data = await generate_content_calendar(month=month)
            
            # Sinh báo cáo tổng hợp
            report = await generate_general_summary_text(koc_data, content_data)
            return report
        
        else:
            # Unknown intent
            return intent_result.get("suggestion", 
                "🤖 Xin chào! Tôi là Jarvis.\n\n"
                "Bạn có thể hỏi tôi về:\n"
                "• Báo cáo KOC: \"Tóm tắt KOC tháng 12\"\n"
                "• Chi phí KOC: \"Chi phí KOC tháng 12 theo sản phẩm\"\n"
                "• Lịch content: \"Lịch content tuần này\"\n"
                "• Phân tích task: \"Task quá hạn theo vị trí\"\n"
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
    
    # Get message_id for deduplication
    message_id = message.get("message_id")
    
    # Check if already processed
    if message_id and is_message_processed(message_id):
        print(f"⏭️ Duplicate message {message_id}, skipping")
        return
    
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
    return {"status": "ok", "message": "Jarvis is running 🤖", "version": "3.9.2"}

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

@app.get("/debug/booking-fields")
async def debug_booking_fields_endpoint():
    """Debug: Xem tất cả fields từ bảng Booking"""
    from lark_base import debug_booking_fields
    return await debug_booking_fields()

@app.get("/debug/task-fields")
async def debug_task_fields_endpoint():
    """Debug: Xem tất cả fields từ bảng Task"""
    from lark_base import debug_task_fields
    return await debug_task_fields()

@app.get("/test/koc-filter")
async def test_koc_filter(month: int = 12):
    """Test KOC filter by month"""
    from lark_base import get_booking_records
    records = await get_booking_records(month=month)
    
    return {
        "month": month,
        "total_records": len(records),
        "sample": [
            {
                "id_koc": r.get("id_koc"),
                "thang_air": r.get("thang_air"),
                "link_air": bool(r.get("link_air_bai"))
            }
            for r in records[:5]
        ]
    }

@app.get("/debug/month-distribution")
async def debug_month_distribution():
    """Debug: Xem distribution của Tháng air trong tất cả records"""
    from lark_base import get_all_records, BOOKING_BASE
    
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=500
    )
    
    # Collect all unique "Tháng air" values and their formats
    month_values = {}
    sample_by_month = {}
    
    for record in records:
        fields = record.get("fields", {})
        raw_value = fields.get("Tháng air")
        
        # Convert to string for grouping
        key = str(raw_value)
        
        if key not in month_values:
            month_values[key] = {
                "count": 0,
                "raw_type": type(raw_value).__name__,
                "raw_sample": str(raw_value)[:200]
            }
            sample_by_month[key] = fields.get("ID KOC")
        
        month_values[key]["count"] += 1
    
    return {
        "total_records": len(records),
        "unique_month_values": len(month_values),
        "distribution": month_values
    }


@app.get("/debug/all-field-names")
async def debug_all_field_names():
    """Debug: Xem TẤT CẢ field names từ Booking table"""
    from lark_base import get_all_records, BOOKING_BASE
    
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=5
    )
    
    if not records:
        return {"error": "No records found"}
    
    # Collect all field names
    all_fields = set()
    for record in records:
        fields = record.get("fields", {})
        all_fields.update(fields.keys())
    
    # Show sample values for product-related fields
    sample_record = records[0].get("fields", {})
    product_fields = {}
    for key in all_fields:
        if any(x in key.lower() for x in ["sản phẩm", "san pham", "phân loại", "phan loai", "product", "category"]):
            product_fields[key] = sample_record.get(key)
    
    return {
        "total_fields": len(all_fields),
        "all_field_names": sorted(list(all_fields)),
        "product_related_fields": product_fields,
        "sample_record": sample_record
    }


# ============ RUN ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
