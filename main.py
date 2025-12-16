"""
Jarvis - Lark AI Report Assistant
Main application with all modules integrated
"""
import os
import json
import base64
import hashlib
import time
import re
import asyncio
from datetime import datetime
from typing import Optional, Dict, List
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.backends import default_backend
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import httpx
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Load environment variables
load_dotenv()

# Import modules
from intent_classifier import classify_intent, INTENT_KOC_REPORT, INTENT_CONTENT_CALENDAR, INTENT_TASK_SUMMARY, INTENT_GENERAL_SUMMARY, INTENT_GPT_CHAT, INTENT_DASHBOARD, INTENT_UNKNOWN
from lark_base import generate_koc_summary, generate_content_calendar, generate_task_summary, generate_dashboard_summary, test_connection
from report_generator import generate_koc_report_text, generate_content_calendar_text, generate_task_summary_text, generate_general_summary_text, generate_dashboard_report_text, chat_with_gpt
from notes_manager import check_note_command, handle_note_command, get_notes_manager

# ============ SCHEDULER CONFIG ============
REMINDER_HOUR = int(os.getenv("REMINDER_HOUR", "9"))  # Giờ gửi reminder (mặc định 9h sáng)
REMINDER_MINUTE = int(os.getenv("REMINDER_MINUTE", "0"))
TIMEZONE = "Asia/Ho_Chi_Minh"

# Initialize scheduler
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

# ============ CONFIG ============
LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")
LARK_ENCRYPT_KEY = os.getenv("LARK_ENCRYPT_KEY")
LARK_VERIFICATION_TOKEN = os.getenv("LARK_VERIFICATION_TOKEN")

LARK_API_BASE = "https://open.larksuite.com/open-apis"
TENANT_ACCESS_TOKEN_URL = f"{LARK_API_BASE}/auth/v3/tenant_access_token/internal"
SEND_MESSAGE_URL = f"{LARK_API_BASE}/im/v1/messages"

# ============ DANH SÁCH NHÓM ĐÃ ĐĂNG KÝ ============
GROUP_CHATS = {
    "booking_sep": "oc_7356c37c72891ea5314507d78ab2e937",        # Kalle - Booking k sếp
    "digital": "oc_f2a9dc7332c3f08e6090c19166a4b47d",            # Cheng & Kalle | Digital
    "leader_marketing": "oc_d178ad558d36919731fb0bdf26a79eb7",   # Kalle - Leader Marketing
    "mkt_sale_kho": "oc_b503e285cdfb700b72b72fca3f1f316c",       # Cheng & Kalle | MKT x Sale x Kho
    "mkt_team": "oc_768c8b7b8680299e36fe889de677578a",           # Kalle - MKT Team
}

# Danh sách nhóm đã nhận tin nhắn (auto-collect từ events)
_discovered_groups = {}

def register_group(chat_id: str, chat_type: str, group_name: str = None):
    """Đăng ký nhóm khi nhận được tin nhắn"""
    if chat_type == "group" and chat_id:
        _discovered_groups[chat_id] = {
            "name": group_name or "Unknown",
            "discovered_at": time.time()
        }

def get_discovered_groups():
    """Lấy danh sách nhóm đã phát hiện"""
    return _discovered_groups

# Message deduplication cache
_processed_messages = {}
MESSAGE_CACHE_TTL = 600  # 10 minutes (tăng từ 5 phút)

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
    
    return False


def mark_message_processed(message_id: str):
    """Mark message as processed"""
    _processed_messages[message_id] = time.time()
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


# ============ SEND REPORT TO GROUP ============
# Mapping tên nhóm trong câu lệnh
GROUP_NAME_MAPPING = {
    # Booking sếp
    "booking": "booking_sep",
    "booking sếp": "booking_sep",
    "booking sep": "booking_sep",
    "booking k sếp": "booking_sep",
    "booking k sep": "booking_sep",
    
    # Digital
    "digital": "digital",
    "cheng digital": "digital",
    
    # Leader Marketing
    "leader": "leader_marketing",
    "leader marketing": "leader_marketing",
    "leader mkt": "leader_marketing",
    
    # MKT x Sale x Kho
    "sale": "mkt_sale_kho",
    "kho": "mkt_sale_kho",
    "mkt sale": "mkt_sale_kho",
    "mkt x sale": "mkt_sale_kho",
    "sale x kho": "mkt_sale_kho",
    
    # MKT Team
    "mkt team": "mkt_team",
    "marketing team": "mkt_team",
    
    # All groups
    "tất cả": "all",
    "tat ca": "all",
    "all": "all",
}

# Tên đầy đủ của nhóm (để hiển thị)
GROUP_DISPLAY_NAMES = {
    "booking_sep": "Kalle - Booking k sếp",
    "digital": "Cheng & Kalle | Digital",
    "leader_marketing": "Kalle - Leader Marketing",
    "mkt_sale_kho": "Cheng & Kalle | MKT x Sale x Kho",
    "mkt_team": "Kalle - MKT Team",
}


def check_custom_message_command(text: str) -> Optional[Dict]:
    """
    Kiểm tra xem có phải lệnh gửi tin nhắn tùy chỉnh không
    Ví dụ: 
    - "Thông báo sản phẩm Dark Beauty đã về hàng vào nhóm MKT Team và Booking"
    - "Jarvis gửi tin nhắn này: [nội dung] đến các nhóm đã kết nối"
    """
    text_lower = text.lower()
    
    # Loại bỏ @Jarvis hoặc Jarvis ở đầu
    text_clean = re.sub(r'^@?jarvis\s*', '', text, flags=re.IGNORECASE).strip()
    text_clean_lower = text_clean.lower()
    
    # SAFEGUARD: Nếu là note command thì skip
    note_keywords = ["note:", "note ", "ghi nhớ:", "ghi nhớ ", "ghi nho:", "todo:", "công việc:", "cong viec:"]
    if any(text_clean_lower.startswith(kw) for kw in note_keywords):
        return None
    
    # Kiểm tra có phải lệnh thông báo/gửi tin không
    notify_keywords = ["thông báo", "thong bao", "gửi tin", "gui tin", "nhắn tin", "nhan tin", "notify", "gởi tin", "gửi tin nhắn", "gui tin nhan"]
    is_notify = any(kw in text_lower for kw in notify_keywords)
    
    if not is_notify:
        return None
    
    # Kiểm tra có nhắc đến nhóm không
    group_indicators = ["nhóm", "nhom", "group"]
    has_group = any(kw in text_lower for kw in group_indicators)
    
    if not has_group:
        return None
    
    # Check pattern "đến các nhóm đã kết nối" hoặc "đến tất cả nhóm" → target all groups
    all_groups_patterns = [
        r'đến\s+(các\s+)?nhóm\s+đã\s+kết\s+nối',
        r'den\s+(cac\s+)?nhom\s+da\s+ket\s+noi',
        r'đến\s+tất\s+cả\s+(các\s+)?nhóm',
        r'den\s+tat\s+ca\s+(cac\s+)?nhom',
        r'cho\s+tất\s+cả\s+(các\s+)?nhóm',
        r'vào\s+tất\s+cả\s+(các\s+)?nhóm',
    ]
    
    is_all_groups = any(re.search(pattern, text_lower) for pattern in all_groups_patterns)
    
    # Tìm tất cả các nhóm được nhắc đến
    target_groups = []
    
    if is_all_groups:
        target_groups = ["all"]
    else:
        for group_name, group_key in GROUP_NAME_MAPPING.items():
            if group_name in text_lower:
                if group_key not in target_groups:
                    target_groups.append(group_key)
    
    if not target_groups:
        return None
    
    # Trích xuất nội dung tin nhắn
    # Pattern 1: "gửi tin nhắn này: [content] đến nhóm"
    match_pattern1 = re.search(r'gửi\s+tin\s+nhắn\s+(này\s*)?[:\s]+(.+?)\s+(đến|vào|cho)\s+(các\s+)?nhóm', text_clean, re.IGNORECASE | re.DOTALL)
    if match_pattern1:
        message_content = match_pattern1.group(2).strip()
        if message_content:
            return {
                "type": "custom_message",
                "message": message_content,
                "target_groups": target_groups
            }
    
    # Pattern 2: "thông báo [content] vào nhóm"
    group_start_patterns = [
        r'vào\s+nhóm', r'vao\s+nhom',
        r'cho\s+nhóm', r'cho\s+nhom',
        r'đến\s+nhóm', r'den\s+nhom',
        r'đến\s+các\s+nhóm', r'den\s+cac\s+nhom',
        r'tới\s+nhóm', r'toi\s+nhom',
        r'vào\s+group', r'cho\s+group',
        r'đến\s+tất\s+cả', r'vào\s+tất\s+cả',
        r'cho\s+tất\s+cả',
    ]
    
    message_content = text_clean
    for pattern in group_start_patterns:
        match = re.search(pattern, text_clean_lower)
        if match:
            message_content = text_clean[:match.start()].strip()
            break
    
    # Loại bỏ các keyword thông báo ở đầu (check từ dài đến ngắn)
    notify_keywords_sorted = sorted(notify_keywords, key=len, reverse=True)
    for kw in notify_keywords_sorted:
        if message_content.lower().startswith(kw):
            message_content = message_content[len(kw):].strip()
            break
    
    # Loại bỏ "là", ":", "này" ở đầu nếu có
    message_content = re.sub(r'^(là|này|:|\s)+', '', message_content, flags=re.IGNORECASE).strip()
    
    if not message_content:
        return None
    
    return {
        "type": "custom_message",
        "message": message_content,
        "target_groups": target_groups
    }


async def handle_custom_message_to_groups(params: Dict) -> str:
    """Xử lý gửi tin nhắn tùy chỉnh đến nhiều nhóm"""
    message = params.get("message", "")
    target_groups = params.get("target_groups", [])
    
    if not message:
        return "❌ Không tìm thấy nội dung tin nhắn"
    
    if not target_groups:
        return "❌ Không tìm thấy nhóm đích"
    
    # Format tin nhắn với emoji
    formatted_message = f"📢 **THÔNG BÁO**\n\n{message}"
    
    results = []
    success_count = 0
    
    for group_key in target_groups:
        if group_key == "all":
            # Gửi đến tất cả nhóm
            for gk, chat_id in GROUP_CHATS.items():
                try:
                    await send_lark_message(chat_id, formatted_message)
                    results.append(f"✅ {GROUP_DISPLAY_NAMES.get(gk, gk)}")
                    success_count += 1
                except Exception as e:
                    results.append(f"❌ {GROUP_DISPLAY_NAMES.get(gk, gk)}: {str(e)}")
            break
        else:
            chat_id = GROUP_CHATS.get(group_key)
            if chat_id:
                try:
                    await send_lark_message(chat_id, formatted_message)
                    results.append(f"✅ {GROUP_DISPLAY_NAMES.get(group_key, group_key)}")
                    success_count += 1
                except Exception as e:
                    results.append(f"❌ {GROUP_DISPLAY_NAMES.get(group_key, group_key)}: {str(e)}")
            else:
                results.append(f"❌ Không tìm thấy nhóm: {group_key}")
    
    return f"📤 Đã gửi thông báo đến {success_count}/{len(results)} nhóm:\n" + "\n".join(results)

def check_send_report_command(text: str) -> Optional[Dict]:
    """
    Kiểm tra xem có phải lệnh gửi báo cáo đến nhóm không
    Ví dụ: "gửi báo cáo KPI cho nhóm MKT Team"
    """
    text_lower = text.lower()
    
    # Kiểm tra có phải lệnh gửi không
    send_keywords = ["gửi", "gui", "send", "broadcast", "gởi"]
    if not any(kw in text_lower for kw in send_keywords):
        return None
    
    # Kiểm tra có nhắc đến nhóm không
    group_keywords = ["nhóm", "nhom", "group", "cho"]
    if not any(kw in text_lower for kw in group_keywords):
        return None
    
    # Xác định loại báo cáo
    report_type = "dashboard"  # Mặc định
    if "kpi" in text_lower:
        report_type = "kpi"
    elif "top koc" in text_lower or "doanh số" in text_lower:
        report_type = "top_koc"
    elif "cảnh báo" in text_lower or "canh bao" in text_lower or "warning" in text_lower:
        report_type = "canh_bao"
    elif "dashboard" in text_lower or "tình hình" in text_lower:
        report_type = "dashboard"
    
    # Xác định tháng
    month = datetime.now().month
    month_match = re.search(r'tháng\s*(\d+)|thang\s*(\d+)', text_lower)
    if month_match:
        month = int(month_match.group(1) or month_match.group(2))
    
    # Xác định nhóm
    target_group = None
    for group_name, group_key in GROUP_NAME_MAPPING.items():
        if group_name in text_lower:
            target_group = group_key
            break
    
    if not target_group:
        return None
    
    return {
        "report_type": report_type,
        "month": month,
        "target_group": target_group
    }


async def handle_send_report_to_group(params: Dict) -> str:
    """Xử lý gửi báo cáo đến nhóm"""
    from lark_base import generate_dashboard_summary
    from report_generator import generate_dashboard_report_text
    
    report_type = params.get("report_type", "dashboard")
    month = params.get("month", datetime.now().month)
    target_group = params.get("target_group")
    
    try:
        # Lấy dữ liệu Dashboard
        dashboard_data = await generate_dashboard_summary(month=month)
        
        # Sinh báo cáo
        if report_type == "kpi":
            report = await generate_dashboard_report_text(dashboard_data, report_type="kpi_nhan_su")
        elif report_type == "top_koc":
            report = await generate_dashboard_report_text(dashboard_data, report_type="top_koc")
        elif report_type == "canh_bao":
            report = await generate_dashboard_report_text(dashboard_data, report_type="canh_bao")
        else:
            report = await generate_dashboard_report_text(dashboard_data, report_type="full")
        
        # Gửi đến nhóm
        if target_group == "all":
            # Gửi đến tất cả nhóm
            results = []
            for group_name, chat_id in GROUP_CHATS.items():
                try:
                    await send_lark_message(chat_id, report)
                    results.append(f"✅ {group_name}")
                except Exception as e:
                    results.append(f"❌ {group_name}: {str(e)}")
            
            return f"📤 Đã gửi báo cáo {report_type.upper()} tháng {month} đến:\n" + "\n".join(results)
        else:
            # Gửi đến 1 nhóm cụ thể
            chat_id = GROUP_CHATS.get(target_group)
            if not chat_id:
                return f"❌ Không tìm thấy nhóm '{target_group}'. Các nhóm có sẵn: {', '.join(GROUP_CHATS.keys())}"
            
            await send_lark_message(chat_id, report)
            return f"✅ Đã gửi báo cáo {report_type.upper()} tháng {month} đến nhóm {target_group}"
    
    except Exception as e:
        return f"❌ Lỗi khi gửi báo cáo: {str(e)}"


# ============ MESSAGE HANDLER ============
async def process_jarvis_query(text: str, chat_id: str = "") -> str:
    """
    Xử lý câu hỏi và trả về response
    """
    print(f"🔍 Processing query: {text}")
    
    # 0a. Kiểm tra lệnh ghi nhớ (notes)
    note_result = check_note_command(text)
    print(f"📝 Note check result: {note_result}")
    if note_result:
        return await handle_note_command(note_result, chat_id=chat_id)
    
    # 0b. Kiểm tra lệnh gửi tin nhắn tùy chỉnh đến nhóm
    custom_msg_result = check_custom_message_command(text)
    if custom_msg_result:
        return await handle_custom_message_to_groups(custom_msg_result)
    
    # 0c. Kiểm tra lệnh gửi báo cáo đến nhóm
    send_report_result = check_send_report_command(text)
    if send_report_result:
        return await handle_send_report_to_group(send_report_result)
    
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
            group_by = intent_result.get("group_by", "product")  # "product" hoặc "brand"
            product_filter = intent_result.get("product_filter")  # "box_qua", "nuoc_hoa", etc.
            
            # Lấy dữ liệu từ Lark Base
            summary_data = await generate_koc_summary(
                month=month, 
                week=week, 
                group_by=group_by,
                product_filter=product_filter
            )
            
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
        
        elif intent == INTENT_DASHBOARD:
            month = intent_result.get("month")
            week = intent_result.get("week")
            report_type = intent_result.get("report_type", "full")
            nhan_su = intent_result.get("nhan_su")  # Tên nhân sự cụ thể (nếu có)
            
            # Lấy dữ liệu Dashboard
            dashboard_data = await generate_dashboard_summary(month=month, week=week)
            
            # Sinh báo cáo
            report = await generate_dashboard_report_text(
                dashboard_data, 
                report_type=report_type,
                nhan_su_filter=nhan_su
            )
            return report
        
        elif intent == INTENT_GPT_CHAT:
            # Gọi ChatGPT trực tiếp
            question = intent_result.get("question", "")
            if not question:
                return "❓ Bạn muốn hỏi gì? Hãy thử: \"GPT: câu hỏi của bạn\""
            
            response = await chat_with_gpt(question)
            return f"🤖 GPT trả lời:\n\n{response}"
        
        else:
            # Unknown intent
            return intent_result.get("suggestion", 
                "🤖 Xin chào! Tôi là Jarvis.\n\n"
                "Bạn có thể hỏi tôi về:\n"
                "• Báo cáo KOC: \"Tóm tắt KOC tháng 12\"\n"
                "• Tình hình booking: \"Cập nhật tình hình booking tháng 12\"\n"
                "• KPI cá nhân: \"KPI của Mai tháng 12\"\n"
                "• Gửi báo cáo: \"Gửi báo cáo KPI cho nhóm MKT Team\"\n"
                "• Thông báo: \"Gửi tin nhắn này: [nội dung] đến các nhóm đã kết nối\"\n"
                "• Ghi nhớ: \"Note: công việc deadline 2 ngày\"\n"
                "• Xem notes: \"Tổng hợp note\"\n"
                "• Hỏi GPT: \"GPT: câu hỏi bất kỳ\"\n\n"
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
    
    # Mark as processed IMMEDIATELY to prevent duplicate processing
    if message_id:
        mark_message_processed(message_id)
    
    chat_id = message.get("chat_id")
    chat_type = message.get("chat_type")  # "p2p" (1-1) hoặc "group"
    message_type = message.get("message_type")
    content_str = message.get("content", "{}")
    
    # LOG CHI TIẾT để lấy chat_id của các nhóm
    print(f"📍 Chat ID: {chat_id}")
    print(f"📍 Chat Type: {chat_type}")
    
    # Auto-register nhóm khi nhận được tin nhắn
    if chat_type == "group":
        register_group(chat_id, chat_type)
        print(f"📍 Group registered: {chat_id}")
    
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
    
    # Process query (truyền chat_id để lưu vào notes nếu cần)
    response_text = await process_jarvis_query(clean_text or text, chat_id=chat_id)
    
    # Double check before sending (in case of race condition)
    if message_id and is_message_processed(message_id):
        # Already sent by another process
        pass
    
    # Send response
    await send_lark_message(chat_id, response_text)
    print(f"✅ Response sent")


# ============ REMINDER SCHEDULER ============

async def check_and_send_reminders():
    """Check notes sắp đến deadline và gửi reminder"""
    print(f"🔔 Running reminder check at {datetime.now()}")
    
    manager = get_notes_manager()
    
    # Lấy notes có deadline trong 1 ngày tới
    due_soon = manager.get_notes_due_soon(days=1)
    
    # Lấy notes đã quá hạn
    overdue = manager.get_overdue_notes()
    
    reminders_sent = 0
    
    # Gửi reminder cho notes sắp đến deadline
    for note in due_soon:
        if note.chat_id:
            days_left = (note.deadline - datetime.now()).days
            hours_left = int((note.deadline - datetime.now()).total_seconds() / 3600)
            
            if days_left <= 0:
                time_str = f"còn {hours_left} giờ" if hours_left > 0 else "HẾT HẠN HÔM NAY"
            else:
                time_str = f"còn {days_left} ngày"
            
            reminder_msg = (
                f"🔔 **NHẮC NHỞ DEADLINE**\n\n"
                f"📝 #{note.id}: {note.content}\n"
                f"⏰ Deadline: {time_str}\n\n"
                f"💡 Reply \"Xong #{note.id}\" khi hoàn thành"
            )
            
            try:
                await send_lark_message(note.chat_id, reminder_msg)
                manager.mark_reminder_sent(note.id)
                reminders_sent += 1
                print(f"✅ Sent reminder for note #{note.id}")
            except Exception as e:
                print(f"❌ Failed to send reminder for note #{note.id}: {e}")
    
    # Gửi cảnh báo cho notes đã quá hạn
    for note in overdue:
        if note.chat_id and not note.reminder_sent:
            overdue_days = (datetime.now() - note.deadline).days
            
            warning_msg = (
                f"⚠️ **CẢNH BÁO QUÁ HẠN**\n\n"
                f"📝 #{note.id}: {note.content}\n"
                f"❌ Đã quá hạn {overdue_days} ngày!\n\n"
                f"💡 Reply \"Xong #{note.id}\" khi hoàn thành"
            )
            
            try:
                await send_lark_message(note.chat_id, warning_msg)
                manager.mark_reminder_sent(note.id)
                reminders_sent += 1
                print(f"✅ Sent overdue warning for note #{note.id}")
            except Exception as e:
                print(f"❌ Failed to send warning for note #{note.id}: {e}")
    
    print(f"🔔 Reminder check complete. Sent {reminders_sent} reminders.")
    return reminders_sent


@app.on_event("startup")
async def startup_event():
    """Khởi động scheduler khi app start"""
    # Schedule reminder check hàng ngày vào giờ cố định
    scheduler.add_job(
        check_and_send_reminders,
        CronTrigger(hour=REMINDER_HOUR, minute=REMINDER_MINUTE, timezone=TIMEZONE),
        id="daily_reminder",
        replace_existing=True
    )
    
    # Thêm job check mỗi 6 giờ để bắt những deadline gấp (0h, 6h, 12h, 18h)
    scheduler.add_job(
        check_and_send_reminders,
        CronTrigger(hour="0,6,12,18", minute=0, timezone=TIMEZONE),
        id="periodic_reminder",
        replace_existing=True
    )
    
    scheduler.start()
    print(f"🚀 Scheduler started. Daily reminder at {REMINDER_HOUR}:{REMINDER_MINUTE:02d} {TIMEZONE}")


@app.on_event("shutdown")
async def shutdown_event():
    """Dừng scheduler khi app shutdown"""
    scheduler.shutdown()
    print("🛑 Scheduler stopped")


# ============ HEALTH & TEST ============
@app.get("/")
async def root():
    return {"status": "ok", "message": "Jarvis is running 🤖", "version": "5.2"}

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


@app.get("/debug/dashboard-koc-fields")
async def debug_dashboard_koc_fields():
    """Debug: Xem TẤT CẢ field names từ Dashboard KOC table"""
    from lark_base import get_all_records, DASHBOARD_KOC_BASE
    
    records = await get_all_records(
        app_token=DASHBOARD_KOC_BASE["app_token"],
        table_id=DASHBOARD_KOC_BASE["table_id"],
        max_records=10
    )
    
    if not records:
        return {"error": "No records found", "table_id": DASHBOARD_KOC_BASE["table_id"]}
    
    # Collect all field names
    all_fields = set()
    for record in records:
        fields = record.get("fields", {})
        all_fields.update(fields.keys())
    
    # Get sample records with values
    sample_records = []
    for record in records[:3]:
        fields = record.get("fields", {})
        sample_records.append(fields)
    
    return {
        "table_id": DASHBOARD_KOC_BASE["table_id"],
        "total_records": len(records),
        "total_fields": len(all_fields),
        "all_field_names": sorted(list(all_fields)),
        "sample_records": sample_records
    }


@app.get("/debug/list-tables")
async def debug_list_tables():
    """Debug: Liệt kê TẤT CẢ tables trong Booking Base"""
    from lark_base import get_tenant_access_token, LARK_API_BASE, BOOKING_BASE
    import httpx
    
    token = await get_tenant_access_token()
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{LARK_API_BASE}/bitable/v1/apps/{BOOKING_BASE['app_token']}/tables",
            headers={"Authorization": f"Bearer {token}"}
        )
        data = response.json()
    
    if data.get("code") != 0:
        return {"error": data.get("msg"), "raw": data}
    
    tables = data.get("data", {}).get("items", [])
    
    return {
        "app_token": BOOKING_BASE["app_token"],
        "total_tables": len(tables),
        "tables": [
            {
                "table_id": t.get("table_id"),
                "name": t.get("name"),
                "revision": t.get("revision")
            }
            for t in tables
        ]
    }


@app.get("/debug/table-fields/{table_id}")
async def debug_table_fields(table_id: str):
    """Debug: Xem fields và sample data của một table cụ thể"""
    from lark_base import get_all_records, BOOKING_BASE
    
    try:
        records = await get_all_records(
            app_token=BOOKING_BASE["app_token"],
            table_id=table_id,
            max_records=5
        )
        
        if not records:
            return {"error": "No records found", "table_id": table_id}
        
        # Collect all field names
        all_fields = set()
        for record in records:
            fields = record.get("fields", {})
            all_fields.update(fields.keys())
        
        # Get sample records
        sample_records = []
        for record in records[:3]:
            fields = record.get("fields", {})
            # Truncate long values
            truncated = {}
            for k, v in fields.items():
                if isinstance(v, str) and len(v) > 100:
                    truncated[k] = v[:100] + "..."
                elif isinstance(v, list) and len(v) > 3:
                    truncated[k] = v[:3]
                else:
                    truncated[k] = v
            sample_records.append(truncated)
        
        return {
            "table_id": table_id,
            "total_records_fetched": len(records),
            "total_fields": len(all_fields),
            "all_field_names": sorted(list(all_fields)),
            "sample_records": sample_records
        }
    except Exception as e:
        return {"error": str(e), "table_id": table_id}


@app.get("/debug/dashboard/{month}")
async def debug_dashboard(month: int):
    """Debug: Test Dashboard data cho một tháng cụ thể"""
    from lark_base import generate_dashboard_summary
    
    try:
        data = await generate_dashboard_summary(month=month)
        return data
    except Exception as e:
        return {"error": str(e), "month": month}


@app.get("/debug/dashboard-raw/{month}")
async def debug_dashboard_raw(month: int):
    """Debug: Xem raw data từ từng bảng Dashboard cho tháng cụ thể"""
    from lark_base import (
        get_dashboard_thang_records, 
        get_lien_he_records, 
        get_doanh_thu_koc_records
    )
    
    try:
        dashboard_records = await get_dashboard_thang_records(month=month)
        lien_he_records = await get_lien_he_records(month=month)
        doanh_thu_records = await get_doanh_thu_koc_records(month=month)
        
        return {
            "month": month,
            "dashboard_thang": {
                "count": len(dashboard_records),
                "sample": dashboard_records[:5] if dashboard_records else []
            },
            "lien_he": {
                "count": len(lien_he_records),
                "sample": lien_he_records[:5] if lien_he_records else []
            },
            "doanh_thu_koc": {
                "count": len(doanh_thu_records),
                "sample": doanh_thu_records[:5] if doanh_thu_records else []
            }
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc(), "month": month}


# ============ GROUP MANAGEMENT ============

@app.get("/groups")
async def list_groups():
    """Xem danh sách nhóm đã đăng ký và đã phát hiện"""
    return {
        "registered_groups": GROUP_CHATS,
        "discovered_groups": get_discovered_groups()
    }


# ============ NOTES MANAGEMENT ============

@app.get("/notes")
async def view_notes():
    """Xem tất cả notes"""
    manager = get_notes_manager()
    summary = manager.get_summary()
    
    result = {}
    for category, notes in summary.items():
        result[category] = [n.to_dict() for n in notes]
    
    return {
        "total": sum(len(notes) for notes in summary.values()),
        "notes": result
    }


@app.get("/notes/add")
async def add_note_api(content: str):
    """Thêm note qua API"""
    manager = get_notes_manager()
    note = manager.add_note(content)
    return {
        "success": True,
        "note": note.to_dict()
    }


@app.get("/notes/done/{note_id}")
async def mark_note_done(note_id: int):
    """Đánh dấu note hoàn thành"""
    manager = get_notes_manager()
    note = manager.get_note(note_id)
    
    if not note:
        return {"success": False, "error": f"Note #{note_id} không tồn tại"}
    
    manager.mark_done(note_id)
    return {"success": True, "message": f"Đã hoàn thành #{note_id}"}


@app.get("/notes/delete/{note_id}")
async def delete_note_api(note_id: int):
    """Xóa note"""
    manager = get_notes_manager()
    note = manager.get_note(note_id)
    
    if not note:
        return {"success": False, "error": f"Note #{note_id} không tồn tại"}
    
    manager.delete_note(note_id)
    return {"success": True, "message": f"Đã xóa #{note_id}"}


# ============ REMINDER ENDPOINTS ============

@app.get("/reminders/check")
async def check_reminders():
    """Trigger kiểm tra và gửi reminders thủ công"""
    try:
        count = await check_and_send_reminders()
        return {
            "success": True,
            "reminders_sent": count,
            "checked_at": datetime.now().isoformat()
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/reminders/status")
async def reminder_status():
    """Xem trạng thái scheduler và notes sắp deadline"""
    manager = get_notes_manager()
    
    due_soon = manager.get_notes_due_soon(days=1)
    overdue = manager.get_overdue_notes()
    
    # Lấy thông tin jobs
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None
        })
    
    return {
        "scheduler_running": scheduler.running,
        "reminder_time": f"{REMINDER_HOUR}:{REMINDER_MINUTE:02d} {TIMEZONE}",
        "scheduled_jobs": jobs,
        "notes_due_soon": [n.to_dict() for n in due_soon],
        "notes_overdue": [n.to_dict() for n in overdue]
    }


@app.get("/reminders/config")
async def reminder_config(hour: int = None, minute: int = None):
    """Xem/thay đổi config reminder (chỉ trong session này)"""
    global REMINDER_HOUR, REMINDER_MINUTE
    
    changed = False
    if hour is not None and 0 <= hour <= 23:
        REMINDER_HOUR = hour
        changed = True
    if minute is not None and 0 <= minute <= 59:
        REMINDER_MINUTE = minute
        changed = True
    
    if changed:
        # Reschedule job với giờ mới
        scheduler.reschedule_job(
            "daily_reminder",
            trigger=CronTrigger(hour=REMINDER_HOUR, minute=REMINDER_MINUTE, timezone=TIMEZONE)
        )
    
    return {
        "reminder_hour": REMINDER_HOUR,
        "reminder_minute": REMINDER_MINUTE,
        "timezone": TIMEZONE,
        "changed": changed
    }


@app.get("/send-to-group/{chat_id}")
async def send_to_group(chat_id: str, message: str = "Test message from Jarvis"):
    """Gửi tin nhắn đến một nhóm cụ thể"""
    try:
        await send_lark_message(chat_id, message)
        return {"success": True, "chat_id": chat_id, "message": message}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/send-report/{report_type}/{chat_id}")
async def send_report_to_group(report_type: str, chat_id: str, month: int = None):
    """
    Gửi báo cáo đến nhóm
    report_type: "kpi", "dashboard", "top_koc", "canh_bao"
    """
    from lark_base import generate_dashboard_summary
    from report_generator import generate_dashboard_report_text
    from datetime import datetime
    
    if month is None:
        month = datetime.now().month
    
    try:
        # Lấy dữ liệu Dashboard
        dashboard_data = await generate_dashboard_summary(month=month)
        
        # Sinh báo cáo theo loại
        if report_type == "kpi":
            report = await generate_dashboard_report_text(dashboard_data, report_type="kpi_nhan_su")
        elif report_type == "top_koc":
            report = await generate_dashboard_report_text(dashboard_data, report_type="top_koc")
        elif report_type == "canh_bao":
            report = await generate_dashboard_report_text(dashboard_data, report_type="canh_bao")
        else:  # dashboard - full report
            report = await generate_dashboard_report_text(dashboard_data, report_type="full")
        
        # Gửi đến nhóm
        await send_lark_message(chat_id, report)
        
        return {
            "success": True,
            "chat_id": chat_id,
            "report_type": report_type,
            "month": month
        }
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }


@app.get("/broadcast-report/{report_type}")
async def broadcast_report(report_type: str, month: int = None):
    """
    Gửi báo cáo đến TẤT CẢ nhóm đã đăng ký
    report_type: "kpi", "dashboard", "top_koc", "canh_bao"
    """
    from lark_base import generate_dashboard_summary
    from report_generator import generate_dashboard_report_text
    from datetime import datetime
    
    if month is None:
        month = datetime.now().month
    
    if not GROUP_CHATS:
        return {"success": False, "error": "Chưa có nhóm nào được đăng ký"}
    
    try:
        # Lấy dữ liệu Dashboard
        dashboard_data = await generate_dashboard_summary(month=month)
        
        # Sinh báo cáo
        if report_type == "kpi":
            report = await generate_dashboard_report_text(dashboard_data, report_type="kpi_nhan_su")
        elif report_type == "top_koc":
            report = await generate_dashboard_report_text(dashboard_data, report_type="top_koc")
        elif report_type == "canh_bao":
            report = await generate_dashboard_report_text(dashboard_data, report_type="canh_bao")
        else:
            report = await generate_dashboard_report_text(dashboard_data, report_type="full")
        
        # Gửi đến tất cả nhóm
        results = {}
        for group_name, chat_id in GROUP_CHATS.items():
            try:
                await send_lark_message(chat_id, report)
                results[group_name] = "✅ Sent"
            except Exception as e:
                results[group_name] = f"❌ Error: {str(e)}"
        
        return {
            "success": True,
            "report_type": report_type,
            "month": month,
            "results": results
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============ RUN ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
