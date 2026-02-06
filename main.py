"""
Jarvis - Lark AI Report Assistant
Main application with all modules integrated
Version 5.7.25 - Fixed NotesManager scheduler integration

Changelog v5.7.12:
- Fixed AttributeError in check_and_send_reminders scheduler job
- Added SchedulerNotesManager class for cross-chat note reminders
- Added get_all_notes() function in lark_base.py
- Added Note dataclass for scheduler compatibility
- Fixed async/await calls in reminder scheduler
"""
import os
import json
import base64
import hashlib
import time
import re
import asyncio
import threading
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
from intent_classifier import classify_intent, INTENT_KOC_REPORT, INTENT_CHENG_REPORT, INTENT_CONTENT_CALENDAR, INTENT_TASK_SUMMARY, INTENT_GENERAL_SUMMARY, INTENT_DASHBOARD, INTENT_UNKNOWN
from lark_base import generate_koc_summary, generate_content_calendar, generate_task_summary, generate_dashboard_summary, test_connection
from report_generator import generate_koc_report_text, generate_content_calendar_text, generate_task_summary_text, generate_general_summary_text, generate_dashboard_report_text, generate_cheng_report_text
from notes_manager import check_note_command, handle_note_command, get_notes_manager
from daily_booking_report import send_daily_booking_reports, BOOKING_GROUP_CHAT_ID
from contract_generator import generate_contract, parse_lark_record_to_contract_data
from google_drive_client import get_drive_client
from seeding_notification import (
    get_tiktok_thumbnail,
    upload_image_to_lark,
    send_seeding_card,
    send_seeding_card_via_webhook,
    send_seeding_notification,
    GAP_2H_CHAT_ID,
    SEEDING_WEBHOOK_URL
)

# ============ SCHEDULER CONFIG ============
REMINDER_HOUR = int(os.getenv("REMINDER_HOUR", "9"))
REMINDER_MINUTE = int(os.getenv("REMINDER_MINUTE", "0"))
TIMEZONE = "Asia/Ho_Chi_Minh"

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
    "booking_sep": "oc_7356c37c72891ea5314507d78ab2e937",
    "digital": "oc_f2a9dc7332c3f08e6090c19166a4b47d",
    "leader_marketing": "oc_d178ad558d36919731fb0bdf26a79eb7",
    "mkt_sale_kho": "oc_b503e285cdfb700b72b72fca3f1f316c",
    "mkt_team": "oc_768c8b7b8680299e36fe889de677578a",
}

TIKTOK_ALERT_CHAT_ID = os.getenv("TIKTOK_ALERT_CHAT_ID", GROUP_CHATS.get("digital", ""))

# ============ CONTRACT GENERATOR CONFIG ============
CONTRACT_BASE_APP_TOKEN = os.getenv("CONTRACT_BASE_APP_TOKEN", "XfHGbvXrRaK1zcsTZ1zl5QR3ghf")
CONTRACT_BASE_TABLE_ID = os.getenv("CONTRACT_BASE_TABLE_ID", "tblndkVZ6Dao620Y")

_discovered_groups = {}

def register_group(chat_id: str, chat_type: str, group_name: str = None):
    if chat_type == "group" and chat_id:
        _discovered_groups[chat_id] = {
            "name": group_name or "Unknown",
            "discovered_at": time.time()
        }

def get_discovered_groups():
    return _discovered_groups

_processed_messages = {}
MESSAGE_CACHE_TTL = 600

def is_message_processed(message_id: str) -> bool:
    now = time.time()
    expired = [mid for mid, ts in _processed_messages.items() if now - ts > MESSAGE_CACHE_TTL]
    for mid in expired:
        del _processed_messages[mid]
    if message_id in _processed_messages:
        return True
    return False

def mark_message_processed(message_id: str):
    _processed_messages[message_id] = time.time()
    if message_id in _processed_messages:
        return True
    _processed_messages[message_id] = time.time()
    return False

app = FastAPI(title="Jarvis - Lark AI Report Assistant")

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

async def get_tenant_access_token() -> str:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            TENANT_ACCESS_TOKEN_URL,
            json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET}
        )
        data = response.json()
        if data.get("code") == 0:
            return data.get("tenant_access_token")
        else:
            raise Exception(f"Failed to get token: {data}")

async def send_lark_message(chat_id: str, text: str):
    token = await get_tenant_access_token()
    async with httpx.AsyncClient() as client:
        response = await client.post(
            SEND_MESSAGE_URL,
            params={"receive_id_type": "chat_id"},
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"receive_id": chat_id, "msg_type": "text", "content": json.dumps({"text": text})},
            timeout=30.0
        )
        return response.json()

GROUP_NAME_MAPPING = {
    "booking": "booking_sep", "booking sếp": "booking_sep", "booking sep": "booking_sep",
    "booking k sếp": "booking_sep", "booking k sep": "booking_sep",
    "digital": "digital", "cheng digital": "digital",
    "leader": "leader_marketing", "leader marketing": "leader_marketing", "leader mkt": "leader_marketing",
    "sale": "mkt_sale_kho", "kho": "mkt_sale_kho", "mkt sale": "mkt_sale_kho",
    "mkt x sale": "mkt_sale_kho", "sale x kho": "mkt_sale_kho",
    "mkt team": "mkt_team", "marketing team": "mkt_team",
    "tất cả": "all", "tat ca": "all", "all": "all",
}

GROUP_DISPLAY_NAMES = {
    "booking_sep": "Kalle - Booking k sếp",
    "digital": "Cheng & Kalle | Digital",
    "leader_marketing": "Kalle - Leader Marketing",
    "mkt_sale_kho": "Cheng & Kalle | MKT x Sale x Kho",
    "mkt_team": "Kalle - MKT Team",
}

def check_custom_message_command(text: str) -> Optional[Dict]:
    text_lower = text.lower()
    text_clean = re.sub(r'^@?jarvis\s*', '', text, flags=re.IGNORECASE).strip()
    text_clean_lower = text_clean.lower()
    
    note_keywords = ["note:", "note ", "ghi nhớ:", "ghi nhớ ", "ghi nho:", "todo:", "công việc:", "cong viec:"]
    if any(text_clean_lower.startswith(kw) for kw in note_keywords):
        return None
    
    notify_keywords = ["thông báo", "thong bao", "gửi tin", "gui tin", "nhắn tin", "nhan tin", "notify", "gởi tin", "gửi tin nhắn", "gui tin nhan"]
    is_notify = any(kw in text_lower for kw in notify_keywords)
    
    if not is_notify:
        return None
    
    group_indicators = ["nhóm", "nhom", "group"]
    has_group = any(kw in text_lower for kw in group_indicators)
    
    if not has_group:
        return None
    
    all_groups_patterns = [
        r'đến\s+(các\s+)?nhóm\s+đã\s+kết\s+nối', r'den\s+(cac\s+)?nhom\s+da\s+ket\s+noi',
        r'đến\s+tất\s+cả\s+(các\s+)?nhóm', r'den\s+tat\s+ca\s+(cac\s+)?nhom',
        r'cho\s+tất\s+cả\s+(các\s+)?nhóm', r'vào\s+tất\s+cả\s+(các\s+)?nhóm',
    ]
    is_all_groups = any(re.search(pattern, text_lower) for pattern in all_groups_patterns)
    
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
    
    match_pattern1 = re.search(r'gửi\s+tin\s+nhắn\s+(này\s*)?[:\s]+(.+?)\s+(đến|vào|cho)\s+(các\s+)?nhóm', text_clean, re.IGNORECASE | re.DOTALL)
    if match_pattern1:
        message_content = match_pattern1.group(2).strip()
        if message_content:
            return {"type": "custom_message", "message": message_content, "target_groups": target_groups}
    
    group_start_patterns = [
        r'vào\s+nhóm', r'vao\s+nhom', r'cho\s+nhóm', r'cho\s+nhom',
        r'đến\s+nhóm', r'den\s+nhom', r'đến\s+các\s+nhóm', r'den\s+cac\s+nhom',
        r'tới\s+nhóm', r'toi\s+nhom', r'vào\s+group', r'cho\s+group',
        r'đến\s+tất\s+cả', r'vào\s+tất\s+cả', r'cho\s+tất\s+cả',
    ]
    
    message_content = text_clean
    for pattern in group_start_patterns:
        match = re.search(pattern, text_clean_lower)
        if match:
            message_content = text_clean[:match.start()].strip()
            break
    
    notify_keywords_sorted = sorted(notify_keywords, key=len, reverse=True)
    for kw in notify_keywords_sorted:
        if message_content.lower().startswith(kw):
            message_content = message_content[len(kw):].strip()
            break
    
    message_content = re.sub(r'^(là|này|:|\s)+', '', message_content, flags=re.IGNORECASE).strip()
    
    if not message_content:
        return None
    
    return {"type": "custom_message", "message": message_content, "target_groups": target_groups}


async def handle_custom_message_to_groups(params: Dict) -> str:
    message = params.get("message", "")
    target_groups = params.get("target_groups", [])
    
    if not message:
        return "❌ Không tìm thấy nội dung tin nhắn"
    if not target_groups:
        return "❌ Không tìm thấy nhóm đích"
    
    formatted_message = f"📢 **THÔNG BÁO**\n\n{message}"
    results = []
    success_count = 0
    
    for group_key in target_groups:
        if group_key == "all":
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
    text_lower = text.lower()
    send_keywords = ["gửi", "gui", "send", "broadcast", "gởi"]
    if not any(kw in text_lower for kw in send_keywords):
        return None
    group_keywords = ["nhóm", "nhom", "group", "cho"]
    if not any(kw in text_lower for kw in group_keywords):
        return None
    
    report_type = "dashboard"
    if "kpi" in text_lower:
        report_type = "kpi"
    elif "top koc" in text_lower or "doanh số" in text_lower:
        report_type = "top_koc"
    elif "cảnh báo" in text_lower or "canh bao" in text_lower or "warning" in text_lower:
        report_type = "canh_bao"
    
    month = datetime.now().month
    month_match = re.search(r'tháng\s*(\d+)|thang\s*(\d+)', text_lower)
    if month_match:
        month = int(month_match.group(1) or month_match.group(2))
    
    target_group = None
    for group_name, group_key in GROUP_NAME_MAPPING.items():
        if group_name in text_lower:
            target_group = group_key
            break
    
    if not target_group:
        return None
    
    return {"report_type": report_type, "month": month, "target_group": target_group}


async def handle_send_report_to_group(params: Dict) -> str:
    from lark_base import generate_dashboard_summary
    from report_generator import generate_dashboard_report_text
    
    report_type = params.get("report_type", "dashboard")
    month = params.get("month", datetime.now().month)
    target_group = params.get("target_group")
    
    try:
        dashboard_data = await generate_dashboard_summary(month=month)
        
        if report_type == "kpi":
            report = await generate_dashboard_report_text(dashboard_data, report_type="kpi_nhan_su")
        elif report_type == "top_koc":
            report = await generate_dashboard_report_text(dashboard_data, report_type="top_koc")
        elif report_type == "canh_bao":
            report = await generate_dashboard_report_text(dashboard_data, report_type="canh_bao")
        else:
            report = await generate_dashboard_report_text(dashboard_data, report_type="full")
        
        if target_group == "all":
            results = []
            for group_name, chat_id in GROUP_CHATS.items():
                try:
                    await send_lark_message(chat_id, report)
                    results.append(f"✅ {group_name}")
                except Exception as e:
                    results.append(f"❌ {group_name}: {str(e)}")
            return f"📤 Đã gửi báo cáo {report_type.upper()} tháng {month} đến:\n" + "\n".join(results)
        else:
            chat_id = GROUP_CHATS.get(target_group)
            if not chat_id:
                return f"❌ Không tìm thấy nhóm '{target_group}'. Các nhóm có sẵn: {', '.join(GROUP_CHATS.keys())}"
            await send_lark_message(chat_id, report)
            return f"✅ Đã gửi báo cáo {report_type.upper()} tháng {month} đến nhóm {target_group}"
    except Exception as e:
        return f"❌ Lỗi khi gửi báo cáo: {str(e)}"


def get_bot_introduction() -> str:
    return """🤖 **JARVIS - TRỢ LÝ ẢO MARKETING**

Xin chào! Tôi là Jarvis, trợ lý ảo hỗ trợ team Marketing.

━━━━━━━━━━━━━━━━━━━━━━
📊 **BÁO CÁO KOC - KALLE**
━━━━━━━━━━━━━━━━━━━━━━
• `Báo cáo KOC tháng 12` - Báo cáo Kalle
• `KPI của Mai tháng 12` - KPI cá nhân KALLE

━━━━━━━━━━━━━━━━━━━━━━
🧴 **BÁO CÁO KOC - CHENG**
━━━━━━━━━━━━━━━━━━━━━━
• `Báo cáo Cheng tháng 12` - Báo cáo Cheng
• `KPI của Phương tháng 12` - KPI cá nhân CHENG
• `KPI của Linh` - KPI cá nhân CHENG

━━━━━━━━━━━━━━━━━━━━━━
💰 **TIKTOK ADS**
━━━━━━━━━━━━━━━━━━━━━━
• `TKQC` hoặc `Dư nợ TikTok Ads` - Xem dư nợ

━━━━━━━━━━━━━━━━━━━━━━
📝 **GHI NHỚ (NOTES)**
━━━━━━━━━━━━━━━━━━━━━━
• `Ghi nhớ: [nội dung]` - Tạo ghi nhớ
• `Xem ghi nhớ` - Xem tất cả

Gõ `help` để xem lại hướng dẫn này 🚀"""


async def process_jarvis_query(text: str, chat_id: str = "") -> str:
    print(f"🔍 Processing query: {text}")
    
    help_keywords = ['help', 'hướng dẫn', 'huong dan', 'giới thiệu', 'gioi thieu', 
                     'chức năng', 'chuc nang', 'lệnh', 'lenh', 'commands', 'menu']
    if any(kw in text.lower() for kw in help_keywords):
        return get_bot_introduction()
    
    note_result = check_note_command(text)
    print(f"📝 Note check result: {note_result}")
    if note_result:
        return await handle_note_command(note_result, chat_id=chat_id)
    
    custom_msg_result = check_custom_message_command(text)
    if custom_msg_result:
        return await handle_custom_message_to_groups(custom_msg_result)
    
    send_report_result = check_send_report_command(text)
    if send_report_result:
        return await handle_send_report_to_group(send_report_result)
    
    from tiktok_ads_crawler import is_tiktok_ads_query, get_spending_data, format_spending_report
    
    if any(kw in text.lower() for kw in ['check tkqc', 'kiểm tra tkqc', 'kiem tra tkqc']):
        if TIKTOK_ALERT_CHAT_ID:
            await check_tiktok_ads_warning()
            return "✅ Đã kiểm tra dư nợ TikTok Ads. Nếu > 85% sẽ gửi cảnh báo vào nhóm Digital!"
        else:
            return "❌ Chưa cấu hình nhóm nhận cảnh báo TikTok Ads"
    
    if is_tiktok_ads_query(text):
        force_refresh = any(kw in text.lower() for kw in ['refresh', 'làm mới', 'lam moi', 'update', 'cập nhật'])
        result = await get_spending_data(force_refresh=force_refresh)
        return format_spending_report(result)
    
    intent_result = classify_intent(text)
    intent = intent_result.get("intent")
    
    print(f"🎯 Intent: {intent}")
    print(f"📊 Params: {intent_result}")
    
    try:
        if intent == INTENT_KOC_REPORT:
            month = intent_result.get("month")
            week = intent_result.get("week")
            group_by = intent_result.get("group_by", "product")
            product_filter = intent_result.get("product_filter")
            
            summary_data = await generate_koc_summary(month=month, week=week, group_by=group_by, product_filter=product_filter)
            report = await generate_koc_report_text(summary_data)
            return report
        
        elif intent == INTENT_CHENG_REPORT:
            # ===== FIXED v5.7.2: Support nhan_su_filter for CHENG =====
            month = intent_result.get("month")
            week = intent_result.get("week")
            report_type = intent_result.get("report_type", "full")
            nhan_su = intent_result.get("nhan_su")  # Tên nhân sự cụ thể (nếu có)
            
            from lark_base import generate_cheng_koc_summary
            summary_data = await generate_cheng_koc_summary(month=month, week=week)
            
            # Sinh báo cáo với nhan_su_filter nếu có
            report = await generate_cheng_report_text(summary_data, report_type=report_type, nhan_su_filter=nhan_su)
            return report
        
        elif intent == INTENT_CONTENT_CALENDAR:
            start_date = intent_result.get("start_date")
            end_date = intent_result.get("end_date")
            team = intent_result.get("team_filter")
            vi_tri = intent_result.get("vi_tri_filter")
            month = intent_result.get("month")
            
            calendar_data = await generate_content_calendar(start_date=start_date, end_date=end_date, month=month, team=team, vi_tri=vi_tri)
            report = await generate_content_calendar_text(calendar_data)
            return report
        
        elif intent == INTENT_TASK_SUMMARY:
            month = intent_result.get("month")
            vi_tri = intent_result.get("vi_tri")
            
            task_data = await generate_task_summary(month=month, vi_tri=vi_tri)
            report = await generate_task_summary_text(task_data)
            return report
        
        elif intent == INTENT_GENERAL_SUMMARY:
            month = intent_result.get("month")
            week = intent_result.get("week")
            
            koc_data = await generate_koc_summary(month=month, week=week)
            content_data = await generate_content_calendar(month=month)
            report = await generate_general_summary_text(koc_data, content_data)
            return report
        
        elif intent == INTENT_DASHBOARD:
            month = intent_result.get("month")
            week = intent_result.get("week")
            report_type = intent_result.get("report_type", "full")
            nhan_su = intent_result.get("nhan_su")
            
            dashboard_data = await generate_dashboard_summary(month=month, week=week)
            report = await generate_dashboard_report_text(dashboard_data, report_type=report_type, nhan_su_filter=nhan_su)
            return report
        
        else:
            return intent_result.get("suggestion", 
                "🤖 Xin chào! Tôi là Jarvis.\n\n"
                "Bạn có thể hỏi tôi về:\n"
                "• Báo cáo KOC: \"Tóm tắt KOC tháng 12\"\n"
                "• KPI KALLE: \"KPI của Mai tháng 12\"\n"
                "• KPI CHENG: \"KPI của Phương tháng 12\"\n"
                "• Dư nợ TikTok Ads: \"TKQC\"\n"
                "• Ghi nhớ: \"Note: công việc deadline 2 ngày\"\n\n"
                "Hãy thử hỏi tôi nhé! 😊"
            )
    
    except Exception as e:
        print(f"❌ Error processing query: {e}")
        import traceback
        traceback.print_exc()
        return f"❌ Có lỗi xảy ra khi xử lý yêu cầu: {str(e)}\n\nVui lòng thử lại sau."


@app.post("/lark/events")
async def handle_lark_events(request: Request):
    body = await request.json()
    print(f"📩 Received raw event")
    
    if "encrypt" in body and decryptor:
        try:
            decrypted_str = decryptor.decrypt(body["encrypt"])
            body = json.loads(decrypted_str)
            print(f"🔓 Decrypted event type: {body.get('header', {}).get('event_type', body.get('type'))}")
        except Exception as e:
            print(f"❌ Decrypt failed: {e}")
            raise HTTPException(status_code=400, detail="Decrypt failed")
    
    if "challenge" in body:
        print("✅ URL Verification challenge received")
        return JSONResponse(content={"challenge": body["challenge"]})
    
    header = body.get("header", {})
    event = body.get("event", {})
    
    token = header.get("token")
    if token and token != LARK_VERIFICATION_TOKEN:
        print(f"❌ Token verification failed")
        raise HTTPException(status_code=401, detail="Invalid token")
    
    event_type = header.get("event_type")
    
    if event_type == "im.message.receive_v1":
        await handle_message_event(event)
    
    return JSONResponse(content={"code": 0, "msg": "success"})


async def handle_message_event(event: dict):
    message = event.get("message", {})
    message_id = message.get("message_id")
    
    if message_id and is_message_processed(message_id):
        print(f"⏭️ Duplicate message {message_id}, skipping")
        return
    
    if message_id:
        mark_message_processed(message_id)
    
    chat_id = message.get("chat_id")
    chat_type = message.get("chat_type")
    message_type = message.get("message_type")
    content_str = message.get("content", "{}")
    
    print(f"📍 Chat ID: {chat_id}")
    print(f"📍 Chat Type: {chat_type}")
    
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
    
    mentions = message.get("mentions", [])
    is_mentioned = len(mentions) > 0 or "jarvis" in text.lower()
    
    if chat_type == "group" and not is_mentioned:
        print("⏭️ Not mentioned, skipping")
        return
    
    clean_text = text
    for mention in mentions:
        mention_key = mention.get("key", "")
        clean_text = clean_text.replace(mention_key, "").strip()
    
    response_text = await process_jarvis_query(clean_text or text, chat_id=chat_id)
    
    if message_id and is_message_processed(message_id):
        pass
    
    await send_lark_message(chat_id, response_text)
    print(f"✅ Response sent")


async def check_and_send_reminders():
    print(f"🔔 Running reminder check at {datetime.now()}")
    manager = get_notes_manager()
    due_soon = await manager.get_notes_due_soon(days=1)
    overdue = await manager.get_overdue_notes()
    reminders_sent = 0
    
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


_tiktok_warning_sent_today = {"date": None, "sent": False}

async def check_tiktok_ads_warning():
    """
    Scheduled check for TikTok Ads spending
    - Always sends a status report every 3 days
    - Sends urgent warning if ratio >= threshold
    """
    if not TIKTOK_ALERT_CHAT_ID:
        return
    
    print("🔍 Scheduled TikTok Ads check running...")
    
    try:
        from tiktok_ads_crawler import get_spending_data, WARNING_THRESHOLD, format_spending_report
        result = await get_spending_data(force_refresh=True)
        
        if result.get("success"):
            spending = result.get("spending", 0)
            credit_limit = result.get("credit_limit", 1)
            ratio = (spending / credit_limit * 100) if credit_limit > 0 else 0
            
            print(f"📊 Current ratio: {ratio:.1f}% (threshold: {WARNING_THRESHOLD}%)")
            
            if ratio >= WARNING_THRESHOLD:
                # Urgent warning
                warning_msg = (
                    f"🚨 **CẢNH BÁO DƯ NỢ TIKTOK ADS**\n\n"
                    f"⚠️ Đã sử dụng **{ratio:.1f}%** hạn mức!\n\n"
                    f"💳 Dư nợ: **{spending:,.0f}** / {credit_limit:,.0f} VND\n"
                    f"📅 Cập nhật: {result.get('updated_at', 'N/A')}\n\n"
                    f"💡 Vui lòng chuẩn bị thanh toán hoặc tăng hạn mức."
                )
                await send_lark_message(TIKTOK_ALERT_CHAT_ID, warning_msg)
                print(f"🚨 Sent URGENT TikTok warning (ratio: {ratio:.1f}%)")
            else:
                # Regular status report (every 3 days)
                status_msg = (
                    f"📊 **Báo cáo định kỳ TikTok Ads**\n\n"
                    f"💳 Dư nợ hiện tại: **{spending:,.0f}** VND\n"
                    f"📈 Đã sử dụng: **{ratio:.1f}%** hạn mức\n"
                    f"🏦 Hạn mức: {credit_limit:,.0f} VND\n"
                    f"📅 Cập nhật: {result.get('updated_at', 'N/A')}\n\n"
                    f"✅ Mức sử dụng an toàn (< {WARNING_THRESHOLD}%)"
                )
                await send_lark_message(TIKTOK_ALERT_CHAT_ID, status_msg)
                print(f"📊 Sent periodic TikTok status report (ratio: {ratio:.1f}%)")
        else:
            error_msg = f"❌ Không thể kiểm tra TikTok Ads: {result.get('error')}"
            await send_lark_message(TIKTOK_ALERT_CHAT_ID, error_msg)
            print(f"❌ Failed to get TikTok data: {result.get('error')}")
    except Exception as e:
        print(f"❌ TikTok warning check error: {e}")
        import traceback
        traceback.print_exc()


@app.on_event("startup")
async def startup_event():
    # Job 1: Nhắc nhở daily (theo config REMINDER_HOUR)
    scheduler.add_job(
        check_and_send_reminders,
        CronTrigger(hour=REMINDER_HOUR, minute=REMINDER_MINUTE, timezone=TIMEZONE),
        id="daily_reminder",
        replace_existing=True
    )
    
    # Job 2: Nhắc nhở định kỳ (0h, 6h, 12h, 18h)
    scheduler.add_job(
        check_and_send_reminders,
        CronTrigger(hour="0,6,12,18", minute=0, timezone=TIMEZONE),
        id="periodic_reminder",
        replace_existing=True
    )
    
    # Job 3: v5.7.22 - Check TikTok Ads (9h VÀ 17h hàng ngày)
    if TIKTOK_ALERT_CHAT_ID:
        scheduler.add_job(
            check_tiktok_ads_warning,
            CronTrigger(hour="9,17", minute=0, timezone=TIMEZONE),
            id="tiktok_ads_warning",
            replace_existing=True
        )
        print(f"📊 TikTok Ads scheduled check: Everyday at 9:00 AM and 17:00 PM")
    
    # Job 4: v5.7.25 - Daily Booking Report (9h hàng ngày, kết thúc 14/2/2026)
    scheduler.add_job(
        send_daily_booking_reports,
        CronTrigger(hour=9, minute=0, timezone=TIMEZONE, end_date="2026-02-14"),
        id="daily_booking_report",
        replace_existing=True
    )
    print(f"📊 Daily Booking Report scheduled: Everyday at 9:00 AM (until 2026-02-14)")
        
    scheduler.start()
    print(f"🚀 Scheduler started. Daily reminder at 9:00 & 17:00 {TIMEZONE}")
    
    # Pre-initialize Google Drive client (avoid cold start on first contract)
    try:
        drive = get_drive_client()
        if drive:
            print("✅ Google Drive client pre-initialized")
    except Exception as e:
        print(f"⚠️ Google Drive init skipped: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()
    print("🛑 Scheduler stopped")


@app.get("/")
async def root():
    return {"status": "ok", "message": "Jarvis is running 🤖", "version": "5.7.25"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/test/base")
async def test_base():
    success = await test_connection()
    return {"success": success}

@app.get("/test/intent")
async def test_intent(q: str = "tóm tắt KOC tháng 12"):
    result = classify_intent(q)
    return result

@app.get("/groups")
async def list_groups():
    return {"registered_groups": GROUP_CHATS, "discovered_groups": get_discovered_groups()}


@app.get("/test/daily-booking")
async def test_daily_booking():
    """Test endpoint để trigger daily booking report manually"""
    try:
        await send_daily_booking_reports()
        return {"status": "ok", "message": "Daily booking reports sent"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ============ SEEDING NOTIFICATION ENDPOINTS ============

@app.post("/webhook/seeding")
async def handle_seeding_webhook(request: Request):
    """
    Webhook nhận thông báo từ Lark Base Automation
    Hỗ trợ cả JSON và form-urlencoded
    """
    try:
        content_type = request.headers.get("content-type", "")
        
        # Parse body theo content type
        if "application/json" in content_type:
            try:
                body = await request.json()
            except:
                # Nếu JSON invalid, thử parse như text
                raw_body = await request.body()
                body_text = raw_body.decode('utf-8')
                print(f"⚠️ Invalid JSON, trying to parse as text: {body_text[:200]}")
                # Thử extract thủ công
                body = extract_fields_from_text(body_text)
        else:
            body = await request.form()
            body = dict(body)
        
        print(f"📩 Seeding webhook received: {body}")
        
        # Parse data - hỗ trợ nhiều format field name khác nhau
        data = body
        
        koc_name = (
            data.get("koc_name") or 
            data.get("Tên KOC") or 
            data.get("ten_koc") or 
            data.get("Tên KOC/Influencer") or
            ""
        )
        
        channel_id = (
            data.get("channel_id") or 
            data.get("ID kênh") or 
            data.get("id_kenh") or 
            ""
        )
        
        tiktok_url = (
            data.get("tiktok_url") or 
            data.get("Link air video") or 
            data.get("link_air_video") or 
            data.get("Link air bài") or
            data.get("link_air_bai") or
            ""
        )
        
        product = (
            data.get("product") or 
            data.get("Sản phẩm") or 
            data.get("san_pham") or 
            data.get("Tên sản phẩm") or
            ""
        )
        
        # Phân loại sản phẩm (optional - append vào product nếu có)
        product_type = (
            data.get("product_type") or
            data.get("Phân loại sản phẩm") or
            data.get("Phân loại sp") or
            data.get("phan_loai_sp") or
            ""
        )
        
        if product_type and product:
            product = f"{product} - {product_type}"
        elif product_type:
            product = product_type
        
        record_url = (
            data.get("record_url") or 
            data.get("Link bản ghi") or 
            data.get("link_ban_ghi") or 
            None
        )
        
        # Validate
        if not tiktok_url:
            return {"success": False, "error": "Missing tiktok_url"}
        
        # Check webhook URL hoặc chat_id
        if not SEEDING_WEBHOOK_URL and not GAP_2H_CHAT_ID:
            return {"success": False, "error": "Missing SEEDING_WEBHOOK_URL or GAP_2H_CHAT_ID environment variable"}
        
        # Gửi notification (với thumbnail)
        result = await send_seeding_notification(
            koc_name=koc_name,
            channel_id=channel_id,
            tiktok_url=tiktok_url,
            product=product,
            get_token_func=get_tenant_access_token,
            webhook_url=SEEDING_WEBHOOK_URL,
            chat_id=GAP_2H_CHAT_ID,
            record_url=record_url,
            with_thumbnail=True  # Bật thumbnail - crawl từ TikTok và upload lên Lark
        )
        
        return result
        
    except Exception as e:
        import traceback
        print(f"❌ Seeding webhook error: {e}")
        print(traceback.format_exc())
        return {"success": False, "error": str(e)}


def extract_fields_from_text(text: str) -> dict:
    """
    Extract fields từ text khi JSON bị lỗi do ký tự đặc biệt
    """
    import re
    result = {}
    
    # Pattern để tìm key-value pairs
    patterns = [
        (r'"tiktok_url"\s*:\s*"([^"]*)"', 'tiktok_url'),
        (r'"product"\s*:\s*"([^"]*)"', 'product'),
        (r'"product_type"\s*:\s*"(.*?)"(?=\s*[,}])', 'product_type'),
        (r'"koc_name"\s*:\s*"([^"]*)"', 'koc_name'),
        (r'"channel_id"\s*:\s*"([^"]*)"', 'channel_id'),
    ]
    
    for pattern, key in patterns:
        match = re.search(pattern, text, re.DOTALL)
        if match:
            result[key] = match.group(1)
    
    return result


@app.post("/test/seeding-card")
async def test_seeding_card(
    tiktok_url: str = "https://www.tiktok.com/@hainguoiiunhau9/video/7602154659691777288",
    koc_name: str = "Hai người yêu nhau 💕",
    channel_id: str = "hainguoiiunhau9",
    product: str = "Box quà YÊU - Ủ+Xịt+Tinh dầu"
):
    """
    Endpoint test gửi seeding card với thumbnail
    Dùng để test trước khi setup automation
    """
    if not SEEDING_WEBHOOK_URL and not GAP_2H_CHAT_ID:
        return {
            "success": False, 
            "error": "Missing SEEDING_WEBHOOK_URL environment variable. Please set it in Railway."
        }
    
    result = await send_seeding_notification(
        koc_name=koc_name,
        channel_id=channel_id,
        tiktok_url=tiktok_url,
        product=product,
        get_token_func=get_tenant_access_token,  # Cần để upload thumbnail
        webhook_url=SEEDING_WEBHOOK_URL,
        chat_id=GAP_2H_CHAT_ID,
        record_url=None,
        with_thumbnail=True  # Bật thumbnail
    )
    
    return result


@app.get("/test/tiktok-thumbnail")
async def test_tiktok_thumbnail(
    url: str = "https://www.tiktok.com/@hainguoiiunhau9/video/7602154659691777288"
):
    """Test crawl thumbnail từ TikTok URL"""
    thumbnail = await get_tiktok_thumbnail(url)
    return {
        "tiktok_url": url,
        "thumbnail_url": thumbnail,
        "success": thumbnail is not None
    }


@app.post("/send/seeding")
async def send_seeding_manual(
    koc_name: str,
    channel_id: str,
    tiktok_url: str,
    product: str,
    record_url: str = None,
    webhook_url: str = None,
    with_thumbnail: bool = True
):
    """
    API gửi seeding card thủ công với thumbnail
    Có thể chỉ định webhook_url khác nếu cần
    """
    target_webhook = webhook_url or SEEDING_WEBHOOK_URL
    if not target_webhook:
        return {"success": False, "error": "Missing webhook_url"}
    
    result = await send_seeding_notification(
        koc_name=koc_name,
        channel_id=channel_id,
        tiktok_url=tiktok_url,
        product=product,
        get_token_func=get_tenant_access_token,
        webhook_url=target_webhook,
        record_url=record_url,
        with_thumbnail=with_thumbnail
    )
    
    return result


# ============ CONTRACT GENERATOR ENDPOINTS ============

@app.post("/webhook/contract")
async def handle_contract_webhook(request: Request):
    """
    Webhook nhận yêu cầu tạo hợp đồng KOC từ Lark Base Automation.
    Returns immediately, processes in background thread for reliability.
    """
    try:
        content_type = request.headers.get("content-type", "")
        
        if "application/json" in content_type:
            body = await request.json()
        else:
            raw = await request.body()
            body = json.loads(raw.decode("utf-8"))
        
        print(f"📩 Contract webhook received: {json.dumps(body, ensure_ascii=False)[:500]}")
        
        record_id = body.get("record_id", "")
        fields = body.get("fields", {})
        
        if not fields and not record_id:
            fields = body
            record_id = body.get("record_id", body.get("Record ID", ""))
        
        if not record_id:
            return {"success": False, "error": "Missing record_id"}
        
        ho_ten = fields.get("Họ và Tên Bên B", "")
        if not ho_ten:
            return {"success": False, "error": "Missing required field: Họ và Tên Bên B"}
        
        # === Fire-and-forget: process in background THREAD (sync, reliable) ===
        thread = threading.Thread(
            target=_process_contract_sync,
            args=(record_id, fields),
            daemon=True,
        )
        thread.start()
        print(f"🚀 Background thread started for: {ho_ten}")
        
        return {
            "success": True,
            "record_id": record_id,
            "koc_name": ho_ten,
            "status": "processing",
        }
    
    except Exception as e:
        print(f"❌ Contract webhook error: {e}")
        return {"success": False, "error": str(e)}


def _process_contract_sync(record_id: str, fields: dict):
    """
    Background thread: generate contract → upload Drive → update Lark.
    Hoàn toàn SYNC (pattern giống script filter_koc_thang10.py đã chạy OK).
    """
    import sys
    import traceback
    from lark_contract import update_record as lark_update_record

    try:
        print(f"🔄 [BG] Starting contract generation for record: {record_id}")
        sys.stdout.flush()

        ho_ten = fields.get("Họ và Tên Bên B", "")
        contract_data = parse_lark_record_to_contract_data(fields)
        id_koc = contract_data.get("id_koc", "N/A")
        print(f"📝 [BG] Generating contract for: {ho_ten} (ID KOC: {id_koc})")
        sys.stdout.flush()

        # 1. Generate Word file
        output_path = generate_contract(contract_data)
        print(f"✅ [BG] Contract file created: {output_path}")
        sys.stdout.flush()

        # 2. Upload to Google Drive (sync)
        drive_client = get_drive_client()
        if not drive_client:
            print("❌ [BG] Google Drive not configured")
            lark_update_record(
                CONTRACT_BASE_APP_TOKEN, CONTRACT_BASE_TABLE_ID, record_id,
                {"Status": "Failed"}
            )
            return

        today = datetime.now().strftime("%d-%m-%Y")
        file_name = f"{id_koc} {today}" if id_koc != "N/A" else f"HD_KOC {today}"

        drive_result = drive_client.upload_docx_as_gdoc(
            file_path=output_path,
            file_name=file_name,
            set_permission=False,
        )
        file_id = drive_result["file_id"]
        gdoc_link = drive_result["web_view_link"]
        print(f"📤 [BG] Uploaded to Google Drive: {gdoc_link}")
        sys.stdout.flush()

        # 3. Set permission (sync)
        drive_client.set_anyone_edit(file_id)
        print(f"🔓 [BG] Permission set for file: {file_id}")

        # 4. Update Lark record (sync - dùng lark_contract.py)
        update_fields = {
            "Status": "Done",
            "Kết quả": {
                "text": gdoc_link,
                "link": gdoc_link,
            },
        }
        print(f"🔧 [BG] Updating Lark: app={CONTRACT_BASE_APP_TOKEN}, table={CONTRACT_BASE_TABLE_ID}, record={record_id}")
        print(f"🔧 [BG] Fields: {update_fields}")
        sys.stdout.flush()

        result = lark_update_record(
            CONTRACT_BASE_APP_TOKEN,
            CONTRACT_BASE_TABLE_ID,
            record_id,
            update_fields,
        )
        print(f"📋 [BG] Lark update result: {result}")
        sys.stdout.flush()

        # 5. Cleanup temp files
        try:
            os.remove(output_path)
            os.rmdir(os.path.dirname(output_path))
        except:
            pass

        print(f"✅ [BG] Contract done: {ho_ten} → {gdoc_link}")
        sys.stdout.flush()

    except Exception as e:
        print(f"❌ [BG] Contract error: {e}")
        print(traceback.format_exc())
        sys.stdout.flush()
        try:
            from lark_contract import update_record as lark_update_record_err
            lark_update_record_err(
                CONTRACT_BASE_APP_TOKEN, CONTRACT_BASE_TABLE_ID, record_id,
                {"Status": "Failed"}
            )
        except:
            pass


@app.post("/test/lark-update/{record_id}")
async def test_lark_update(record_id: str):
    """Test Lark Base update only - dùng lark_contract.py (sync)."""
    from lark_contract import update_record as lark_update_record
    
    print(f"🧪 Testing Lark update: app={CONTRACT_BASE_APP_TOKEN}, table={CONTRACT_BASE_TABLE_ID}, record={record_id}")
    
    try:
        result = lark_update_record(
            CONTRACT_BASE_APP_TOKEN,
            CONTRACT_BASE_TABLE_ID,
            record_id,
            {"Status": "Test"}
        )
        print(f"🧪 Result: {result}")
        return {"success": True, "result": result, "app_token": CONTRACT_BASE_APP_TOKEN, "table_id": CONTRACT_BASE_TABLE_ID}
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"success": False, "error": str(e), "app_token": CONTRACT_BASE_APP_TOKEN, "table_id": CONTRACT_BASE_TABLE_ID}


@app.post("/test/contract")
async def test_contract_generate():
    """
    Test endpoint - generate contract with sample data (không update Lark).
    """
    sample_data = {
        "id_koc": "TEST001",
        "ho_ten": "Nguyễn Văn Test",
        "dia_chi": "123 Đường Test, Quận 1, TP.HCM",
        "mst": "0123456789",
        "sdt": "0901234567",
        "cccd": "001099012345",
        "cccd_ngay_cap": "15/06/2021",
        "cccd_noi_cap": "Cục CS QLHC về TTXH",
        "gmail": "test@gmail.com",
        "stk": "1234567890",
    }
    
    try:
        output_path = generate_contract(sample_data)
        
        # Try upload to Google Drive if configured
        drive_client = get_drive_client()
        gdoc_link = None
        if drive_client:
            drive_result = drive_client.upload_docx_as_gdoc(
                file_path=output_path,
                file_name="TEST_HD_KOC_Nguyen_Van_Test",
            )
            gdoc_link = drive_result["web_view_link"]
        
        return {
            "success": True,
            "local_path": output_path,
            "google_docs_link": gdoc_link,
            "drive_configured": drive_client is not None,
        }
    except Exception as e:
        import traceback
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
