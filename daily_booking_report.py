"""
Daily Booking Report Module
v5.7.25 - Feature: Thông báo cá nhân + Báo cáo team hàng ngày

Features:
1. Thông báo cá nhân: Video air hôm qua, KPI cần hôm nay (cộng dồn)
2. Báo cáo team: Tình hình booking tháng hiện tại gửi vào nhóm 9h
"""

import os
from datetime import datetime, timedelta
import pytz

# Vietnam timezone
VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
from typing import Dict, List, Optional
import httpx

# ============ STAFF MAPPING ============
# Map từ User ID Lark -> Tên trong Dashboard/Booking
BOOKING_STAFF = {
    "7ad1g7b9": {
        "name": "Nguyễn Như Mai",
        "dashboard_names": [
            "Nguyễn Như Mai - PR Bookingg",  # Dashboard (2 chữ g)
            "Nguyễn Như Mai - PR Booking",   # Booking table (1 chữ g)
            "Nguyễn Như Mai"
        ],
        "short_name": "Mai"
    },
    "bbc7c22c": {
        "name": "Lê Thuỳ Dương",
        "dashboard_names": [
            "Lê Thuỳ Dương",
            "Lê Thuỳ Dương (vịt)",
            "Lê Thuỳ Dương (vịt) - PR Booking"  # Booking table
        ],
        "short_name": "Dương"
    },
    "f987ca64": {
        "name": "Quân Nguyễn",
        "dashboard_names": [
            "Quân Nguyễn - Booking Remote",
            "Quân Nguyễn"
        ],
        "short_name": "Quân"
    },
    "29545d7g": {
        "name": "Châu Đặng",
        "dashboard_names": [
            "Bảo Châu - Booking Remote",
            "Châu Đặng - Booking Remote",
            "Châu Đặng"
        ],
        "short_name": "Châu"
    },
    "2ccaca2e": {
        "name": "Huyền Trang",
        "dashboard_names": [
            "Huyền Trang - Booking Kalle Remote",
            "Huyền Trang"
        ],
        "short_name": "Trang"
    },
    "9g9634c2": {
        "name": "Phương Thảo",
        "dashboard_names": [
            "Phương Thảo - Intern Booking",
            "Phương Thảo - Intern booking",  # Lark Base format (chữ b thường)
            "Phương Thảo intern booking",
            "Phương Thảo Intern Booking",
            "Phương Thảo"
        ],
        "short_name": "Thảo"
    },
    "d2294g8g": {
        "name": "Trà Mi",
        "dashboard_names": [
            "Trà Mi - Intern Booking",
            "Trà Mi"
        ],
        "short_name": "Mi"
    },
}

# ============ CONFIG ============
BOOKING_GROUP_CHAT_ID = "oc_7356c37c72891ea5314507d78ab2e937"  # Nhóm "Kalle - Booking k sếp"
DAILY_KPI = 2  # KPI: 2 video/ngày
DAILY_DEAL_KPI = 5  # KPI: 5 deal/ngày

# Schedule end date (stop sending reports after this date)
SCHEDULE_END_DATE = datetime(2026, 2, 14, 0, 0, 0, tzinfo=VN_TZ)

# Lark API
LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")
LARK_API_BASE = "https://open.larksuite.com/open-apis"


async def get_tenant_access_token() -> str:
    """Get Lark tenant access token"""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{LARK_API_BASE}/auth/v3/tenant_access_token/internal",
            json={
                "app_id": LARK_APP_ID,
                "app_secret": LARK_APP_SECRET
            }
        )
        data = response.json()
        return data.get("tenant_access_token", "")


async def send_message_to_user(user_id: str, message: str) -> bool:
    """
    Gửi tin nhắn đến user qua user_id
    """
    try:
        token = await get_tenant_access_token()
        
        # Escape special characters for JSON
        escaped_message = message.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{LARK_API_BASE}/im/v1/messages?receive_id_type=user_id",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={
                    "receive_id": user_id,
                    "msg_type": "text",
                    "content": f'{{"text": "{escaped_message}"}}'
                }
            )
            result = response.json()
            
            if result.get("code") == 0:
                print(f"✅ Sent message to user {user_id}")
                return True
            else:
                print(f"❌ Failed to send to {user_id}: {result}")
                return False
    except Exception as e:
        print(f"❌ Error sending to {user_id}: {e}")
        return False


async def send_message_to_chat(chat_id: str, message: str) -> bool:
    """Gửi tin nhắn đến group chat"""
    try:
        token = await get_tenant_access_token()
        
        # Escape special characters for JSON
        escaped_message = message.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{LARK_API_BASE}/im/v1/messages?receive_id_type=chat_id",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={
                    "receive_id": chat_id,
                    "msg_type": "text",
                    "content": f'{{"text": "{escaped_message}"}}'
                }
            )
            result = response.json()
            
            if result.get("code") == 0:
                print(f"✅ Sent message to chat {chat_id}")
                return True
            else:
                print(f"❌ Failed to send to chat {chat_id}: {result}")
                return False
    except Exception as e:
        print(f"❌ Error sending to chat {chat_id}: {e}")
        return False


def normalize_name(name: str) -> str:
    """Normalize tên để so sánh"""
    import re
    # Lấy phần trước " - "
    name = name.split(" - ")[0].strip()
    # Loại bỏ phần trong ngoặc
    name = re.sub(r'\s*\([^)]*\)', '', name).strip()
    return name.lower()


def match_staff_name(search_name: str, dashboard_names: List[str]) -> bool:
    """
    Kiểm tra tên có match không
    """
    search_normalized = normalize_name(search_name)
    
    for dash_name in dashboard_names:
        dash_normalized = normalize_name(dash_name)
        
        # Exact match
        if search_normalized == dash_normalized:
            return True
        
        # Partial match - có ít nhất 2 từ giống nhau
        search_parts = set(search_normalized.split())
        dash_parts = set(dash_normalized.split())
        common = search_parts & dash_parts
        if len(common) >= 2:
            return True
        
        # Contains match
        if search_normalized in dash_normalized or dash_normalized in search_normalized:
            return True
    
    return False


def normalize_staff_name_for_aggregation(raw_name: str) -> str:
    """
    Normalize tên nhân sự để merge các cách viết khác nhau.
    Ví dụ: "Nguyễn Như Mai - PR Booking" và "Nguyễn Như Mai - PR Bookingg" 
           → cùng trả về "Nguyễn Như Mai - PR Bookingg" (tên chuẩn trong Dashboard)
    """
    if not raw_name:
        return raw_name
    
    raw_name = raw_name.strip()
    raw_name_lower = raw_name.lower()
    
    # Tìm trong BOOKING_STAFF xem raw_name có match với dashboard_names không
    for user_id, staff_info in BOOKING_STAFF.items():
        dashboard_names = staff_info.get("dashboard_names", [])
        for db_name in dashboard_names:
            # Case-insensitive comparison
            if raw_name_lower == db_name.lower():
                # Trả về tên đầu tiên (chuẩn) trong dashboard_names
                return dashboard_names[0]
    
    # Nếu không match, trả về tên gốc
    return raw_name


async def get_video_air_by_date(target_date: datetime) -> Dict[str, Dict]:
    """
    Lấy số video air theo ngày từ Booking table
    
    Returns:
        Dict[nhan_su_name, {"count": int, "cart": int, "text": int}]
    """
    from lark_base import get_all_records, BOOKING_BASE, safe_extract_person_name
    
    target_date_str = target_date.strftime("%Y/%m/%d")
    # Also prepare alternate format for comparison
    target_ts_start = int(target_date.replace(hour=0, minute=0, second=0).timestamp() * 1000)
    target_ts_end = int(target_date.replace(hour=23, minute=59, second=59).timestamp() * 1000)
    
    print(f"📅 Getting video air for date: {target_date_str}")
    print(f"📅 Target timestamp range: {target_ts_start} - {target_ts_end}")
    
    # Get all records (without sort to avoid API error)
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=50000  # Increased significantly to ensure we get all records
    )
    
    print(f"📊 Total records from Booking: {len(records)}")
    
    # Debug: Count records with Thời gian air in January 2026
    jan_2026_start = int(datetime(2026, 1, 1).timestamp() * 1000)
    jan_2026_end = int(datetime(2026, 1, 31, 23, 59, 59).timestamp() * 1000)
    jan_2026_records = 0
    jan_2026_with_link = 0
    sample_timestamps = []
    
    for r in records:
        f = r.get("fields", {})
        thoi_gian_air = f.get("Thời gian air")
        link_air = f.get("Link air bài")
        
        if isinstance(thoi_gian_air, (int, float)) and jan_2026_start <= thoi_gian_air <= jan_2026_end:
            jan_2026_records += 1
            if link_air:
                jan_2026_with_link += 1
                # Collect sample timestamps (first 10)
                if len(sample_timestamps) < 10:
                    ts = thoi_gian_air / 1000 if thoi_gian_air > 1e12 else thoi_gian_air
                    date_str = datetime.fromtimestamp(ts, VN_TZ).strftime("%Y/%m/%d %H:%M:%S")
                    sample_timestamps.append(f"{thoi_gian_air} ({date_str})")
    
    print(f"📊 Records with Thời gian air in Jan 2026: {jan_2026_records}")
    print(f"📊 Records with Thời gian air in Jan 2026 AND Link air bài: {jan_2026_with_link}")
    print(f"📊 Sample timestamps from Jan 2026: {sample_timestamps}")
    print(f"📊 Target range: {target_ts_start} - {target_ts_end}")
    
    # Debug: Print all field names from FIRST record (regardless of content)
    if records:
        first_record_fields = records[0].get("fields", {})
        print(f"🔍 ALL field names in first record: {list(first_record_fields.keys())}")
        
        # Find ALL records of Thảo/Châu that have Link air bài
        thao_chau_count = 0
        for r in records:
            f = r.get("fields", {})
            nhan_su_raw = f.get("Nhân sự book")
            link_air = f.get("Link air bài")
            thoi_gian_air = f.get("Thời gian air")
            
            if nhan_su_raw and link_air:
                nhan_su_str = str(nhan_su_raw)
                if "Thảo" in nhan_su_str or "Châu" in nhan_su_str:
                    thao_chau_count += 1
                    if thao_chau_count <= 5:  # Print first 5 records
                        # Parse the date
                        date_str = "N/A"
                        if isinstance(thoi_gian_air, (int, float)) and thoi_gian_air > 0:
                            ts = thoi_gian_air / 1000 if thoi_gian_air > 1e12 else thoi_gian_air
                            date_str = datetime.fromtimestamp(ts, VN_TZ).strftime("%Y/%m/%d")
                        print(f"   🔍 Thảo/Châu record #{thao_chau_count}: Nhân sự={safe_extract_person_name(nhan_su_raw)}, Thời gian air={thoi_gian_air} ({date_str})")
        
        print(f"📊 Total Thảo/Châu records with Link air bài: {thao_chau_count}")
    
    result = {}
    debug_count = 0
    matched_count = 0
    records_with_link_air = 0
    records_with_thoi_gian = 0
    unique_raw_names = set()  # Track all unique raw names on this date
    
    for record in records:
        fields = record.get("fields", {})
        
        # Chỉ đếm records đã air (có Link air bài)
        link_air = fields.get("Link air bài") or fields.get("link_air_bai") or fields.get("Link air")
        if not link_air:
            continue
        
        records_with_link_air += 1
        
        # Check thời gian air - try multiple field names
        thoi_gian_air = fields.get("Thời gian air") or fields.get("thoi_gian_air") or fields.get("Thoi gian air")
        if not thoi_gian_air:
            continue
        
        records_with_thoi_gian += 1
        
        # Parse date - handle multiple formats
        air_date_str = None
        
        if isinstance(thoi_gian_air, (int, float)):
            # Lark Date field returns timestamp in milliseconds
            try:
                # Convert milliseconds to seconds
                ts = thoi_gian_air / 1000 if thoi_gian_air > 1e12 else thoi_gian_air
                # Use Vietnam timezone for conversion
                dt = datetime.fromtimestamp(ts, VN_TZ)
                air_date_str = dt.strftime("%Y/%m/%d")
                
                # Debug: Check if timestamp is in target range
                if target_ts_start <= thoi_gian_air <= target_ts_end:
                    nhan_su_debug = safe_extract_person_name(fields.get("Nhân sự book"))
                    id_koc = fields.get("ID KOC") or fields.get("id_koc") or "N/A"
                    print(f"   ✅ Found match: ID_KOC={id_koc}, Nhân sự={nhan_su_debug}, ts={thoi_gian_air}, date={air_date_str}")
                
            except Exception as e:
                print(f"   ⚠️ Failed to parse timestamp {thoi_gian_air}: {e}")
                continue
        elif isinstance(thoi_gian_air, str):
            # String format - could be "2026/01/30" or "30/01/2026" or timestamp as string
            thoi_gian_air = thoi_gian_air.strip()
            
            # Check if it's a timestamp string
            if thoi_gian_air.isdigit():
                try:
                    ts = int(thoi_gian_air)
                    ts = ts / 1000 if ts > 1e12 else ts
                    dt = datetime.fromtimestamp(ts, VN_TZ)
                    air_date_str = dt.strftime("%Y/%m/%d")
                except:
                    pass
            # Check format YYYY/MM/DD or YYYY-MM-DD
            elif len(thoi_gian_air) >= 10 and (thoi_gian_air[4] == '/' or thoi_gian_air[4] == '-'):
                air_date_str = thoi_gian_air[:10].replace('-', '/')
            # Check format DD/MM/YYYY
            elif len(thoi_gian_air) >= 10 and thoi_gian_air[2] == '/':
                parts = thoi_gian_air[:10].split('/')
                if len(parts) == 3:
                    air_date_str = f"{parts[2]}/{parts[1]}/{parts[0]}"
        
        # Debug: In ra 5 records đầu tiên để xem format
        if debug_count < 5:
            nhan_su_debug = safe_extract_person_name(fields.get("Nhân sự book"))
            print(f"   🔍 Debug record: Nhân sự={nhan_su_debug}, Thời gian air={thoi_gian_air} (type={type(thoi_gian_air).__name__}) -> parsed={air_date_str}")
            debug_count += 1
        
        if not air_date_str:
            continue
            
        # Compare dates
        if air_date_str != target_date_str:
            continue
        
        matched_count += 1
        
        # Lấy nhân sự
        nhan_su = safe_extract_person_name(fields.get("Nhân sự book"))
        if not nhan_su:
            # Debug: Record matched but no Nhân sự book
            id_koc = fields.get("ID KOC") or fields.get("id_koc") or "N/A"
            print(f"   ⚠️ Record matched but no Nhân sự book: ID_KOC={id_koc}, date={air_date_str}")
            continue
        nhan_su = nhan_su.strip()
        
        # Track unique raw names
        unique_raw_names.add(nhan_su)
        
        # Normalize tên để merge các cách viết khác nhau
        # Ví dụ: "PR Bookingg" và "PR Booking" → merge vào cùng 1 entry
        nhan_su_normalized = normalize_staff_name_for_aggregation(nhan_su)
        
        # Debug: Show normalize result for first 15 records
        if matched_count <= 15:
            print(f"   🔄 Normalize: '{nhan_su}' → '{nhan_su_normalized}'")
        
        # Lấy loại content (Cart/Text/Video)
        content_raw = fields.get("Content")
        content_type = "video"  # default
        
        if content_raw:
            # Handle different formats from Lark Select/Option field
            if isinstance(content_raw, str):
                content_type = content_raw.strip().lower()
            elif isinstance(content_raw, list) and len(content_raw) > 0:
                first_item = content_raw[0]
                if isinstance(first_item, str):
                    content_type = first_item.strip().lower()
                elif isinstance(first_item, dict):
                    content_type = first_item.get("text", "video").strip().lower()
            elif isinstance(content_raw, dict):
                content_type = content_raw.get("text", "video").strip().lower()
        
        # Debug: Log content type for matched records
        if matched_count <= 10:
            print(f"   📝 Content debug: raw={content_raw}, parsed={content_type}")
        
        # Aggregate using normalized name
        if nhan_su_normalized not in result:
            result[nhan_su_normalized] = {"count": 0, "cart": 0, "text": 0}
        
        result[nhan_su_normalized]["count"] += 1
        if "cart" in content_type:
            result[nhan_su_normalized]["cart"] += 1
        elif "text" in content_type:
            result[nhan_su_normalized]["text"] += 1
    
    print(f"📊 Records with Link air: {records_with_link_air}")
    print(f"📊 Records with Thời gian air: {records_with_thoi_gian}")
    print(f"📊 Matched records for {target_date_str}: {matched_count}")
    
    # Debug: Show all unique raw names found on this date
    print(f"📋 Unique raw names on {target_date_str}: {list(unique_raw_names)}")
    
    print(f"📊 Video air on {target_date_str}: {result}")
    return result


async def get_deal_by_date(target_date: datetime) -> Dict[str, int]:
    """
    Đếm số deal theo ngày từ Booking table
    Deal được tính khi record có đủ 3 cột:
    1. Link social
    2. Thông tin nhận hàng  
    3. Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)
    
    Filter theo cột "Ngày deal (gần nhất)"
    
    Returns:
        Dict[nhan_su_name, deal_count]
    """
    from lark_base import get_all_records, BOOKING_BASE, safe_extract_person_name
    
    target_date_str = target_date.strftime("%Y/%m/%d")
    target_ts_start = int(target_date.replace(hour=0, minute=0, second=0).timestamp() * 1000)
    target_ts_end = int(target_date.replace(hour=23, minute=59, second=59).timestamp() * 1000)
    
    print(f"📅 Getting deal count for date: {target_date_str}")
    
    # Get all records
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=50000
    )
    
    # Debug: Find and print records that have "Ngày deal" field
    debug_count = 0
    records_with_ngay_deal = 0
    for r in records:
        f = r.get("fields", {})
        ngay_deal = f.get("Ngày deal")
        if ngay_deal:
            records_with_ngay_deal += 1
            if debug_count < 3:
                # Print all fields of this record to see what's available
                print(f"   🔍 Record with Ngày deal:")
                print(f"      - Ngày deal: {ngay_deal}")
                print(f"      - Nhân sự book: {f.get('Nhân sự book')}")
                print(f"      - Link social: {f.get('Link social')}")
                print(f"      - Thông tin nhận hàng: {f.get('Thông tin nhận hàng')}")
                print(f"      - Phân loại sp gửi hàng: {f.get('Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)')}")
                print(f"      - ALL FIELDS: {list(f.keys())}")
                debug_count += 1
    
    print(f"📊 Total records with 'Ngày deal': {records_with_ngay_deal}/{len(records)}")
    
    result = {}
    matched_count = 0
    
    for record in records:
        fields = record.get("fields", {})
        
        # Check 3 required fields
        link_social = fields.get("Link social")
        thong_tin_nhan_hang = fields.get("Thông tin nhận hàng")
        phan_loai_sp_gh = fields.get("Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)")
        
        # All 3 fields must have value
        if not link_social or not thong_tin_nhan_hang or not phan_loai_sp_gh:
            continue
        
        # Get deal date - prioritize "Ngày deal"
        ngay_deal = fields.get("Ngày deal") or fields.get("Ngày deal (gần nhất)")
        if not ngay_deal:
            continue
        
        # Parse date
        deal_date_str = None
        
        if isinstance(ngay_deal, (int, float)):
            try:
                ts = ngay_deal / 1000 if ngay_deal > 1e12 else ngay_deal
                dt = datetime.fromtimestamp(ts, VN_TZ)
                deal_date_str = dt.strftime("%Y/%m/%d")
            except:
                continue
        elif isinstance(ngay_deal, str):
            ngay_deal = ngay_deal.strip()
            if ngay_deal.isdigit():
                try:
                    ts = int(ngay_deal)
                    ts = ts / 1000 if ts > 1e12 else ts
                    dt = datetime.fromtimestamp(ts, VN_TZ)
                    deal_date_str = dt.strftime("%Y/%m/%d")
                except:
                    pass
            elif len(ngay_deal) >= 10 and (ngay_deal[4] == '/' or ngay_deal[4] == '-'):
                deal_date_str = ngay_deal[:10].replace('-', '/')
            elif len(ngay_deal) >= 10 and ngay_deal[2] == '/':
                parts = ngay_deal[:10].split('/')
                if len(parts) == 3:
                    deal_date_str = f"{parts[2]}/{parts[1]}/{parts[0]}"
        
        if not deal_date_str or deal_date_str != target_date_str:
            continue
        
        matched_count += 1
        
        # Get staff name
        nhan_su = safe_extract_person_name(fields.get("Nhân sự book"))
        if not nhan_su:
            continue
        nhan_su = nhan_su.strip()
        
        # Normalize name
        nhan_su_normalized = normalize_staff_name_for_aggregation(nhan_su)
        
        if nhan_su_normalized not in result:
            result[nhan_su_normalized] = 0
        result[nhan_su_normalized] += 1
    
    print(f"📊 Deal count on {target_date_str}: {result}")
    return result


async def get_monthly_deal_stats(target_month: int) -> Dict[str, int]:
    """
    Đếm tổng số deal trong tháng (cộng dồn) theo nhân sự
    
    Returns:
        Dict[nhan_su_name, total_deal_count_in_month]
    """
    from lark_base import get_all_records, BOOKING_BASE, safe_extract_person_name
    
    print(f"📅 Getting monthly deal stats for month: {target_month}")
    
    # Get all records
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=50000
    )
    
    result = {}
    matched_count = 0
    
    for record in records:
        fields = record.get("fields", {})
        
        # Check 3 required fields
        link_social = fields.get("Link social")
        thong_tin_nhan_hang = fields.get("Thông tin nhận hàng")
        phan_loai_sp_gh = fields.get("Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)")
        
        # All 3 fields must have value
        if not link_social or not thong_tin_nhan_hang or not phan_loai_sp_gh:
            continue
        
        # Get deal date - prioritize "Ngày deal"
        ngay_deal = fields.get("Ngày deal") or fields.get("Ngày deal (gần nhất)")
        if not ngay_deal:
            continue
        
        # Parse month from deal date
        deal_month = None
        
        if isinstance(ngay_deal, (int, float)):
            try:
                ts = ngay_deal / 1000 if ngay_deal > 1e12 else ngay_deal
                dt = datetime.fromtimestamp(ts, VN_TZ)
                deal_month = dt.month
            except:
                continue
        elif isinstance(ngay_deal, str):
            ngay_deal = ngay_deal.strip()
            if ngay_deal.isdigit():
                try:
                    ts = int(ngay_deal)
                    ts = ts / 1000 if ts > 1e12 else ts
                    dt = datetime.fromtimestamp(ts, VN_TZ)
                    deal_month = dt.month
                except:
                    pass
            elif len(ngay_deal) >= 10 and (ngay_deal[4] == '/' or ngay_deal[4] == '-'):
                # Format YYYY/MM/DD or YYYY-MM-DD
                try:
                    deal_month = int(ngay_deal[5:7])
                except:
                    pass
            elif len(ngay_deal) >= 10 and ngay_deal[2] == '/':
                # Format DD/MM/YYYY
                try:
                    deal_month = int(ngay_deal[3:5])
                except:
                    pass
        
        if deal_month != target_month:
            continue
        
        matched_count += 1
        
        # Get staff name
        nhan_su = safe_extract_person_name(fields.get("Nhân sự book"))
        if not nhan_su:
            continue
        nhan_su = nhan_su.strip()
        
        # Normalize name
        nhan_su_normalized = normalize_staff_name_for_aggregation(nhan_su)
        
        if nhan_su_normalized not in result:
            result[nhan_su_normalized] = 0
        result[nhan_su_normalized] += 1
    
    print(f"📊 Monthly deal stats (month {target_month}): {result}")
    print(f"📊 Total matched deal records: {matched_count}")
    return result


async def get_monthly_stats() -> Optional[Dict]:
    """
    Lấy thống kê tháng hiện tại từ Dashboard
    """
    from lark_base import generate_dashboard_summary
    
    now = datetime.now(VN_TZ)
    month = now.month
    
    print(f"📊 Getting monthly stats for month {month}...")
    
    data = await generate_dashboard_summary(month=month)
    
    if not data:
        print("❌ Failed to get dashboard data")
        return None
    
    totals = data.get("totals", {})
    staff_list = data.get("staff_list", [])
    
    print(f"📊 Monthly stats: KPI={totals.get('video_kpi', 0)}, Done={totals.get('video_done', 0)}")
    print(f"📊 Staff count: {len(staff_list)}")
    
    return {
        "month": month,
        "total_kpi": totals.get("video_kpi", 0),
        "total_done": totals.get("video_done", 0),
        "total_percent": totals.get("video_percent", 0),
        "content_text": totals.get("content_text", 0),
        "content_cart": totals.get("content_cart", 0),
        "staff_list": staff_list
    }


def get_status_emoji(percent: float) -> str:
    """
    Lấy emoji trạng thái theo phần trăm
    10%-40%: 🔴
    41%-69%: 🟡  
    70%-100%: 🟢
    """
    if percent >= 70:
        return "🟢"
    elif percent >= 41:
        return "🟡"
    else:
        return "🔴"


def find_staff_data(staff_info: Dict, data_dict: Dict, monthly_list: List[Dict]) -> tuple:
    """
    Tìm data của nhân sự từ yesterday_data và monthly_stats
    """
    dashboard_names = staff_info.get("dashboard_names", [])
    
    # Tìm trong yesterday_data
    yesterday_stats = None
    for key, value in data_dict.items():
        if match_staff_name(key, dashboard_names):
            yesterday_stats = value
            break
    
    if not yesterday_stats:
        yesterday_stats = {"count": 0, "cart": 0, "text": 0}
    
    # Tìm trong monthly_list
    monthly_personal = None
    for staff in monthly_list:
        staff_name = staff.get("name", "")
        if match_staff_name(staff_name, dashboard_names):
            monthly_personal = staff
            break
    
    return yesterday_stats, monthly_personal


async def generate_personal_report(
    user_id: str, 
    staff_info: Dict, 
    yesterday_data: Dict, 
    monthly_stats: Dict,
    yesterday_deal_data: Dict = None,
    monthly_deal_data: Dict = None
) -> str:
    """
    Tạo báo cáo cá nhân cho 1 nhân sự
    """
    name = staff_info["short_name"]
    dashboard_names = staff_info["dashboard_names"]
    
    # Tìm data của nhân sự
    yesterday_stats, monthly_personal = find_staff_data(
        staff_info, 
        yesterday_data, 
        monthly_stats.get("staff_list", [])
    )
    
    # Tính toán video air
    yesterday = datetime.now(VN_TZ) - timedelta(days=1)
    today = datetime.now(VN_TZ)
    
    video_yesterday = yesterday_stats["count"]
    cart_yesterday = yesterday_stats["cart"]
    text_yesterday = yesterday_stats["text"]
    
    # Logic video: Chỉ tính thiếu từ hôm qua
    deficit_yesterday = max(0, DAILY_KPI - video_yesterday)
    need_air_today = DAILY_KPI + deficit_yesterday
    
    # Status emoji for video
    video_status = "✅ Đạt KPI!" if video_yesterday >= DAILY_KPI else f"⚠️ Thiếu {deficit_yesterday} video"
    
    # Tính toán deal
    deal_yesterday = 0
    deal_month_total = 0
    
    if yesterday_deal_data:
        for deal_name, count in yesterday_deal_data.items():
            if match_staff_name(deal_name, dashboard_names):
                deal_yesterday = count
                break
    
    if monthly_deal_data:
        for deal_name, count in monthly_deal_data.items():
            if match_staff_name(deal_name, dashboard_names):
                deal_month_total = count
                break
    
    # Logic deal: Tính KPI deal theo ngày trong tháng
    # Số ngày đã qua trong tháng (tính đến hôm qua)
    days_passed = yesterday.day
    expected_deal_by_yesterday = days_passed * DAILY_DEAL_KPI  # Tổng KPI deal tính đến hôm qua
    
    # Thiếu deal = max(0, expected - actual)
    deal_deficit = max(0, expected_deal_by_yesterday - deal_month_total)
    
    # Cần deal hôm nay = KPI ngày + thiếu cộng dồn
    need_deal_today = DAILY_DEAL_KPI + deal_deficit
    
    # Status emoji for deal
    deal_status_emoji = "✅" if deal_yesterday >= DAILY_DEAL_KPI else "⚠️"
    
    # Format message
    message = f"""🔔 Chào {name}, báo cáo booking ngày {today.strftime('%d/%m')}:

📊 HÔM QUA ({yesterday.strftime('%d/%m')}):
• Đã air: {video_yesterday} video (KPI: {DAILY_KPI}/ngày)
• Phân loại: {cart_yesterday} Cart, {text_yesterday} Text
• {video_status}
• Đã deal: {deal_yesterday}/{DAILY_DEAL_KPI} KOC {deal_status_emoji}

📌 HÔM NAY ({today.strftime('%d/%m')}):
• Cần air: {need_air_today} video ({DAILY_KPI} KPI + {deficit_yesterday} thiếu hôm qua)
• Cần deal: {need_deal_today} KOC ({DAILY_DEAL_KPI} KPI + {deal_deficit} thiếu cộng dồn)

💪 Cố lên {name}!"""
    
    return message


async def generate_team_report(monthly_stats: Dict) -> str:
    """
    Tạo báo cáo team cho nhóm
    """
    if not monthly_stats:
        return "❌ Không thể lấy dữ liệu báo cáo team"
    
    month = monthly_stats["month"]
    total_kpi = monthly_stats["total_kpi"]
    total_done = monthly_stats["total_done"]
    total_percent = monthly_stats["total_percent"]
    staff_list = monthly_stats["staff_list"]
    
    today = datetime.now(VN_TZ)
    
    # Sort staff by percent descending
    sorted_staff = sorted(staff_list, key=lambda x: x.get("video_percent", 0), reverse=True)
    
    # Build staff details
    staff_lines = []
    for staff in sorted_staff:
        name = staff.get("name", "N/A")
        done = staff.get("video_done", 0)
        kpi = staff.get("video_kpi", 0)
        percent = staff.get("video_percent", 0)
        emoji = get_status_emoji(percent)
        staff_lines.append(f"   {emoji} {name}: {done}/{kpi} ({percent}%)")
    
    staff_details = "\n".join(staff_lines)
    
    message = f"""🧴 **BÁO CÁO TEAM BOOKING - KALLE**
📅 Tháng {month} - Cập nhật {today.strftime('%d/%m')}

👥 **TEAM PR Booking KALLE** ({len(sorted_staff)} nhân sự)

📦 **SỐ LƯỢNG VIDEO:**
• KPI: {total_kpi} video
• Đã air: {total_done} video
• Tỷ lệ: **{total_percent}%**

👤 **CHI TIẾT TỪNG NHÂN SỰ:**
{staff_details}

📊 Chú thích: 🟢 ≥70% | 🟡 41-69% | 🔴 ≤40%"""
    
    return message


async def send_daily_booking_reports():
    """
    Main function: Gửi báo cáo hàng ngày
    1. Gửi thông báo cá nhân cho từng nhân sự
    2. Gửi báo cáo team vào nhóm
    
    Schedule: 9h00 sáng hàng ngày, kết thúc 14/2/2026
    """
    now = datetime.now(VN_TZ)
    
    print(f"\n{'='*50}")
    print(f"📊 Starting daily booking reports at {now}")
    print(f"{'='*50}")
    
    # Check if schedule has ended
    if now >= SCHEDULE_END_DATE:
        print(f"⏰ Schedule ended. Current: {now}, End date: {SCHEDULE_END_DATE}")
        print("📊 Daily booking reports are no longer scheduled.")
        return
    
    try:
        # Lấy data - use Vietnam timezone
        yesterday = now - timedelta(days=1)
        current_month = now.month
        print(f"📅 Getting data for yesterday: {yesterday.strftime('%Y/%m/%d')}")
        
        # Get video air data
        yesterday_data = await get_video_air_by_date(yesterday)
        monthly_stats = await get_monthly_stats()
        
        # Get deal data
        print(f"📅 Getting deal data...")
        yesterday_deal_data = await get_deal_by_date(yesterday)
        monthly_deal_data = await get_monthly_deal_stats(current_month)
        
        if not monthly_stats:
            print("❌ Failed to get monthly stats, aborting...")
            return
        
        # 1. Gửi thông báo cá nhân
        print(f"\n📤 Sending personal reports to {len(BOOKING_STAFF)} staff members...")
        success_count = 0
        for user_id, staff_info in BOOKING_STAFF.items():
            try:
                message = await generate_personal_report(
                    user_id, 
                    staff_info, 
                    yesterday_data, 
                    monthly_stats,
                    yesterday_deal_data,
                    monthly_deal_data
                )
                result = await send_message_to_user(user_id, message)
                if result:
                    success_count += 1
                    print(f"   ✅ {staff_info['name']}")
                else:
                    print(f"   ❌ {staff_info['name']} - Failed to send")
            except Exception as e:
                print(f"   ❌ {staff_info['name']} - Error: {e}")
        
        print(f"📊 Personal reports sent: {success_count}/{len(BOOKING_STAFF)}")
        
        # 2. Gửi báo cáo team
        print(f"\n📤 Sending team report to group...")
        try:
            team_message = await generate_team_report(monthly_stats)
            result = await send_message_to_chat(BOOKING_GROUP_CHAT_ID, team_message)
            if result:
                print("   ✅ Team report sent successfully")
            else:
                print("   ❌ Failed to send team report")
        except Exception as e:
            print(f"   ❌ Team report error: {e}")
        
        print(f"\n{'='*50}")
        print(f"📊 Daily booking reports completed at {datetime.now(VN_TZ)}")
        print(f"{'='*50}\n")
        
    except Exception as e:
        print(f"❌ Error in daily booking reports: {e}")
        import traceback
        traceback.print_exc()


# For testing
async def test_daily_report():
    """Test function - có thể gọi manual"""
    print("🧪 Testing daily booking report...")
    await send_daily_booking_reports()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_daily_report())
