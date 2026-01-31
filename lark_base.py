"""
Lark Base API Module
Kết nối và đọc dữ liệu từ Lark Bitable
Version 5.7.20 - Fix content matching between Dashboard and Booking names
"""
import os
import re
import logging
import httpx
import aiohttp
import pytz
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

# Vietnam timezone
VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

# Setup logger
logger = logging.getLogger(__name__)

# ============ CONFIG ============
LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")

LARK_API_BASE = "https://open.larksuite.com/open-apis"

# Base configurations - KALLE
BOOKING_BASE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tbleiRLSCGwgLCUT"
}

TASK_BASE = {
    "app_token": "LMNIbdCEkajlvYsoyzRl7Dhog5e",
    "table_id": "tblq7TUkSHSulafy"
}

# Bảng Dashboard KOC - cùng Base với Booking nhưng khác table
DASHBOARD_KOC_BASE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "blko05Rb76NGi5nd"
}

# === KALLE DASHBOARD TABLES ===
DASHBOARD_THANG_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tblhf6x9hciClWGz"
}

DOANH_THU_KOC_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tbl2TQywnQTYxpNj"
}

LIEN_HE_TUAN_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tbl18EP44c0MAnKR"
}

KALODATA_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tblX6CB3BshhwloA"
}

# === CHENG BASE ===
CHENG_BASE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
}

CHENG_BOOKING_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tblB2pmRRoMA1IzO"
}

CHENG_LIEN_HE_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tbl6DXM3ZCTQrEm2"
}

CHENG_DOANH_THU_KOC_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tbl1xp8cdxzeccoM"
}

CHENG_DOANH_THU_TONG_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tblbOLW7wp2713M6"
}

CHENG_DASHBOARD_THANG_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tblhfbIOby6kDYnx"
}

# === NOTES TABLE (Persistent Storage) ===
# Bảng Notes riêng - để lưu ghi chú của users
# Link: https://chenglovehair.sg.larksuite.com/base/XfHGbvXrRaK1zcsTZ1zl5QR3ghf
NOTES_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",  # Jarvis Notes Base
    "table_id": "tbl6LiH9n7xs4VMs"  # Bảng Jarvis Notes
}

# === CALENDAR CONFIG ===
JARVIS_CALENDAR_ID = "7585485663517069021"


async def create_calendar_event(
    summary: str,
    start_time: datetime,
    end_time: datetime = None,
    description: str = None,
    calendar_id: str = None
) -> Dict:
    """
    Tạo event trong Lark Calendar
    
    Args:
        summary: Tiêu đề event
        start_time: Thời gian bắt đầu (datetime object)
        end_time: Thời gian kết thúc (mặc định = start_time + 1 hour)
        description: Mô tả chi tiết
        calendar_id: ID calendar (mặc định dùng JARVIS_CALENDAR_ID)
    
    Returns:
        Dict với event_id nếu thành công, hoặc error
    """
    token = await get_tenant_access_token()
    cal_id = calendar_id or JARVIS_CALENDAR_ID
    
    if end_time is None:
        end_time = start_time + timedelta(hours=1)
    
    # Convert to timestamp (seconds)
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    
    url = f"https://open.larksuite.com/open-apis/calendar/v4/calendars/{cal_id}/events"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "summary": summary,
        "start_time": {
            "timestamp": str(start_ts)
        },
        "end_time": {
            "timestamp": str(end_ts)
        }
    }
    
    if description:
        payload["description"] = description
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as resp:
            result = await resp.json()
            
            if result.get("code") != 0:
                print(f"❌ Calendar event error: {result}")
                return {"error": result.get("msg", "Unknown error"), "code": result.get("code")}
            
            event_id = result.get("data", {}).get("event", {}).get("event_id")
            print(f"✅ Calendar event created: {event_id}")
            return {"success": True, "event_id": event_id}


# ============ AUTH ============
_token_cache = {
    "token": None,
    "expires_at": None
}

async def get_tenant_access_token() -> str:
    """Lấy tenant access token từ Lark (có cache)"""
    now = datetime.now()
    
    if _token_cache["token"] and _token_cache["expires_at"]:
        if now < _token_cache["expires_at"]:
            return _token_cache["token"]
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{LARK_API_BASE}/auth/v3/tenant_access_token/internal",
            json={
                "app_id": LARK_APP_ID,
                "app_secret": LARK_APP_SECRET
            }
        )
        data = response.json()
        
        if data.get("code") == 0:
            token = data.get("tenant_access_token")
            expire = data.get("expire", 7200)
            
            _token_cache["token"] = token
            _token_cache["expires_at"] = now + timedelta(seconds=expire - 300)
            
            return token
        else:
            raise Exception(f"Failed to get token: {data}")

# ============ BASE API ============
async def get_table_records(
    app_token: str,
    table_id: str,
    filter_formula: Optional[str] = None,
    page_size: int = 100,
    page_token: Optional[str] = None,
    sort: Optional[List[Dict]] = None
) -> Dict[str, Any]:
    """Lấy records từ Lark Base table"""
    token = await get_tenant_access_token()
    
    url = f"{LARK_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    
    params = {
        "page_size": min(page_size, 500)
    }
    
    if filter_formula:
        params["filter"] = filter_formula
    
    if page_token:
        params["page_token"] = page_token
    
    if sort:
        import json
        params["sort"] = json.dumps(sort)
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30.0
        )
        data = response.json()
        
        if data.get("code") == 0:
            return data.get("data", {})
        else:
            print(f"❌ Lark Base API Error: {data}")
            raise Exception(f"Lark Base API Error: {data.get('msg')}")

async def get_all_records(
    app_token: str,
    table_id: str,
    filter_formula: Optional[str] = None,
    max_records: int = 2000,
    sort: Optional[List[Dict]] = None
) -> List[Dict[str, Any]]:
    """Lấy tất cả records (với pagination)"""
    all_records = []
    page_token = None
    
    while len(all_records) < max_records:
        result = await get_table_records(
            app_token=app_token,
            table_id=table_id,
            filter_formula=filter_formula,
            page_size=500,
            page_token=page_token,
            sort=sort
        )
        
        items = result.get("items", [])
        all_records.extend(items)
        
        if not result.get("has_more"):
            break
        
        page_token = result.get("page_token")
    
    return all_records[:max_records]


async def create_record(app_token: str, table_id: str, fields: Dict) -> Dict:
    """Tạo record mới trong Bitable"""
    token = await get_tenant_access_token()
    
    url = f"{LARK_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json={"fields": fields}
        )
        
        data = response.json()
        
        if data.get("code") != 0:
            print(f"❌ Create record error: {data}")
            return {"error": data.get("msg", "Unknown error")}
        
        return data.get("data", {}).get("record", {})


async def update_record(app_token: str, table_id: str, record_id: str, fields: Dict) -> Dict:
    """Cập nhật record trong Bitable"""
    token = await get_tenant_access_token()
    
    url = f"{LARK_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.put(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json={"fields": fields}
        )
        
        data = response.json()
        
        if data.get("code") != 0:
            print(f"❌ Update record error: {data}")
            return {"error": data.get("msg", "Unknown error")}
        
        return data.get("data", {}).get("record", {})


async def delete_record(app_token: str, table_id: str, record_id: str) -> Dict:
    """Xóa record trong Bitable"""
    token = await get_tenant_access_token()
    
    url = f"{LARK_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.delete(
            url,
            headers={
                "Authorization": f"Bearer {token}"
            }
        )
        
        data = response.json()
        
        if data.get("code") != 0:
            print(f"❌ Delete record error: {data}")
            return {"error": data.get("msg", "Unknown error")}
        
        return {"deleted": True, "record_id": record_id}


# ============ HELPER FUNCTIONS ============
def safe_extract_text(value):
    """Extract text value from Lark field (handles list, dict, string)"""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, list) and len(value) > 0:
        first = value[0]
        if isinstance(first, dict):
            return first.get("text") or first.get("name") or first.get("value")
        return str(first)
    if isinstance(value, dict):
        return value.get("text") or value.get("name") or value.get("value")
    return str(value)


def safe_extract_person_name(value):
    """Extract person name from Lark person field"""
    if value is None:
        return "Không xác định"
    if isinstance(value, list) and len(value) > 0:
        first = value[0]
        if isinstance(first, dict):
            return first.get("name") or first.get("en_name") or "Không xác định"
    if isinstance(value, dict):
        return value.get("name") or value.get("en_name") or "Không xác định"
    return str(value)


def safe_number(val, default=0):
    """Safely convert value to number"""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        try:
            # Remove commas and dots used as thousand separators
            cleaned = val.replace(",", "").replace(" ", "")
            return float(cleaned)
        except:
            return default
    if isinstance(val, list) and len(val) > 0:
        return safe_number(val[0], default)
    if isinstance(val, dict):
        return safe_number(val.get("value") or val.get("text"), default)
    return default


def extract_loai_video(record: Dict) -> Optional[str]:
    """
    Trích xuất field "Content" từ record
    Các giá trị: Cart, Text, Video
    v5.7.17 - Fixed: field tên là "Content Text" trong Dashboard Tháng
    """
    fields = record if "fields" not in record else record.get("fields", {})
    
    # Các tên field có thể có (ưu tiên từ trên xuống)
    possible_names = [
        "Content Text",     # v5.7.17 - Tên field thực tế trong Dashboard Tháng
        "Content",
        "Loại video",
        "Loai video", 
        "Loại Video",
        "Content Type",
        "Type",
    ]
    
    for name in possible_names:
        value = fields.get(name)
        if value:
            return safe_extract_text(value)
    
    return None


def find_phan_loai_field(fields: Dict) -> Optional[str]:
    """Tìm field phân loại sản phẩm trong record."""
    possible_names = [
        "Phân loại sp (Chỉ được chọn - Không được add mới)",
        "Phân loại sản phẩm",
        "Phân loại sp",
        "Phan loai san pham",
        "Phan loai sp",
    ]
    
    value = None
    
    for name in possible_names:
        if name in fields:
            value = fields.get(name)
            break
    
    if value is None:
        for key in fields.keys():
            key_lower = key.lower()
            if "phân loại" in key_lower or "phan loai" in key_lower:
                value = fields.get(key)
                break
    
    if value is None:
        return None
    
    if isinstance(value, str):
        return value
    
    if isinstance(value, list):
        if len(value) > 0:
            first = value[0]
            if isinstance(first, dict):
                return first.get("text") or first.get("value") or str(first)
            return str(first)
        return None
    
    if isinstance(value, dict):
        return value.get("text") or value.get("value") or str(value)
    
    return str(value) if value else None


def extract_field_value(fields: Dict, field_name: str, default=None):
    """Extract giá trị từ field, xử lý các loại field khác nhau"""
    value = fields.get(field_name)
    
    if value is None:
        return default
    
    if isinstance(value, str):
        return value
    
    if isinstance(value, (int, float)):
        return value
    
    if isinstance(value, list):
        if len(value) == 0:
            return default
        if isinstance(value[0], dict):
            return [v.get("text", v.get("name", str(v))) for v in value]
        return value
    
    if isinstance(value, dict):
        if "date" in value:
            return value.get("date")
        if "link" in value:
            return value.get("link")
        if "text" in value:
            return value.get("text")
        return str(value)
    
    return value


# ============ CHENG FUNCTIONS (UPDATED v5.7.0) ============

async def get_cheng_booking_records(month: int = None, week: int = None) -> List[Dict]:
    """Lấy danh sách booking từ bảng CHENG"""
    records = await get_all_records(
        CHENG_BOOKING_TABLE["app_token"],
        CHENG_BOOKING_TABLE["table_id"]
    )
    
    print(f"📋 CHENG Booking: Total records = {len(records)}, filter month = {month}, week = {week}")
    
    if not month and not week:
        return records
    
    filtered = []
    month_dist = {}
    
    for record in records:
        fields = record.get("fields", {})
        
        thang_du_kien_raw = fields.get("Tháng dự kiến") or fields.get("Tháng dự kiến air")
        thang_du_kien = None
        
        try:
            if isinstance(thang_du_kien_raw, list) and len(thang_du_kien_raw) > 0:
                first = thang_du_kien_raw[0]
                thang_du_kien = int(first.get("text", 0)) if isinstance(first, dict) else int(first)
            elif isinstance(thang_du_kien_raw, (int, float)):
                thang_du_kien = int(thang_du_kien_raw)
            elif isinstance(thang_du_kien_raw, str):
                thang_du_kien = int(thang_du_kien_raw)
        except:
            pass
        
        if thang_du_kien:
            month_dist[thang_du_kien] = month_dist.get(thang_du_kien, 0) + 1
        
        if month and thang_du_kien != month:
            continue
        
        filtered.append(record)
    
    print(f"📋 CHENG Month distribution: {month_dist}")
    print(f"📋 CHENG After filter: {len(filtered)} records")
    
    return filtered


async def get_cheng_dashboard_records(month: int = None) -> List[Dict]:
    """
    Lấy records từ bảng CHENG - DASHBOARD THÁNG
    Updated v5.7.0: Fixed field names based on actual screenshots
    
    Fields từ screenshot:
    - Tháng báo cáo (số: 12)
    - Ngày báo cáo (08/12/2025)
    - Tuần báo cáo (Tuần 1)
    - Nhân sự book (Person field)
    - Sản phẩm (Nhuộm bột, Ủ tóc, etc.)
    - KPI Số lượng, Số lượng, % KPI Số lượng
    - KPI ngân sách, Ngân sách tổng - Deal, Ngân sách tổng - Air
    - Số lượng - Deal, Ngân sách - Deal (TUẦN)
    - Số lượng - Air, Ngân sách - Air (TUẦN)
    """
    records = await get_all_records(
        CHENG_DASHBOARD_THANG_TABLE["app_token"],
        CHENG_DASHBOARD_THANG_TABLE["table_id"]
    )
    
    print(f"📊 CHENG Dashboard: Total records = {len(records)}, filter month = {month}")
    
    # Debug: in ra các field names của record đầu tiên
    if records:
        first_fields = records[0].get("fields", {})
        print(f"   🔍 CHENG Dashboard field names: {list(first_fields.keys())[:20]}")
        print(f"   🔍 Sample Tháng báo cáo: {first_fields.get('Tháng báo cáo')}")
        print(f"   🔍 Sample Tuần báo cáo: {first_fields.get('Tuần báo cáo')}")
        print(f"   🔍 Sample Nhân sự book: {first_fields.get('Nhân sự book')}")
    
    parsed = []
    month_dist = {}
    
    for r in records:
        fields = r.get("fields", {})
        
        # Parse tháng - "Tháng báo cáo" là số nguyên (12)
        thang_raw = fields.get("Tháng báo cáo")
        thang = None
        try:
            if isinstance(thang_raw, (int, float)):
                thang = int(thang_raw)
            elif isinstance(thang_raw, list) and len(thang_raw) > 0:
                first = thang_raw[0]
                if isinstance(first, dict):
                    thang = int(first.get("text", 0))
                else:
                    thang = int(first)
            elif isinstance(thang_raw, str):
                match = re.search(r'\d+', thang_raw)
                if match:
                    thang = int(match.group())
        except:
            pass
        
        month_dist[thang] = month_dist.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        # Parse tuần - "Tuần báo cáo" có thể là "Tuần 1" hoặc object
        tuan_raw = fields.get("Tuần báo cáo")
        tuan = None
        if isinstance(tuan_raw, str):
            tuan = tuan_raw
        elif isinstance(tuan_raw, list) and len(tuan_raw) > 0:
            first = tuan_raw[0]
            if isinstance(first, dict):
                tuan = first.get("text") or first.get("name")
            else:
                tuan = str(first)
        elif isinstance(tuan_raw, dict):
            tuan = tuan_raw.get("text") or tuan_raw.get("name")
        
        # Parse nhân sự - "Nhân sự book" (Person field)
        nhan_su = safe_extract_person_name(fields.get("Nhân sự book"))
        
        # Parse sản phẩm
        san_pham_raw = fields.get("Sản phẩm")
        san_pham = None
        if isinstance(san_pham_raw, str):
            san_pham = san_pham_raw
        elif isinstance(san_pham_raw, list) and len(san_pham_raw) > 0:
            first = san_pham_raw[0]
            if isinstance(first, dict):
                san_pham = first.get("text") or first.get("name")
            else:
                san_pham = str(first)
        
        # Parse loại video (v5.7.15)
        loai_video = extract_loai_video(fields)
        
        parsed.append({
            "record_id": r.get("record_id"),
            "thang": thang,
            "tuan": tuan,
            "san_pham": san_pham,
            "nhan_su": nhan_su,
            "loai_video": loai_video,  # v5.7.15 - content breakdown
            # KPI targets (THÁNG)
            "kpi_so_luong": safe_number(fields.get("KPI Số lượng") or fields.get("KPI số lượng")),
            "kpi_ngan_sach": safe_number(fields.get("KPI ngân sách")),
            # Số lượng thực tế (THÁNG)
            "so_luong": safe_number(fields.get("Số lượng")),
            "pct_kpi_so_luong": safe_number(fields.get("% KPI Số lượng") or fields.get("% KPI số lượng")),
            # Ngân sách thực tế (THÁNG)
            "ngan_sach_tong_deal": safe_number(fields.get("Ngân sách tổng - Deal")),
            "ngan_sach_tong_air": safe_number(fields.get("Ngân sách tổng - Air")),
            "pct_kpi_ngan_sach": safe_number(fields.get("% KPI Ngân sách") or fields.get("% KPI ngân sách")),
            # DEAL - TUẦN
            "so_luong_deal": safe_number(fields.get("Số lượng - Deal")),
            "pct_so_luong_deal": safe_number(fields.get("% số lượng - Deal") or fields.get("% Số lượng - Deal")),
            "ngan_sach_deal": safe_number(fields.get("Ngân sách - Deal")),
            "pct_ngan_sach_deal": safe_number(fields.get("% Ngân sách - Deal")),
            # ĐÃ AIR - TUẦN
            "so_luong_air": safe_number(fields.get("Số lượng - Air")),
            "pct_so_luong_air": safe_number(fields.get("% Số lượng - Air") or fields.get("% số lượng - Air")),
            "ngan_sach_air": safe_number(fields.get("Ngân sách - Air")),
            "pct_ngan_sach_air": safe_number(fields.get("% Ngân sách - Air")),
            # Số lượng tổng - dùng "Số lượng tổng - Air" hoặc fallback về "Số lượng - Air"
            "so_luong_tong_air": safe_number(fields.get("Số lượng tổng - Air") or fields.get("Số lượng - Air")),
        })
    
    print(f"   📊 CHENG Month distribution: {month_dist}")
    print(f"📊 CHENG Dashboard after filter: {len(parsed)} records")
    return parsed


async def get_cheng_lien_he_records(month: int = None, week: int = None) -> List[Dict]:
    """
    Lấy records từ bảng CHENG - PR - Data liên hệ (tuần)
    Updated v5.7.0: Fixed field names based on actual screenshots
    
    Fields từ screenshot:
    - Ngày báo cáo
    - Tháng báo cáo (số: 12)
    - Tuần báo cáo (Tuần 1)
    - Thời gian tuần (1/12 - 7/12)
    - Người tạo (Person field)
    - # Đã deal, Tỷ lệ đã deal
    - # Đang trao đổi, Tỷ lệ đang trao đổi
    - # Từ chối, Tỷ lệ từ chối
    - Không phản hồi khi nhắn, Không phản hồi từ đầu
    - Tổng liên hệ
    """
    records = await get_all_records(
        CHENG_LIEN_HE_TABLE["app_token"],
        CHENG_LIEN_HE_TABLE["table_id"]
    )
    
    print(f"📞 CHENG Liên hệ: Total records = {len(records)}, filter month = {month}")
    
    # Debug field names
    if records:
        first_fields = records[0].get("fields", {})
        print(f"   🔍 CHENG Liên hệ field names: {list(first_fields.keys())[:20]}")
    
    parsed = []
    month_dist = {}
    
    for r in records:
        fields = r.get("fields", {})
        
        # Parse tháng
        thang_raw = fields.get("Tháng báo cáo")
        thang = None
        try:
            if isinstance(thang_raw, (int, float)):
                thang = int(thang_raw)
            elif isinstance(thang_raw, list) and len(thang_raw) > 0:
                first = thang_raw[0]
                if isinstance(first, dict):
                    thang = int(first.get("text", 0))
                else:
                    thang = int(first)
            elif isinstance(thang_raw, str):
                match = re.search(r'\d+', thang_raw)
                if match:
                    thang = int(match.group())
        except:
            pass
        
        month_dist[thang] = month_dist.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        # Parse tuần
        tuan_raw = fields.get("Tuần báo cáo")
        tuan = None
        if isinstance(tuan_raw, str):
            tuan = tuan_raw
        elif isinstance(tuan_raw, list) and len(tuan_raw) > 0:
            first = tuan_raw[0]
            if isinstance(first, dict):
                tuan = first.get("text") or first.get("name")
            else:
                tuan = str(first)
        
        # Parse nhân sự - "Người tạo" (Person field)
        nhan_su = safe_extract_person_name(fields.get("Người tạo"))
        
        parsed.append({
            "record_id": r.get("record_id"),
            "thang": thang,
            "tuan": tuan,
            "thoi_gian_tuan": fields.get("Thời gian tuần"),
            "nhan_su": nhan_su,
            # Số liệu liên hệ - dùng "#" prefix theo screenshot
            "da_deal": safe_number(fields.get("# Đã deal") or fields.get("Đã deal")),
            "ty_le_deal": safe_number(fields.get("Tỷ lệ đã deal")),
            "dang_trao_doi": safe_number(fields.get("# Đang trao đổi") or fields.get("Đang trao đổi")),
            "ty_le_trao_doi": safe_number(fields.get("Tỷ lệ đang trao đổi")),
            "tu_choi": safe_number(fields.get("# Từ chối") or fields.get("Từ chối")),
            "ty_le_tu_choi": safe_number(fields.get("Tỷ lệ từ chối")),
            "khong_phan_hoi_nhan": safe_number(fields.get("Không phản hồi khi nhắn") or fields.get("Không phản hồi khi n...")),
            "khong_phan_hoi_dau": safe_number(fields.get("Không phản hồi từ đầu") or fields.get("Không phản hồi hồi t...")),
            "tong_lien_he": safe_number(fields.get("Tổng liên hệ")),
        })
    
    print(f"📞 CHENG Month distribution: {month_dist}")
    print(f"📞 CHENG After filter: {len(parsed)} records")
    
    return parsed


async def get_cheng_doanh_thu_records(month: int = None, week: int = None) -> List[Dict]:
    """
    Lấy records từ bảng CHENG - PR - Data doanh thu Koc (tuần)
    Updated v5.7.0: Fixed field names based on actual screenshots
    
    Fields từ screenshot (Ảnh 4):
    - Ngày báo cáo
    - Tháng báo cáo (số: 09)
    - Tuần báo cáo (Tuần 1)
    - Thời gian tuần (1/9 - 7/9)
    - Năm air (2024, 2025)
    - Link video
    - Ngày đăng
    - ID kênh
    - GMV
    - Nhân sự book (Person field)
    - Nhận xét nhân sự
    """
    records = await get_all_records(
        CHENG_DOANH_THU_KOC_TABLE["app_token"],
        CHENG_DOANH_THU_KOC_TABLE["table_id"]
    )
    
    print(f"💰 CHENG Doanh thu: Total records = {len(records)}, filter month = {month}")
    
    # Debug field names
    if records:
        first_fields = records[0].get("fields", {})
        print(f"   🔍 CHENG Doanh thu field names: {list(first_fields.keys())}")
    
    parsed = []
    month_dist = {}
    
    for r in records:
        fields = r.get("fields", {})
        
        # Parse tháng
        thang_raw = fields.get("Tháng báo cáo")
        thang = None
        try:
            if isinstance(thang_raw, (int, float)):
                thang = int(thang_raw)
            elif isinstance(thang_raw, list) and len(thang_raw) > 0:
                first = thang_raw[0]
                if isinstance(first, dict):
                    thang = int(first.get("text", 0))
                else:
                    thang = int(first)
            elif isinstance(thang_raw, str):
                match = re.search(r'\d+', thang_raw)
                if match:
                    thang = int(match.group())
        except:
            pass
        
        month_dist[thang] = month_dist.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        # Parse GMV
        gmv = safe_number(fields.get("GMV"))
        
        # Parse nhân sự
        nhan_su = safe_extract_person_name(fields.get("Nhân sự book"))
        
        parsed.append({
            "record_id": r.get("record_id"),
            "thang": thang,
            "tuan": fields.get("Tuần báo cáo"),
            "thoi_gian_tuan": fields.get("Thời gian tuần"),
            "nam_air": fields.get("Năm air"),
            "link_video": safe_extract_text(fields.get("Link video")),
            "ngay_dang": fields.get("Ngày đăng"),
            "id_kenh": fields.get("ID kênh"),
            "gmv": gmv,
            "nhan_su": nhan_su,
            "nhan_xet": fields.get("Nhận xét nhân sự"),
        })
    
    print(f"💰 CHENG Month distribution: {month_dist}")
    print(f"💰 CHENG After filter: {len(parsed)} records")
    
    return parsed


async def get_cheng_doanh_thu_tong_records(month: int = None, week: int = None) -> List[Dict]:
    """
    Lấy records từ bảng CHENG - PR - Data doanh thu tổng Cheng (tuần)
    Đây là bảng GMV chính xác theo tuần
    
    Fields từ screenshot:
    - Tháng báo cáo (số: 09, 10, 11, 12)
    - Ngày báo cáo
    - Tuần báo cáo (Tuần 1, Tuần 2, Tuần 3, Tuần 4, Tuần 5)
    - Ngày xuất doanh thu (1/10 - 5/10, 6/10 - 12/10, etc.)
    - GMV (số lớn như 526,111,441.00)
    - Nhận xét nhân sự
    """
    records = await get_all_records(
        CHENG_DOANH_THU_TONG_TABLE["app_token"],
        CHENG_DOANH_THU_TONG_TABLE["table_id"]
    )
    
    print(f"📊 CHENG Doanh thu TỔNG: Total records = {len(records)}, filter month = {month}, week = {week}")
    
    # Debug field names
    if records:
        first_fields = records[0].get("fields", {})
        print(f"   🔍 CHENG DT Tổng field names: {list(first_fields.keys())}")
    
    parsed = []
    
    for r in records:
        fields = r.get("fields", {})
        
        # Parse tháng
        thang_raw = fields.get("Tháng báo cáo")
        thang = None
        try:
            if isinstance(thang_raw, (int, float)):
                thang = int(thang_raw)
            elif isinstance(thang_raw, list) and len(thang_raw) > 0:
                first = thang_raw[0]
                if isinstance(first, dict):
                    thang = int(first.get("text", 0))
                else:
                    thang = int(first)
            elif isinstance(thang_raw, str):
                match = re.search(r'\d+', thang_raw)
                if match:
                    thang = int(match.group())
        except:
            pass
        
        if month and thang != month:
            continue
        
        # Parse tuần
        tuan_raw = fields.get("Tuần báo cáo")
        tuan_num = None
        tuan_str = None
        if tuan_raw:
            if isinstance(tuan_raw, str):
                tuan_str = tuan_raw
            elif isinstance(tuan_raw, list) and len(tuan_raw) > 0:
                first = tuan_raw[0]
                if isinstance(first, dict):
                    tuan_str = first.get("text") or first.get("name")
                else:
                    tuan_str = str(first)
            
            # Extract number from "Tuần 1", "Tuần 2", etc.
            if tuan_str:
                match = re.search(r'\d+', tuan_str)
                if match:
                    tuan_num = int(match.group())
        
        # Filter by week if specified
        if week and tuan_num != week:
            continue
        
        # Parse GMV
        gmv = safe_number(fields.get("GMV"))
        
        parsed.append({
            "record_id": r.get("record_id"),
            "thang": thang,
            "tuan": tuan_str,
            "tuan_num": tuan_num,
            "ngay_xuat_doanh_thu": fields.get("Ngày xuất doanh thu") or fields.get("Ngày xuất doanh thu"),
            "gmv": gmv,
            "nhan_xet": fields.get("Nhận xét nhân sự"),
        })
    
    print(f"📊 CHENG DT Tổng after filter: {len(parsed)} records, total GMV = {sum(r['gmv'] for r in parsed):,.0f}")
    
    return parsed


async def generate_cheng_koc_summary(month: int = None, week: int = None) -> Dict:
    """
    Tổng hợp báo cáo KOC cho CHENG
    Updated v5.7.1: Fixed GMV từ bảng Doanh thu tổng
    """
    # Lấy dữ liệu từ các bảng Cheng
    dashboard_records = await get_cheng_dashboard_records(month=month)
    lien_he_records = await get_cheng_lien_he_records(month=month, week=week)
    doanh_thu_koc_records = await get_cheng_doanh_thu_records(month=month, week=week)
    
    # LẤY GMV TỪ BẢNG DOANH THU TỔNG (mới - v5.7.1)
    doanh_thu_tong_records = await get_cheng_doanh_thu_tong_records(month=month, week=week)
    
    # === Tổng hợp KPI theo nhân sự từ DASHBOARD THÁNG ===
    # Logic: Cộng tổng KPI và Air từ tất cả sản phẩm, CHỈ LẤY TUẦN 1
    
    kpi_by_nhan_su = {}
    
    # Debug tuần distribution
    tuan_dist = {}
    for r in dashboard_records:
        tuan = r.get("tuan")
        tuan_dist[tuan] = tuan_dist.get(tuan, 0) + 1
    print(f"   📊 CHENG Tuần distribution: {tuan_dist}")
    
    for r in dashboard_records:
        nhan_su = r["nhan_su"]
        if nhan_su:
            nhan_su = nhan_su.strip()
        
        # CHỈ LẤY TUẦN 1 để tránh tính trùng
        tuan = r.get("tuan")
        is_tuan_1 = False
        if tuan:
            tuan_str = str(tuan).lower()
            if "1" in tuan_str and ("tuần" in tuan_str or tuan_str.strip() == "1"):
                is_tuan_1 = True
        
        if tuan and not is_tuan_1:
            continue
        
        if nhan_su not in kpi_by_nhan_su:
            kpi_by_nhan_su[nhan_su] = {
                "kpi_so_luong": 0,
                "kpi_ngan_sach": 0,
                "so_luong_air": 0,
                "ngan_sach_air": 0,
                "pct_kpi_so_luong": 0,
                "pct_kpi_ngan_sach": 0,
            }
        
        # CỘNG TỔNG từ tất cả sản phẩm
        kpi_sl = int(r.get("kpi_so_luong") or 0)
        kpi_ns = int(r.get("kpi_ngan_sach") or 0)
        sl_air = int(r.get("so_luong_tong_air") or r.get("so_luong_air") or 0)
        ns_air = int(r.get("ngan_sach_tong_air") or r.get("ngan_sach_air") or 0)
        
        kpi_by_nhan_su[nhan_su]["kpi_so_luong"] += kpi_sl
        kpi_by_nhan_su[nhan_su]["kpi_ngan_sach"] += kpi_ns
        kpi_by_nhan_su[nhan_su]["so_luong_air"] += sl_air
        kpi_by_nhan_su[nhan_su]["ngan_sach_air"] += ns_air
        
        san_pham = r.get("san_pham") or "N/A"
        print(f"   📌 CHENG {nhan_su} | {san_pham}: KPI={kpi_sl}, Air={sl_air}")
    
    # Tính % KPI
    for nhan_su, data in kpi_by_nhan_su.items():
        if data["kpi_so_luong"] > 0:
            data["pct_kpi_so_luong"] = round(data["so_luong_air"] / data["kpi_so_luong"] * 100, 1)
        if data["kpi_ngan_sach"] > 0:
            data["pct_kpi_ngan_sach"] = round(data["ngan_sach_air"] / data["kpi_ngan_sach"] * 100, 1)
        
        print(f"   ✅ CHENG TỔNG {nhan_su}: {data['so_luong_air']}/{data['kpi_so_luong']} ({data['pct_kpi_so_luong']}%)")
    
    # === Tổng hợp liên hệ theo nhân sự ===
    lien_he_by_nhan_su = {}
    for r in lien_he_records:
        nhan_su = r["nhan_su"]
        if nhan_su:
            nhan_su = nhan_su.strip()
        
        if nhan_su not in lien_he_by_nhan_su:
            lien_he_by_nhan_su[nhan_su] = {
                "tong_lien_he": 0,
                "da_deal": 0,
                "dang_trao_doi": 0,
                "tu_choi": 0,
            }
        
        lien_he_by_nhan_su[nhan_su]["tong_lien_he"] += int(r.get("tong_lien_he") or 0)
        lien_he_by_nhan_su[nhan_su]["da_deal"] += int(r.get("da_deal") or 0)
        lien_he_by_nhan_su[nhan_su]["dang_trao_doi"] += int(r.get("dang_trao_doi") or 0)
        lien_he_by_nhan_su[nhan_su]["tu_choi"] += int(r.get("tu_choi") or 0)
    
    # Tính tỷ lệ
    for ns, data in lien_he_by_nhan_su.items():
        total = data["tong_lien_he"]
        if total > 0:
            data["ty_le_deal"] = round(data["da_deal"] / total * 100, 1)
            data["ty_le_trao_doi"] = round(data["dang_trao_doi"] / total * 100, 1)
            data["ty_le_tu_choi"] = round(data["tu_choi"] / total * 100, 1)
        else:
            data["ty_le_deal"] = 0
            data["ty_le_trao_doi"] = 0
            data["ty_le_tu_choi"] = 0
    
    # === Top KOC doanh số (từ bảng KOC chi tiết) ===
    koc_gmv = {}
    for r in doanh_thu_koc_records:
        id_kenh = r.get("id_kenh")
        if id_kenh:
            if id_kenh not in koc_gmv:
                koc_gmv[id_kenh] = 0
            koc_gmv[id_kenh] += r.get("gmv") or 0
    
    # Sort by GMV
    top_koc = sorted(koc_gmv.items(), key=lambda x: x[1], reverse=True)[:10]
    
    # === TÍNH GMV TỪ BẢNG DOANH THU TỔNG (chính xác) ===
    total_gmv = sum(r.get("gmv", 0) for r in doanh_thu_tong_records)
    
    # === CONTENT BREAKDOWN BY NHÂN SỰ (v5.7.15) ===
    # Aggregate content theo nhân sự, sản phẩm và loại video (Cart/Text/Video)
    content_by_nhan_su = {}
    for r in dashboard_records:
        nhan_su = r.get("nhan_su")
        if nhan_su:
            nhan_su = nhan_su.strip()
        else:
            continue
        
        san_pham = r.get("san_pham") or "N/A"
        loai_video = r.get("loai_video") or "Video"  # Default to "Video" if not specified
        so_luong_air = int(r.get("so_luong_tong_air") or r.get("so_luong_air") or 0)
        
        if so_luong_air == 0:
            continue
        
        if nhan_su not in content_by_nhan_su:
            content_by_nhan_su[nhan_su] = []
        
        # Tìm xem đã có entry cho sản phẩm + loại này chưa
        found = False
        for item in content_by_nhan_su[nhan_su]:
            if item["san_pham"] == san_pham and item["loai"] == loai_video:
                item["so_luong"] += so_luong_air
                found = True
                break
        
        if not found:
            content_by_nhan_su[nhan_su].append({
                "san_pham": san_pham,
                "loai": loai_video,
                "so_luong": so_luong_air
            })
    
    # Sort content items theo số lượng giảm dần
    for nhan_su in content_by_nhan_su:
        content_by_nhan_su[nhan_su].sort(key=lambda x: x["so_luong"], reverse=True)
    
    print(f"📝 CHENG Content breakdown: {len(content_by_nhan_su)} nhân sự")
    
    # === Tổng quan ===
    total_kpi_so_luong = sum(d["kpi_so_luong"] for d in kpi_by_nhan_su.values())
    total_so_luong_air = sum(d["so_luong_air"] for d in kpi_by_nhan_su.values())
    total_kpi_ngan_sach = sum(d["kpi_ngan_sach"] for d in kpi_by_nhan_su.values())
    total_ngan_sach_air = sum(d["ngan_sach_air"] for d in kpi_by_nhan_su.values())
    
    print(f"📊 CHENG TỔNG QUAN: {total_so_luong_air}/{total_kpi_so_luong} ({round(total_so_luong_air / total_kpi_so_luong * 100, 1) if total_kpi_so_luong > 0 else 0}%)")
    
    return {
        "brand": "CHENG",
        "month": month,
        "week": week,
        "tong_quan": {
            "kpi_so_luong": total_kpi_so_luong,
            "so_luong_air": total_so_luong_air,
            "pct_kpi_so_luong": round(total_so_luong_air / total_kpi_so_luong * 100, 1) if total_kpi_so_luong > 0 else 0,
            "kpi_ngan_sach": total_kpi_ngan_sach,
            "ngan_sach_air": total_ngan_sach_air,
            "pct_kpi_ngan_sach": round(total_ngan_sach_air / total_kpi_ngan_sach * 100, 1) if total_kpi_ngan_sach > 0 else 0,
            "total_gmv": total_gmv,
        },
        "kpi_nhan_su": kpi_by_nhan_su,
        "lien_he_nhan_su": lien_he_by_nhan_su,
        "top_koc": top_koc,
        "content_by_nhan_su": content_by_nhan_su,  # v5.7.15 - content breakdown
    }


# ============ KALLE FUNCTIONS (Existing) ============

async def get_booking_records(
    month: Optional[int] = None,
    week: Optional[int] = None,
    year: int = 2025
) -> List[Dict[str, Any]]:
    """Lấy records từ bảng Booking/KOC KALLE"""
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=2000
    )
    
    def parse_lark_value(value):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, list):
            if len(value) == 0:
                return None
            first = value[0]
            if isinstance(first, dict):
                return first.get("text") or first.get("value") or first.get("name")
            return first
        if isinstance(value, dict):
            return value.get("text") or value.get("link") or value.get("value")
        return str(value)
    
    def extract_month(value) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            month_val = int(value)
            if 1 <= month_val <= 12:
                return month_val
            return None
        if isinstance(value, str):
            match = re.search(r'(\d{1,2})', value)
            if match:
                month_val = int(match.group(1))
                if 1 <= month_val <= 12:
                    return month_val
            return None
        if isinstance(value, list):
            if len(value) == 0:
                return None
            first = value[0]
            if isinstance(first, dict):
                text_val = first.get("text") or first.get("value")
                if text_val:
                    match = re.search(r'(\d{1,2})', str(text_val))
                    if match:
                        month_val = int(match.group(1))
                        if 1 <= month_val <= 12:
                            return month_val
            elif isinstance(first, (int, float)):
                month_val = int(first)
                if 1 <= month_val <= 12:
                    return month_val
            elif isinstance(first, str):
                match = re.search(r'(\d{1,2})', first)
                if match:
                    month_val = int(match.group(1))
                    if 1 <= month_val <= 12:
                        return month_val
            return None
        if isinstance(value, dict):
            text_val = value.get("text") or value.get("value")
            if text_val:
                return extract_month(text_val)
            return None
        return None
    
    results = []
    skipped_wrong_month = 0
    total_records = len(records)
    
    print(f"📥 Fetched {total_records} total records from Lark Base")
    
    month_counts = {}
    for record in records:
        fields = record.get("fields", {})
        raw_month = fields.get("Tháng air")
        parsed_month = extract_month(raw_month)
        if parsed_month not in month_counts:
            month_counts[parsed_month] = 0
        month_counts[parsed_month] += 1
    
    print(f"📊 Month distribution: {month_counts}")
    
    for record in records:
        fields = record.get("fields", {})
        
        koc_data = {
            "record_id": record.get("record_id"),
            "id_koc": parse_lark_value(fields.get("ID KOC")),
            "id_kenh": parse_lark_value(fields.get("ID kênh")),
            "thang_air": extract_month(fields.get("Tháng air")),
            "thoi_gian_air": fields.get("Thời gian air"),
            "thoi_gian_air_video": parse_lark_value(fields.get("Thời gian air video")),
            "link_air_bai": parse_lark_value(fields.get("Link air bài")),
            "trang_thai_gan_gio": fields.get("Trạng thái gắn giỏ"),
            "ngay_gan_gio": parse_lark_value(fields.get("Ngày gắn giỏ")),
            "nhan_su_book": parse_lark_value(fields.get("Nhân sự book")),
            "san_pham": fields.get("Sản phẩm"),
            "phan_loai_san_pham": find_phan_loai_field(fields),
            "status": parse_lark_value(fields.get("Status")),
            "luot_xem": parse_lark_value(fields.get("Lượt xem hiện tại")),
            "da_air": fields.get("Đã air"),
            "da_nhan": fields.get("Đã nhận"),
            "da_di_don": fields.get("Đã đi đơn"),
            "da_deal": parse_lark_value(fields.get("Đã deal")),
            "so_tien_tt": parse_lark_value(fields.get("Số tiền TT")),
        }
        
        if month is not None:
            koc_month = koc_data.get("thang_air")
            if koc_month is None:
                skipped_wrong_month += 1
                continue
            if koc_month != month:
                skipped_wrong_month += 1
                continue
        
        results.append(koc_data)
    
    print(f"📊 Result: {len(results)} records for month={month}, skipped {skipped_wrong_month}")
    
    return results


async def get_task_records(
    team: Optional[str] = None,
    vi_tri: Optional[str] = None,
    month: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Lấy records từ bảng Task"""
    records = await get_all_records(
        app_token=TASK_BASE["app_token"],
        table_id=TASK_BASE["table_id"],
        max_records=2000
    )
    
    def parse_person_field(value):
        if value is None:
            return None
        if isinstance(value, list) and len(value) > 0:
            first = value[0]
            if isinstance(first, dict):
                return first.get("en_name") or first.get("name") or first.get("email")
        if isinstance(value, str):
            return value
        return str(value) if value else None
    
    def extract_task_month(value) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            m = int(value)
            return m if 1 <= m <= 12 else None
        if isinstance(value, str):
            match = re.search(r'(\d{1,2})', value)
            if match:
                m = int(match.group(1))
                return m if 1 <= m <= 12 else None
        if isinstance(value, list) and len(value) > 0:
            first = value[0]
            if isinstance(first, dict):
                text_val = first.get("text") or first.get("value")
                if text_val:
                    match = re.search(r'(\d{1,2})', str(text_val))
                    if match:
                        m = int(match.group(1))
                        return m if 1 <= m <= 12 else None
        return None
    
    results = []
    for record in records:
        fields = record.get("fields", {})
        
        deadline_raw = fields.get("Deadline")
        deadline_ts = None
        deadline_str = None
        if deadline_raw:
            try:
                if isinstance(deadline_raw, (int, float)):
                    deadline_ts = deadline_raw
                    deadline_str = datetime.fromtimestamp(deadline_raw / 1000).strftime("%Y-%m-%d %H:%M")
                else:
                    deadline_str = str(deadline_raw)[:10]
            except:
                pass
        
        task_month = extract_task_month(fields.get("Tháng"))
        
        task_data = {
            "record_id": record.get("record_id"),
            "du_an": extract_field_value(fields, "Dự án"),
            "cong_viec": extract_field_value(fields, "Công việc"),
            "mo_ta": extract_field_value(fields, "Mô tả chi tiết"),
            "nguoi_phu_trach": parse_person_field(fields.get("Người phụ trách")),
            "nguoi_duyet": parse_person_field(fields.get("Người duyệt")),
            "vi_tri": fields.get("Vị trí"),
            "ngay_tao": fields.get("Ngày tạo"),
            "deadline": deadline_str,
            "deadline_ts": deadline_ts,
            "link_ket_qua": extract_field_value(fields, "Link Kết quả"),
            "duyet": fields.get("Duyệt"),
            "overdue": fields.get("Overdue"),
            "ghi_chu": extract_field_value(fields, "Ghi chú"),
            "thang": task_month,
            "nam": extract_field_value(fields, "Năm"),
        }
        
        if month is not None:
            if task_data.get("thang") != month:
                continue
        
        if vi_tri:
            task_vi_tri = task_data.get("vi_tri")
            if task_vi_tri:
                if vi_tri.lower() not in str(task_vi_tri).lower():
                    continue
            else:
                continue
        
        if team:
            phu_trach = task_data.get("nguoi_phu_trach")
            if phu_trach:
                if team.lower() not in str(phu_trach).lower():
                    continue
            else:
                continue
        
        if start_date or end_date:
            if deadline_ts:
                try:
                    deadline_dt = datetime.fromtimestamp(deadline_ts / 1000)
                    
                    if start_date:
                        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                        if deadline_dt < start_dt:
                            continue
                    
                    if end_date:
                        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
                        if deadline_dt > end_dt:
                            continue
                except Exception as e:
                    print(f"Date parse error: {e}")
        
        results.append(task_data)
    
    return results


# ============ REPORT GENERATORS (KALLE) ============

PRODUCT_FILTER_PATTERNS = {
    "box_qua": ["box quà", "box qua", "set quà", "set qua"],
    "nuoc_hoa": ["nước hoa", "nuoc hoa"],
    "sua_tam": ["sữa tắm", "sua tam"],
}

async def generate_koc_summary(
    month: int, 
    week: Optional[int] = None, 
    group_by: str = "product",
    product_filter: Optional[str] = None
) -> Dict[str, Any]:
    """Tạo báo cáo tổng hợp KOC KALLE theo tháng/tuần"""
    records = await get_booking_records(month=month, week=week)
    
    if product_filter and product_filter in PRODUCT_FILTER_PATTERNS:
        patterns = PRODUCT_FILTER_PATTERNS[product_filter]
        filtered_records = []
        for koc in records:
            san_pham = str(koc.get("san_pham") or "").lower()
            if any(p in san_pham for p in patterns):
                filtered_records.append(koc)
        records = filtered_records
        print(f"📦 Product filter '{product_filter}': {len(records)} records match")
    
    total = len(records)
    da_air = 0
    chua_air = 0
    da_air_chua_link = 0
    da_air_chua_gan_gio = 0
    
    tong_chi_phi_deal = 0
    tong_chi_phi_thanh_toan = 0
    
    by_product = {}
    by_brand = {}
    
    missing_link_kocs = []
    missing_gio_kocs = []
    
    def safe_string(value):
        if value is None:
            return "Không xác định"
        if isinstance(value, str):
            return value if value else "Không xác định"
        if isinstance(value, list):
            if len(value) > 0:
                first = value[0]
                if isinstance(first, dict):
                    return first.get("text") or first.get("value") or str(first)
                return str(first)
            return "Không xác định"
        if isinstance(value, dict):
            return value.get("text") or value.get("value") or str(value)
        return str(value) if value else "Không xác định"
    
    for koc in records:
        da_deal = koc.get("da_deal")
        if da_deal:
            try:
                chi_phi = float(str(da_deal).replace(",", ""))
                tong_chi_phi_deal += chi_phi
            except:
                pass
        
        so_tien_tt = koc.get("so_tien_tt")
        if so_tien_tt:
            try:
                chi_phi_tt = float(str(so_tien_tt).replace(",", ""))
                tong_chi_phi_thanh_toan += chi_phi_tt
            except:
                pass
        
        san_pham = safe_string(koc.get("san_pham"))
        phan_loai = safe_string(koc.get("phan_loai_san_pham"))
        
        if phan_loai == "Không xác định" and san_pham != "Không xác định":
            phan_loai = san_pham
        
        if san_pham not in by_product:
            by_product[san_pham] = {"count": 0, "chi_phi": 0, "da_air": 0, "chua_air": 0, "kocs": []}
        by_product[san_pham]["count"] += 1
        
        if phan_loai not in by_brand:
            by_brand[phan_loai] = {"count": 0, "chi_phi": 0, "da_air": 0, "chua_air": 0, "kocs": []}
        by_brand[phan_loai]["count"] += 1
        
        if da_deal:
            try:
                chi_phi_val = float(str(da_deal).replace(",", ""))
                by_product[san_pham]["chi_phi"] += chi_phi_val
                by_brand[phan_loai]["chi_phi"] += chi_phi_val
            except:
                pass
        
        link_air = koc.get("link_air_bai")
        thoi_gian_air = koc.get("thoi_gian_air_video")
        da_air_field = koc.get("da_air")
        
        has_aired = bool(link_air or thoi_gian_air or da_air_field)
        
        koc_info = {
            "id_koc": koc.get("id_koc"),
            "id_kenh": koc.get("id_kenh"),
            "link_air": link_air,
            "da_air": has_aired,
            "trang_thai_gio": koc.get("trang_thai_gan_gio"),
            "chi_phi": da_deal
        }
        by_product[san_pham]["kocs"].append(koc_info)
        by_brand[phan_loai]["kocs"].append(koc_info)
        
        if has_aired:
            da_air += 1
            by_product[san_pham]["da_air"] += 1
            by_brand[phan_loai]["da_air"] += 1
            
            if not link_air:
                da_air_chua_link += 1
                missing_link_kocs.append(koc)
            
            trang_thai_gio = koc.get("trang_thai_gan_gio")
            if trang_thai_gio:
                trang_thai_str = str(trang_thai_gio).lower()
                if "chưa" in trang_thai_str or "chua" in trang_thai_str or "không" in trang_thai_str or "khong" in trang_thai_str:
                    da_air_chua_gan_gio += 1
                    missing_gio_kocs.append(koc)
            else:
                da_air_chua_gan_gio += 1
                missing_gio_kocs.append(koc)
        else:
            chua_air += 1
            by_product[san_pham]["chua_air"] += 1
            by_brand[phan_loai]["chua_air"] += 1
    
    by_group = by_brand if group_by == "brand" else by_product
    group_label = "phân loại sản phẩm" if group_by == "brand" else "sản phẩm"
    
    return {
        "month": month,
        "week": week,
        "group_by": group_by,
        "group_label": group_label,
        "summary": {
            "total": total,
            "da_air": da_air,
            "chua_air": chua_air,
            "da_air_chua_link": da_air_chua_link,
            "da_air_chua_gan_gio": da_air_chua_gan_gio,
            "tong_chi_phi_deal": tong_chi_phi_deal,
            "tong_chi_phi_thanh_toan": tong_chi_phi_thanh_toan
        },
        "by_group": by_group,
        "by_product": by_product,
        "by_brand": by_brand,
        "missing_link_kocs": missing_link_kocs[:10],
        "missing_gio_kocs": missing_gio_kocs[:10],
        "all_records": records
    }


async def generate_content_calendar(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    month: Optional[int] = None,
    team: Optional[str] = None,
    vi_tri: Optional[str] = None
) -> Dict[str, Any]:
    """Tạo báo cáo lịch content theo tuần hoặc tháng"""
    if month:
        records = await get_task_records(team=team, vi_tri=vi_tri, month=month)
        date_range = f"Tháng {month}"
    else:
        records = await get_task_records(team=team, vi_tri=vi_tri, start_date=start_date, end_date=end_date)
        date_range = f"{start_date} → {end_date}" if start_date and end_date else "tuần này"
    
    by_date = {}
    by_vi_tri = {}
    overdue = []
    
    for task in records:
        deadline = task.get("deadline")
        vi_tri_task = task.get("vi_tri") or "Không xác định"
        overdue_field = task.get("overdue")
        
        if deadline:
            date_key = str(deadline)[:10]
            if date_key not in by_date:
                by_date[date_key] = []
            by_date[date_key].append(task)
        
        if vi_tri_task not in by_vi_tri:
            by_vi_tri[vi_tri_task] = []
        by_vi_tri[vi_tri_task].append(task)
        
        if overdue_field:
            overdue.append(task)
    
    return {
        "date_range": date_range,
        "month": month,
        "team_filter": team,
        "vi_tri_filter": vi_tri,
        "summary": {
            "total_tasks": len(records),
            "total_overdue": len(overdue),
            "days_with_content": len(by_date),
            "vi_tri_count": len(by_vi_tri)
        },
        "by_date": by_date,
        "by_vi_tri": by_vi_tri,
        "overdue_tasks": overdue,
        "all_records": records
    }


async def generate_task_summary(
    month: Optional[int] = None,
    vi_tri: Optional[str] = None
) -> Dict[str, Any]:
    """Tạo báo cáo phân tích task theo vị trí"""
    tasks = await get_task_records(vi_tri=vi_tri, month=month)
    
    now = datetime.now()
    today = now.date()
    
    by_vi_tri = {}
    total_overdue = 0
    total_sap_deadline = 0
    total_chua_duyet = 0
    
    overdue_tasks = []
    sap_deadline_tasks = []
    
    for task in tasks:
        vi_tri_task = task.get("vi_tri") or "Không xác định"
        overdue_field = task.get("overdue")
        deadline_ts = task.get("deadline_ts")
        duyet = task.get("duyet")
        
        if vi_tri_task not in by_vi_tri:
            by_vi_tri[vi_tri_task] = {
                "total": 0,
                "overdue": 0,
                "sap_deadline": 0,
                "da_duyet": 0,
                "chua_duyet": 0,
                "tasks_overdue": [],
                "tasks_sap_deadline": []
            }
        
        by_vi_tri[vi_tri_task]["total"] += 1
        
        if overdue_field:
            by_vi_tri[vi_tri_task]["overdue"] += 1
            by_vi_tri[vi_tri_task]["tasks_overdue"].append(task)
            overdue_tasks.append(task)
            total_overdue += 1
        
        if deadline_ts and not overdue_field:
            try:
                deadline_dt = datetime.fromtimestamp(deadline_ts / 1000).date()
                days_until = (deadline_dt - today).days
                
                if 0 <= days_until <= 3:
                    by_vi_tri[vi_tri_task]["sap_deadline"] += 1
                    by_vi_tri[vi_tri_task]["tasks_sap_deadline"].append(task)
                    sap_deadline_tasks.append(task)
                    total_sap_deadline += 1
            except:
                pass
        
        if duyet:
            by_vi_tri[vi_tri_task]["da_duyet"] += 1
        else:
            by_vi_tri[vi_tri_task]["chua_duyet"] += 1
            total_chua_duyet += 1
    
    for vi_tri_key in by_vi_tri:
        by_vi_tri[vi_tri_key]["tasks_overdue"] = by_vi_tri[vi_tri_key]["tasks_overdue"][:5]
        by_vi_tri[vi_tri_key]["tasks_sap_deadline"] = by_vi_tri[vi_tri_key]["tasks_sap_deadline"][:5]
    
    return {
        "month": month,
        "vi_tri_filter": vi_tri,
        "summary": {
            "total_tasks": len(tasks),
            "total_overdue": total_overdue,
            "total_sap_deadline": total_sap_deadline,
            "total_chua_duyet": total_chua_duyet,
            "so_vi_tri": len(by_vi_tri)
        },
        "by_vi_tri": by_vi_tri,
        "overdue_tasks": overdue_tasks[:20],
        "sap_deadline_tasks": sap_deadline_tasks[:20]
    }


# ============ KALLE DASHBOARD FUNCTIONS ============

async def get_dashboard_thang_records(month: Optional[int] = None, week: Optional[str] = None) -> List[Dict]:
    """Lấy records từ bảng Dashboard Tháng KALLE"""
    records = await get_all_records(
        app_token=DASHBOARD_THANG_TABLE["app_token"],
        table_id=DASHBOARD_THANG_TABLE["table_id"],
        max_records=2000  # Increased from 500 to get all records
    )
    
    print(f"📊 Dashboard Tháng: Total records = {len(records)}, filter month = {month}")
    
    result = []
    month_distribution = {}
    
    for record in records:
        fields = record.get("fields", {})
        
        thang_raw = safe_extract_text(fields.get("Tháng báo cáo"))
        try:
            thang = int(thang_raw) if thang_raw else None
        except:
            thang = None
        
        month_distribution[thang] = month_distribution.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        tuan = fields.get("Tuần báo cáo")
        if week and tuan != week:
            continue
        
        # Parse loại video (v5.7.15)
        loai_video = extract_loai_video(fields)
        
        # Parse phân loại gửi hàng (v5.7.17)
        phan_loai_gh = find_phan_loai_field(fields)
        
        # Debug: log field names để check (chỉ log 1 lần)
        if len(result) == 0:
            print(f"📋 Available fields: {list(fields.keys())}")
            print(f"   Content Text raw: {fields.get('Content Text')}")
            print(f"   Loại video extracted: {loai_video}")
        
        result.append({
            "nhan_su": safe_extract_person_name(fields.get("Nhân sự book")),
            "san_pham": fields.get("Sản phẩm"),
            "thang": thang,
            "tuan": tuan,
            "loai_video": loai_video,  # v5.7.15 - content breakdown
            "phan_loai_gh": phan_loai_gh,  # v5.7.17 - phân loại gửi hàng
            "kpi_so_luong": fields.get("KPI Số lượng"),
            "kpi_ngan_sach": fields.get("KPI ngân sách"),
            "so_luong_deal": fields.get("Số lượng - Deal", 0),
            "so_luong_air": fields.get("Số lượng - Air", 0),
            "so_luong_tong_air": fields.get("Số lượng tổng - Air", 0),
            "ngan_sach_deal": fields.get("Ngân sách - Deal", 0),
            "ngan_sach_air": fields.get("Ngân sách - Air", 0),
            "ngan_sach_tong_air": fields.get("Ngân sách tổng - Air", 0),
            "pct_kpi_so_luong": fields.get("% KPI Số lượng tổng", 0),
            "pct_kpi_ngan_sach": fields.get("% KPI Ngân sách tổng - Air", 0),
            # v5.7.24: Content fields
            "content_text": fields.get("Content Text") or 0,
            "content_cart": fields.get("Content cart") or 0,
        })
    
    print(f"📊 Month distribution: {month_distribution}")
    print(f"📊 After filter: {len(result)} records")
    
    return result


async def get_doanh_thu_koc_records(month: Optional[int] = None, week: Optional[str] = None) -> List[Dict]:
    """Lấy records từ bảng Doanh thu KOC KALLE (tuần)"""
    records = await get_all_records(
        app_token=DOANH_THU_KOC_TABLE["app_token"],
        table_id=DOANH_THU_KOC_TABLE["table_id"],
        max_records=1000
    )
    
    result = []
    for record in records:
        fields = record.get("fields", {})
        
        thang_raw = safe_extract_text(fields.get("Tháng báo cáo"))
        try:
            thang = int(thang_raw) if thang_raw else None
        except:
            thang = None
        
        if month and thang != month:
            continue
        
        tuan = fields.get("Tuần báo cáo")
        if week and tuan != week:
            continue
        
        gmv_raw = fields.get("GMV", "0")
        try:
            gmv = float(str(gmv_raw).replace(",", ""))
        except:
            gmv = 0
        
        result.append({
            "id_kenh": fields.get("ID kênh"),
            "gmv": gmv,
            "link_video": fields.get("Link video", {}).get("link") if isinstance(fields.get("Link video"), dict) else None,
            "thang": thang,
            "tuan": tuan,
            "ngay_dang": fields.get("Ngày đăng"),
        })
    
    return result


async def get_lien_he_records(month: Optional[int] = None, week: Optional[str] = None) -> List[Dict]:
    """Lấy records từ bảng Data liên hệ KALLE (tuần)"""
    records = await get_all_records(
        app_token=LIEN_HE_TUAN_TABLE["app_token"],
        table_id=LIEN_HE_TUAN_TABLE["table_id"],
        max_records=500
    )
    
    print(f"📞 Liên hệ: Total records = {len(records)}, filter month = {month}")
    
    result = []
    month_distribution = {}
    
    for record in records:
        fields = record.get("fields", {})
        
        thang_raw = safe_extract_text(fields.get("Tháng báo cáo"))
        try:
            thang = int(thang_raw) if thang_raw else None
        except:
            thang = None
        
        month_distribution[thang] = month_distribution.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        tuan = fields.get("Tuần báo cáo")
        if week and tuan != week:
            continue
        
        result.append({
            "nhan_su": safe_extract_person_name(fields.get("Người tạo")),
            "thang": thang,
            "tuan": tuan,
            "thoi_gian_tuan": fields.get("Thời gian tuần"),
            "tong_lien_he": fields.get("Tổng liên hệ", 0),
            "da_deal": fields.get("Đã deal", "0"),
            "dang_trao_doi": fields.get("Đang trao đổi", "0"),
            "tu_choi": fields.get("Từ chối", "0"),
            "khong_phan_hoi": fields.get("Không phản hồi từ đầu", "0"),
            "ty_le_deal": fields.get("Tỷ lệ đã deal", 0),
            "ty_le_trao_doi": fields.get("Tỷ lệ đang trao đổi", 0),
            "ty_le_tu_choi": fields.get("Tỷ lệ từ chối", 0),
        })
    
    print(f"📞 Month distribution: {month_distribution}")
    print(f"📞 After filter: {len(result)} records")
    
    return result


async def generate_dashboard_summary(month: Optional[int] = None, week: Optional[str] = None) -> Dict[str, Any]:
    """Tạo báo cáo Dashboard tổng hợp KALLE"""
    logger.info(f"🎯 generate_dashboard_summary called: month={month}, week={week}")
    dashboard_records = await get_dashboard_thang_records(month=month, week=week)
    logger.info(f"📊 Dashboard records fetched: {len(dashboard_records)}")
    
    booking_records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=2000
    )
    
    doanh_thu_records = await get_doanh_thu_koc_records(month=month, week=week)
    lien_he_records = await get_lien_he_records(month=month, week=week)
    
    # Đếm video đã air theo nhân sự
    video_air_by_nhan_su = {}
    for record in booking_records:
        fields = record.get("fields", {})
        
        link_air = fields.get("Link air bài") or fields.get("link_air_bai") or fields.get("Link air")
        if not link_air:
            continue
        
        thoi_gian_air = fields.get("Thời gian air") or fields.get("thoi_gian_air")
        thang_air = None
        
        if thoi_gian_air:
            try:
                if isinstance(thoi_gian_air, (int, float)):
                    dt = datetime.fromtimestamp(thoi_gian_air / 1000, VN_TZ)
                    thang_air = dt.month
                elif isinstance(thoi_gian_air, str):
                    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]:
                        try:
                            dt = datetime.strptime(thoi_gian_air[:10], fmt)
                            thang_air = dt.month
                            break
                        except:
                            continue
            except:
                pass
        
        if thang_air is None:
            thang_du_kien_raw = fields.get("Tháng dự kiến") or fields.get("Tháng dự kiến air")
            try:
                if isinstance(thang_du_kien_raw, list) and len(thang_du_kien_raw) > 0:
                    first = thang_du_kien_raw[0]
                    thang_air = int(first.get("text", 0)) if isinstance(first, dict) else int(first)
                elif isinstance(thang_du_kien_raw, (int, float)):
                    thang_air = int(thang_du_kien_raw)
                elif isinstance(thang_du_kien_raw, str):
                    thang_air = int(thang_du_kien_raw)
            except:
                pass
        
        if month and thang_air != month:
            continue
        
        nhan_su = safe_extract_person_name(fields.get("Nhân sự book"))
        if nhan_su:
            nhan_su = nhan_su.strip()
        
        if nhan_su not in video_air_by_nhan_su:
            video_air_by_nhan_su[nhan_su] = 0
        video_air_by_nhan_su[nhan_su] += 1
    
    print(f"📹 Video air by nhân sự (tháng air {month}): {video_air_by_nhan_su}")
    
    # Tổng hợp KPI theo nhân sự
    kpi_by_nhan_su = {}
    
    for r in dashboard_records:
        nhan_su = r["nhan_su"]
        if nhan_su:
            nhan_su = nhan_su.strip()
        
        # v5.7.16: Removed Tuần 1 filter - KPI should aggregate all weeks
        # Note: KPI values are stored per-row, so we need to be careful about double-counting
        # If KPI is duplicated across weeks, we should only count from Tuần 1
        tuan = r.get("tuan")
        # If KPI values are same across weeks, only count from first week to avoid duplication
        # Otherwise comment these lines to aggregate all weeks
        if tuan and tuan != "Tuần 1":
            continue
        
        if nhan_su not in kpi_by_nhan_su:
            kpi_by_nhan_su[nhan_su] = {
                "kpi_so_luong": 0,
                "kpi_ngan_sach": 0,
                "so_luong_air": 0,
                "ngan_sach_air": 0,
                "pct_kpi_so_luong": 0,
                "pct_kpi_ngan_sach": 0,
            }
        
        try:
            kpi_sl = int(r.get("kpi_so_luong") or 0)
            kpi_ns = int(r.get("kpi_ngan_sach") or 0)
            sl_air = int(r.get("so_luong_tong_air") or 0)
            ns_air = int(r.get("ngan_sach_tong_air") or 0)
            
            kpi_by_nhan_su[nhan_su]["kpi_so_luong"] += kpi_sl
            kpi_by_nhan_su[nhan_su]["kpi_ngan_sach"] += kpi_ns
            kpi_by_nhan_su[nhan_su]["so_luong_air"] += sl_air
            kpi_by_nhan_su[nhan_su]["ngan_sach_air"] += ns_air
            
            san_pham = r.get("san_pham") or "N/A"
            print(f"   📌 {nhan_su} | {san_pham}: KPI={kpi_sl}, Air={sl_air}")
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    for nhan_su, data in kpi_by_nhan_su.items():
        if data["kpi_so_luong"] > 0:
            data["pct_kpi_so_luong"] = round(data["so_luong_air"] / data["kpi_so_luong"] * 100, 1)
        if data["kpi_ngan_sach"] > 0:
            data["pct_kpi_ngan_sach"] = round(data["ngan_sach_air"] / data["kpi_ngan_sach"] * 100, 1)
        
        print(f"   ✅ TỔNG {nhan_su}: {data['so_luong_air']}/{data['kpi_so_luong']}")
    
    print(f"📊 KPI by nhân sự (từ Dashboard): {kpi_by_nhan_su}")
    
    # Top KOC doanh số
    koc_gmv = {}
    for r in doanh_thu_records:
        id_kenh = r["id_kenh"]
        if id_kenh:
            if id_kenh not in koc_gmv:
                koc_gmv[id_kenh] = 0
            koc_gmv[id_kenh] += r["gmv"]
    
    top_koc = sorted(koc_gmv.items(), key=lambda x: x[1], reverse=True)[:10]
    
    # Tổng hợp liên hệ theo nhân sự
    lien_he_by_nhan_su = {}
    for r in lien_he_records:
        nhan_su = r["nhan_su"]
        if nhan_su not in lien_he_by_nhan_su:
            lien_he_by_nhan_su[nhan_su] = {
                "tong_lien_he": 0,
                "da_deal": 0,
                "dang_trao_doi": 0,
                "tu_choi": 0,
            }
        
        lien_he_by_nhan_su[nhan_su]["tong_lien_he"] += r.get("tong_lien_he") or 0
        try:
            lien_he_by_nhan_su[nhan_su]["da_deal"] += int(r.get("da_deal") or 0)
            lien_he_by_nhan_su[nhan_su]["dang_trao_doi"] += int(r.get("dang_trao_doi") or 0)
            lien_he_by_nhan_su[nhan_su]["tu_choi"] += int(r.get("tu_choi") or 0)
        except:
            pass
    
    for ns, data in lien_he_by_nhan_su.items():
        total = data["tong_lien_he"]
        if total > 0:
            data["ty_le_deal"] = round(data["da_deal"] / total * 100, 1)
            data["ty_le_trao_doi"] = round(data["dang_trao_doi"] / total * 100, 1)
            data["ty_le_tu_choi"] = round(data["tu_choi"] / total * 100, 1)
        else:
            data["ty_le_deal"] = 0
            data["ty_le_trao_doi"] = 0
            data["ty_le_tu_choi"] = 0
    
    # === CONTENT BREAKDOWN BY NHÂN SỰ (v5.7.24 KALLE) ===
    # Lấy từ DASHBOARD THÁNG - đã parse sẵn content_text và content_cart
    content_by_nhan_su = {}
    
    total_content_text = 0
    total_content_cart = 0
    
    for r in dashboard_records:
        nhan_su = r.get("nhan_su")
        if not nhan_su:
            continue
        nhan_su = nhan_su.strip()
        
        # Lấy sản phẩm
        san_pham = r.get("san_pham") or "N/A"
        if isinstance(san_pham, list) and len(san_pham) > 0:
            san_pham = san_pham[0] if isinstance(san_pham[0], str) else san_pham[0].get("text", "N/A")
        san_pham = str(san_pham).strip() if san_pham else "N/A"
        
        # Lấy Content Text và Cart (đã parse trong get_dashboard_thang_records)
        try:
            content_text = int(r.get("content_text") or 0)
        except:
            content_text = 0
        
        try:
            content_cart = int(r.get("content_cart") or 0)
        except:
            content_cart = 0
        
        total_content_text += content_text
        total_content_cart += content_cart
        
        # Aggregate theo nhân sự
        if nhan_su not in content_by_nhan_su:
            content_by_nhan_su[nhan_su] = []
        
        # Thêm Content cart nếu có
        if content_cart > 0:
            found = False
            for item in content_by_nhan_su[nhan_su]:
                if item["san_pham"] == san_pham and item["loai"] == "Cart":
                    item["so_luong"] += content_cart
                    found = True
                    break
            if not found:
                content_by_nhan_su[nhan_su].append({
                    "san_pham": san_pham,
                    "loai": "Cart",
                    "phan_loai": "",
                    "so_luong": content_cart
                })
        
        # Thêm Content text nếu có
        if content_text > 0:
            found = False
            for item in content_by_nhan_su[nhan_su]:
                if item["san_pham"] == san_pham and item["loai"] == "Text":
                    item["so_luong"] += content_text
                    found = True
                    break
            if not found:
                content_by_nhan_su[nhan_su].append({
                    "san_pham": san_pham,
                    "loai": "Text",
                    "phan_loai": "",
                    "so_luong": content_text
                })
    
    # Sort content items theo số lượng giảm dần
    for nhan_su in content_by_nhan_su:
        content_by_nhan_su[nhan_su].sort(key=lambda x: x["so_luong"], reverse=True)
    
    # v5.7.24: Debug log
    total_content_count = total_content_text + total_content_cart
    print(f"📝 KALLE Content (from Dashboard): Cart={total_content_cart}, Text={total_content_text}, Tổng={total_content_count}")
    for ns, items in list(content_by_nhan_su.items())[:2]:
        print(f"   {ns}: {items[:3]}")
    
    # Tổng quan
    total_kpi_so_luong = sum(d["kpi_so_luong"] for d in kpi_by_nhan_su.values())
    total_so_luong_air = sum(d["so_luong_air"] for d in kpi_by_nhan_su.values())
    total_kpi_ngan_sach = sum(d["kpi_ngan_sach"] for d in kpi_by_nhan_su.values())
    total_ngan_sach_air = sum(d["ngan_sach_air"] for d in kpi_by_nhan_su.values())
    total_gmv = sum(koc_gmv.values())
    
    print(f"📊 TỔNG QUAN: {total_so_luong_air}/{total_kpi_so_luong} ({round(total_so_luong_air / total_kpi_so_luong * 100, 1) if total_kpi_so_luong > 0 else 0}%)")
    
    # === TRANSFORM DATA FOR REPORT_GENERATOR (v5.7.23) ===
    # Convert Dict format to List format expected by report_generator
    # Giờ content_by_nhan_su cũng từ Dashboard nên tên sẽ match
    staff_list = []
    
    # v5.7.23: Debug - kiểm tra tên
    print(f"🔍 Tên từ KPI (kpi_by_nhan_su): {list(kpi_by_nhan_su.keys())}")
    print(f"🔍 Tên từ Content (content_by_nhan_su): {list(content_by_nhan_su.keys())}")
    
    for nhan_su_name, kpi_data in kpi_by_nhan_su.items():
        # Get contact info for this staff
        contact_info = lien_he_by_nhan_su.get(nhan_su_name, {})
        
        # v5.7.23: Get content - exact match vì cùng source Dashboard
        content_items = content_by_nhan_su.get(nhan_su_name, [])
        if content_items:
            total_count = sum(item.get("so_luong", 0) for item in content_items)
            print(f"   ✅ {nhan_su_name}: {len(content_items)} loại, tổng {total_count}")
        else:
            print(f"   ⚠️ No content for: '{nhan_su_name}'")
        
        content_breakdown = {}
        total_by_type = {"Cart": 0, "Text": 0, "Video": 0}
        
        for item in content_items:
            san_pham = item.get('san_pham', 'N/A')
            loai = item.get('loai', 'Video')
            phan_loai = item.get('phan_loai', '')
            so_luong = item.get("so_luong", 0)
            
            # v5.7.17: Format key theo yêu cầu: "{loai} {sản_phẩm},{loai},{phân_loại}"
            # Example: "cart Nước hoa,Cart,Dark Beauty 30ml"
            if phan_loai:
                key = f"{loai.lower()} {san_pham},{loai},{phan_loai}"
            else:
                key = f"{loai.lower()} {san_pham},{loai}"
            
            if key in content_breakdown:
                content_breakdown[key] += so_luong
            else:
                content_breakdown[key] = so_luong
            
            # Track totals by type
            if loai in total_by_type:
                total_by_type[loai] += so_luong
        
        # Add totals to content breakdown
        content_breakdown["total"] = sum(v for k, v in content_breakdown.items() if k not in ("total", "total_cart", "total_text"))
        content_breakdown["total_cart"] = total_by_type.get("Cart", 0)
        content_breakdown["total_text"] = total_by_type.get("Text", 0)
        
        # v5.7.20: Debug log cho content của từng staff
        if content_breakdown.get("total", 0) > 0:
            print(f"   📦 {nhan_su_name}: content_total={content_breakdown.get('total', 0)}, items={len(content_items)}")
        
        # Calculate percentages
        video_kpi = kpi_data.get("kpi_so_luong", 0)
        video_done = kpi_data.get("so_luong_air", 0)
        budget_kpi = kpi_data.get("kpi_ngan_sach", 0)
        budget_done = kpi_data.get("ngan_sach_air", 0)
        
        video_percent = round(video_done / video_kpi * 100, 1) if video_kpi > 0 else 0
        budget_percent = round(budget_done / budget_kpi * 100, 1) if budget_kpi > 0 else 0
        
        staff_list.append({
            "name": nhan_su_name,
            # Video KPI
            "video_kpi": video_kpi,
            "video_done": video_done,
            "video_percent": video_percent,
            # Budget KPI
            "budget_kpi": budget_kpi,
            "budget_done": budget_done,
            "budget_percent": budget_percent,
            # Contact info
            "contact_total": contact_info.get("tong_lien_he", 0),
            "contact_deal": contact_info.get("da_deal", 0),
            "contact_deal_percent": contact_info.get("ty_le_deal", 0),
            # Content breakdown
            "content_breakdown": content_breakdown,
        })
    
    # Sort by video_done descending
    staff_list.sort(key=lambda x: x.get("video_done", 0), reverse=True)
    
    print(f"📊 Staff list created: {len(staff_list)} items")
    for s in staff_list[:3]:
        print(f"   • {s['name']}: {s['video_done']}/{s['video_kpi']} ({s['video_percent']}%)")
    
    return {
        "month": month,
        "week": week,
        "brand": "KALLE",
        # New format for report_generator (v5.7.16)
        "staff_list": staff_list,
        "totals": {
            "video_kpi": total_kpi_so_luong,
            "video_done": total_so_luong_air,
            "video_percent": round(total_so_luong_air / total_kpi_so_luong * 100, 1) if total_kpi_so_luong > 0 else 0,
            "budget_kpi": total_kpi_ngan_sach,
            "budget_done": total_ngan_sach_air,
            "budget_percent": round(total_ngan_sach_air / total_kpi_ngan_sach * 100, 1) if total_kpi_ngan_sach > 0 else 0,
            "total_gmv": total_gmv,
        },
        # Keep old format for backward compatibility
        "tong_quan": {
            "kpi_so_luong": total_kpi_so_luong,
            "so_luong_air": total_so_luong_air,
            "pct_kpi_so_luong": round(total_so_luong_air / total_kpi_so_luong * 100, 1) if total_kpi_so_luong > 0 else 0,
            "kpi_ngan_sach": total_kpi_ngan_sach,
            "ngan_sach_air": total_ngan_sach_air,
            "pct_kpi_ngan_sach": round(total_ngan_sach_air / total_kpi_ngan_sach * 100, 1) if total_kpi_ngan_sach > 0 else 0,
            "total_gmv": total_gmv,
        },
        "kpi_nhan_su": kpi_by_nhan_su,
        "top_koc": top_koc,
        "lien_he_nhan_su": lien_he_by_nhan_su,
        "content_by_nhan_su": content_by_nhan_su,
    }


# ============ TEST ============
async def get_field_names(app_token: str, table_id: str) -> list:
    """Lấy danh sách tất cả field names từ một bảng"""
    records = await get_all_records(app_token, table_id, max_records=1)
    if records:
        return list(records[0].get("fields", {}).keys())
    return []

async def test_connection():
    """Test kết nối với Lark Base"""
    try:
        print("🔄 Testing Lark Base connection...")
        
        booking_records = await get_all_records(
            app_token=BOOKING_BASE["app_token"],
            table_id=BOOKING_BASE["table_id"],
            max_records=5
        )
        print(f"✅ Booking Base: {len(booking_records)} records found")
        
        if booking_records:
            all_fields = list(booking_records[0].get('fields', {}).keys())
            print(f"   All fields ({len(all_fields)}): {all_fields}")
        
        task_records = await get_all_records(
            app_token=TASK_BASE["app_token"],
            table_id=TASK_BASE["table_id"],
            max_records=5
        )
        print(f"✅ Task Base: {len(task_records)} records found")
        
        if task_records:
            all_fields = list(task_records[0].get('fields', {}).keys())
            print(f"   All fields ({len(all_fields)}): {all_fields}")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

async def debug_booking_fields():
    """Debug: Xem tất cả fields từ Booking table"""
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=3
    )
    
    result = {
        "total_sample": len(records),
        "fields": {},
        "sample_records": []
    }
    
    if records:
        all_fields = list(records[0].get("fields", {}).keys())
        result["all_field_names"] = all_fields
        
        for record in records:
            fields = record.get("fields", {})
            sample = {}
            for key, value in fields.items():
                sample[key] = str(value)[:100] if value else None
            result["sample_records"].append(sample)
    
    return result

async def debug_task_fields():
    """Debug: Xem tất cả fields từ Task table"""
    records = await get_all_records(
        app_token=TASK_BASE["app_token"],
        table_id=TASK_BASE["table_id"],
        max_records=3
    )
    
    result = {
        "total_sample": len(records),
        "fields": {},
        "sample_records": []
    }
    
    if records:
        all_fields = list(records[0].get("fields", {}).keys())
        result["all_field_names"] = all_fields
        
        for record in records:
            fields = record.get("fields", {})
            sample = {}
            for key, value in fields.items():
                sample[key] = str(value)[:100] if value else None
            result["sample_records"].append(sample)
    
    return result


# ============ NOTES FUNCTIONS ============
async def get_all_notes() -> List[Dict]:
    """Lấy TẤT CẢ notes (cho scheduler reminder)"""
    records = await get_all_records(
        NOTES_TABLE["app_token"],
        NOTES_TABLE["table_id"]
    )
    
    notes = []
    for r in records:
        fields = r.get("fields", {})
        notes.append({
            "record_id": r.get("record_id"),
            "chat_id": fields.get("chat_id"),
            "note_key": fields.get("note_key"),
            "note_value": fields.get("note_value"),
            "deadline": fields.get("deadline"),
            "created_at": fields.get("created_at"),
        })
    
    return notes


async def get_notes_due_soon(minutes: int = 30) -> List[Dict]:
    """Lấy các notes sắp đến hạn trong khoảng thời gian nhất định
    
    Args:
        minutes: Số phút từ bây giờ để kiểm tra deadline (mặc định 30 phút)
        
    Returns:
        List các notes có deadline trong khoảng thời gian sắp tới
    """
    all_notes = await get_all_notes()
    
    now = datetime.now()
    deadline_threshold = now + timedelta(minutes=minutes)
    
    due_soon_notes = []
    for note in all_notes:
        deadline = note.get("deadline")
        if deadline:
            try:
                # Lark Bitable stores timestamp in milliseconds
                if isinstance(deadline, (int, float)):
                    deadline_dt = datetime.fromtimestamp(deadline / 1000)
                else:
                    # Try to parse as string
                    from dateutil import parser
                    deadline_dt = parser.parse(str(deadline))
                
                # Check if deadline is between now and threshold
                if now <= deadline_dt <= deadline_threshold:
                    note["deadline_dt"] = deadline_dt  # Add parsed datetime
                    due_soon_notes.append(note)
            except Exception as e:
                # Skip notes with invalid deadline
                continue
    
    return due_soon_notes


async def get_notes_by_chat_id(chat_id: str) -> List[Dict]:
    """Lấy tất cả notes của một chat"""
    records = await get_all_records(
        NOTES_TABLE["app_token"],
        NOTES_TABLE["table_id"],
        filter_formula=f'CurrentValue.[chat_id] = "{chat_id}"'
    )
    
    notes = []
    for r in records:
        fields = r.get("fields", {})
        notes.append({
            "record_id": r.get("record_id"),
            "chat_id": fields.get("chat_id"),
            "note_key": fields.get("note_key"),
            "note_value": fields.get("note_value"),
            "deadline": fields.get("deadline"),
            "created_at": fields.get("created_at"),
        })
    
    return notes


async def get_note_by_key(chat_id: str, note_key: str) -> Optional[Dict]:
    """Lấy một note theo key"""
    records = await get_all_records(
        NOTES_TABLE["app_token"],
        NOTES_TABLE["table_id"],
        filter_formula=f'AND(CurrentValue.[chat_id] = "{chat_id}", CurrentValue.[note_key] = "{note_key}")'
    )
    
    if not records:
        return None
    
    r = records[0]
    fields = r.get("fields", {})
    return {
        "record_id": r.get("record_id"),
        "chat_id": fields.get("chat_id"),
        "note_key": fields.get("note_key"),
        "note_value": fields.get("note_value"),
        "deadline": fields.get("deadline"),
        "created_at": fields.get("created_at"),
    }


async def create_note(chat_id: str, note_key: str, note_value: str, deadline: str = None) -> Dict:
    """Tạo note mới
    Lark Bitable DateTime fields require timestamp in milliseconds
    """
    # Convert datetime to milliseconds timestamp for Lark Bitable
    now_timestamp = int(datetime.now().timestamp() * 1000)
    
    fields = {
        "chat_id": chat_id,
        "note_key": note_key,
        "note_value": note_value,
        "created_at": now_timestamp,
    }
    
    if deadline:
        # Try to parse deadline and convert to timestamp
        try:
            from dateutil import parser
            deadline_dt = parser.parse(deadline)
            fields["deadline"] = int(deadline_dt.timestamp() * 1000)
        except:
            # If parsing fails, try to use as-is or skip
            try:
                # Maybe it's already a timestamp string
                fields["deadline"] = int(float(deadline) * 1000) if float(deadline) < 2000000000 else int(deadline)
            except:
                pass  # Skip deadline if can't parse
    
    result = await create_record(
        NOTES_TABLE["app_token"],
        NOTES_TABLE["table_id"],
        fields
    )
    
    return result


async def update_note(record_id: str, note_value: str = None, deadline: str = None) -> Dict:
    """Cập nhật note"""
    fields = {}
    
    if note_value is not None:
        fields["note_value"] = note_value
    
    if deadline is not None:
        # Convert deadline to timestamp
        try:
            from dateutil import parser
            deadline_dt = parser.parse(deadline)
            fields["deadline"] = int(deadline_dt.timestamp() * 1000)
        except:
            try:
                fields["deadline"] = int(float(deadline) * 1000) if float(deadline) < 2000000000 else int(deadline)
            except:
                pass
    
    if not fields:
        return {"error": "No fields to update"}
    
    result = await update_record(
        NOTES_TABLE["app_token"],
        NOTES_TABLE["table_id"],
        record_id,
        fields
    )
    
    return result


async def delete_note(record_id: str) -> Dict:
    """Xóa note"""
    result = await delete_record(
        NOTES_TABLE["app_token"],
        NOTES_TABLE["table_id"],
        record_id
    )
    
    return result


async def get_notes_due_soon(hours: int = 24) -> List[Dict]:
    """Lấy các notes sắp đến hạn trong số giờ tới."""
    now_ts = int(datetime.now().timestamp() * 1000)
    future_ts = now_ts + int(hours * 3600 * 1000)

    all_notes = await get_all_notes()
    due_notes = []

    for note in all_notes:
        deadline = note.get("deadline")
        if deadline is None:
            continue
        try:
            deadline_ts = int(deadline)
        except (TypeError, ValueError):
            continue
        if now_ts <= deadline_ts <= future_ts:
            due_notes.append(note)

    return due_notes


async def debug_notes_table():
    """Debug: Xem cấu trúc bảng Notes"""
    records = await get_all_records(
        app_token=NOTES_TABLE["app_token"],
        table_id=NOTES_TABLE["table_id"],
        max_records=5
    )
    
    result = {
        "table_info": NOTES_TABLE,
        "total_sample": len(records),
        "fields": [],
        "sample_records": []
    }
    
    if records:
        all_fields = list(records[0].get("fields", {}).keys())
        result["fields"] = all_fields
        
        for record in records:
            fields = record.get("fields", {})
            sample = {"record_id": record.get("record_id")}
            for key, value in fields.items():
                sample[key] = str(value)[:100] if value else None
            result["sample_records"].append(sample)
    
    return result
