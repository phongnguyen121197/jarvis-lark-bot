# lark_base.py - Version 5.8.0
# Fixed: Content breakdown aggregation for KALLE and CHENG reports
# Added: content_by_nhan_su data structure for report_generator.py

import os
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - LARK BITABLE
# ============================================================================

LARK_APP_ID = os.getenv("LARK_APP_ID", "")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET", "")

# KALLE Bitable Config
KALLE_BASE_ID = os.getenv("KALLE_BASE_ID", "")
KALLE_TABLE_BOOKING = os.getenv("KALLE_TABLE_BOOKING", "")  # Data list booking
KALLE_TABLE_DASHBOARD = os.getenv("KALLE_TABLE_DASHBOARD", "")  # Dashboard tháng

# CHENG Bitable Config  
CHENG_BASE_ID = os.getenv("CHENG_BASE_ID", "")
CHENG_TABLE_DASHBOARD = os.getenv("CHENG_TABLE_DASHBOARD", "")  # Dashboard Tháng
CHENG_TABLE_LIEN_HE = os.getenv("CHENG_TABLE_LIEN_HE", "")  # Liên hệ tuần
CHENG_TABLE_KOC = os.getenv("CHENG_TABLE_KOC", "")  # Doanh thu KOC
CHENG_TABLE_TONG = os.getenv("CHENG_TABLE_TONG", "")  # Doanh thu tổng

# Notes Bitable Config
NOTES_BASE_ID = os.getenv("NOTES_BASE_ID", "XfHGbvXrRaK1zcsTZ1zl5QR3ghf")
NOTES_TABLE_ID = os.getenv("NOTES_TABLE_ID", "tbl6LiH9n7xs4VMs")

# Calendar Config
CALENDAR_ID = os.getenv("LARK_CALENDAR_ID", "7585485663517069021")

# ============================================================================
# LARK API - TOKEN MANAGEMENT
# ============================================================================

_token_cache = {
    "access_token": None,
    "expires_at": None
}

def get_tenant_access_token() -> str:
    """Get Lark tenant access token with caching"""
    now = datetime.now()
    
    # Return cached token if valid
    if _token_cache["access_token"] and _token_cache["expires_at"]:
        if now < _token_cache["expires_at"]:
            return _token_cache["access_token"]
    
    # Request new token
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": LARK_APP_ID,
        "app_secret": LARK_APP_SECRET
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        data = response.json()
        
        if data.get("code") == 0:
            token = data.get("tenant_access_token")
            expire_seconds = data.get("expire", 7200)
            
            _token_cache["access_token"] = token
            _token_cache["expires_at"] = now + timedelta(seconds=expire_seconds - 300)
            
            return token
        else:
            logger.error(f"Failed to get token: {data}")
            return ""
    except Exception as e:
        logger.error(f"Token request error: {e}")
        return ""


def get_headers() -> Dict[str, str]:
    """Get headers with authorization"""
    token = get_tenant_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

# ============================================================================
# LARK BITABLE - GENERIC CRUD OPERATIONS
# ============================================================================

def get_bitable_records(
    base_id: str,
    table_id: str,
    filter_formula: str = None,
    page_size: int = 500,
    page_token: str = None
) -> Tuple[List[Dict], str]:
    """
    Fetch records from Lark Bitable
    Returns: (records_list, next_page_token)
    """
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"
    
    params = {"page_size": page_size}
    if filter_formula:
        params["filter"] = filter_formula
    if page_token:
        params["page_token"] = page_token
    
    try:
        response = requests.get(url, headers=get_headers(), params=params, timeout=30)
        data = response.json()
        
        if data.get("code") == 0:
            items = data.get("data", {}).get("items", [])
            next_token = data.get("data", {}).get("page_token", "")
            return items, next_token
        else:
            logger.error(f"Bitable fetch error: {data}")
            return [], ""
    except Exception as e:
        logger.error(f"Bitable request error: {e}")
        return [], ""


def get_all_bitable_records(
    base_id: str,
    table_id: str,
    filter_formula: str = None,
    max_pages: int = 10
) -> List[Dict]:
    """Fetch all records with pagination"""
    all_records = []
    page_token = None
    
    for _ in range(max_pages):
        records, next_token = get_bitable_records(
            base_id, table_id, filter_formula, page_token=page_token
        )
        all_records.extend(records)
        
        if not next_token:
            break
        page_token = next_token
    
    return all_records


def create_record(base_id: str, table_id: str, fields: Dict) -> Optional[str]:
    """Create a new record in Bitable. Returns record_id or None."""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"
    
    payload = {"fields": fields}
    
    try:
        response = requests.post(url, headers=get_headers(), json=payload, timeout=10)
        data = response.json()
        
        if data.get("code") == 0:
            record_id = data.get("data", {}).get("record", {}).get("record_id")
            return record_id
        else:
            logger.error(f"Create record error: {data}")
            return None
    except Exception as e:
        logger.error(f"Create record exception: {e}")
        return None


def update_record(base_id: str, table_id: str, record_id: str, fields: Dict) -> bool:
    """Update an existing record. Returns True on success."""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}"
    
    payload = {"fields": fields}
    
    try:
        response = requests.put(url, headers=get_headers(), json=payload, timeout=10)
        data = response.json()
        
        if data.get("code") == 0:
            return True
        else:
            logger.error(f"Update record error: {data}")
            return False
    except Exception as e:
        logger.error(f"Update record exception: {e}")
        return False


def delete_record(base_id: str, table_id: str, record_id: str) -> bool:
    """Delete a record. Returns True on success."""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}"
    
    try:
        response = requests.delete(url, headers=get_headers(), timeout=10)
        data = response.json()
        
        if data.get("code") == 0:
            return True
        else:
            logger.error(f"Delete record error: {data}")
            return False
    except Exception as e:
        logger.error(f"Delete record exception: {e}")
        return False

# ============================================================================
# NOTES MANAGEMENT - CRUD OPERATIONS
# ============================================================================

def get_notes_by_chat(chat_id: str) -> List[Dict]:
    """Get all notes for a specific chat"""
    filter_formula = f'CurrentValue.[chat_id]="{chat_id}"'
    return get_all_bitable_records(NOTES_BASE_ID, NOTES_TABLE_ID, filter_formula)


def create_note(chat_id: str, note_key: str, note_value: str, deadline: datetime = None) -> Optional[str]:
    """Create a new note"""
    fields = {
        "chat_id": chat_id,
        "note_key": note_key,
        "note_value": note_value,
        "created_at": int(datetime.now().timestamp() * 1000)
    }
    
    if deadline:
        fields["deadline"] = int(deadline.timestamp() * 1000)
    
    return create_record(NOTES_BASE_ID, NOTES_TABLE_ID, fields)


def update_note(record_id: str, fields: Dict) -> bool:
    """Update an existing note"""
    # Convert datetime to milliseconds if present
    if "deadline" in fields and isinstance(fields["deadline"], datetime):
        fields["deadline"] = int(fields["deadline"].timestamp() * 1000)
    
    return update_record(NOTES_BASE_ID, NOTES_TABLE_ID, record_id, fields)


def delete_note(record_id: str) -> bool:
    """Delete a note"""
    return delete_record(NOTES_BASE_ID, NOTES_TABLE_ID, record_id)


def get_notes_due_soon(hours: int = 24) -> List[Dict]:
    """Get all notes with deadlines within the next N hours"""
    now = datetime.now()
    future = now + timedelta(hours=hours)
    
    now_ms = int(now.timestamp() * 1000)
    future_ms = int(future.timestamp() * 1000)
    
    filter_formula = f'AND(CurrentValue.[deadline]>={now_ms}, CurrentValue.[deadline]<={future_ms})'
    return get_all_bitable_records(NOTES_BASE_ID, NOTES_TABLE_ID, filter_formula)

# ============================================================================
# KALLE - DATA FETCHING
# ============================================================================

def get_kalle_booking_records(month: int = None, year: int = None) -> List[Dict]:
    """Get KALLE booking records, optionally filtered by month/year"""
    filter_formula = None
    
    if month and year:
        # Format: "Tháng 12/2024" or similar
        month_str = f"Tháng {month}"
        filter_formula = f'CurrentValue.[Tháng air]="{month_str}"'
    
    return get_all_bitable_records(KALLE_BASE_ID, KALLE_TABLE_BOOKING, filter_formula)


def get_kalle_dashboard_records(month: int = None) -> List[Dict]:
    """Get KALLE dashboard KPI records"""
    filter_formula = None
    
    if month:
        filter_formula = f'CurrentValue.[Tháng]="{month}"'
    
    return get_all_bitable_records(KALLE_BASE_ID, KALLE_TABLE_DASHBOARD, filter_formula)

# ============================================================================
# CHENG - DATA FETCHING
# ============================================================================

def get_cheng_dashboard_records(month: int = None) -> List[Dict]:
    """Get CHENG dashboard records"""
    filter_formula = None
    
    if month:
        filter_formula = f'CurrentValue.[Tháng]="{month}"'
    
    return get_all_bitable_records(CHENG_BASE_ID, CHENG_TABLE_DASHBOARD, filter_formula)


def get_cheng_lien_he_records(week: int = None, month: int = None) -> List[Dict]:
    """Get CHENG weekly contact records"""
    filters = []
    
    if week:
        filters.append(f'CurrentValue.[Tuần]={week}')
    if month:
        filters.append(f'CurrentValue.[Tháng]="{month}"')
    
    filter_formula = f"AND({','.join(filters)})" if len(filters) > 1 else (filters[0] if filters else None)
    
    return get_all_bitable_records(CHENG_BASE_ID, CHENG_TABLE_LIEN_HE, filter_formula)


def get_cheng_koc_records(month: int = None) -> List[Dict]:
    """Get CHENG KOC revenue records"""
    filter_formula = None
    
    if month:
        filter_formula = f'CurrentValue.[Tháng]="{month}"'
    
    return get_all_bitable_records(CHENG_BASE_ID, CHENG_TABLE_KOC, filter_formula)


def get_cheng_doanh_thu_tong_records(month: int = None) -> List[Dict]:
    """Get CHENG total revenue records (for GMV calculation)"""
    filter_formula = None
    
    if month:
        filter_formula = f'CurrentValue.[Tháng]="{month}"'
    
    return get_all_bitable_records(CHENG_BASE_ID, CHENG_TABLE_TONG, filter_formula)

# ============================================================================
# CONTENT AGGREGATION - NEW IN v5.8.0
# ============================================================================

def aggregate_content_by_staff(
    records: List[Dict],
    staff_field: str = "Nhân sự book",
    content_field: str = "Content",
    product_field: str = "Phân loại sp (Chỉ được chọn - Không được add mới)",
    product_gh_field: str = "Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)"
) -> Dict[str, Dict[str, int]]:
    """
    Aggregate content counts by staff member
    
    Returns:
    {
        "Như Mai": {
            "Nước hoa,Cart,Dark Beauty 30ml": 30,
            "Nước hoa,Text,Dark Beauty 30ml": 10,
            "total_cart": 30,
            "total_text": 10,
            "total": 40
        },
        ...
    }
    """
    content_by_staff = defaultdict(lambda: defaultdict(int))
    
    for record in records:
        fields = record.get("fields", {})
        
        # Extract staff name (handle both string and list)
        staff = fields.get(staff_field, "")
        if isinstance(staff, list):
            staff = staff[0].get("text", "") if staff else ""
        
        if not staff:
            continue
        
        # Extract content type (Text/Cart)
        content_type = fields.get(content_field, "")
        if isinstance(content_type, list):
            content_type = content_type[0] if content_type else ""
        
        # Extract product category
        product = fields.get(product_field, "")
        if isinstance(product, list):
            product = product[0] if product else ""
        
        # Extract product GH category  
        product_gh = fields.get(product_gh_field, "")
        if isinstance(product_gh, list):
            product_gh = product_gh[0] if product_gh else ""
        
        # Build content key
        if product and content_type:
            content_key = f"{product},{content_type}"
            if product_gh:
                content_key = f"{product},{content_type},{product_gh}"
            
            content_by_staff[staff][content_key] += 1
        
        # Track totals by content type
        if content_type.lower() == "cart":
            content_by_staff[staff]["total_cart"] += 1
        elif content_type.lower() == "text":
            content_by_staff[staff]["total_text"] += 1
        
        content_by_staff[staff]["total"] += 1
    
    return dict(content_by_staff)


def format_content_breakdown_for_staff(content_data: Dict[str, int]) -> str:
    """
    Format content breakdown for a single staff member
    
    Input: {"Nước hoa,Cart,Dark Beauty": 30, "Nước hoa,Text,Dark Beauty": 10, "total": 40}
    Output: "30 Nước hoa,Cart,Dark Beauty và 10 Nước hoa,Text,Dark Beauty"
    """
    if not content_data:
        return ""
    
    # Filter out total fields
    items = []
    for key, count in content_data.items():
        if key not in ("total", "total_cart", "total_text"):
            items.append(f"{count} {key}")
    
    if not items:
        return ""
    
    # Join with "và" for Vietnamese
    if len(items) == 1:
        return items[0]
    elif len(items) == 2:
        return f"{items[0]} và {items[1]}"
    else:
        return ", ".join(items[:-1]) + f" và {items[-1]}"

# ============================================================================
# KALLE - SUMMARY GENERATION
# ============================================================================

def generate_koc_summary(staff_name: str, month: int = None) -> Dict[str, Any]:
    """
    Generate KOC summary for KALLE staff
    
    Returns dict with:
    - staff_name, month
    - video_kpi, video_done, video_percent
    - budget_kpi, budget_done, budget_percent
    - contact_total, contact_deal, contact_percent
    - content_by_nhan_su (NEW in v5.8.0)
    - status, progress
    """
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    
    # Fetch booking records
    booking_records = get_kalle_booking_records(month, year)
    
    # Fetch dashboard KPI
    dashboard_records = get_kalle_dashboard_records(month)
    
    # Filter by staff name
    staff_bookings = []
    for record in booking_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự book", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if staff_name.lower() in nhan_su.lower():
            staff_bookings.append(record)
    
    # Get KPI from dashboard
    staff_dashboard = None
    for record in dashboard_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if staff_name.lower() in nhan_su.lower():
            staff_dashboard = fields
            break
    
    # Calculate metrics
    video_done = len(staff_bookings)
    video_kpi = staff_dashboard.get("KPI video", 0) if staff_dashboard else 0
    
    budget_done = sum(
        record.get("fields", {}).get("Giá book", 0) or 0
        for record in staff_bookings
    )
    budget_kpi = staff_dashboard.get("KPI ngân sách", 0) if staff_dashboard else 0
    
    # Calculate percentages
    video_percent = (video_done / video_kpi * 100) if video_kpi > 0 else 0
    budget_percent = (budget_done / budget_kpi * 100) if budget_kpi > 0 else 0
    
    # Contact stats (from Lark base if available)
    contact_total = staff_dashboard.get("Tổng liên hệ", 0) if staff_dashboard else 0
    contact_deal = staff_dashboard.get("Đã deal", 0) if staff_dashboard else 0
    contact_percent = (contact_deal / contact_total * 100) if contact_total > 0 else 0
    
    # === NEW: Content aggregation ===
    content_by_nhan_su = aggregate_content_by_staff(staff_bookings)
    staff_content = content_by_nhan_su.get(staff_name, {})
    
    # Also check partial name match
    if not staff_content:
        for name, data in content_by_nhan_su.items():
            if staff_name.lower() in name.lower():
                staff_content = data
                break
    
    # Determine status
    avg_percent = (video_percent + budget_percent) / 2
    if avg_percent >= 100:
        status = "🟢 Đạt KPI"
    elif avg_percent >= 80:
        status = "🟢 Gần đạt"
    elif avg_percent >= 50:
        status = "🟡 Đang tiến hành"
    else:
        status = "🔴 Cần cải thiện"
    
    # Progress bar
    progress = min(100, int(avg_percent))
    
    return {
        "staff_name": staff_name,
        "month": month,
        "year": year,
        "brand": "KALLE",
        
        # Video metrics
        "video_kpi": video_kpi,
        "video_done": video_done,
        "video_percent": round(video_percent, 1),
        
        # Budget metrics
        "budget_kpi": budget_kpi,
        "budget_done": budget_done,
        "budget_percent": round(budget_percent, 1),
        
        # Contact metrics
        "contact_total": contact_total,
        "contact_deal": contact_deal,
        "contact_percent": round(contact_percent, 1),
        
        # === NEW: Content breakdown ===
        "content_by_nhan_su": content_by_nhan_su,
        "content_breakdown": staff_content,
        "content_breakdown_text": format_content_breakdown_for_staff(staff_content),
        
        # Status
        "status": status,
        "progress": progress
    }


def generate_dashboard_summary(month: int = None) -> Dict[str, Any]:
    """
    Generate KALLE dashboard summary for all staff
    
    Returns dict with:
    - month, year, brand
    - staff_list: List of staff summaries
    - totals: Aggregate totals
    - content_by_nhan_su (NEW in v5.8.0)
    """
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    
    # Fetch all data
    booking_records = get_kalle_booking_records(month, year)
    dashboard_records = get_kalle_dashboard_records(month)
    
    # Build staff KPI lookup
    staff_kpi = {}
    for record in dashboard_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if nhan_su:
            staff_kpi[nhan_su] = {
                "video_kpi": fields.get("KPI video", 0) or 0,
                "budget_kpi": fields.get("KPI ngân sách", 0) or 0,
                "contact_total": fields.get("Tổng liên hệ", 0) or 0,
                "contact_deal": fields.get("Đã deal", 0) or 0
            }
    
    # Aggregate by staff from bookings
    staff_stats = defaultdict(lambda: {"video_done": 0, "budget_done": 0})
    
    for record in booking_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự book", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if nhan_su:
            staff_stats[nhan_su]["video_done"] += 1
            staff_stats[nhan_su]["budget_done"] += fields.get("Giá book", 0) or 0
    
    # === NEW: Content aggregation ===
    content_by_nhan_su = aggregate_content_by_staff(booking_records)
    
    # Build staff list
    staff_list = []
    for name, kpi in staff_kpi.items():
        stats = staff_stats.get(name, {"video_done": 0, "budget_done": 0})
        content_data = content_by_nhan_su.get(name, {})
        
        video_percent = (stats["video_done"] / kpi["video_kpi"] * 100) if kpi["video_kpi"] > 0 else 0
        budget_percent = (stats["budget_done"] / kpi["budget_kpi"] * 100) if kpi["budget_kpi"] > 0 else 0
        contact_percent = (kpi["contact_deal"] / kpi["contact_total"] * 100) if kpi["contact_total"] > 0 else 0
        
        avg_percent = (video_percent + budget_percent) / 2
        if avg_percent >= 100:
            status = "🟢 Đạt"
        elif avg_percent >= 80:
            status = "🟢 Gần đạt"
        elif avg_percent >= 50:
            status = "🟡 Đang làm"
        else:
            status = "🔴 Cần cải thiện"
        
        staff_list.append({
            "name": name,
            "video_kpi": kpi["video_kpi"],
            "video_done": stats["video_done"],
            "video_percent": round(video_percent, 1),
            "budget_kpi": kpi["budget_kpi"],
            "budget_done": stats["budget_done"],
            "budget_percent": round(budget_percent, 1),
            "contact_total": kpi["contact_total"],
            "contact_deal": kpi["contact_deal"],
            "contact_percent": round(contact_percent, 1),
            "status": status,
            "progress": min(100, int(avg_percent)),
            # === NEW ===
            "content_breakdown": content_data,
            "content_breakdown_text": format_content_breakdown_for_staff(content_data)
        })
    
    # Calculate totals
    total_video_kpi = sum(s["video_kpi"] for s in staff_list)
    total_video_done = sum(s["video_done"] for s in staff_list)
    total_budget_kpi = sum(s["budget_kpi"] for s in staff_list)
    total_budget_done = sum(s["budget_done"] for s in staff_list)
    
    return {
        "month": month,
        "year": year,
        "brand": "KALLE",
        "staff_list": staff_list,
        "content_by_nhan_su": content_by_nhan_su,
        "totals": {
            "video_kpi": total_video_kpi,
            "video_done": total_video_done,
            "video_percent": round(total_video_done / total_video_kpi * 100, 1) if total_video_kpi > 0 else 0,
            "budget_kpi": total_budget_kpi,
            "budget_done": total_budget_done,
            "budget_percent": round(total_budget_done / total_budget_kpi * 100, 1) if total_budget_kpi > 0 else 0
        }
    }

# ============================================================================
# CHENG - SUMMARY GENERATION
# ============================================================================

def generate_cheng_koc_summary(staff_name: str, month: int = None) -> Dict[str, Any]:
    """
    Generate KOC summary for CHENG staff
    
    Returns dict with:
    - staff_name, month, brand
    - video_kpi, video_done, video_percent
    - gmv_kpi, gmv_done, gmv_percent
    - contact stats
    - content_by_nhan_su (NEW in v5.8.0)
    """
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    
    # Fetch CHENG data
    dashboard_records = get_cheng_dashboard_records(month)
    lien_he_records = get_cheng_lien_he_records(month=month)
    koc_records = get_cheng_koc_records(month)
    tong_records = get_cheng_doanh_thu_tong_records(month)
    
    # Find staff in dashboard
    staff_dashboard = None
    for record in dashboard_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "") or fields.get("Tên nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if staff_name.lower() in nhan_su.lower():
            staff_dashboard = fields
            break
    
    # Get KPIs
    video_kpi = 0
    gmv_kpi = 0
    video_done = 0
    gmv_done = 0
    
    if staff_dashboard:
        video_kpi = staff_dashboard.get("KPI video", 0) or staff_dashboard.get("KPI Video", 0) or 0
        gmv_kpi = staff_dashboard.get("KPI GMV", 0) or staff_dashboard.get("KPI doanh thu", 0) or 0
        video_done = staff_dashboard.get("Video đã air", 0) or staff_dashboard.get("Đã air", 0) or 0
    
    # Get GMV from total revenue table (fixed in v5.7.1)
    for record in tong_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "") or fields.get("Tên nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if staff_name.lower() in nhan_su.lower():
            gmv_done = fields.get("GMV", 0) or fields.get("Doanh thu", 0) or 0
            break
    
    # Contact stats from lien he
    contact_total = 0
    contact_deal = 0
    
    for record in lien_he_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "") or fields.get("Tên nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if staff_name.lower() in nhan_su.lower():
            contact_total += fields.get("Tổng liên hệ", 0) or fields.get("Số lượng liên hệ", 0) or 0
            contact_deal += fields.get("Đã deal", 0) or fields.get("Deal thành công", 0) or 0
    
    # Calculate percentages
    video_percent = (video_done / video_kpi * 100) if video_kpi > 0 else 0
    gmv_percent = (gmv_done / gmv_kpi * 100) if gmv_kpi > 0 else 0
    contact_percent = (contact_deal / contact_total * 100) if contact_total > 0 else 0
    
    # === NEW: Content aggregation for CHENG ===
    # Note: CHENG may have different field names, adjust as needed
    content_by_nhan_su = {}
    
    # Try to aggregate from KOC records
    staff_koc_records = []
    for record in koc_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "") or fields.get("Tên nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if staff_name.lower() in nhan_su.lower():
            staff_koc_records.append(record)
    
    if staff_koc_records:
        content_by_nhan_su = aggregate_content_by_staff(
            staff_koc_records,
            staff_field="Nhân sự",
            content_field="Loại content",  # CHENG might use different field name
            product_field="Phân loại SP",
            product_gh_field="Phân loại GH"
        )
    
    staff_content = content_by_nhan_su.get(staff_name, {})
    if not staff_content:
        for name, data in content_by_nhan_su.items():
            if staff_name.lower() in name.lower():
                staff_content = data
                break
    
    # Status
    avg_percent = (video_percent + gmv_percent) / 2
    if avg_percent >= 100:
        status = "🟢 Đạt KPI"
    elif avg_percent >= 80:
        status = "🟢 Gần đạt"
    elif avg_percent >= 50:
        status = "🟡 Đang tiến hành"
    else:
        status = "🔴 Cần cải thiện"
    
    return {
        "staff_name": staff_name,
        "month": month,
        "year": year,
        "brand": "CHENG",
        
        # Video metrics
        "video_kpi": video_kpi,
        "video_done": video_done,
        "video_percent": round(video_percent, 1),
        
        # GMV metrics (instead of budget for CHENG)
        "gmv_kpi": gmv_kpi,
        "gmv_done": gmv_done,
        "gmv_percent": round(gmv_percent, 1),
        
        # Contact metrics
        "contact_total": contact_total,
        "contact_deal": contact_deal,
        "contact_percent": round(contact_percent, 1),
        
        # === NEW: Content breakdown ===
        "content_by_nhan_su": content_by_nhan_su,
        "content_breakdown": staff_content,
        "content_breakdown_text": format_content_breakdown_for_staff(staff_content),
        
        # Status
        "status": status,
        "progress": min(100, int(avg_percent))
    }


def generate_cheng_dashboard_summary(month: int = None) -> Dict[str, Any]:
    """
    Generate CHENG dashboard summary for all staff
    """
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    
    # Fetch data
    dashboard_records = get_cheng_dashboard_records(month)
    tong_records = get_cheng_doanh_thu_tong_records(month)
    koc_records = get_cheng_koc_records(month)
    
    # Build GMV lookup from total table
    gmv_by_staff = {}
    for record in tong_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "") or fields.get("Tên nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if nhan_su:
            gmv_by_staff[nhan_su] = fields.get("GMV", 0) or fields.get("Doanh thu", 0) or 0
    
    # === NEW: Content aggregation ===
    content_by_nhan_su = aggregate_content_by_staff(
        koc_records,
        staff_field="Nhân sự",
        content_field="Loại content",
        product_field="Phân loại SP",
        product_gh_field="Phân loại GH"
    )
    
    # Build staff list
    staff_list = []
    for record in dashboard_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "") or fields.get("Tên nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if not nhan_su:
            continue
        
        video_kpi = fields.get("KPI video", 0) or fields.get("KPI Video", 0) or 0
        video_done = fields.get("Video đã air", 0) or fields.get("Đã air", 0) or 0
        gmv_kpi = fields.get("KPI GMV", 0) or fields.get("KPI doanh thu", 0) or 0
        gmv_done = gmv_by_staff.get(nhan_su, 0)
        
        video_percent = (video_done / video_kpi * 100) if video_kpi > 0 else 0
        gmv_percent = (gmv_done / gmv_kpi * 100) if gmv_kpi > 0 else 0
        avg_percent = (video_percent + gmv_percent) / 2
        
        if avg_percent >= 100:
            status = "🟢 Đạt"
        elif avg_percent >= 80:
            status = "🟢 Gần đạt"
        elif avg_percent >= 50:
            status = "🟡 Đang làm"
        else:
            status = "🔴 Cần cải thiện"
        
        content_data = content_by_nhan_su.get(nhan_su, {})
        
        staff_list.append({
            "name": nhan_su,
            "video_kpi": video_kpi,
            "video_done": video_done,
            "video_percent": round(video_percent, 1),
            "gmv_kpi": gmv_kpi,
            "gmv_done": gmv_done,
            "gmv_percent": round(gmv_percent, 1),
            "status": status,
            "progress": min(100, int(avg_percent)),
            # === NEW ===
            "content_breakdown": content_data,
            "content_breakdown_text": format_content_breakdown_for_staff(content_data)
        })
    
    # Totals
    total_video_kpi = sum(s["video_kpi"] for s in staff_list)
    total_video_done = sum(s["video_done"] for s in staff_list)
    total_gmv_kpi = sum(s["gmv_kpi"] for s in staff_list)
    total_gmv_done = sum(s["gmv_done"] for s in staff_list)
    
    return {
        "month": month,
        "year": year,
        "brand": "CHENG",
        "staff_list": staff_list,
        "content_by_nhan_su": content_by_nhan_su,
        "totals": {
            "video_kpi": total_video_kpi,
            "video_done": total_video_done,
            "video_percent": round(total_video_done / total_video_kpi * 100, 1) if total_video_kpi > 0 else 0,
            "gmv_kpi": total_gmv_kpi,
            "gmv_done": total_gmv_done,
            "gmv_percent": round(total_gmv_done / total_gmv_kpi * 100, 1) if total_gmv_kpi > 0 else 0
        }
    }

# ============================================================================
# CALENDAR INTEGRATION
# ============================================================================

def get_calendar_events(start_time: datetime = None, end_time: datetime = None) -> List[Dict]:
    """Get calendar events within time range"""
    if not CALENDAR_ID:
        logger.warning("Calendar ID not configured")
        return []
    
    if start_time is None:
        start_time = datetime.now()
    if end_time is None:
        end_time = start_time + timedelta(days=7)
    
    # Convert to Unix timestamp (seconds)
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    
    url = f"https://open.larksuite.com/open-apis/calendar/v4/calendars/{CALENDAR_ID}/events"
    params = {
        "start_time": str(start_ts),
        "end_time": str(end_ts)
    }
    
    try:
        response = requests.get(url, headers=get_headers(), params=params, timeout=10)
        data = response.json()
        
        if data.get("code") == 0:
            return data.get("data", {}).get("items", [])
        else:
            logger.error(f"Calendar fetch error: {data}")
            return []
    except Exception as e:
        logger.error(f"Calendar request error: {e}")
        return []


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_current_month_range() -> Tuple[datetime, datetime]:
    """Get start and end of current month"""
    now = datetime.now()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Get last day of month
    if now.month == 12:
        end = now.replace(year=now.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        end = now.replace(month=now.month + 1, day=1) - timedelta(days=1)
    
    end = end.replace(hour=23, minute=59, second=59)
    return start, end


def format_number(num: float, suffix: str = "") -> str:
    """Format number with Vietnamese locale"""
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B{suffix}"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M{suffix}"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K{suffix}"
    else:
        return f"{num:.0f}{suffix}"


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    # Test content aggregation
    print("Testing lark_base.py v5.8.0...")
    
    # Test with mock data
    mock_records = [
        {"fields": {"Nhân sự book": "Như Mai", "Content": "Cart", "Phân loại sp (Chỉ được chọn - Không được add mới)": "Nước hoa", "Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)": "Dark Beauty 30ml"}},
        {"fields": {"Nhân sự book": "Như Mai", "Content": "Cart", "Phân loại sp (Chỉ được chọn - Không được add mới)": "Nước hoa", "Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)": "Dark Beauty 30ml"}},
        {"fields": {"Nhân sự book": "Như Mai", "Content": "Text", "Phân loại sp (Chỉ được chọn - Không được add mới)": "Nước hoa", "Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)": "Dark Beauty 30ml"}},
    ]
    
    result = aggregate_content_by_staff(mock_records)
    print(f"Aggregated: {result}")
    
    if "Như Mai" in result:
        text = format_content_breakdown_for_staff(result["Như Mai"])
        print(f"Formatted: {text}")
