# lark_base.py - Version 5.8.1
# Fixed: Added missing functions required by main.py
# - generate_content_calendar
# - generate_task_summary  
# - test_connection
# - get_all_notes (for scheduler)

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
KALLE_TABLE_BOOKING = os.getenv("KALLE_TABLE_BOOKING", "")
KALLE_TABLE_DASHBOARD = os.getenv("KALLE_TABLE_DASHBOARD", "")
KALLE_TABLE_CONTENT = os.getenv("KALLE_TABLE_CONTENT", "")  # Content calendar table
KALLE_TABLE_TASK = os.getenv("KALLE_TABLE_TASK", "")  # Task table

# CHENG Bitable Config  
CHENG_BASE_ID = os.getenv("CHENG_BASE_ID", "")
CHENG_TABLE_DASHBOARD = os.getenv("CHENG_TABLE_DASHBOARD", "")
CHENG_TABLE_LIEN_HE = os.getenv("CHENG_TABLE_LIEN_HE", "")
CHENG_TABLE_KOC = os.getenv("CHENG_TABLE_KOC", "")
CHENG_TABLE_TONG = os.getenv("CHENG_TABLE_TONG", "")

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
    
    if _token_cache["access_token"] and _token_cache["expires_at"]:
        if now < _token_cache["expires_at"]:
            return _token_cache["access_token"]
    
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
    """Fetch records from Lark Bitable"""
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
    """Create a new record in Bitable"""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"
    payload = {"fields": fields}
    
    try:
        response = requests.post(url, headers=get_headers(), json=payload, timeout=10)
        data = response.json()
        
        if data.get("code") == 0:
            return data.get("data", {}).get("record", {}).get("record_id")
        else:
            logger.error(f"Create record error: {data}")
            return None
    except Exception as e:
        logger.error(f"Create record exception: {e}")
        return None


def update_record(base_id: str, table_id: str, record_id: str, fields: Dict) -> bool:
    """Update an existing record"""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}"
    payload = {"fields": fields}
    
    try:
        response = requests.put(url, headers=get_headers(), json=payload, timeout=10)
        data = response.json()
        return data.get("code") == 0
    except Exception as e:
        logger.error(f"Update record exception: {e}")
        return False


def delete_record(base_id: str, table_id: str, record_id: str) -> bool:
    """Delete a record"""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/{record_id}"
    
    try:
        response = requests.delete(url, headers=get_headers(), timeout=10)
        data = response.json()
        return data.get("code") == 0
    except Exception as e:
        logger.error(f"Delete record exception: {e}")
        return False

# ============================================================================
# TEST CONNECTION - REQUIRED BY main.py
# ============================================================================

async def test_connection() -> bool:
    """
    Test connection to Lark API
    Required by main.py for /test/base endpoint
    """
    try:
        token = get_tenant_access_token()
        if token:
            logger.info("✅ Lark connection successful")
            return True
        else:
            logger.error("❌ Failed to get Lark token")
            return False
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False

# ============================================================================
# NOTES MANAGEMENT
# ============================================================================

def get_notes_by_chat(chat_id: str) -> List[Dict]:
    """Get all notes for a specific chat"""
    filter_formula = f'CurrentValue.[chat_id]="{chat_id}"'
    return get_all_bitable_records(NOTES_BASE_ID, NOTES_TABLE_ID, filter_formula)


def get_all_notes() -> List[Dict]:
    """
    Get ALL notes from database (for scheduler reminders)
    Required by notes_manager.py scheduler
    """
    return get_all_bitable_records(NOTES_BASE_ID, NOTES_TABLE_ID)


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
    """Get KALLE booking records"""
    # Get ALL records first, then filter in Python
    all_records = get_all_bitable_records(KALLE_BASE_ID, KALLE_TABLE_BOOKING, None)
    
    if not month:
        return all_records
    
    # Filter by month in Python
    month_str_01 = f"{month:02d}"  # "01", "02", etc.
    month_str_1 = str(month)       # "1", "2", etc.
    
    filtered = []
    for record in all_records:
        fields = record.get("fields", {})
        # Try different column names
        thang_value = fields.get("Tháng air", "") or fields.get("Tháng", "") or fields.get("Tháng báo cáo", "")
        
        # Handle if it's a list or dict
        if isinstance(thang_value, list) and thang_value:
            thang_value = thang_value[0].get("text", "") if isinstance(thang_value[0], dict) else str(thang_value[0])
        elif isinstance(thang_value, dict):
            thang_value = thang_value.get("text", "") or str(thang_value)
        else:
            thang_value = str(thang_value) if thang_value else ""
        
        # Match month
        if thang_value in [month_str_01, month_str_1, f"Tháng {month}"]:
            filtered.append(record)
    
    logger.info(f"📊 Booking: {len(all_records)} total, {len(filtered)} for month {month}")
    return filtered


def get_kalle_dashboard_records(month: int = None) -> List[Dict]:
    """Get KALLE dashboard KPI records"""
    # Get ALL records first (no filter), then filter in Python if needed
    # This is because Bitable column names may vary
    all_records = get_all_bitable_records(KALLE_BASE_ID, KALLE_TABLE_DASHBOARD, None)
    
    if not month:
        return all_records
    
    # Filter by month in Python - try multiple column names and formats
    month_str_01 = f"{month:02d}"  # "01", "02", etc.
    month_str_1 = str(month)       # "1", "2", etc.
    
    filtered = []
    for record in all_records:
        fields = record.get("fields", {})
        # Try different column names
        thang_value = fields.get("Tháng báo cáo", "") or fields.get("Tháng", "") or fields.get("Tháng air", "")
        
        # Handle if it's a list or dict
        if isinstance(thang_value, list) and thang_value:
            thang_value = thang_value[0].get("text", "") if isinstance(thang_value[0], dict) else str(thang_value[0])
        elif isinstance(thang_value, dict):
            thang_value = thang_value.get("text", "") or str(thang_value)
        else:
            thang_value = str(thang_value) if thang_value else ""
        
        # Match month
        if thang_value in [month_str_01, month_str_1, f"Tháng {month}"]:
            filtered.append(record)
    
    logger.info(f"📊 Dashboard: {len(all_records)} total, {len(filtered)} for month {month}")
    return filtered


def get_kalle_content_records(month: int = None, start_date: str = None, end_date: str = None) -> List[Dict]:
    """Get KALLE content calendar records"""
    if not KALLE_TABLE_CONTENT:
        logger.warning("KALLE_TABLE_CONTENT not configured")
        return []
    
    filter_formula = None
    # Add filter logic if needed
    
    return get_all_bitable_records(KALLE_BASE_ID, KALLE_TABLE_CONTENT, filter_formula)


def get_kalle_task_records(month: int = None, vi_tri: str = None) -> List[Dict]:
    """Get KALLE task records"""
    if not KALLE_TABLE_TASK:
        logger.warning("KALLE_TABLE_TASK not configured")
        return []
    
    filters = []
    if month:
        filters.append(f'CurrentValue.[Tháng]="{month}"')
    if vi_tri:
        filters.append(f'CurrentValue.[Vị trí]="{vi_tri}"')
    
    filter_formula = f"AND({','.join(filters)})" if len(filters) > 1 else (filters[0] if filters else None)
    
    return get_all_bitable_records(KALLE_BASE_ID, KALLE_TABLE_TASK, filter_formula)

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
    """Get CHENG total revenue records"""
    filter_formula = None
    if month:
        filter_formula = f'CurrentValue.[Tháng]="{month}"'
    return get_all_bitable_records(CHENG_BASE_ID, CHENG_TABLE_TONG, filter_formula)

# ============================================================================
# CONTENT AGGREGATION
# ============================================================================

def aggregate_content_by_staff(
    records: List[Dict],
    staff_field: str = "Nhân sự book",
    content_field: str = "Content",
    product_field: str = "Phân loại sp (Chỉ được chọn - Không được add mới)",
    product_gh_field: str = "Phân loại sp gửi hàng (Chỉ được chọn - Không được add mới)"
) -> Dict[str, Dict[str, int]]:
    """Aggregate content counts by staff member"""
    content_by_staff = defaultdict(lambda: defaultdict(int))
    
    for record in records:
        fields = record.get("fields", {})
        
        staff = fields.get(staff_field, "")
        if isinstance(staff, list):
            staff = staff[0].get("text", "") if staff else ""
        
        if not staff:
            continue
        
        content_type = fields.get(content_field, "")
        if isinstance(content_type, list):
            content_type = content_type[0] if content_type else ""
        
        product = fields.get(product_field, "")
        if isinstance(product, list):
            product = product[0] if product else ""
        
        product_gh = fields.get(product_gh_field, "")
        if isinstance(product_gh, list):
            product_gh = product_gh[0] if product_gh else ""
        
        if product and content_type:
            content_key = f"{product},{content_type}"
            if product_gh:
                content_key = f"{product},{content_type},{product_gh}"
            content_by_staff[staff][content_key] += 1
        
        if content_type.lower() == "cart":
            content_by_staff[staff]["total_cart"] += 1
        elif content_type.lower() == "text":
            content_by_staff[staff]["total_text"] += 1
        
        content_by_staff[staff]["total"] += 1
    
    return dict(content_by_staff)


def format_content_breakdown_for_staff(content_data: Dict[str, int]) -> str:
    """Format content breakdown for display"""
    if not content_data:
        return ""
    
    items = []
    for key, count in content_data.items():
        if key not in ("total", "total_cart", "total_text"):
            items.append(f"{count} {key}")
    
    if not items:
        return ""
    
    if len(items) == 1:
        return items[0]
    elif len(items) == 2:
        return f"{items[0]} và {items[1]}"
    else:
        return ", ".join(items[:-1]) + f" và {items[-1]}"

# ============================================================================
# GENERATE CONTENT CALENDAR - REQUIRED BY main.py
# ============================================================================

async def generate_content_calendar(
    start_date: str = None,
    end_date: str = None,
    month: int = None,
    team: str = None,
    vi_tri: str = None
) -> Dict[str, Any]:
    """
    Generate content calendar data
    Required by main.py for INTENT_CONTENT_CALENDAR
    """
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    
    # Fetch content records
    content_records = get_kalle_content_records(month=month, start_date=start_date, end_date=end_date)
    
    # Process records
    calendar_items = []
    teams_count = defaultdict(int)
    status_count = defaultdict(int)
    
    for record in content_records:
        fields = record.get("fields", {})
        
        title = fields.get("Tiêu đề", "") or fields.get("Title", "")
        if isinstance(title, list):
            title = title[0].get("text", "") if title else ""
        
        record_team = fields.get("Team", "") or fields.get("Bộ phận", "")
        if isinstance(record_team, list):
            record_team = record_team[0] if record_team else ""
        
        status = fields.get("Trạng thái", "") or fields.get("Status", "")
        if isinstance(status, list):
            status = status[0] if status else ""
        
        deadline = fields.get("Deadline", "") or fields.get("Ngày đăng", "")
        
        # Filter by team if specified
        if team and team.lower() not in str(record_team).lower():
            continue
        
        calendar_items.append({
            "title": title,
            "team": record_team,
            "status": status,
            "deadline": deadline
        })
        
        teams_count[record_team] += 1
        status_count[status] += 1
    
    return {
        "month": month,
        "year": year,
        "start_date": start_date,
        "end_date": end_date,
        "team_filter": team,
        "items": calendar_items,
        "total": len(calendar_items),
        "by_team": dict(teams_count),
        "by_status": dict(status_count)
    }

# ============================================================================
# GENERATE TASK SUMMARY - REQUIRED BY main.py
# ============================================================================

async def generate_task_summary(
    month: int = None,
    vi_tri: str = None
) -> Dict[str, Any]:
    """
    Generate task summary with deadline analysis
    Required by main.py for INTENT_TASK_SUMMARY
    """
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    now = datetime.now()
    
    # Fetch task records
    task_records = get_kalle_task_records(month=month, vi_tri=vi_tri)
    
    # Process records
    tasks = []
    overdue_count = 0
    upcoming_count = 0
    completed_count = 0
    by_position = defaultdict(lambda: {"total": 0, "overdue": 0, "completed": 0})
    
    for record in task_records:
        fields = record.get("fields", {})
        
        title = fields.get("Tên task", "") or fields.get("Task name", "")
        if isinstance(title, list):
            title = title[0].get("text", "") if title else ""
        
        position = fields.get("Vị trí", "") or fields.get("Position", "")
        if isinstance(position, list):
            position = position[0] if position else ""
        
        status = fields.get("Trạng thái", "") or fields.get("Status", "")
        if isinstance(status, list):
            status = status[0] if status else ""
        
        deadline = fields.get("Deadline", "")
        
        # Check deadline status
        is_overdue = False
        is_completed = "hoàn thành" in str(status).lower() or "done" in str(status).lower()
        
        if deadline and not is_completed:
            try:
                if isinstance(deadline, (int, float)):
                    deadline_dt = datetime.fromtimestamp(deadline / 1000)
                else:
                    deadline_dt = datetime.fromisoformat(str(deadline).replace('Z', '+00:00'))
                
                if deadline_dt < now:
                    is_overdue = True
                    overdue_count += 1
                else:
                    upcoming_count += 1
            except:
                pass
        
        if is_completed:
            completed_count += 1
        
        # Update position stats
        by_position[position]["total"] += 1
        if is_overdue:
            by_position[position]["overdue"] += 1
        if is_completed:
            by_position[position]["completed"] += 1
        
        tasks.append({
            "title": title,
            "position": position,
            "status": status,
            "deadline": deadline,
            "is_overdue": is_overdue,
            "is_completed": is_completed
        })
    
    return {
        "month": month,
        "year": year,
        "vi_tri_filter": vi_tri,
        "tasks": tasks,
        "total": len(tasks),
        "overdue": overdue_count,
        "upcoming": upcoming_count,
        "completed": completed_count,
        "by_position": dict(by_position)
    }

# ============================================================================
# GENERATE KOC SUMMARY - KALLE
# ============================================================================

async def generate_koc_summary(
    month: int = None,
    week: int = None,
    group_by: str = "product",
    product_filter: str = None
) -> Dict[str, Any]:
    """Generate KOC summary for KALLE"""
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    
    # Fetch data
    booking_records = get_kalle_booking_records(month, year)
    dashboard_records = get_kalle_dashboard_records(month)
    
    # Aggregate content
    content_by_nhan_su = aggregate_content_by_staff(booking_records)
    
    # Build staff KPI lookup
    staff_kpi = {}
    logger.info(f"📊 Dashboard records count: {len(dashboard_records)}")
    
    for record in dashboard_records:
        fields = record.get("fields", {})
        # Try multiple possible field names for staff
        nhan_su_raw = fields.get("Nhân sự book", "") or fields.get("Nhân sự", "")
        
        # Handle different field types
        nhan_su = ""
        if isinstance(nhan_su_raw, list):
            # Link field or multi-select
            if nhan_su_raw:
                first_item = nhan_su_raw[0]
                if isinstance(first_item, dict):
                    nhan_su = first_item.get("text", "") or first_item.get("name", "") or str(first_item)
                else:
                    nhan_su = str(first_item)
        elif isinstance(nhan_su_raw, dict):
            nhan_su = nhan_su_raw.get("text", "") or nhan_su_raw.get("name", "") or str(nhan_su_raw)
        else:
            nhan_su = str(nhan_su_raw) if nhan_su_raw else ""
        
        if nhan_su:
            # Get product for this record
            san_pham = fields.get("Sản phẩm", "")
            if isinstance(san_pham, list) and san_pham:
                san_pham = san_pham[0].get("text", "") if isinstance(san_pham[0], dict) else str(san_pham[0])
            
            # Aggregate KPI by staff (sum across all products)
            if nhan_su not in staff_kpi:
                staff_kpi[nhan_su] = {
                    "video_kpi": 0,
                    "budget_kpi": 0,
                    "contact_total": 0,
                    "contact_deal": 0
                }
            
            # Add KPI values (they might be per product row) - safely convert to number
            def safe_number(val):
                """Convert value to number safely"""
                if val is None:
                    return 0
                if isinstance(val, (int, float)):
                    return val
                if isinstance(val, str):
                    try:
                        # Remove commas and convert
                        return float(val.replace(",", "").replace(" ", "")) if val.strip() else 0
                    except:
                        return 0
                return 0
            
            staff_kpi[nhan_su]["video_kpi"] += safe_number(fields.get("KPI Số lư...", 0) or fields.get("KPI Số lượng", 0) or fields.get("KPI số lượng tổ...", 0))
            staff_kpi[nhan_su]["budget_kpi"] += safe_number(fields.get("KPI ngân sách", 0) or fields.get("Ngân sách tổng...", 0))
            staff_kpi[nhan_su]["contact_total"] += safe_number(fields.get("Tổng liên hệ", 0))
            staff_kpi[nhan_su]["contact_deal"] += safe_number(fields.get("Đã deal", 0) or fields.get("# Đã deal", 0))
    
    logger.info(f"📊 Staff KPI lookup: {list(staff_kpi.keys())}")
    
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
        "week": week,
        "year": year,
        "brand": "KALLE",
        "group_by": group_by,
        "product_filter": product_filter,
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
# GENERATE DASHBOARD SUMMARY - KALLE
# ============================================================================

async def generate_dashboard_summary(
    month: int = None,
    week: int = None
) -> Dict[str, Any]:
    """Generate KALLE dashboard summary for all staff"""
    # Reuse generate_koc_summary logic
    return await generate_koc_summary(month=month, week=week)

# ============================================================================
# CHENG SUMMARY GENERATION
# ============================================================================

async def generate_cheng_koc_summary(
    month: int = None,
    week: int = None
) -> Dict[str, Any]:
    """Generate KOC summary for CHENG"""
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    
    # Fetch data
    dashboard_records = get_cheng_dashboard_records(month)
    tong_records = get_cheng_doanh_thu_tong_records(month)
    koc_records = get_cheng_koc_records(month)
    
    # Build GMV lookup
    gmv_by_staff = {}
    for record in tong_records:
        fields = record.get("fields", {})
        nhan_su = fields.get("Nhân sự", "") or fields.get("Tên nhân sự", "")
        if isinstance(nhan_su, list):
            nhan_su = nhan_su[0].get("text", "") if nhan_su else ""
        
        if nhan_su:
            gmv_by_staff[nhan_su] = fields.get("GMV", 0) or fields.get("Doanh thu", 0) or 0
    
    # Content aggregation
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
        "week": week,
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
    """Get calendar events"""
    if not CALENDAR_ID:
        logger.warning("Calendar ID not configured")
        return []
    
    if start_time is None:
        start_time = datetime.now()
    if end_time is None:
        end_time = start_time + timedelta(days=7)
    
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    
    url = f"https://open.larksuite.com/open-apis/calendar/v4/calendars/{CALENDAR_ID}/events"
    params = {"start_time": str(start_ts), "end_time": str(end_ts)}
    
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
# DEBUG / TESTING
# ============================================================================

def debug_booking_fields():
    """Debug: Print booking table field names"""
    records = get_kalle_booking_records()
    if records:
        fields = records[0].get("fields", {})
        logger.info(f"Booking fields: {list(fields.keys())}")
        return list(fields.keys())
    return []


def debug_task_fields():
    """Debug: Print task table field names"""
    records = get_kalle_task_records()
    if records:
        fields = records[0].get("fields", {})
        logger.info(f"Task fields: {list(fields.keys())}")
        return list(fields.keys())
    return []


def debug_notes_table():
    """Debug: Print notes table structure"""
    records = get_all_notes()
    if records:
        fields = records[0].get("fields", {})
        logger.info(f"Notes fields: {list(fields.keys())}")
        return list(fields.keys())
    return []


if __name__ == "__main__":
    print("Testing lark_base.py v5.8.1...")
    print("Functions available:")
    print("  - test_connection()")
    print("  - generate_koc_summary()")
    print("  - generate_content_calendar()")
    print("  - generate_task_summary()")
    print("  - generate_dashboard_summary()")
    print("  - generate_cheng_koc_summary()")
    print("  - get_all_notes()")
