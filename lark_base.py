"""
Lark Base API Module
Kết nối và đọc dữ liệu từ Lark Bitable
"""
import os
import httpx
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

# ============ CONFIG ============
LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")

LARK_API_BASE = "https://open.larksuite.com/open-apis"

# Base configurations
BOOKING_BASE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tbleiRLSCGwgLCUT"
}

TASK_BASE = {
    "app_token": "LMNIbdCEkajlvYsoyzRl7Dhog5e",
    "table_id": "tblq7TUkSHSulafy"
}

# ============ AUTH ============
_token_cache = {
    "token": None,
    "expires_at": None
}

async def get_tenant_access_token() -> str:
    """Lấy tenant access token từ Lark (có cache)"""
    now = datetime.now()
    
    # Check cache
    if _token_cache["token"] and _token_cache["expires_at"]:
        if now < _token_cache["expires_at"]:
            return _token_cache["token"]
    
    # Get new token
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
            
            # Cache token (với buffer 5 phút)
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
    page_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    Lấy records từ Lark Base table
    
    Args:
        app_token: Base app token
        table_id: Table ID
        filter_formula: Công thức filter (optional)
        page_size: Số records mỗi trang (max 500)
        page_token: Token để lấy trang tiếp theo
    
    Returns:
        Dict chứa items và page info
    """
    token = await get_tenant_access_token()
    
    url = f"{LARK_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    
    params = {
        "page_size": min(page_size, 500)
    }
    
    if filter_formula:
        params["filter"] = filter_formula
    
    if page_token:
        params["page_token"] = page_token
    
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
    max_records: int = 1000
) -> List[Dict[str, Any]]:
    """
    Lấy tất cả records (với pagination)
    """
    all_records = []
    page_token = None
    
    while len(all_records) < max_records:
        result = await get_table_records(
            app_token=app_token,
            table_id=table_id,
            filter_formula=filter_formula,
            page_size=100,
            page_token=page_token
        )
        
        items = result.get("items", [])
        all_records.extend(items)
        
        # Check if more pages
        if not result.get("has_more"):
            break
        
        page_token = result.get("page_token")
    
    return all_records[:max_records]

# ============ BOOKING/KOC HELPERS ============
def extract_field_value(fields: Dict, field_name: str, default=None):
    """Extract giá trị từ field, xử lý các loại field khác nhau"""
    value = fields.get(field_name)
    
    if value is None:
        return default
    
    # Text field
    if isinstance(value, str):
        return value
    
    # Number field
    if isinstance(value, (int, float)):
        return value
    
    # List/Array field (như Select, Multi-select)
    if isinstance(value, list):
        if len(value) == 0:
            return default
        # Nếu là list of dicts (như Person field)
        if isinstance(value[0], dict):
            return [v.get("text", v.get("name", str(v))) for v in value]
        return value
    
    # Dict field (như Date, Link)
    if isinstance(value, dict):
        # Date field
        if "date" in value:
            return value.get("date")
        # Link field
        if "link" in value:
            return value.get("link")
        if "text" in value:
            return value.get("text")
        return str(value)
    
    return value

async def get_booking_records(
    month: Optional[int] = None,
    week: Optional[int] = None,
    year: int = 2025
) -> List[Dict[str, Any]]:
    """
    Lấy records từ bảng Booking/KOC
    
    Args:
        month: Tháng cần lọc (1-12)
        week: Tuần cần lọc (1-4)
        year: Năm
    
    Returns:
        List các KOC records
    """
    # Tạm thời lấy tất cả records, filter sau
    # TODO: Sử dụng filter formula khi biết chính xác tên field
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=500
    )
    
    results = []
    for record in records:
        fields = record.get("fields", {})
        
        # Extract các field quan trọng
        koc_data = {
            "record_id": record.get("record_id"),
            "id_koc": extract_field_value(fields, "ID KOC"),
            "thang_deal": extract_field_value(fields, "Tháng deal"),
            "tuan_deal": extract_field_value(fields, "Tuần deal"),
            "thang_air": extract_field_value(fields, "Tháng air"),
            "tuan_air": extract_field_value(fields, "Tuần air"),
            "du_kien_air": extract_field_value(fields, "Dự kiến air"),
            "thoi_gian_air_video": extract_field_value(fields, "Thời gian air video"),
            "link_air_bai": extract_field_value(fields, "Link air bài"),
            "trang_thai_gan_gio": extract_field_value(fields, "Trạng thái gắn giỏ"),
            "ngay_gan_gio": extract_field_value(fields, "Ngày gắn giỏ"),
            "nhan_su_book": extract_field_value(fields, "Nhân sự book"),
            "san_pham": extract_field_value(fields, "Sản phẩm"),
            "trang_thai": extract_field_value(fields, "Trạng thái"),
            "luot_xem": extract_field_value(fields, "Lượt xem hiện tại"),
            "raw_fields": fields  # Giữ lại để debug
        }
        
        # Filter theo tháng nếu có
        if month:
            koc_month = koc_data.get("thang_deal") or koc_data.get("thang_air")
            if koc_month:
                try:
                    if int(koc_month) != month:
                        continue
                except:
                    pass
        
        # Filter theo tuần nếu có
        if week:
            koc_week = koc_data.get("tuan_air")
            if koc_week:
                # Tuần có thể là "Tuần 1", "Tuần 2", etc.
                week_str = f"Tuần {week}"
                if isinstance(koc_week, str) and week_str not in koc_week:
                    continue
                elif isinstance(koc_week, list) and week_str not in str(koc_week):
                    continue
        
        results.append(koc_data)
    
    return results

async def get_task_records(
    team: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Lấy records từ bảng Task
    
    Args:
        team: Tên team để filter
        start_date: Ngày bắt đầu (YYYY-MM-DD)
        end_date: Ngày kết thúc (YYYY-MM-DD)
    
    Returns:
        List các task records
    """
    records = await get_all_records(
        app_token=TASK_BASE["app_token"],
        table_id=TASK_BASE["table_id"],
        max_records=500
    )
    
    results = []
    for record in records:
        fields = record.get("fields", {})
        
        task_data = {
            "record_id": record.get("record_id"),
            "du_an": extract_field_value(fields, "Dự án"),
            "phu_trach": extract_field_value(fields, "Phụ trách"),
            "nguoi_duyet": extract_field_value(fields, "Người duyệt"),
            "ngay_tao": extract_field_value(fields, "Ngày tạo"),
            "deadline": extract_field_value(fields, "Deadline"),
            "link_ket_qua": extract_field_value(fields, "Link Kết quả"),
            "trang_thai": extract_field_value(fields, "Trạng thái"),
            "duyet": extract_field_value(fields, "Duyệt"),
            "ghi_chu": extract_field_value(fields, "Ghi chú"),
            "raw_fields": fields
        }
        
        # Filter theo team
        if team:
            phu_trach = task_data.get("phu_trach")
            if phu_trach:
                phu_trach_str = str(phu_trach).lower()
                if team.lower() not in phu_trach_str:
                    continue
        
        # Filter theo ngày
        if start_date or end_date:
            deadline = task_data.get("deadline")
            if deadline:
                try:
                    # Xử lý deadline dạng timestamp hoặc string
                    if isinstance(deadline, (int, float)):
                        deadline_dt = datetime.fromtimestamp(deadline / 1000)
                    else:
                        deadline_dt = datetime.strptime(str(deadline)[:10], "%Y-%m-%d")
                    
                    if start_date:
                        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                        if deadline_dt < start_dt:
                            continue
                    
                    if end_date:
                        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                        if deadline_dt > end_dt:
                            continue
                except Exception as e:
                    print(f"Date parse error: {e}")
        
        results.append(task_data)
    
    return results

# ============ REPORT GENERATORS ============
async def generate_koc_summary(month: int, week: Optional[int] = None) -> Dict[str, Any]:
    """
    Tạo báo cáo tổng hợp KOC theo tháng/tuần
    
    Returns:
        Dict chứa summary và danh sách chi tiết
    """
    records = await get_booking_records(month=month, week=week)
    
    total = len(records)
    da_air = 0
    chua_air = 0
    da_air_chua_link = 0
    da_air_chua_gan_gio = 0
    
    missing_link_kocs = []
    missing_gio_kocs = []
    
    for koc in records:
        # Kiểm tra đã air chưa
        link_air = koc.get("link_air_bai")
        thoi_gian_air = koc.get("thoi_gian_air_video")
        trang_thai_gio = koc.get("trang_thai_gan_gio")
        
        has_aired = bool(link_air or thoi_gian_air)
        
        if has_aired:
            da_air += 1
            
            # Đã air nhưng chưa có link
            if not link_air:
                da_air_chua_link += 1
                missing_link_kocs.append(koc)
            
            # Đã air nhưng chưa gắn giỏ
            if trang_thai_gio:
                trang_thai_str = str(trang_thai_gio).lower()
                if "chưa" in trang_thai_str or trang_thai_str == "":
                    da_air_chua_gan_gio += 1
                    missing_gio_kocs.append(koc)
            else:
                da_air_chua_gan_gio += 1
                missing_gio_kocs.append(koc)
        else:
            chua_air += 1
    
    return {
        "month": month,
        "week": week,
        "summary": {
            "total": total,
            "da_air": da_air,
            "chua_air": chua_air,
            "da_air_chua_link": da_air_chua_link,
            "da_air_chua_gan_gio": da_air_chua_gan_gio
        },
        "missing_link_kocs": missing_link_kocs[:10],  # Top 10
        "missing_gio_kocs": missing_gio_kocs[:10],
        "all_records": records
    }

async def generate_content_calendar(
    start_date: str,
    end_date: str,
    team: Optional[str] = None
) -> Dict[str, Any]:
    """
    Tạo báo cáo lịch content theo tuần
    
    Args:
        start_date: Ngày bắt đầu (YYYY-MM-DD)
        end_date: Ngày kết thúc (YYYY-MM-DD)
        team: Filter theo team (optional)
    
    Returns:
        Dict chứa calendar summary
    """
    records = await get_task_records(
        team=team,
        start_date=start_date,
        end_date=end_date
    )
    
    # Group theo ngày
    by_date = {}
    by_team = {}
    overdue = []
    
    for task in records:
        deadline = task.get("deadline")
        trang_thai = task.get("trang_thai")
        phu_trach = str(task.get("phu_trach", "Unknown"))
        
        # Group by date
        if deadline:
            date_key = str(deadline)[:10] if isinstance(deadline, str) else "No date"
            if date_key not in by_date:
                by_date[date_key] = []
            by_date[date_key].append(task)
        
        # Group by team/person
        if phu_trach not in by_team:
            by_team[phu_trach] = []
        by_team[phu_trach].append(task)
        
        # Check overdue
        if trang_thai and "overdue" in str(trang_thai).lower():
            overdue.append(task)
    
    return {
        "date_range": f"{start_date} → {end_date}",
        "team_filter": team,
        "summary": {
            "total_tasks": len(records),
            "total_overdue": len(overdue),
            "days_with_content": len(by_date),
            "teams_involved": len(by_team)
        },
        "by_date": by_date,
        "by_team": by_team,
        "overdue_tasks": overdue,
        "all_records": records
    }

# ============ TEST ============
async def test_connection():
    """Test kết nối với Lark Base"""
    try:
        print("🔄 Testing Lark Base connection...")
        
        # Test Booking base
        booking_records = await get_all_records(
            app_token=BOOKING_BASE["app_token"],
            table_id=BOOKING_BASE["table_id"],
            max_records=5
        )
        print(f"✅ Booking Base: {len(booking_records)} records found")
        
        if booking_records:
            print(f"   Sample fields: {list(booking_records[0].get('fields', {}).keys())[:10]}")
        
        # Test Task base
        task_records = await get_all_records(
            app_token=TASK_BASE["app_token"],
            table_id=TASK_BASE["table_id"],
            max_records=5
        )
        print(f"✅ Task Base: {len(task_records)} records found")
        
        if task_records:
            print(f"   Sample fields: {list(task_records[0].get('fields', {}).keys())[:10]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False
