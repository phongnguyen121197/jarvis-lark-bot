"""
Lark Base API Module
Kết nối và đọc dữ liệu từ Lark Bitable
"""
import os
import re
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
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=500
    )
    
    results = []
    for record in records:
        fields = record.get("fields", {})
        
        # Tìm các field theo pattern (vì tên field có thể khác)
        def find_field(patterns: list, fields: dict):
            """Tìm field value theo list patterns"""
            for key, value in fields.items():
                key_lower = key.lower()
                for pattern in patterns:
                    if pattern.lower() in key_lower:
                        return value
            return None
        
        # Extract các field quan trọng với pattern matching
        koc_data = {
            "record_id": record.get("record_id"),
            "id_koc": find_field(["id koc", "id_koc", "koc"], fields) or find_field(["tên", "name"], fields),
            "thang_deal": find_field(["tháng deal", "thang deal", "month deal"], fields),
            "tuan_deal": find_field(["tuần deal", "tuan deal", "week deal"], fields),
            "thang_air": find_field(["tháng air", "thang air", "tháng dự kiến"], fields),
            "tuan_air": find_field(["tuần air", "tuan air", "tuần báo cáo"], fields),
            "du_kien_air": find_field(["dự kiến air", "du kien air"], fields),
            "thoi_gian_air_video": find_field(["thời gian air video", "ngày air"], fields),
            "link_air_bai": find_field(["link air", "link bài"], fields),
            "trang_thai_gan_gio": find_field(["trạng thái gắn giỏ", "gắn giỏ", "gan gio"], fields),
            "ngay_gan_gio": find_field(["ngày gắn giỏ"], fields),
            "nhan_su_book": find_field(["nhân sự book", "người book"], fields),
            "san_pham": find_field(["sản phẩm", "product"], fields),
            "trang_thai": find_field(["trạng thái"], fields),
            "luot_xem": find_field(["lượt xem", "view"], fields),
            "raw_fields": fields  # Giữ lại để debug
        }
        
        # Filter theo tháng nếu có
        if month:
            # Ưu tiên tháng deal, nếu không có thì dùng tháng air
            koc_month = koc_data.get("thang_deal") or koc_data.get("thang_air")
            
            if koc_month is not None:
                try:
                    # Xử lý nhiều định dạng: số, string "12", hoặc list
                    if isinstance(koc_month, (int, float)):
                        month_val = int(koc_month)
                    elif isinstance(koc_month, str):
                        # Tìm số trong string
                        import re
                        match = re.search(r'(\d+)', str(koc_month))
                        month_val = int(match.group(1)) if match else None
                    elif isinstance(koc_month, list) and len(koc_month) > 0:
                        # Nếu là list, lấy phần tử đầu
                        first = koc_month[0]
                        if isinstance(first, dict):
                            month_val = first.get("text") or first.get("value")
                        else:
                            month_val = first
                        if month_val:
                            match = re.search(r'(\d+)', str(month_val))
                            month_val = int(match.group(1)) if match else None
                    else:
                        month_val = None
                    
                    if month_val is not None and month_val != month:
                        continue  # Skip record không đúng tháng
                except Exception as e:
                    print(f"Month parse error: {e}, value: {koc_month}")
        
        # Filter theo tuần nếu có
        if week:
            koc_week = koc_data.get("tuan_air") or koc_data.get("tuan_deal")
            if koc_week:
                week_str = f"Tuần {week}"
                week_match = False
                
                if isinstance(koc_week, str):
                    week_match = week_str.lower() in koc_week.lower() or str(week) == koc_week
                elif isinstance(koc_week, list):
                    for item in koc_week:
                        if isinstance(item, dict):
                            item_text = item.get("text", "")
                        else:
                            item_text = str(item)
                        if week_str.lower() in item_text.lower():
                            week_match = True
                            break
                
                if not week_match:
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
        
        # Test Booking base
        booking_records = await get_all_records(
            app_token=BOOKING_BASE["app_token"],
            table_id=BOOKING_BASE["table_id"],
            max_records=5
        )
        print(f"✅ Booking Base: {len(booking_records)} records found")
        
        if booking_records:
            all_fields = list(booking_records[0].get('fields', {}).keys())
            print(f"   All fields ({len(all_fields)}): {all_fields}")
        
        # Test Task base
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
    """Debug: Xem tất cả fields và sample values từ Booking table"""
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
        # Lấy tất cả field names
        all_fields = list(records[0].get("fields", {}).keys())
        result["all_field_names"] = all_fields
        
        # Lấy sample values cho mỗi field
        for record in records:
            fields = record.get("fields", {})
            sample = {}
            for key, value in fields.items():
                sample[key] = str(value)[:100] if value else None
            result["sample_records"].append(sample)
    
    return result
