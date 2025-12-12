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
    max_records: int = 2000
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
            page_size=500,  # Max allowed by API
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
    # Lấy tất cả records (tăng max lên 2000 để đảm bảo lấy hết)
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=2000
    )
    
    def parse_lark_value(value):
        """Parse giá trị từ Lark API, xử lý các format khác nhau"""
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
        """Extract tháng từ giá trị, trả về int 1-12"""
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
    
    # Count months distribution for debugging
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
            "status": parse_lark_value(fields.get("Status")),
            "luot_xem": parse_lark_value(fields.get("Lượt xem hiện tại")),
            "da_air": fields.get("Đã air"),
            "da_nhan": fields.get("Đã nhận"),
            "da_di_don": fields.get("Đã đi đơn"),
        }
        
        # Filter theo tháng nếu có
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
        # Kiểm tra đã air chưa - dựa trên link air hoặc thời gian air
        link_air = koc.get("link_air_bai")
        thoi_gian_air = koc.get("thoi_gian_air_video")
        da_air_field = koc.get("da_air")
        
        # Coi là đã air nếu có link HOẶC có thời gian air HOẶC field "Đã air" có giá trị
        has_aired = bool(link_air or thoi_gian_air or da_air_field)
        
        if has_aired:
            da_air += 1
            
            # Đã air nhưng chưa có link
            if not link_air:
                da_air_chua_link += 1
                missing_link_kocs.append(koc)
            
            # Đã air nhưng chưa gắn giỏ
            trang_thai_gio = koc.get("trang_thai_gan_gio")
            if trang_thai_gio:
                trang_thai_str = str(trang_thai_gio).lower()
                if "chưa" in trang_thai_str or "chua" in trang_thai_str:
                    da_air_chua_gan_gio += 1
                    missing_gio_kocs.append(koc)
                elif "không" in trang_thai_str or "khong" in trang_thai_str:
                    # Không gắn giỏ cũng tính
                    da_air_chua_gan_gio += 1
                    missing_gio_kocs.append(koc)
            else:
                # Không có trạng thái = chưa gắn
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

async def debug_task_fields():
    """Debug: Xem tất cả fields và sample values từ Task table"""
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
