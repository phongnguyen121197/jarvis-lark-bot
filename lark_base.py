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
            "phan_loai_san_pham": fields.get("Phân loại sản phẩm"),  # Thêm field này
            "status": parse_lark_value(fields.get("Status")),
            "luot_xem": parse_lark_value(fields.get("Lượt xem hiện tại")),
            "da_air": fields.get("Đã air"),
            "da_nhan": fields.get("Đã nhận"),
            "da_di_don": fields.get("Đã đi đơn"),
            # Thêm chi phí
            "da_deal": parse_lark_value(fields.get("Đã deal")),
            "so_tien_tt": parse_lark_value(fields.get("Số tiền TT")),
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
    vi_tri: Optional[str] = None,
    month: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Lấy records từ bảng Task
    
    Args:
        team: Tên team để filter (theo Người phụ trách)
        vi_tri: Vị trí để filter (HR, Content Creator, Ecommerce...)
        month: Tháng để filter (1-12) - dựa trên field "Tháng"
        start_date: Ngày bắt đầu (YYYY-MM-DD)
        end_date: Ngày kết thúc (YYYY-MM-DD)
    
    Returns:
        List các task records
    """
    records = await get_all_records(
        app_token=TASK_BASE["app_token"],
        table_id=TASK_BASE["table_id"],
        max_records=2000
    )
    
    def parse_person_field(value):
        """Parse field người phụ trách"""
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
        """Extract tháng từ field Tháng của Task"""
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
        
        # Parse deadline
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
        
        # Extract tháng từ field "Tháng"
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
            "thang": task_month,  # Đã parse thành int
            "nam": extract_field_value(fields, "Năm"),
        }
        
        # Filter theo tháng (field "Tháng")
        if month is not None:
            if task_data.get("thang") != month:
                continue
        
        # Filter theo vị trí
        if vi_tri:
            task_vi_tri = task_data.get("vi_tri")
            if task_vi_tri:
                if vi_tri.lower() not in str(task_vi_tri).lower():
                    continue
            else:
                continue
        
        # Filter theo team (người phụ trách)
        if team:
            phu_trach = task_data.get("nguoi_phu_trach")
            if phu_trach:
                if team.lower() not in str(phu_trach).lower():
                    continue
            else:
                continue
        
        # Filter theo ngày deadline
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

# ============ REPORT GENERATORS ============
async def generate_koc_summary(month: int, week: Optional[int] = None) -> Dict[str, Any]:
    """
    Tạo báo cáo tổng hợp KOC theo tháng/tuần
    Bao gồm: chi phí deal, số lượng theo sản phẩm
    
    Returns:
        Dict chứa summary và danh sách chi tiết
    """
    records = await get_booking_records(month=month, week=week)
    
    total = len(records)
    da_air = 0
    chua_air = 0
    da_air_chua_link = 0
    da_air_chua_gan_gio = 0
    
    # Chi phí
    tong_chi_phi_deal = 0
    tong_chi_phi_thanh_toan = 0
    
    # Theo phân loại sản phẩm (thay vì sản phẩm)
    by_phan_loai = {}  # {phan_loai: {count, chi_phi, da_air, chua_air, kocs: []}}
    
    missing_link_kocs = []
    missing_gio_kocs = []
    
    for koc in records:
        # === Chi phí ===
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
        
        # === Theo phân loại sản phẩm ===
        phan_loai = koc.get("phan_loai_san_pham") or koc.get("san_pham") or "Không xác định"
        if phan_loai not in by_phan_loai:
            by_phan_loai[phan_loai] = {
                "count": 0,
                "chi_phi": 0,
                "da_air": 0,
                "chua_air": 0,
                "kocs": []  # Lưu danh sách KOC để đề xuất cụ thể
            }
        by_phan_loai[phan_loai]["count"] += 1
        
        if da_deal:
            try:
                by_phan_loai[phan_loai]["chi_phi"] += float(str(da_deal).replace(",", ""))
            except:
                pass
        
        # === Kiểm tra đã air ===
        link_air = koc.get("link_air_bai")
        thoi_gian_air = koc.get("thoi_gian_air_video")
        da_air_field = koc.get("da_air")
        
        has_aired = bool(link_air or thoi_gian_air or da_air_field)
        
        # Lưu thông tin KOC để đề xuất cụ thể
        koc_info = {
            "id_koc": koc.get("id_koc"),
            "id_kenh": koc.get("id_kenh"),
            "link_air": link_air,
            "da_air": has_aired,
            "trang_thai_gio": koc.get("trang_thai_gan_gio"),
            "chi_phi": da_deal
        }
        by_phan_loai[phan_loai]["kocs"].append(koc_info)
        
        if has_aired:
            da_air += 1
            by_phan_loai[phan_loai]["da_air"] += 1
            
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
            by_phan_loai[phan_loai]["chua_air"] += 1
    
    return {
        "month": month,
        "week": week,
        "summary": {
            "total": total,
            "da_air": da_air,
            "chua_air": chua_air,
            "da_air_chua_link": da_air_chua_link,
            "da_air_chua_gan_gio": da_air_chua_gan_gio,
            # Chi phí mới
            "tong_chi_phi_deal": tong_chi_phi_deal,
            "tong_chi_phi_thanh_toan": tong_chi_phi_thanh_toan
        },
        "by_phan_loai": by_phan_loai,  # Đổi từ by_san_pham
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
    """
    Tạo báo cáo lịch content theo tuần hoặc tháng
    
    Args:
        start_date: Ngày bắt đầu (YYYY-MM-DD) - optional
        end_date: Ngày kết thúc (YYYY-MM-DD) - optional
        month: Tháng cần filter (1-12) - ưu tiên hơn start_date/end_date
        team: Filter theo team (optional)
        vi_tri: Filter theo vị trí (optional)
    
    Returns:
        Dict chứa calendar summary
    """
    # Nếu có month, ưu tiên filter theo month
    if month:
        records = await get_task_records(
            team=team,
            vi_tri=vi_tri,
            month=month
        )
        date_range = f"Tháng {month}"
    else:
        records = await get_task_records(
            team=team,
            vi_tri=vi_tri,
            start_date=start_date,
            end_date=end_date
        )
        date_range = f"{start_date} → {end_date}" if start_date and end_date else "tuần này"
    
    # Group theo ngày
    by_date = {}
    by_vi_tri = {}
    overdue = []
    
    for task in records:
        deadline = task.get("deadline")
        vi_tri_task = task.get("vi_tri") or "Không xác định"
        overdue_field = task.get("overdue")
        
        # Group by date
        if deadline:
            date_key = str(deadline)[:10]
            if date_key not in by_date:
                by_date[date_key] = []
            by_date[date_key].append(task)
        
        # Group by vị trí
        if vi_tri_task not in by_vi_tri:
            by_vi_tri[vi_tri_task] = []
        by_vi_tri[vi_tri_task].append(task)
        
        # Check overdue (field Overdue có giá trị = đã quá hạn)
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
    """
    Tạo báo cáo phân tích task theo vị trí
    Bao gồm: task quá hạn, sắp đến deadline theo từng vị trí
    
    Args:
        month: Tháng cần phân tích (1-12)
        vi_tri: Vị trí cụ thể để filter (optional)
    
    Returns:
        Dict chứa task summary theo vị trí
    """
    # Lấy tasks với filter
    tasks = await get_task_records(vi_tri=vi_tri, month=month)
    
    now = datetime.now()
    today = now.date()
    
    # Phân tích theo vị trí
    by_vi_tri = {}
    total_overdue = 0
    total_sap_deadline = 0  # Trong 3 ngày tới
    total_chua_duyet = 0
    
    overdue_tasks = []
    sap_deadline_tasks = []
    
    for task in tasks:
        vi_tri_task = task.get("vi_tri") or "Không xác định"
        overdue_field = task.get("overdue")
        deadline_ts = task.get("deadline_ts")
        duyet = task.get("duyet")
        
        # Khởi tạo vị trí nếu chưa có
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
        
        # Kiểm tra overdue
        if overdue_field:
            by_vi_tri[vi_tri_task]["overdue"] += 1
            by_vi_tri[vi_tri_task]["tasks_overdue"].append(task)
            overdue_tasks.append(task)
            total_overdue += 1
        
        # Kiểm tra sắp đến deadline (trong 3 ngày tới)
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
        
        # Kiểm tra duyệt
        if duyet:
            by_vi_tri[vi_tri_task]["da_duyet"] += 1
        else:
            by_vi_tri[vi_tri_task]["chua_duyet"] += 1
            total_chua_duyet += 1
    
    # Giới hạn số task trả về
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
