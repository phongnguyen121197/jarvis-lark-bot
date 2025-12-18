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
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",  # Giống Booking
    "table_id": "blko05Rb76NGi5nd"  # Table mới từ URL
}

# === KALLE DASHBOARD TABLES ===
DASHBOARD_THANG_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tblhf6x9hciClWGz"  # KALLE - DASHBOARD THÁNG
}

DOANH_THU_KOC_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tbl2TQywnQTYxpNj"  # KALLE - PR - Data doanh thu Koc (tuần)
}

LIEN_HE_TUAN_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tbl18EP44c0MAnKR"  # KALLE - PR - Data liên hệ (tuần)
}

KALODATA_TABLE = {
    "app_token": "XfHGbvXrRaK1zcsTZ1zl5QR3ghf",
    "table_id": "tblX6CB3BshhwloA"  # KALLE- PR - Data Kalodata
}

# === CHENG BASE (MỚI) ===
CHENG_BASE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
}

CHENG_BOOKING_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tblB2pmRRoMA1IzO"  # CHENG - PR - Data list booking (ngày)
}

CHENG_LIEN_HE_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tbl6DXM3ZCTQrEm2"  # CHENG - PR - Data liên hệ (tuần)
}

CHENG_DOANH_THU_KOC_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tbl1xp8cdxzeccoM"  # CHENG - PR - Data doanh thu Koc (tuần)
}

CHENG_DOANH_THU_TONG_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tblbOLW7wp2713M6"  # CHENG - PR - Data doanh thu tổng Cheng (tuần)
}

CHENG_DASHBOARD_THANG_TABLE = {
    "app_token": "QRRwboNSqaBSXhshmzHlCf0EgRc",
    "table_id": "tblhfbIOby6kDYnx"  # CHENG - DASHBOARD THÁNG
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
def find_phan_loai_field(fields: Dict) -> Optional[str]:
    """
    Tìm field phân loại sản phẩm trong record.
    Thử nhiều tên có thể: "Phân loại sp", "Phân loại sản phẩm", etc.
    """
    # Danh sách các tên field có thể (ưu tiên exact match)
    possible_names = [
        "Phân loại sp (Chỉ được chọn - Không được add mới)",  # Tên chính xác
        "Phân loại sản phẩm",
        "Phân loại sp",
        "Phan loai san pham",
        "Phan loai sp",
    ]
    
    value = None
    
    # Thử tìm exact match trước
    for name in possible_names:
        if name in fields:
            value = fields.get(name)
            break
    
    # Nếu chưa tìm thấy, thử tìm field có chứa "phân loại"
    if value is None:
        for key in fields.keys():
            key_lower = key.lower()
            if "phân loại" in key_lower or "phan loai" in key_lower:
                value = fields.get(key)
                break
    
    # Xử lý giá trị - có thể là list, dict, hoặc string
    if value is None:
        return None
    
    if isinstance(value, str):
        return value
    
    if isinstance(value, list):
        # Nếu là list, lấy phần tử đầu tiên
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
            # Tìm field phân loại sản phẩm - thử nhiều tên có thể
            "phan_loai_san_pham": find_phan_loai_field(fields),
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

# Product filter patterns
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
    """
    Tạo báo cáo tổng hợp KOC theo tháng/tuần
    Bao gồm: chi phí deal, số lượng theo sản phẩm hoặc phân loại
    
    Args:
        month: Tháng cần lấy
        week: Tuần cần lấy (optional)
        group_by: "product" (Nước hoa, Box quà) hoặc "brand" (Dark Beauty, Lady Killer)
        product_filter: Filter theo loại sản phẩm ("box_qua", "nuoc_hoa", etc.)
    
    Returns:
        Dict chứa summary và danh sách chi tiết
    """
    records = await get_booking_records(month=month, week=week)
    
    # Filter by product if specified
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
    
    # Chi phí
    tong_chi_phi_deal = 0
    tong_chi_phi_thanh_toan = 0
    
    # Theo sản phẩm (Nước hoa, Box quà) - mặc định
    by_product = {}
    # Theo phân loại/brand (Dark Beauty, Lady Killer)
    by_brand = {}
    
    missing_link_kocs = []
    missing_gio_kocs = []
    
    def safe_string(value):
        """Convert value to string safely"""
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
        
        # === Lấy sản phẩm và phân loại ===
        san_pham = safe_string(koc.get("san_pham"))  # Nước hoa, Box quà 30ml
        phan_loai = safe_string(koc.get("phan_loai_san_pham"))  # Dark Beauty, Lady Killer
        
        # Nếu không có phân loại, fallback về sản phẩm
        if phan_loai == "Không xác định" and san_pham != "Không xác định":
            phan_loai = san_pham
        
        # === Group theo sản phẩm ===
        if san_pham not in by_product:
            by_product[san_pham] = {"count": 0, "chi_phi": 0, "da_air": 0, "chua_air": 0, "kocs": []}
        by_product[san_pham]["count"] += 1
        
        # === Group theo phân loại/brand ===
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
    
    # Chọn data theo group_by
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
        "by_group": by_group,  # Data theo group_by
        "by_product": by_product,  # Luôn có để backup
        "by_brand": by_brand,  # Luôn có để backup
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


# ============ DASHBOARD FUNCTIONS ============

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


async def get_dashboard_thang_records(month: Optional[int] = None, week: Optional[str] = None) -> List[Dict]:
    """Lấy records từ bảng Dashboard Tháng"""
    records = await get_all_records(
        app_token=DASHBOARD_THANG_TABLE["app_token"],
        table_id=DASHBOARD_THANG_TABLE["table_id"],
        max_records=500
    )
    
    print(f"📊 Dashboard Tháng: Total records = {len(records)}, filter month = {month}")
    
    result = []
    month_distribution = {}  # Debug: đếm số record theo tháng
    
    for record in records:
        fields = record.get("fields", {})
        
        # Filter by month if specified
        thang_raw = safe_extract_text(fields.get("Tháng báo cáo"))
        try:
            thang = int(thang_raw) if thang_raw else None
        except:
            thang = None
        
        # Debug: đếm distribution
        month_distribution[thang] = month_distribution.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        # Filter by week if specified
        tuan = fields.get("Tuần báo cáo")
        if week and tuan != week:
            continue
        
        result.append({
            "nhan_su": safe_extract_person_name(fields.get("Nhân sự book")),
            "san_pham": fields.get("Sản phẩm"),
            "thang": thang,
            "tuan": tuan,
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
        })
    
    print(f"📊 Month distribution: {month_distribution}")
    print(f"📊 After filter: {len(result)} records")
    
    return result


async def get_doanh_thu_koc_records(month: Optional[int] = None, week: Optional[str] = None) -> List[Dict]:
    """Lấy records từ bảng Doanh thu KOC (tuần)"""
    records = await get_all_records(
        app_token=DOANH_THU_KOC_TABLE["app_token"],
        table_id=DOANH_THU_KOC_TABLE["table_id"],
        max_records=1000
    )
    
    result = []
    for record in records:
        fields = record.get("fields", {})
        
        # Filter by month
        thang_raw = safe_extract_text(fields.get("Tháng báo cáo"))
        try:
            thang = int(thang_raw) if thang_raw else None
        except:
            thang = None
        
        if month and thang != month:
            continue
        
        # Filter by week
        tuan = fields.get("Tuần báo cáo")
        if week and tuan != week:
            continue
        
        # Parse GMV
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
    """Lấy records từ bảng Data liên hệ (tuần)"""
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
        
        # Filter by month
        thang_raw = safe_extract_text(fields.get("Tháng báo cáo"))
        try:
            thang = int(thang_raw) if thang_raw else None
        except:
            thang = None
        
        # Debug
        month_distribution[thang] = month_distribution.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        # Filter by week
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
    """
    Tạo báo cáo Dashboard tổng hợp
    Bao gồm: KPI nhân sự, Top KOC, Tỷ lệ liên hệ
    
    Logic mới:
    - Số video đã air = Đếm từ bảng Booking (có Link air bài)
    - KPI = Lấy từ Dashboard Tháng nhưng CHỈ 1 LẦN (không cộng tổng các tuần)
    """
    # 1. Lấy data Dashboard Tháng (KPI) - CHỈ ĐỂ LẤY KPI TARGET
    dashboard_records = await get_dashboard_thang_records(month=month, week=week)
    
    # 2. Lấy data từ Booking để đếm số video đã air
    booking_records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=2000
    )
    
    # 3. Lấy data Doanh thu KOC
    doanh_thu_records = await get_doanh_thu_koc_records(month=month, week=week)
    
    # 4. Lấy data Liên hệ
    lien_he_records = await get_lien_he_records(month=month, week=week)
    
    # === Đếm số video đã air từ Booking (theo nhân sự và THÁNG AIR THỰC TẾ) ===
    video_air_by_nhan_su = {}
    for record in booking_records:
        fields = record.get("fields", {})
        
        # Kiểm tra có link air không
        link_air = fields.get("Link air bài") or fields.get("link_air_bai") or fields.get("Link air")
        if not link_air:
            continue
        
        # Lấy thời gian air thực tế (ưu tiên) hoặc tháng dự kiến (fallback)
        thoi_gian_air = fields.get("Thời gian air") or fields.get("thoi_gian_air")
        thang_air = None
        
        # Parse thời gian air để lấy tháng
        if thoi_gian_air:
            try:
                if isinstance(thoi_gian_air, (int, float)):
                    # Unix timestamp (ms)
                    dt = datetime.fromtimestamp(thoi_gian_air / 1000)
                    thang_air = dt.month
                elif isinstance(thoi_gian_air, str):
                    # Try parse string date
                    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]:
                        try:
                            dt = datetime.strptime(thoi_gian_air[:10], fmt)
                            thang_air = dt.month
                            break
                        except:
                            continue
            except:
                pass
        
        # Fallback: dùng tháng dự kiến nếu không có thời gian air
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
        
        # Filter theo tháng
        if month and thang_air != month:
            continue
        
        # Lấy tên nhân sự (strip để bỏ khoảng trắng thừa)
        nhan_su = safe_extract_person_name(fields.get("Nhân sự book"))
        if nhan_su:
            nhan_su = nhan_su.strip()
        
        if nhan_su not in video_air_by_nhan_su:
            video_air_by_nhan_su[nhan_su] = 0
        video_air_by_nhan_su[nhan_su] += 1
    
    print(f"📹 Video air by nhân sự (tháng air {month}): {video_air_by_nhan_su}")
    
    # === Tổng hợp KPI theo nhân sự từ DASHBOARD THÁNG ===
    # LOGIC ĐÚNG:
    # - "KPI Số lượng" = KPI của từng sản phẩm → CỘNG TỔNG tất cả sản phẩm
    # - "Số lượng tổng - Air" = Air của từng sản phẩm → CỘNG TỔNG tất cả sản phẩm
    # Chỉ lấy Tuần 1 để tránh nhân đôi
    
    kpi_by_nhan_su = {}
    
    for r in dashboard_records:
        nhan_su = r["nhan_su"]
        if nhan_su:
            nhan_su = nhan_su.strip()
        
        # CHỈ LẤY TUẦN 1
        tuan = r.get("tuan")
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
        
        # CỘNG TỔNG KPI và Air từ tất cả sản phẩm
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
    
    # Tính % KPI
    for nhan_su, data in kpi_by_nhan_su.items():
        if data["kpi_so_luong"] > 0:
            data["pct_kpi_so_luong"] = round(data["so_luong_air"] / data["kpi_so_luong"] * 100, 1)
        if data["kpi_ngan_sach"] > 0:
            data["pct_kpi_ngan_sach"] = round(data["ngan_sach_air"] / data["kpi_ngan_sach"] * 100, 1)
        
        print(f"   ✅ TỔNG {nhan_su}: {data['so_luong_air']}/{data['kpi_so_luong']}")
    
    print(f"📊 KPI by nhân sự (từ Dashboard): {kpi_by_nhan_su}")
    
    # === Top KOC doanh số ===
    koc_gmv = {}
    for r in doanh_thu_records:
        id_kenh = r["id_kenh"]
        if id_kenh:
            if id_kenh not in koc_gmv:
                koc_gmv[id_kenh] = 0
            koc_gmv[id_kenh] += r["gmv"]
    
    # Sort by GMV
    top_koc = sorted(koc_gmv.items(), key=lambda x: x[1], reverse=True)[:10]
    
    # === Tổng hợp liên hệ theo nhân sự ===
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
    
    # === Tổng quan ===
    total_kpi_so_luong = sum(d["kpi_so_luong"] for d in kpi_by_nhan_su.values())
    total_so_luong_air = sum(d["so_luong_air"] for d in kpi_by_nhan_su.values())
    total_kpi_ngan_sach = sum(d["kpi_ngan_sach"] for d in kpi_by_nhan_su.values())
    total_ngan_sach_air = sum(d["ngan_sach_air"] for d in kpi_by_nhan_su.values())
    total_gmv = sum(koc_gmv.values())
    
    print(f"📊 TỔNG QUAN: {total_so_luong_air}/{total_kpi_so_luong} ({round(total_so_luong_air / total_kpi_so_luong * 100, 1) if total_kpi_so_luong > 0 else 0}%)")
    
    return {
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
        "top_koc": top_koc,
        "lien_he_nhan_su": lien_he_by_nhan_su,
    }


# ============ CHENG FUNCTIONS ============

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
        
        # Lấy tháng dự kiến
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
        
        # Filter by month
        if month and thang_du_kien != month:
            continue
        
        filtered.append(record)
    
    print(f"📋 CHENG Month distribution: {month_dist}")
    print(f"📋 CHENG After filter: {len(filtered)} records")
    
    return filtered


async def get_cheng_dashboard_records(month: int = None) -> List[Dict]:
    """Lấy records từ bảng CHENG - DASHBOARD THÁNG"""
    records = await get_all_records(
        CHENG_DASHBOARD_THANG_TABLE["app_token"],
        CHENG_DASHBOARD_THANG_TABLE["table_id"]
    )
    
    print(f"📊 CHENG Dashboard: Total records = {len(records)}, filter month = {month}")
    
    # Debug: in ra các field names của record đầu tiên
    if records:
        first_fields = records[0].get("fields", {})
        print(f"   🔍 CHENG Dashboard field names: {list(first_fields.keys())[:10]}")
        # Debug: in ra giá trị của các field tháng có thể có
        for key in ["Tháng", "thang", "Tháng báo cáo", "Month"]:
            if key in first_fields:
                print(f"   🔍 Field '{key}' = {first_fields[key]}")
    
    parsed = []
    month_dist = {}  # Debug distribution
    
    for r in records:
        fields = r.get("fields", {})
        
        # Parse tháng - thử nhiều field names có thể
        thang_raw = fields.get("Tháng") or fields.get("thang") or fields.get("Tháng báo cáo")
        thang = None
        try:
            if isinstance(thang_raw, list) and len(thang_raw) > 0:
                first = thang_raw[0]
                thang = int(first.get("text", 0)) if isinstance(first, dict) else int(first)
            elif isinstance(thang_raw, (int, float)):
                thang = int(thang_raw)
            elif isinstance(thang_raw, str):
                # Có thể là "Tháng 12" hoặc "12"
                import re
                match = re.search(r'\d+', thang_raw)
                if match:
                    thang = int(match.group())
        except:
            pass
        
        # Debug distribution
        month_dist[thang] = month_dist.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        parsed.append({
            "record_id": r.get("record_id"),
            "thang": thang,
            "tuan": fields.get("Tuần") or fields.get("tuan") or fields.get("Tuần báo cáo"),
            "san_pham": fields.get("Sản phẩm") or fields.get("san_pham"),
            "nhan_su": safe_extract_person_name(fields.get("Nhân sự")),
            "kpi_so_luong": fields.get("KPI - Số lượng") or fields.get("kpi_so_luong") or 0,
            "kpi_ngan_sach": fields.get("KPI - Ngân sách") or fields.get("kpi_ngan_sach") or 0,
            "so_luong_deal": fields.get("Số lượng - Deal") or fields.get("so_luong_deal") or 0,
            "so_luong_air": fields.get("Số lượng - Air") or fields.get("so_luong_air") or 0,
            "so_luong_tong_air": fields.get("Số lượng tổng - Air") or fields.get("so_luong_tong_air") or 0,
            "ngan_sach_air": fields.get("Ngân sách - Air") or fields.get("ngan_sach_air") or 0,
            "ngan_sach_tong_air": fields.get("Ngân sách tổng - Air") or fields.get("ngan_sach_tong_air") or 0,
        })
    
    print(f"   📊 CHENG Month distribution: {month_dist}")
    print(f"📊 CHENG Dashboard after filter: {len(parsed)} records")
    return parsed


async def get_cheng_lien_he_records(month: int = None, week: int = None) -> List[Dict]:
    """Lấy records từ bảng CHENG - PR - Data liên hệ (tuần)"""
    records = await get_all_records(
        CHENG_LIEN_HE_TABLE["app_token"],
        CHENG_LIEN_HE_TABLE["table_id"]
    )
    
    print(f"📞 CHENG Liên hệ: Total records = {len(records)}, filter month = {month}")
    
    # Debug: in ra các field names của record đầu tiên
    if records:
        first_fields = records[0].get("fields", {})
        print(f"   🔍 CHENG Liên hệ field names: {list(first_fields.keys())[:10]}")
    
    parsed = []
    month_dist = {}
    
    for r in records:
        fields = r.get("fields", {})
        
        # Parse tháng - thử nhiều field names
        thang_raw = fields.get("Tháng") or fields.get("thang") or fields.get("Tháng báo cáo")
        thang = None
        try:
            if isinstance(thang_raw, list) and len(thang_raw) > 0:
                first = thang_raw[0]
                thang = int(first.get("text", 0)) if isinstance(first, dict) else int(first)
            elif isinstance(thang_raw, (int, float)):
                thang = int(thang_raw)
            elif isinstance(thang_raw, str):
                import re
                match = re.search(r'\d+', thang_raw)
                if match:
                    thang = int(match.group())
        except:
            pass
        
        month_dist[thang] = month_dist.get(thang, 0) + 1
        
        if month and thang != month:
            continue
        
        parsed.append({
            "record_id": r.get("record_id"),
            "thang": thang,
            "tuan": fields.get("Tuần") or fields.get("tuan") or fields.get("Tuần báo cáo"),
            "nhan_su": safe_extract_person_name(fields.get("Nhân sự")),
            "tong_lien_he": fields.get("Tổng liên hệ") or fields.get("tong_lien_he") or 0,
            "da_deal": fields.get("Đã deal") or fields.get("da_deal") or 0,
            "dang_trao_doi": fields.get("Đang trao đổi") or fields.get("dang_trao_doi") or 0,
            "tu_choi": fields.get("Từ chối") or fields.get("tu_choi") or 0,
        })
    
    print(f"📞 CHENG Month distribution: {month_dist}")
    print(f"📞 CHENG After filter: {len(parsed)} records")
    
    return parsed


async def generate_cheng_koc_summary(month: int = None, week: int = None) -> Dict:
    """
    Tổng hợp báo cáo KOC cho CHENG
    Tương tự generate_koc_summary nhưng cho bảng Cheng
    """
    # Lấy dữ liệu từ các bảng Cheng
    booking_records = await get_cheng_booking_records(month=month, week=week)
    dashboard_records = await get_cheng_dashboard_records(month=month)
    lien_he_records = await get_cheng_lien_he_records(month=month, week=week)
    
    # === Đếm số video đã air từ Booking (theo nhân sự và THÁNG AIR THỰC TẾ) ===
    video_air_by_nhan_su = {}
    for record in booking_records:
        fields = record.get("fields", {})
        
        # Kiểm tra có link air không
        link_air = fields.get("Link air bài") or fields.get("link_air_bai") or fields.get("Link air")
        if not link_air:
            continue
        
        # Lấy thời gian air thực tế
        thoi_gian_air = fields.get("Thời gian air") or fields.get("thoi_gian_air")
        thang_air = None
        
        if thoi_gian_air:
            try:
                if isinstance(thoi_gian_air, (int, float)):
                    dt = datetime.fromtimestamp(thoi_gian_air / 1000)
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
        
        # Fallback: dùng tháng dự kiến
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
    
    print(f"📹 CHENG Video air by nhân sự (tháng air {month}): {video_air_by_nhan_su}")
    
    # === Tổng hợp KPI theo nhân sự từ DASHBOARD THÁNG ===
    # CỘNG TỔNG cả KPI và Air từ tất cả sản phẩm (Tuần 1)
    
    kpi_by_nhan_su = {}
    
    for r in dashboard_records:
        nhan_su = r["nhan_su"]
        if nhan_su:
            nhan_su = nhan_su.strip()
        
        # CHỈ LẤY TUẦN 1
        tuan = r.get("tuan")
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
        
        # CỘNG TỔNG từ tất cả sản phẩm
        try:
            kpi_sl = int(r.get("kpi_so_luong") or 0)
            kpi_ns = int(r.get("kpi_ngan_sach") or 0)
            sl_air = int(r.get("so_luong_tong_air") or 0)
            ns_air = int(r.get("ngan_sach_tong_air") or 0)
            
            kpi_by_nhan_su[nhan_su]["kpi_so_luong"] += kpi_sl
            kpi_by_nhan_su[nhan_su]["kpi_ngan_sach"] += kpi_ns
            kpi_by_nhan_su[nhan_su]["so_luong_air"] += sl_air
            kpi_by_nhan_su[nhan_su]["ngan_sach_air"] += ns_air
        except:
            pass
    
    # Tính %
    for nhan_su, data in kpi_by_nhan_su.items():
        if data["kpi_so_luong"] > 0:
            data["pct_kpi_so_luong"] = round(data["so_luong_air"] / data["kpi_so_luong"] * 100, 1)
        if data["kpi_ngan_sach"] > 0:
            data["pct_kpi_ngan_sach"] = round(data["ngan_sach_air"] / data["kpi_ngan_sach"] * 100, 1)
    
    print(f"📊 CHENG KPI by nhân sự (từ Dashboard): {kpi_by_nhan_su}")
    
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
        
        try:
            lien_he_by_nhan_su[nhan_su]["tong_lien_he"] += int(r.get("tong_lien_he") or 0)
            lien_he_by_nhan_su[nhan_su]["da_deal"] += int(r.get("da_deal") or 0)
            lien_he_by_nhan_su[nhan_su]["dang_trao_doi"] += int(r.get("dang_trao_doi") or 0)
            lien_he_by_nhan_su[nhan_su]["tu_choi"] += int(r.get("tu_choi") or 0)
        except:
            pass
    
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
    
    # === Tổng quan ===
    total_kpi_so_luong = sum(d["kpi_so_luong"] for d in kpi_by_nhan_su.values())
    total_so_luong_air = sum(d["so_luong_air"] for d in kpi_by_nhan_su.values())
    total_kpi_ngan_sach = sum(d["kpi_ngan_sach"] for d in kpi_by_nhan_su.values())
    total_ngan_sach_air = sum(d["ngan_sach_air"] for d in kpi_by_nhan_su.values())
    
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
        },
        "kpi_nhan_su": kpi_by_nhan_su,
        "lien_he_nhan_su": lien_he_by_nhan_su,
    }
