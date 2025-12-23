"""
Intent Classifier Module
Phân loại câu hỏi của người dùng thành các intent
Version 5.7.2 - Fixed CHENG staff KPI routing
"""
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# ============ INTENT TYPES ============
INTENT_KOC_REPORT = "KOC_REPORT"  # Báo cáo KOC Kalle (mặc định)
INTENT_CHENG_REPORT = "CHENG_REPORT"  # Báo cáo KOC Cheng
INTENT_CONTENT_CALENDAR = "CONTENT_CALENDAR_SUMMARY"
INTENT_TASK_SUMMARY = "TASK_SUMMARY"  # Phân tích task theo vị trí
INTENT_GENERAL_SUMMARY = "GENERAL_SUMMARY"
INTENT_GPT_CHAT = "GPT_CHAT"  # Hỏi ChatGPT trực tiếp
INTENT_DASHBOARD = "DASHBOARD"  # Dashboard tổng hợp
INTENT_UNKNOWN = "UNKNOWN"

# Keywords để nhận dạng brand Cheng
CHENG_KEYWORDS = ["cheng", "chenglovehair", "cheng love hair"]

# ============ KEYWORDS ============
KOC_KEYWORDS = [
    "koc", "booking", "air", "gắn giỏ", "gan gio", "pr", 
    "đã air", "chưa air", "link air", "tháng deal", "tuần deal",
    "chi phí", "chi phi", "sản phẩm", "san pham"
]

CONTENT_KEYWORDS = [
    "content", "lịch", "lich", "công việc", "cong viec",
    "bài đăng", "tiktok", "design", "digital"
]

TASK_KEYWORDS = [
    "task", "deadline", "quá hạn", "qua han", "overdue", "trễ hạn", "tre han",
    "vị trí", "vi tri", "hr", "ecommerce", "content creator",
    "sắp deadline", "sap deadline", "công việc", "phân tích task"
]

# Keywords để gọi GPT trực tiếp
GPT_KEYWORDS = [
    "gpt", "chatgpt", "hỏi gpt", "hoi gpt", "ask gpt",
    "ai:", "gpt:", "hỏi ai", "hoi ai"
]

# Tên các phân loại sản phẩm cụ thể (brands)
BRAND_KEYWORDS = [
    "dark beauty", "lady killer", "ladykiller", "venus", 
    "kalle", "dark", "lady", "killer"
]

# Keywords để filter theo loại sản phẩm
PRODUCT_FILTER_KEYWORDS = {
    "box_qua": ["box quà", "set quà", "box qua", "set qua", "quà tặng", "qua tang", "gift box", "giftbox"],
    "nuoc_hoa": ["nước hoa", "nuoc hoa", "perfume"],
    "sua_tam": ["sữa tắm", "sua tam", "body wash"],
}

# Keywords cho Dashboard
DASHBOARD_KEYWORDS = [
    "dashboard", "kpi", "top koc", "doanh số", "doanh so",
    "liên hệ", "lien he", "tỷ lệ deal", "ty le deal",
    "nhân sự", "nhan su", "hiệu suất", "hieu suat",
    "gmv", "performance",
    # Trigger mới
    "cập nhật tình hình", "cap nhat tinh hinh",
    "tình hình booking", "tinh hinh booking",
    "cập nhật booking", "cap nhat booking"
]

# ============ NHÂN SỰ MAPPING ============
# Danh sách nhân sự CHENG (để detect và route sang CHENG_REPORT)
# Updated v5.7.3 - Danh sách đầy đủ từ bảng CHENG Dashboard
CHENG_NHAN_SU_MAPPING = {
    # Tên đơn - sẽ được match thêm trong report_generator
    "phương": "Phương",  # Could be Phương Anh or Nguyên Phương
    "phuong": "Phương",
    # Phương Anh (specific)
    "phương anh": "Phương Anh",
    "phuong anh": "Phương Anh",
    # Nguyên Phương (specific)
    "nguyên phương": "Nguyên Phương",
    "nguyen phuong": "Nguyên Phương",
    "nguyên": "Nguyên Phương",
    "nguyen": "Nguyên Phương",
    # Trà Mi CHENG
    "trà mi cheng": "Trà Mi",
    # Quỳnh Anh
    "quỳnh anh": "Quỳnh Anh",
    "quynh anh": "Quỳnh Anh",
    "quỳnh": "Quỳnh Anh",
    "quynh": "Quỳnh Anh",
    # Thanh Nhàn
    "thanh nhàn": "Thanh Nhàn",
    "thanh nhan": "Thanh Nhàn",
    "nhàn": "Thanh Nhàn",
    "nhan": "Thanh Nhàn",
    # Thanh Ngân  
    "thanh ngân": "Thanh Ngân",
    "thanh ngan": "Thanh Ngân",
    "ngân": "Thanh Ngân",
    "ngan": "Thanh Ngân",
    # Hạnh Diệu
    "hạnh diệu": "Ngô Hạnh Diệu",
    "hanh dieu": "Ngô Hạnh Diệu",
    "diệu": "Ngô Hạnh Diệu",
    "dieu": "Ngô Hạnh Diệu",
    # Trà Giang
    "trà giang": "Trà Giang",
    "tra giang": "Trà Giang",
    "giang": "Trà Giang",
    # Hải Anh
    "hải anh": "Hải Anh",
    "hai anh": "Hải Anh",
}

# Danh sách nhân sự booking KALLE (để detect tên cụ thể)
NHAN_SU_MAPPING = {
    # Tên thường gọi -> Tên đầy đủ trong Lark
    "trà mi": "Trà Mi - Intern Booking",
    "tra mi": "Trà Mi - Intern Booking",
    "mi": "Trà Mi - Intern Booking",
    "mai": "Như Mai",
    "như mai": "Như Mai",
    "nhu mai": "Như Mai",
    "thảo": "Phương Thảo - Intern Booking",
    "thao": "Phương Thảo - Intern Booking",
    "phương thảo": "Phương Thảo - Intern Booking",
    "dương": "Lê Thuỳ Dương",
    "duong": "Lê Thuỳ Dương",
    "thuỳ dương": "Lê Thuỳ Dương",
    "thuy duong": "Lê Thuỳ Dương",
    "vịt": "Lê Thuỳ Dương",
    "vit": "Lê Thuỳ Dương",
    "ngọc linh": "Ngọc Linh - Booking Re...",
    "ngoc linh": "Ngọc Linh - Booking Re...",
    "quân": "Quân",
    "quan": "Quân",
    "châu": "Bảo Châu - Booking Remote",
    "chau": "Bảo Châu - Booking Remote",
    "bảo châu": "Bảo Châu - Booking Remote",
    "bao chau": "Bảo Châu - Booking Remote",
}

# Vị trí cụ thể
VI_TRI_MAPPING = {
    "hr": ["hr", "nhân sự", "nhan su"],
    "content creator tiktok": ["content creator", "content tiktok", "creator tiktok"],
    "ecommerce": ["ecommerce", "e-commerce", "tmdt", "thương mại điện tử"],
    "design": ["design", "thiết kế", "thiet ke"],
    "pr": ["pr", "pr booking"],
}

# ============ TIME PARSING ============
def parse_month(text: str) -> Optional[int]:
    """Extract tháng từ text"""
    text = text.lower()
    
    # Pattern: tháng 12, tháng 1, t12, t1
    patterns = [
        r'tháng\s*(\d{1,2})',
        r'thang\s*(\d{1,2})',
        r't(\d{1,2})\b',
        r'(\d{1,2})/\d{4}',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            month = int(match.group(1))
            if 1 <= month <= 12:
                return month
    
    # Tháng hiện tại nếu không tìm thấy
    return None

def parse_week(text: str) -> Optional[int]:
    """Extract tuần từ text"""
    text = text.lower()
    
    # Pattern: tuần 1, tuần 2, tuần này
    patterns = [
        r'tuần\s*(\d)',
        r'tuan\s*(\d)',
        r'week\s*(\d)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            week = int(match.group(1))
            if 1 <= week <= 5:
                return week
    
    # "tuần này" -> tính tuần hiện tại trong tháng
    if "tuần này" in text or "tuan nay" in text or "this week" in text:
        today = datetime.now()
        week_of_month = (today.day - 1) // 7 + 1
        return min(week_of_month, 4)
    
    return None

def parse_team(text: str) -> Optional[str]:
    """Extract team từ text"""
    text = text.lower()
    
    teams = {
        "content": ["content", "content social", "content tiktok"],
        "design": ["design", "thiết kế"],
        "digital": ["digital", "ads"],
        "tiktok": ["tiktok", "tik tok"],
        "tmdt": ["tmdt", "thương mại điện tử", "e-commerce"],
        "pr": ["pr", "booking", "pr booking"],
    }
    
    for team_name, keywords in teams.items():
        for kw in keywords:
            if kw in text:
                return team_name
    
    return None

def parse_vi_tri(text: str) -> Optional[str]:
    """Extract vị trí từ text"""
    text = text.lower()
    
    for vi_tri, keywords in VI_TRI_MAPPING.items():
        for kw in keywords:
            if kw in text:
                return vi_tri
    
    return None

def get_current_week_range() -> tuple:
    """Lấy ngày đầu và cuối của tuần hiện tại"""
    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)  # Sunday
    
    return (
        start_of_week.strftime("%Y-%m-%d"),
        end_of_week.strftime("%Y-%m-%d")
    )

# ============ CLASSIFIER ============
def extract_gpt_question(text: str) -> str:
    """
    Trích xuất câu hỏi cho GPT từ text.
    Loại bỏ prefix như "gpt:", "hỏi gpt", etc.
    """
    text_lower = text.lower()
    
    # Các pattern cần loại bỏ
    prefixes = [
        r'^gpt[:\s]+',
        r'^chatgpt[:\s]+',
        r'^hỏi gpt[:\s]+',
        r'^hoi gpt[:\s]+',
        r'^ask gpt[:\s]+',
        r'^ai[:\s]+',
        r'^hỏi ai[:\s]+',
        r'^hoi ai[:\s]+',
    ]
    
    result = text
    for prefix in prefixes:
        result = re.sub(prefix, '', result, flags=re.IGNORECASE)
    
    return result.strip()


def classify_intent(text: str) -> Dict[str, Any]:
    """
    Phân loại intent từ câu hỏi
    
    Args:
        text: Câu hỏi của người dùng
    
    Returns:
        Dict chứa intent và các parameters
    """
    text_lower = text.lower()
    
    # ========== CHECK GPT CHAT FIRST ==========
    # Ưu tiên cao nhất: Nếu user muốn hỏi GPT trực tiếp
    # Patterns: "gpt:", "gpt viết", "gpt hỏi", "chatgpt", etc.
    gpt_triggers = [
        "gpt:", "gpt ", "chatgpt:", "chatgpt ", 
        "hỏi gpt", "hoi gpt", "ask gpt",
        "ai:", "hỏi ai", "hoi ai"
    ]
    is_gpt_chat = any(trigger in text_lower for trigger in gpt_triggers)
    
    # Nhưng KHÔNG phải GPT chat nếu có từ khóa KOC/booking/task đi kèm
    # Ví dụ: "GPT viết brief cho KOC" → vẫn là GPT chat
    # Nhưng: "Tổng hợp KOC tháng 12" → KHÔNG phải GPT chat
    has_report_keywords = any(kw in text_lower for kw in ["tổng hợp", "tong hop", "báo cáo", "bao cao", "thống kê", "thong ke", "tóm tắt", "tom tat"])
    
    if is_gpt_chat and not has_report_keywords:
        question = extract_gpt_question(text)
        return {
            "intent": INTENT_GPT_CHAT,
            "question": question,
            "original_text": text
        }
    
    # Count keywords
    koc_score = sum(1 for kw in KOC_KEYWORDS if kw in text_lower)
    content_score = sum(1 for kw in CONTENT_KEYWORDS if kw in text_lower)
    task_score = sum(1 for kw in TASK_KEYWORDS if kw in text_lower)
    
    # Check for GENERAL summary - chỉ khi hỏi về "kết quả công việc", "tổng hợp tuần/tháng" mà KHÔNG có KOC/task cụ thể
    general_keywords = ["tổng hợp kết quả", "tổng hợp công việc", "báo cáo tuần", "báo cáo tháng", "overview tuần", "summary tuần"]
    is_general = any(kw in text_lower for kw in general_keywords)
    
    # Nếu có "tổng hợp" NHƯNG đi kèm KOC -> vẫn là KOC report
    # Ví dụ: "tổng hợp chi phí KOC" -> KOC_REPORT, không phải GENERAL
    has_tong_hop = "tổng hợp" in text_lower or "tong hop" in text_lower
    
    # Check for task analysis specifically
    is_task_analysis = any(kw in text_lower for kw in [
        "quá hạn", "qua han", "overdue", "trễ hạn", "deadline",
        "phân tích task", "vị trí", "vi tri"
    ])
    
    # Check if asking about specific brand (Dark Beauty, Lady Killer, etc.)
    is_brand_specific = any(brand in text_lower for brand in BRAND_KEYWORDS)
    
    # Parse time info
    month = parse_month(text)
    week = parse_week(text)
    team = parse_team(text)
    vi_tri = parse_vi_tri(text)
    
    # Detect product filter (box quà, nước hoa, etc.)
    product_filter = None
    for filter_key, keywords in PRODUCT_FILTER_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            product_filter = filter_key
            break
    
    # Default to current month if not specified
    current_month = datetime.now().month
    year = datetime.now().year
    
    # ========== DETERMINE INTENT ==========
    
    # 0. Dashboard - khi hỏi về KPI, top KOC, doanh số, liên hệ nhân sự
    dashboard_score = sum(1 for kw in DASHBOARD_KEYWORDS if kw in text_lower)
    is_dashboard = dashboard_score >= 1 or any(kw in text_lower for kw in [
        "top koc", "kpi nhân sự", "kpi nhan su", "doanh số koc", "doanh so koc",
        "tỷ lệ liên hệ", "ty le lien he", "dashboard", "hiệu suất nhân sự",
        # Trigger mới
        "cập nhật tình hình", "cap nhat tinh hinh",
        "tình hình booking", "tinh hinh booking"
    ])
    
    # === CHECK CHENG TRƯỚC - nếu có keyword "cheng" ===
    is_cheng = any(kw in text_lower for kw in CHENG_KEYWORDS)
    
    if is_cheng:
        # Nếu có "cheng" + (booking/koc/báo cáo/cập nhật) → CHENG_REPORT
        if koc_score > 0 or has_tong_hop or has_report_keywords or is_dashboard:
            return {
                "intent": INTENT_CHENG_REPORT,
                "month": month if month else current_month,
                "week": week,
                "year": year,
                "group_by": "product",
                "product_filter": product_filter,
                "original_text": text
            }
    
    # ========== DETECT NHÂN SỰ CỤ THỂ ==========
    # IMPORTANT: Sort by length descending to prioritize longer matches first
    # This prevents "phương" matching when "phương thảo" was intended
    
    # Check KALLE staff FIRST (because has longer specific names like "phương thảo")
    kalle_nhan_su_detected = None
    sorted_kalle_mapping = sorted(NHAN_SU_MAPPING.items(), key=lambda x: len(x[0]), reverse=True)
    for short_name, full_name in sorted_kalle_mapping:
        if short_name in text_lower:
            pattern = r'\b' + re.escape(short_name) + r'\b'
            if re.search(pattern, text_lower):
                kalle_nhan_su_detected = full_name
                is_dashboard = True
                break
    
    # Check CHENG staff (only if no KALLE match found)
    cheng_nhan_su_detected = None
    if not kalle_nhan_su_detected:
        sorted_cheng_mapping = sorted(CHENG_NHAN_SU_MAPPING.items(), key=lambda x: len(x[0]), reverse=True)
        for short_name, full_name in sorted_cheng_mapping:
            if short_name in text_lower:
                pattern = r'\b' + re.escape(short_name) + r'\b'
                if re.search(pattern, text_lower):
                    cheng_nhan_su_detected = full_name
                    break
    
    # Nếu detect được nhân sự CHENG → route sang CHENG_REPORT với nhan_su_filter
    if cheng_nhan_su_detected and is_dashboard:
        return {
            "intent": INTENT_CHENG_REPORT,
            "month": month if month else current_month,
            "week": week,
            "year": year,
            "report_type": "kpi_ca_nhan",
            "nhan_su": cheng_nhan_su_detected,
            "original_text": text
        }
    
    # KALLE staff already checked above - no need to duplicate
    
    if is_dashboard:
        # Xác định loại báo cáo dashboard
        report_type = "full"  # Mặc định: báo cáo đầy đủ
        
        # Nếu có nhân sự cụ thể -> báo cáo cá nhân
        if kalle_nhan_su_detected:
            report_type = "kpi_ca_nhan"
        elif "top koc" in text_lower or "doanh số" in text_lower or "doanh so" in text_lower or "gmv" in text_lower:
            report_type = "top_koc"
        elif "liên hệ" in text_lower or "lien he" in text_lower or "tỷ lệ deal" in text_lower:
            report_type = "lien_he"
        elif "kpi" in text_lower and ("nhân sự" in text_lower or "nhan su" in text_lower):
            report_type = "kpi_nhan_su"
        elif "cảnh báo" in text_lower or "canh bao" in text_lower or "warning" in text_lower or "alert" in text_lower:
            report_type = "canh_bao"
        
        return {
            "intent": INTENT_DASHBOARD,
            "month": month if month else current_month,
            "week": week,
            "year": year,
            "report_type": report_type,  # "full", "top_koc", "lien_he", "kpi_nhan_su", "kpi_ca_nhan", "canh_bao"
            "nhan_su": kalle_nhan_su_detected,  # Tên nhân sự cụ thể (nếu có)
            "original_text": text
        }
    
    # 2. KOC Report (KALLE - mặc định) - ưu tiên cao nhất khi có từ khóa KOC
    if koc_score > 0 and koc_score >= content_score:
        # Quyết định group_by:
        # - "brand" nếu hỏi cụ thể Dark Beauty, Lady Killer, etc.
        # - "product" mặc định (Nước hoa, Box quà)
        group_by = "brand" if is_brand_specific else "product"
        
        return {
            "intent": INTENT_KOC_REPORT,
            "month": month if month else current_month,
            "week": week,
            "year": year,
            "filters": extract_koc_filters(text_lower),
            "group_by": group_by,  # "product" hoặc "brand"
            "product_filter": product_filter,  # "box_qua", "nuoc_hoa", etc.
            "original_text": text
        }
    
    # 2. Task Summary - khi hỏi về deadline, quá hạn, vị trí
    if task_score > 0 and is_task_analysis:
        return {
            "intent": INTENT_TASK_SUMMARY,
            "month": month,  # Có thể None để lấy tất cả
            "vi_tri": vi_tri,
            "year": year,
            "original_text": text
        }
    
    # 3. Content Calendar - khi hỏi về lịch content
    if content_score > 0 and not is_general:
        start_date, end_date = get_week_range_for_month(month, year) if month else get_current_week_range()
        
        return {
            "intent": INTENT_CONTENT_CALENDAR,
            "range_type": "month" if month else "week",
            "start_date": start_date,
            "end_date": end_date,
            "month": month,
            "team_filter": team,
            "vi_tri_filter": vi_tri,
            "original_text": text
        }
    
    # 4. General Summary - CHỈ khi hỏi tổng hợp chung (không có KOC/task cụ thể)
    if is_general:
        return {
            "intent": INTENT_GENERAL_SUMMARY,
            "month": month if month else current_month,
            "week": week,
            "year": year,
            "team": team,
            "original_text": text
        }
    
    # 5. Unknown
    return {
        "intent": INTENT_UNKNOWN,
        "original_text": text,
        "suggestion": "Bạn có thể hỏi về:\n• Báo cáo KOC: \"Tóm tắt KOC tháng 12\"\n• Lịch content: \"Lịch content tháng 12\"\n• Phân tích task: \"Task quá hạn theo vị trí\"\n• Tổng hợp: \"Tổng hợp kết quả công việc tháng 12\""
    }


def get_week_range_for_month(month: int, year: int) -> tuple:
    """Lấy ngày đầu và cuối của tháng"""
    start_date = f"{year}-{month:02d}-01"
    
    # Cuối tháng
    if month == 12:
        end_date = f"{year}-12-31"
    else:
        # Ngày đầu tháng sau - 1
        from datetime import date
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        end_date = f"{year}-{month:02d}-{last_day:02d}"
    
    return (start_date, end_date)

def extract_koc_filters(text: str) -> list:
    """Extract các filter cụ thể cho KOC report"""
    filters = []
    
    if "chưa air" in text or "chua air" in text:
        filters.append("chua_air")
    if "đã air" in text or "da air" in text:
        filters.append("da_air")
    if "chưa có link" in text or "thiếu link" in text or "chưa link" in text:
        filters.append("link_missing")
    if "chưa gắn giỏ" in text or "chua gan gio" in text:
        filters.append("chua_gan_gio")
    if "đã gắn giỏ" in text or "da gan gio" in text:
        filters.append("da_gan_gio")
    
    return filters

# ============ TEST ============
def test_classifier():
    """Test intent classifier"""
    test_cases = [
        "Tóm tắt KOC tháng 12 giúp chị",
        "KOC tuần 2 ai air rồi?",
        "Liệt kê KOC chưa gắn giỏ tháng 12",
        "ai đã air nhưng chưa có link bài trong tháng 12?",
        "Lịch content tuần này",
        "Các task TikTok tuần này có đầu nào trễ không?",
        "Cho chị list content có từ Noel trong tháng 12",
        "Summary overview tuần này: content + booking",
        "Xin chào Jarvis",
        # Test CHENG staff detection
        "KPI của Phương tháng 12",
        "KPI của Linh",
        "Jarvis KPI của Trang",
        # Test KALLE staff detection
        "KPI của Mai tháng 12",
        "KPI của Thảo",
    ]
    
    print("=" * 50)
    print("INTENT CLASSIFIER TEST")
    print("=" * 50)
    
    for text in test_cases:
        result = classify_intent(text)
        print(f"\n📝 Input: {text}")
        print(f"🎯 Intent: {result['intent']}")
        if result.get('nhan_su'):
            print(f"👤 Nhân sự: {result['nhan_su']}")
        print(f"📊 Params: {result}")

if __name__ == "__main__":
    test_classifier()
