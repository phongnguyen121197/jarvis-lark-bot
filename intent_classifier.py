"""
Intent Classifier Module
Phân loại câu hỏi của người dùng thành các intent
"""
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# ============ INTENT TYPES ============
INTENT_KOC_REPORT = "KOC_REPORT"
INTENT_CONTENT_CALENDAR = "CONTENT_CALENDAR_SUMMARY"
INTENT_TASK_SUMMARY = "TASK_SUMMARY"  # Phân tích task theo vị trí
INTENT_GENERAL_SUMMARY = "GENERAL_SUMMARY"
INTENT_GPT_CHAT = "GPT_CHAT"  # Hỏi ChatGPT trực tiếp
INTENT_UNKNOWN = "UNKNOWN"

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
    gpt_triggers = ["gpt:", "chatgpt:", "hỏi gpt", "hoi gpt", "ask gpt", "ai:", "hỏi ai", "hoi ai"]
    is_gpt_chat = any(trigger in text_lower for trigger in gpt_triggers)
    
    if is_gpt_chat:
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
    
    # Default to current month if not specified
    current_month = datetime.now().month
    year = datetime.now().year
    
    # ========== DETERMINE INTENT ==========
    
    # 1. KOC Report - ưu tiên cao nhất khi có từ khóa KOC
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
    ]
    
    print("=" * 50)
    print("INTENT CLASSIFIER TEST")
    print("=" * 50)
    
    for text in test_cases:
        result = classify_intent(text)
        print(f"\n📝 Input: {text}")
        print(f"🎯 Intent: {result['intent']}")
        print(f"📊 Params: {result}")

if __name__ == "__main__":
    test_classifier()
