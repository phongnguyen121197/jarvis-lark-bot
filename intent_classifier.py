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
INTENT_GENERAL_SUMMARY = "GENERAL_SUMMARY"
INTENT_UNKNOWN = "UNKNOWN"

# ============ KEYWORDS ============
KOC_KEYWORDS = [
    "koc", "booking", "air", "gắn giỏ", "gan gio", "pr", 
    "đã air", "chưa air", "link air", "tháng deal", "tuần deal"
]

CONTENT_KEYWORDS = [
    "content", "lịch", "lich", "task", "công việc", "cong viec",
    "bài đăng", "tiktok", "design", "digital", "deadline"
]

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
def classify_intent(text: str) -> Dict[str, Any]:
    """
    Phân loại intent từ câu hỏi
    
    Args:
        text: Câu hỏi của người dùng
    
    Returns:
        Dict chứa intent và các parameters
    """
    text_lower = text.lower()
    
    # Count keywords
    koc_score = sum(1 for kw in KOC_KEYWORDS if kw in text_lower)
    content_score = sum(1 for kw in CONTENT_KEYWORDS if kw in text_lower)
    
    # Check for general summary
    is_general = any(kw in text_lower for kw in ["tổng hợp", "overview", "summary", "tóm tắt tuần"])
    
    # Parse time info
    month = parse_month(text)
    week = parse_week(text)
    team = parse_team(text)
    
    # Default to current month if asking about KOC
    if month is None:
        month = datetime.now().month
    
    year = datetime.now().year
    
    # Determine intent
    if is_general and (koc_score > 0 or content_score > 0):
        # General summary combining both
        return {
            "intent": INTENT_GENERAL_SUMMARY,
            "components": ["koc", "content"] if koc_score > 0 and content_score > 0 
                         else (["koc"] if koc_score > 0 else ["content"]),
            "month": month,
            "week": week,
            "year": year,
            "team": team,
            "original_text": text
        }
    
    elif koc_score > content_score or koc_score > 0:
        # KOC Report
        return {
            "intent": INTENT_KOC_REPORT,
            "month": month,
            "week": week,
            "year": year,
            "filters": extract_koc_filters(text_lower),
            "original_text": text
        }
    
    elif content_score > 0:
        # Content Calendar
        start_date, end_date = get_current_week_range()
        
        return {
            "intent": INTENT_CONTENT_CALENDAR,
            "range_type": "week",
            "start_date": start_date,
            "end_date": end_date,
            "team_filter": team,
            "original_text": text
        }
    
    else:
        # Unknown - có thể cần hỏi thêm
        return {
            "intent": INTENT_UNKNOWN,
            "original_text": text,
            "suggestion": "Bạn có thể hỏi về:\n• Báo cáo KOC (ví dụ: 'tóm tắt KOC tháng 12')\n• Lịch content (ví dụ: 'lịch content tuần này')"
        }

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
