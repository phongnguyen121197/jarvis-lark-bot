"""
Notes Manager Module
Quản lý ghi nhớ và phân loại công việc cho Jarvis
"""
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
import json

# ============ NOTE CATEGORIES ============
CATEGORY_URGENT = "🔴 Công việc cần xử lý gấp"        # Deadline 1-2 ngày
CATEGORY_LATER = "🟡 Công việc xử lý sau"             # Deadline 3-5+ ngày
CATEGORY_DAILY = "🔵 Công việc hàng ngày"             # Keyword: daily
CATEGORY_ISSUE = "🟠 Vấn đề tồn đọng"                 # Keyword: Vấn đề
CATEGORY_NOTE = "📝 Việc cần ghi nhớ"                 # Keyword: Note hoặc mặc định


@dataclass
class Note:
    """Class đại diện cho một note"""
    id: int
    content: str
    category: str
    created_at: datetime
    created_by: str = ""
    chat_id: str = ""  # Chat ID để gửi reminder
    deadline: Optional[datetime] = None
    is_done: bool = False
    reminder_sent: bool = False  # Đã gửi reminder chưa
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "content": self.content,
            "category": self.category,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "chat_id": self.chat_id,
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "is_done": self.is_done,
            "reminder_sent": self.reminder_sent
        }


class NotesManager:
    """Quản lý notes trong memory"""
    
    def __init__(self):
        self._notes: Dict[int, Note] = {}
        self._next_id: int = 1
    
    def add_note(self, content: str, created_by: str = "", chat_id: str = "") -> Note:
        """Thêm note mới và tự động phân loại"""
        category, deadline = self._classify_note(content)
        
        note = Note(
            id=self._next_id,
            content=content,
            category=category,
            created_at=datetime.now(),
            created_by=created_by,
            chat_id=chat_id,
            deadline=deadline
        )
        
        self._notes[self._next_id] = note
        self._next_id += 1
        
        return note
    
    def _classify_note(self, content: str) -> tuple[str, Optional[datetime]]:
        """Phân loại note dựa trên nội dung và keywords"""
        content_lower = content.lower()
        now = datetime.now()
        deadline = None
        
        # 1. Check keyword "Vấn đề" ở đầu
        if content_lower.startswith("vấn đề") or content_lower.startswith("van de"):
            return CATEGORY_ISSUE, None
        
        # 2. Check keyword "daily" / "hàng ngày"
        daily_keywords = ["daily", "hàng ngày", "hang ngay", "mỗi ngày", "moi ngay"]
        if any(kw in content_lower for kw in daily_keywords):
            return CATEGORY_DAILY, None
        
        # 3. Check deadline trong nội dung
        deadline_days = self._extract_deadline_days(content_lower)
        
        if deadline_days is not None:
            deadline = now + timedelta(days=deadline_days)
            
            if deadline_days <= 2:
                return CATEGORY_URGENT, deadline
            else:
                return CATEGORY_LATER, deadline
        
        # 4. Check keywords khẩn cấp
        urgent_keywords = ["gấp", "gap", "urgent", "khẩn cấp", "khan cap", "asap", "ngay", "hôm nay", "hom nay", "mai"]
        if any(kw in content_lower for kw in urgent_keywords):
            return CATEGORY_URGENT, now + timedelta(days=1)
        
        # 5. Mặc định là "Việc cần ghi nhớ"
        return CATEGORY_NOTE, None
    
    def _extract_deadline_days(self, content: str) -> Optional[int]:
        """Trích xuất số ngày deadline từ nội dung"""
        now = datetime.now()
        
        # 1. Pattern: "deadline X ngày", "X ngày nữa", "trong X ngày"
        patterns = [
            r'deadline\s*(\d+)\s*ngày',
            r'(\d+)\s*ngày\s*nữa',
            r'trong\s*(\d+)\s*ngày',
            r'(\d+)\s*ngay\s*nua',
            r'deadline\s*(\d+)\s*ngay',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return int(match.group(1))
        
        # 2. Pattern: ngày cụ thể "ngày DD/MM" hoặc "DD/MM"
        date_patterns = [
            r'ngày\s*(\d{1,2})[/\-](\d{1,2})',  # ngày 16/12
            r'deadline\s*ngày\s*(\d{1,2})[/\-](\d{1,2})',  # deadline ngày 16/12
            r'(\d{1,2})[/\-](\d{1,2})',  # 16/12
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, content)
            if match:
                try:
                    day = int(match.group(1))
                    month = int(match.group(2))
                    year = now.year
                    
                    # Nếu tháng đã qua, có thể là năm sau
                    target_date = datetime(year, month, day)
                    if target_date < now:
                        target_date = datetime(year + 1, month, day)
                    
                    days_diff = (target_date - now).days
                    if days_diff >= 0:
                        return days_diff
                except:
                    pass
        
        # 3. Check "tuần sau", "tuần tới"
        if "tuần sau" in content or "tuan sau" in content or "tuần tới" in content:
            return 7
        
        # 4. Check "tháng sau"
        if "tháng sau" in content or "thang sau" in content:
            return 30
        
        # 5. Check "thứ X" - tính ngày đến thứ X tiếp theo
        weekday_map = {
            "thứ 2": 0, "thứ hai": 0, "thu 2": 0,
            "thứ 3": 1, "thứ ba": 1, "thu 3": 1,
            "thứ 4": 2, "thứ tư": 2, "thu 4": 2,
            "thứ 5": 3, "thứ năm": 3, "thu 5": 3,
            "thứ 6": 4, "thứ sáu": 4, "thu 6": 4,
            "thứ 7": 5, "thứ bảy": 5, "thu 7": 5,
            "chủ nhật": 6, "chu nhat": 6, "cn": 6,
        }
        
        for weekday_name, weekday_num in weekday_map.items():
            if weekday_name in content:
                current_weekday = now.weekday()
                days_until = (weekday_num - current_weekday) % 7
                if days_until == 0:
                    days_until = 7  # Nếu là cùng ngày, tính tuần sau
                return days_until
        
        return None
    
    def get_note(self, note_id: int) -> Optional[Note]:
        """Lấy note theo ID"""
        return self._notes.get(note_id)
    
    def delete_note(self, note_id: int) -> bool:
        """Xóa note theo ID"""
        if note_id in self._notes:
            del self._notes[note_id]
            return True
        return False
    
    def mark_done(self, note_id: int) -> bool:
        """Đánh dấu note đã hoàn thành"""
        note = self._notes.get(note_id)
        if note:
            note.is_done = True
            return True
        return False
    
    def get_all_notes(self, include_done: bool = False) -> List[Note]:
        """Lấy tất cả notes"""
        notes = list(self._notes.values())
        if not include_done:
            notes = [n for n in notes if not n.is_done]
        return sorted(notes, key=lambda x: x.created_at, reverse=True)
    
    def get_notes_by_category(self, category: str) -> List[Note]:
        """Lấy notes theo category"""
        return [n for n in self._notes.values() if n.category == category and not n.is_done]
    
    def get_summary(self) -> Dict[str, Any]:
        """Tổng hợp notes theo category"""
        all_notes = self.get_all_notes(include_done=False)
        
        summary = {
            CATEGORY_URGENT: [],
            CATEGORY_LATER: [],
            CATEGORY_DAILY: [],
            CATEGORY_ISSUE: [],
            CATEGORY_NOTE: [],
        }
        
        for note in all_notes:
            if note.category in summary:
                summary[note.category].append(note)
        
        return summary
    
    def clear_all(self) -> int:
        """Xóa tất cả notes"""
        count = len(self._notes)
        self._notes.clear()
        self._next_id = 1
        return count
    
    def get_notes_due_soon(self, days: int = 1) -> List[Note]:
        """Lấy notes có deadline trong X ngày tới và chưa gửi reminder"""
        now = datetime.now()
        due_notes = []
        
        for note in self._notes.values():
            if note.is_done or note.reminder_sent:
                continue
            
            if note.deadline:
                days_left = (note.deadline - now).days
                if 0 <= days_left <= days:
                    due_notes.append(note)
        
        return due_notes
    
    def get_overdue_notes(self) -> List[Note]:
        """Lấy notes đã quá hạn"""
        now = datetime.now()
        overdue = []
        
        for note in self._notes.values():
            if note.is_done:
                continue
            
            if note.deadline and note.deadline < now:
                overdue.append(note)
        
        return overdue
    
    def mark_reminder_sent(self, note_id: int) -> bool:
        """Đánh dấu đã gửi reminder"""
        note = self._notes.get(note_id)
        if note:
            note.reminder_sent = True
            return True
        return False


# Global instance
_notes_manager = NotesManager()


def get_notes_manager() -> NotesManager:
    """Get global notes manager instance"""
    return _notes_manager


# ============ HELPER FUNCTIONS ============

def format_note_summary(summary: Dict[str, List[Note]]) -> str:
    """Format summary thành text đẹp"""
    lines = ["📋 **TỔNG HỢP GHI NHỚ**\n"]
    
    total_notes = sum(len(notes) for notes in summary.values())
    
    if total_notes == 0:
        return "📋 Chưa có ghi nhớ nào. Hãy thử:\n• \"Note: nội dung cần ghi nhớ\"\n• \"Ghi nhớ: công việc deadline 2 ngày\""
    
    lines.append(f"Tổng cộng: {total_notes} ghi nhớ\n")
    
    for category, notes in summary.items():
        if notes:
            lines.append(f"\n{category} ({len(notes)})")
            lines.append("-" * 30)
            
            for note in notes:
                deadline_str = ""
                if note.deadline:
                    days_left = (note.deadline - datetime.now()).days
                    if days_left <= 0:
                        deadline_str = " ⚠️ HẾT HẠN!"
                    elif days_left == 1:
                        deadline_str = " (còn 1 ngày)"
                    else:
                        deadline_str = f" (còn {days_left} ngày)"
                
                lines.append(f"  #{note.id}: {note.content}{deadline_str}")
    
    lines.append("\n💡 Tip: Dùng \"Xong #ID\" để đánh dấu hoàn thành")
    
    return "\n".join(lines)


def check_note_command(text: str) -> Optional[Dict]:
    """
    Kiểm tra xem có phải lệnh note không
    Returns: Dict với action và params, hoặc None
    """
    # Loại bỏ "Jarvis" ở đầu nếu có
    text_clean = text.strip()
    text_clean = re.sub(r'^jarvis\s*', '', text_clean, flags=re.IGNORECASE).strip()
    
    text_lower = text_clean.lower().strip()
    
    # 1. Lệnh xem tổng hợp notes
    summary_keywords = [
        "tổng hợp note", "tong hop note",
        "xem note", "xem ghi nhớ", "xem ghi nho",
        "danh sách note", "danh sach note",
        "list note", "notes", "ghi nhớ của tôi",
        "việc cần làm", "viec can lam",
        "todo", "to do", "to-do",
        "xem todo", "danh sách công việc"
    ]
    if any(kw in text_lower for kw in summary_keywords):
        return {"action": "summary"}
    
    # 2. Lệnh đánh dấu hoàn thành
    done_patterns = [
        r'xong\s*#?(\d+)',
        r'done\s*#?(\d+)',
        r'hoàn thành\s*#?(\d+)',
        r'hoan thanh\s*#?(\d+)',
        r'đã xong\s*#?(\d+)',
        r'da xong\s*#?(\d+)',
    ]
    for pattern in done_patterns:
        match = re.search(pattern, text_lower)
        if match:
            return {"action": "done", "note_id": int(match.group(1))}
    
    # 3. Lệnh xóa note
    delete_patterns = [
        r'xóa\s*note\s*#?(\d+)',
        r'xoa\s*note\s*#?(\d+)',
        r'delete\s*note\s*#?(\d+)',
        r'xóa\s*#?(\d+)',
        r'xoa\s*#?(\d+)',
    ]
    for pattern in delete_patterns:
        match = re.search(pattern, text_lower)
        if match:
            return {"action": "delete", "note_id": int(match.group(1))}
    
    # 4. Lệnh xóa tất cả
    if "xóa tất cả note" in text_lower or "xoa tat ca note" in text_lower or "clear notes" in text_lower:
        return {"action": "clear_all"}
    
    # 5. Kiểm tra có phải lệnh thêm note không
    # Patterns để nhận diện bắt đầu note
    note_start_patterns = [
        r'^note\s*[:\s]',
        r'^ghi nhớ\s*[:\s]',
        r'^ghi nho\s*[:\s]',
        r'^nhớ\s*[:\s]',
        r'^nho\s*[:\s]',
        r'^todo\s*[:\s]',
        r'^deadline\s*[:\s]',
        r'^công việc\s*[:\s]',
        r'^cong viec\s*[:\s]',
        r'^vấn đề\s*[:\s]',
        r'^van de\s*[:\s]',
    ]
    
    is_note_command = any(re.search(pattern, text_lower) for pattern in note_start_patterns)
    
    if is_note_command:
        # Lấy nội dung sau keyword
        content = re.sub(
            r'^(note|ghi nhớ|ghi nho|nhớ|nho|todo|deadline|công việc|cong viec|vấn đề|van de)\s*[:\s]*',
            '', text_clean, flags=re.IGNORECASE
        ).strip()
        
        if not content:
            return None
        
        # Check nếu có nhiều items (bullet points)
        # Chỉ split khi có newline + bullet, KHÔNG split nếu chỉ có "-" trong content
        lines = content.split('\n')
        items = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Remove bullet/number prefix nếu ở đầu dòng
            # Patterns: •, - ở đầu dòng (với space sau), *, số thứ tự
            cleaned_line = re.sub(r'^[•\*]\s*', '', line)  # • hoặc *
            cleaned_line = re.sub(r'^-\s+', '', cleaned_line)  # - với space (để phân biệt với - trong content)
            cleaned_line = re.sub(r'^\d+[\.\)]\s*', '', cleaned_line)  # 1. hoặc 1)
            cleaned_line = cleaned_line.strip()
            
            if cleaned_line:
                items.append(cleaned_line)
        
        if len(items) > 1:
            # Nhiều notes
            return {"action": "add_multiple", "contents": items}
        elif len(items) == 1:
            # 1 note
            return {"action": "add", "content": items[0]}
        else:
            return {"action": "add", "content": content}
    
    # 6. Check nếu bắt đầu bằng "Vấn đề" (không cần dấu :)
    if text_lower.startswith("vấn đề") or text_lower.startswith("van de"):
        content = re.sub(r'^(vấn đề|van de)[:\s]*', '', text_clean, flags=re.IGNORECASE).strip()
        if content:
            return {"action": "add", "content": f"Vấn đề: {content}"}
    
    return None


async def handle_note_command(params: Dict, user_name: str = "", chat_id: str = "") -> str:
    """Xử lý các lệnh note"""
    manager = get_notes_manager()
    action = params.get("action")
    
    if action == "summary":
        summary = manager.get_summary()
        return format_note_summary(summary)
    
    elif action == "add":
        content = params.get("content", "")
        if not content:
            return "❌ Nội dung note không được trống"
        
        note = manager.add_note(content, created_by=user_name, chat_id=chat_id)
        
        deadline_str = ""
        reminder_str = ""
        if note.deadline:
            days = (note.deadline - datetime.now()).days
            deadline_str = f"\n📅 Deadline: {days} ngày"
            reminder_str = f"\n🔔 Sẽ nhắc nhở khi còn 1 ngày"
        
        return f"✅ Đã ghi nhớ #{note.id}\n\n{note.category}\n📝 {note.content}{deadline_str}{reminder_str}"
    
    elif action == "add_multiple":
        contents = params.get("contents", [])
        if not contents:
            return "❌ Không có nội dung note"
        
        results = []
        success_count = 0
        
        for content in contents:
            note = manager.add_note(content, created_by=user_name, chat_id=chat_id)
            
            deadline_str = ""
            if note.deadline:
                days = (note.deadline - datetime.now()).days
                deadline_str = f" (deadline {days} ngày)"
            
            results.append(f"  #{note.id}: {note.content[:50]}{'...' if len(note.content) > 50 else ''}{deadline_str}")
            success_count += 1
        
        response = f"✅ Đã ghi nhớ {success_count} công việc:\n\n"
        response += "\n".join(results)
        response += "\n\n💡 Dùng \"Xem note\" để xem chi tiết"
        
        return response
    
    elif action == "done":
        note_id = params.get("note_id")
        note = manager.get_note(note_id)
        
        if not note:
            return f"❌ Không tìm thấy note #{note_id}"
        
        manager.mark_done(note_id)
        return f"✅ Đã hoàn thành #{note_id}: {note.content}"
    
    elif action == "delete":
        note_id = params.get("note_id")
        note = manager.get_note(note_id)
        
        if not note:
            return f"❌ Không tìm thấy note #{note_id}"
        
        manager.delete_note(note_id)
        return f"🗑️ Đã xóa #{note_id}: {note.content}"
    
    elif action == "clear_all":
        count = manager.clear_all()
        return f"🗑️ Đã xóa tất cả {count} notes"
    
    return "❌ Lệnh không hợp lệ"
