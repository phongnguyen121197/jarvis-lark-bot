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
    deadline: Optional[datetime] = None
    is_done: bool = False
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "content": self.content,
            "category": self.category,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "is_done": self.is_done
        }


class NotesManager:
    """Quản lý notes trong memory"""
    
    def __init__(self):
        self._notes: Dict[int, Note] = {}
        self._next_id: int = 1
    
    def add_note(self, content: str, created_by: str = "") -> Note:
        """Thêm note mới và tự động phân loại"""
        category, deadline = self._classify_note(content)
        
        note = Note(
            id=self._next_id,
            content=content,
            category=category,
            created_at=datetime.now(),
            created_by=created_by,
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
        # Pattern: "deadline X ngày", "X ngày nữa", "trong X ngày"
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
        
        # Check "tuần sau", "tuần tới"
        if "tuần sau" in content or "tuan sau" in content or "tuần tới" in content:
            return 7
        
        # Check "tháng sau"
        if "tháng sau" in content or "thang sau" in content:
            return 30
        
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
    text_lower = text.lower().strip()
    
    # 1. Lệnh xem tổng hợp notes
    summary_keywords = [
        "tổng hợp note", "tong hop note",
        "xem note", "xem ghi nhớ", "xem ghi nho",
        "danh sách note", "danh sach note",
        "list note", "notes", "ghi nhớ",
        "việc cần làm", "viec can lam",
        "todo", "to do", "to-do"
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
    
    # 5. Lệnh thêm note mới
    add_patterns = [
        (r'^note[:\s]+(.+)$', 1),
        (r'^ghi nhớ[:\s]+(.+)$', 1),
        (r'^ghi nho[:\s]+(.+)$', 1),
        (r'^nhớ[:\s]+(.+)$', 1),
        (r'^nho[:\s]+(.+)$', 1),
        (r'^todo[:\s]+(.+)$', 1),
        (r'^vấn đề[:\s]+(.+)$', 1),
        (r'^van de[:\s]+(.+)$', 1),
        (r'^deadline[:\s]+(.+)$', 1),
    ]
    
    for pattern, group in add_patterns:
        match = re.search(pattern, text_lower)
        if match:
            # Lấy nội dung gốc (giữ nguyên case)
            original_match = re.search(pattern, text, re.IGNORECASE)
            if original_match:
                content = original_match.group(group).strip()
                return {"action": "add", "content": content}
    
    # 6. Check nếu bắt đầu bằng "Vấn đề" (không cần dấu :)
    if text_lower.startswith("vấn đề") or text_lower.startswith("van de"):
        return {"action": "add", "content": text.strip()}
    
    return None


async def handle_note_command(params: Dict, user_name: str = "") -> str:
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
        
        note = manager.add_note(content, created_by=user_name)
        
        deadline_str = ""
        if note.deadline:
            days = (note.deadline - datetime.now()).days
            deadline_str = f"\n📅 Deadline: {days} ngày"
        
        return f"✅ Đã ghi nhớ #{note.id}\n\n{note.category}\n📝 {note.content}{deadline_str}"
    
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
