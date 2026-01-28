# notes_manager.py - Version 5.8.1
# Fixed: Added compatibility functions required by main.py
# - check_note_command(text) - single arg version
# - handle_note_command(params, chat_id, user_name) - for main.py
# - get_notes_manager(chat_id) - returns NotesManager instance

import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from lark_base import (
    get_notes_by_chat_id,
    create_note,
    update_note,
    delete_note,
    get_notes_due_soon,
    get_all_notes
)

logger = logging.getLogger(__name__)

# ============================================================================
# NOTE DATA CLASS (for scheduler compatibility)
# ============================================================================

@dataclass
class Note:
    """Note dataclass for scheduler compatibility"""
    id: str
    content: str
    chat_id: str
    deadline: Optional[datetime] = None
    created_at: Optional[datetime] = None
    reminder_sent: bool = False


# ============================================================================
# COMMAND PATTERNS
# ============================================================================

# Add note patterns
ADD_PATTERNS = [
    r'(?:ghi chú|ghi chu|note|nhắc|nhac|reminder|remember|nhớ|nho)\s*[:\-]?\s*(.+)',
    r'(?:thêm|them|add)\s+(?:ghi chú|note|nhắc|reminder)\s*[:\-]?\s*(.+)',
    r'(?:lưu|luu|save)\s+(?:ghi chú|note)\s*[:\-]?\s*(.+)',
]

# View note patterns
VIEW_PATTERNS = [
    r'(?:xem|show|list|danh sách|danhsach)\s+(?:ghi chú|note|nhắc|reminder)',
    r'(?:ghi chú|note|nhắc|reminder)\s+(?:của tôi|cua toi|of mine)',
    r'(?:các|cac|all)\s+(?:ghi chú|note|nhắc|reminder)',
]

# Delete note patterns
DELETE_PATTERNS = [
    r'(?:xóa|xoa|delete|remove|hủy|huy)\s+(?:ghi chú|note|nhắc|reminder)\s*#?(\d+)',
    r'(?:xóa|xoa|delete|remove)\s*#(\d+)',
]

# Done/Complete patterns
DONE_PATTERNS = [
    r'(?:done|xong|hoàn thành|hoan thanh|complete|completed)\s*#\s*(\d+)',  # "Done # 1", "Done #1"
    r'#\s*(\d+)\s*(?:done|xong|hoàn thành|hoan thanh|complete|completed)',  # "# 1 done"
    r'(?:done|xong|hoàn thành|hoan thanh|complete|completed)\s+(.+)',
    r'(?:đã xong|da xong|đã hoàn thành|da hoan thanh)\s+(.+)',
]

# Update note patterns
UPDATE_PATTERNS = [
    r'(?:sửa|sua|edit|update|cập nhật|cap nhat)\s+(?:ghi chú|note|nhắc|reminder)\s*#?(\d+)\s*[:\-]?\s*(.+)',
    r'(?:sửa|sua|edit|update)\s*#(\d+)\s*[:\-]?\s*(.+)',
]

# Set deadline patterns
DEADLINE_PATTERNS = [
    r'(?:hạn|han|deadline|đến hạn|den han|nhắc lúc|nhac luc|nhắc vào|nhac vao)\s*[:\-]?\s*(.+)',
    r'(?:vào|vao|lúc|luc|at)\s+(\d{1,2}[:\-h]\d{2})',
    r'(\d{1,2}/\d{1,2}(?:/\d{2,4})?)',
    r'(ngày mai|hôm nay|tuần sau|tháng sau)',
]


# ============================================================================
# PARSING UTILITIES
# ============================================================================

def parse_datetime(text: str) -> Optional[datetime]:
    """Parse datetime from Vietnamese/English text"""
    text = text.lower().strip()
    now = datetime.now()
    
    # Relative dates
    if "hôm nay" in text or "hom nay" in text or "today" in text:
        base_date = now
    elif "ngày mai" in text or "ngay mai" in text or "tomorrow" in text:
        base_date = now + timedelta(days=1)
    elif "tuần sau" in text or "tuan sau" in text or "next week" in text:
        base_date = now + timedelta(weeks=1)
    elif "tháng sau" in text or "thang sau" in text or "next month" in text:
        base_date = now + timedelta(days=30)
    else:
        base_date = now
    
    # Try to extract date
    date_match = re.search(r'(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?', text)
    if date_match:
        day = int(date_match.group(1))
        month = int(date_match.group(2))
        year = int(date_match.group(3)) if date_match.group(3) else now.year
        
        if year < 100:
            year += 2000
        
        try:
            base_date = base_date.replace(year=year, month=month, day=day)
        except ValueError:
            pass
    
    # Try to extract time
    time_match = re.search(r'(\d{1,2})[:\-h](\d{2})', text)
    if time_match:
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        
        try:
            base_date = base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
            pass
    else:
        # Default to 9:00 AM if no time specified
        base_date = base_date.replace(hour=9, minute=0, second=0, microsecond=0)
    
    return base_date


def extract_deadline_from_text(text: str) -> Tuple[str, Optional[datetime]]:
    """Extract deadline from note text"""
    deadline = None
    cleaned_text = text
    
    for pattern in DEADLINE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            deadline_str = match.group(1) if match.groups() else match.group(0)
            deadline = parse_datetime(deadline_str)
            cleaned_text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
            cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
            break
    
    return cleaned_text, deadline


# ============================================================================
# COMPATIBILITY FUNCTIONS FOR main.py
# ============================================================================

def check_note_command(text: str) -> Optional[Dict[str, Any]]:
    """
    Check if message is a note command (single arg version for main.py)
    
    Args:
        text: Message text
        
    Returns:
        Dict with action and params if note command, None otherwise
    """
    message = text.strip()
    
    # Check DONE patterns
    for pattern in DONE_PATTERNS:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            identifier = match.group(1).strip()
            if identifier.isdigit():
                return {
                    "action": "done",
                    "identifier_type": "id",
                    "note_id": identifier
                }
            else:
                return {
                    "action": "done",
                    "identifier_type": "title",
                    "note_title": identifier
                }
    
    # Check ADD patterns
    for pattern in ADD_PATTERNS:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            note_content = match.group(1).strip()
            note_key, deadline = extract_deadline_from_text(note_content)
            return {
                "action": "add",
                "note_key": note_key[:100],
                "note_value": note_content,
                "deadline": deadline
            }
    
    # Check VIEW patterns
    for pattern in VIEW_PATTERNS:
        if re.search(pattern, message, re.IGNORECASE):
            return {"action": "view"}
    
    # Check DELETE patterns
    for pattern in DELETE_PATTERNS:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            return {
                "action": "delete",
                "note_id": match.group(1)
            }
    
    # Check UPDATE patterns
    for pattern in UPDATE_PATTERNS:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            note_id = match.group(1)
            new_content = match.group(2).strip()
            note_key, deadline = extract_deadline_from_text(new_content)
            return {
                "action": "update",
                "note_id": note_id,
                "note_key": note_key[:100],
                "note_value": new_content,
                "deadline": deadline
            }
    
    return None


async def handle_note_command(params: Dict[str, Any], chat_id: str = "", user_name: str = "") -> str:
    """
    Handle note command (async wrapper for main.py compatibility)
    
    Args:
        params: Dict from check_note_command()
        chat_id: Chat ID
        user_name: User name (optional)
        
    Returns:
        Response string
    """
    if not params:
        return "❌ Không có lệnh ghi chú"
    
    action = params.get("action")
    
    if action == "add":
        return await handle_add_note(
            chat_id,
            params.get("note_key", ""),
            params.get("note_value", ""),
            params.get("deadline")
        )
    
    elif action == "view":
        return await handle_view_notes(chat_id)
    
    elif action == "delete":
        return await handle_delete_note(chat_id, params.get("note_id", "0"))
    
    elif action == "done":
        return await handle_done_note(
            chat_id,
            params.get("note_id") or params.get("note_title", ""),
            params.get("identifier_type", "id")
        )
    
    elif action == "update":
        return await handle_update_note(
            chat_id,
            params.get("note_id", "0"),
            params.get("note_key", ""),
            params.get("note_value", ""),
            params.get("deadline")
        )
    
    return "❌ Lệnh không hợp lệ"


# ============================================================================
# NOTES MANAGER CLASS (for scheduler)
# ============================================================================

class NotesManager:
    """NotesManager class for scheduler integration"""
    
    def __init__(self, chat_id: str = None):
        self.chat_id = chat_id
        self._reminder_sent = {}
    
    async def get_notes_due_soon(self, days: int = 1) -> List[Note]:
        """Get notes with deadlines within N days"""
        try:
            all_notes = await get_all_notes()
            due_notes = []
            now = datetime.now()
            future = now + timedelta(days=days)
            
            for record in all_notes:
                deadline = record.get("deadline")
                
                if deadline:
                    try:
                        deadline_dt = datetime.fromtimestamp(deadline / 1000) if isinstance(deadline, (int, float)) else None
                        if deadline_dt and now <= deadline_dt <= future:
                            note = Note(
                                id=str(len(due_notes) + 1),
                                content=record.get("note_key", "") or record.get("note_value", ""),
                                chat_id=record.get("chat_id", ""),
                                deadline=deadline_dt,
                                reminder_sent=self._reminder_sent.get(record.get("record_id"), False)
                            )
                            due_notes.append(note)
                    except:
                        pass
            
            return due_notes
        except Exception as e:
            logger.error(f"Error getting due notes: {e}")
            return []
    
    async def get_overdue_notes(self) -> List[Note]:
        """Get overdue notes - v5.7.21 fixed to check note exists"""
        try:
            all_notes = await get_all_notes()
            overdue_notes = []
            now = datetime.now()
            
            # v5.7.21: Group notes by chat_id để check index đúng
            notes_by_chat = {}
            for record in all_notes:
                chat_id = record.get("chat_id", "")
                if chat_id not in notes_by_chat:
                    notes_by_chat[chat_id] = []
                notes_by_chat[chat_id].append(record)
            
            for chat_id, chat_notes in notes_by_chat.items():
                for idx, record in enumerate(chat_notes):
                    deadline = record.get("deadline")
                    record_id = record.get("record_id", "")
                    
                    if deadline:
                        try:
                            deadline_dt = datetime.fromtimestamp(deadline / 1000) if isinstance(deadline, (int, float)) else None
                            if deadline_dt and deadline_dt < now:
                                # v5.7.21: Use actual index (1-based) trong chat
                                note_id = str(idx + 1)
                                note = Note(
                                    id=note_id,
                                    content=record.get("note_key", "") or record.get("note_value", ""),
                                    chat_id=chat_id,
                                    deadline=deadline_dt,
                                    reminder_sent=self._reminder_sent.get(record_id, False)
                                )
                                # v5.7.21: Track by record_id để không gửi lại
                                if record_id and record_id not in self._reminder_sent:
                                    overdue_notes.append(note)
                        except:
                            pass
            
            return overdue_notes
        except Exception as e:
            logger.error(f"Error getting overdue notes: {e}")
            return []
    
    def mark_reminder_sent(self, note_id: str):
        """Mark a note as having had its reminder sent"""
        self._reminder_sent[note_id] = True


# Global manager instance
_global_manager = None

def get_notes_manager(chat_id: str = None) -> NotesManager:
    """
    Get NotesManager instance (for main.py scheduler)
    
    Args:
        chat_id: Optional chat ID
        
    Returns:
        NotesManager instance
    """
    global _global_manager
    if _global_manager is None:
        _global_manager = NotesManager(chat_id)
    return _global_manager


# ============================================================================
# NOTE OPERATIONS
# ============================================================================

async def handle_add_note(chat_id: str, note_key: str, note_value: str, deadline: datetime = None) -> str:
    """Add a new note"""
    deadline_str = deadline.isoformat() if isinstance(deadline, datetime) else deadline
    record = await create_note(chat_id, note_key, note_value, deadline_str)
    
    if record and not record.get("error"):
        response = f"✅ Đã thêm ghi chú: **{note_key}**"
        if deadline:
            response += f"\n📅 Hạn nhắc: {deadline.strftime('%d/%m/%Y %H:%M')}"
        return response
    else:
        return "❌ Không thể thêm ghi chú. Vui lòng thử lại."


async def handle_view_notes(chat_id: str) -> str:
    """View all notes for a chat"""
    notes = await get_notes_by_chat_id(chat_id)
    
    if not notes:
        return "📝 Bạn chưa có ghi chú nào."
    
    lines = ["📝 **GHI CHÚ CỦA BẠN:**", ""]
    
    for i, note in enumerate(notes, 1):
        note_key = note.get("note_key", "Không có tiêu đề")
        note_value = note.get("note_value", "")
        deadline = note.get("deadline")
        
        deadline_str = ""
        if deadline:
            try:
                deadline_dt = datetime.fromtimestamp(deadline / 1000) if isinstance(deadline, (int, float)) else None
                if deadline_dt:
                    deadline_str = f" 📅 {deadline_dt.strftime('%d/%m/%Y %H:%M')}"
                    if deadline_dt < datetime.now():
                        deadline_str += " ⚠️ Quá hạn!"
            except:
                pass
        
        lines.append(f"**#{i}** {note_key}{deadline_str}")
        if note_value and note_value != note_key:
            lines.append(f"   {note_value[:100]}...")
        lines.append("")
    
    lines.append("💡 Dùng `Done #1` hoặc `Xong #1` để đánh dấu hoàn thành")
    lines.append("💡 Dùng `Xóa #1` để xóa ghi chú")
    
    return "\n".join(lines)


async def handle_delete_note(chat_id: str, note_index: str) -> str:
    """Delete a note by index"""
    notes = await get_notes_by_chat_id(chat_id)
    
    try:
        index = int(note_index) - 1
        if index < 0 or index >= len(notes):
            return f"❌ Không tìm thấy ghi chú #{note_index}"
        
        note = notes[index]
        record_id = note.get("record_id")
        note_key = note.get("note_key", "")
        
        result = await delete_note(record_id)
        if result and not result.get("error"):
            return f"✅ Đã xóa ghi chú: **{note_key}**"
        else:
            return "❌ Không thể xóa ghi chú. Vui lòng thử lại."
    
    except (ValueError, IndexError):
        return f"❌ Số ghi chú không hợp lệ: {note_index}"


async def handle_done_note(chat_id: str, identifier: str, identifier_type: str = "id") -> str:
    """Mark a note as done"""
    notes = await get_notes_by_chat_id(chat_id)
    
    if not notes:
        return "📝 Bạn chưa có ghi chú nào."
    
    if identifier_type == "id":
        try:
            index = int(identifier) - 1
            if index < 0 or index >= len(notes):
                return f"❌ Không tìm thấy ghi chú #{identifier}"
            note = notes[index]
        except (ValueError, IndexError):
            return f"❌ Số ghi chú không hợp lệ: {identifier}"
    else:
        note = None
        identifier_lower = identifier.lower()
        
        for n in notes:
            note_key = n.get("note_key", "").lower()
            note_value = n.get("note_value", "").lower()
            
            if identifier_lower in note_key or identifier_lower in note_value:
                note = n
                break
        
        if not note:
            return f"❌ Không tìm thấy ghi chú: **{identifier}**"
    
    record_id = note.get("record_id")
    note_key = note.get("note_key", "")
    
    result = await delete_note(record_id)
    if result and not result.get("error"):
        return f"✅ Đã hoàn thành: **{note_key}**\n🔔 Sẽ dừng nhắc nhở về ghi chú này."
    else:
        return "❌ Không thể đánh dấu hoàn thành. Vui lòng thử lại."


async def handle_update_note(chat_id: str, note_index: str, note_key: str, note_value: str, deadline: datetime = None) -> str:
    """Update an existing note"""
    notes = await get_notes_by_chat_id(chat_id)
    
    try:
        index = int(note_index) - 1
        if index < 0 or index >= len(notes):
            return f"❌ Không tìm thấy ghi chú #{note_index}"
        
        note = notes[index]
        record_id = note.get("record_id")
        
        deadline_str = deadline.isoformat() if isinstance(deadline, datetime) else deadline
        result = await update_note(record_id, note_value=note_value or note_key, deadline=deadline_str)
        if result and not result.get("error"):
            response = f"✅ Đã cập nhật ghi chú #{note_index}: **{note_key}**"
            if deadline:
                response += f"\n📅 Hạn mới: {deadline.strftime('%d/%m/%Y %H:%M')}"
            return response
        else:
            return "❌ Không thể cập nhật ghi chú. Vui lòng thử lại."
    
    except (ValueError, IndexError):
        return f"❌ Số ghi chú không hợp lệ: {note_index}"


# ============================================================================
# REMINDER FUNCTIONS (for scheduler)
# ============================================================================

async def get_due_reminders(hours: int = 24) -> List[Dict]:
    """Get notes that are due within the next N hours"""
    return await get_notes_due_soon(hours)


def format_reminder_message(note: Dict) -> str:
    """Format a reminder notification"""
    note_key = note.get("note_key", "Nhắc nhở")
    note_value = note.get("note_value", "")
    deadline = note.get("deadline")
    
    lines = ["🔔 **NHẮC NHỞ**", ""]
    lines.append(f"📌 **{note_key}**")
    
    if note_value and note_value != note_key:
        lines.append(f"📝 {note_value}")
    
    if deadline:
        try:
            deadline_dt = datetime.fromtimestamp(deadline / 1000) if isinstance(deadline, (int, float)) else None
            if deadline_dt:
                time_str = deadline_dt.strftime('%H:%M %d/%m/%Y')
                
                delta = deadline_dt - datetime.now()
                if delta.total_seconds() > 0:
                    hours = int(delta.total_seconds() // 3600)
                    minutes = int((delta.total_seconds() % 3600) // 60)
                    
                    if hours > 0:
                        lines.append(f"⏰ Còn {hours} giờ {minutes} phút")
                    else:
                        lines.append(f"⏰ Còn {minutes} phút")
                else:
                    lines.append("⚠️ Đã quá hạn!")
                
                lines.append(f"📅 Hạn: {time_str}")
        except:
            pass
    
    lines.append("")
    lines.append("💡 Dùng `Done [tiêu đề]` hoặc `Xong #ID` để hoàn thành")
    
    return "\n".join(lines)


# ============================================================================
# DEBUG FUNCTIONS
# ============================================================================

async def debug_notes():
    """Debug notes table"""
    try:
        all_notes = await get_all_notes()
        print(f"Total notes: {len(all_notes)}")
        for note in all_notes[:5]:
            print(f"  - {note}")
        return all_notes
    except Exception as e:
        print(f"Error: {e}")
        return []


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("Testing notes_manager.py v5.8.1...")
    print("Functions available:")
    print("  - check_note_command(text)")
    print("  - handle_note_command(params, chat_id, user_name)")
    print("  - get_notes_manager(chat_id)")
    print("  - NotesManager class")
    
    # Test check_note_command
    test_messages = [
        "Note: họp team lúc 3h",
        "Xem ghi chú",
        "Done #1",
        "Xóa #2",
    ]
    
    print("\n=== Testing check_note_command ===")
    for msg in test_messages:
        result = check_note_command(msg)
        print(f"'{msg}' -> {result}")
