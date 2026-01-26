# notes_manager.py - Version 5.8.0
# Fixed: "Done" command to mark reminders as complete
# Added: Multiple patterns for done/complete commands

import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

from lark_base import (
    get_notes_by_chat,
    create_note,
    update_note,
    delete_note,
    get_notes_due_soon
)

logger = logging.getLogger(__name__)

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

# === NEW in v5.8.0: Done/Complete patterns ===
DONE_PATTERNS = [
    # "Done #1" or "Done 1"
    r'(?:done|xong|hoàn thành|hoan thanh|complete|completed)\s*#?(\d+)',
    # "#1 done" or "#1 xong"
    r'#(\d+)\s*(?:done|xong|hoàn thành|hoan thanh|complete|completed)',
    # "Done [title]" - match by title
    r'(?:done|xong|hoàn thành|hoan thanh|complete|completed)\s+(.+)',
    # "Đã xong [title]"
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
    """
    Parse datetime from Vietnamese/English text
    
    Supports:
    - "hôm nay", "ngày mai", "tuần sau"
    - "15:30", "15h30", "3pm"
    - "25/12", "25/12/2024"
    - "25/12 15:30"
    """
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
    """
    Extract deadline from note text
    
    Returns: (cleaned_text, deadline)
    
    Examples:
    - "Họp team deadline 15/12" -> ("Họp team", datetime(2024, 12, 15, 9, 0))
    - "Gọi khách hạn 10h30 ngày mai" -> ("Gọi khách", tomorrow at 10:30)
    """
    deadline = None
    cleaned_text = text
    
    # Check for deadline keywords
    for pattern in DEADLINE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            deadline_str = match.group(1) if match.groups() else match.group(0)
            deadline = parse_datetime(deadline_str)
            
            # Remove deadline part from text
            cleaned_text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
            cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
            break
    
    return cleaned_text, deadline

# ============================================================================
# NOTE COMMANDS
# ============================================================================

def check_note_command(message: str, chat_id: str) -> Optional[Dict[str, Any]]:
    """
    Check if message is a note command and return action
    
    Returns:
    {
        "action": "add|view|delete|update|done",
        "note_key": str,
        "note_value": str,
        "deadline": datetime,
        "note_id": str,
        ...
    }
    """
    message = message.strip()
    
    # === Check DONE patterns first (NEW in v5.8.0) ===
    for pattern in DONE_PATTERNS:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            identifier = match.group(1).strip()
            
            # Check if it's a number (ID) or text (title)
            if identifier.isdigit():
                return {
                    "action": "done",
                    "identifier_type": "id",
                    "note_id": identifier,
                    "chat_id": chat_id
                }
            else:
                return {
                    "action": "done",
                    "identifier_type": "title",
                    "note_title": identifier,
                    "chat_id": chat_id
                }
    
    # Check ADD patterns
    for pattern in ADD_PATTERNS:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            note_content = match.group(1).strip()
            note_key, deadline = extract_deadline_from_text(note_content)
            
            return {
                "action": "add",
                "note_key": note_key[:100],  # Limit title length
                "note_value": note_content,
                "deadline": deadline,
                "chat_id": chat_id
            }
    
    # Check VIEW patterns
    for pattern in VIEW_PATTERNS:
        if re.search(pattern, message, re.IGNORECASE):
            return {
                "action": "view",
                "chat_id": chat_id
            }
    
    # Check DELETE patterns
    for pattern in DELETE_PATTERNS:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            note_id = match.group(1)
            return {
                "action": "delete",
                "note_id": note_id,
                "chat_id": chat_id
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
                "deadline": deadline,
                "chat_id": chat_id
            }
    
    return None


# ============================================================================
# NOTE OPERATIONS
# ============================================================================

def handle_add_note(chat_id: str, note_key: str, note_value: str, deadline: datetime = None) -> str:
    """Add a new note"""
    record_id = create_note(chat_id, note_key, note_value, deadline)
    
    if record_id:
        response = f"✅ Đã thêm ghi chú: **{note_key}**"
        if deadline:
            response += f"\n📅 Hạn nhắc: {deadline.strftime('%d/%m/%Y %H:%M')}"
        return response
    else:
        return "❌ Không thể thêm ghi chú. Vui lòng thử lại."


def handle_view_notes(chat_id: str) -> str:
    """View all notes for a chat"""
    notes = get_notes_by_chat(chat_id)
    
    if not notes:
        return "📝 Bạn chưa có ghi chú nào."
    
    lines = ["📝 **GHI CHÚ CỦA BẠN:**", ""]
    
    for i, note in enumerate(notes, 1):
        fields = note.get("fields", {})
        record_id = note.get("record_id", "")
        
        note_key = fields.get("note_key", "Không có tiêu đề")
        note_value = fields.get("note_value", "")
        deadline = fields.get("deadline")
        created_at = fields.get("created_at")
        
        # Format deadline
        deadline_str = ""
        if deadline:
            # Convert from milliseconds
            deadline_dt = datetime.fromtimestamp(deadline / 1000)
            deadline_str = f" 📅 {deadline_dt.strftime('%d/%m/%Y %H:%M')}"
            
            # Check if overdue
            if deadline_dt < datetime.now():
                deadline_str += " ⚠️ Quá hạn!"
        
        lines.append(f"**#{i}** {note_key}{deadline_str}")
        if note_value and note_value != note_key:
            lines.append(f"   {note_value[:100]}...")
        lines.append("")
    
    lines.append("💡 Dùng `Done #1` hoặc `Xong #1` để đánh dấu hoàn thành")
    lines.append("💡 Dùng `Xóa #1` để xóa ghi chú")
    
    return "\n".join(lines)


def handle_delete_note(chat_id: str, note_index: str) -> str:
    """Delete a note by index (1-based)"""
    notes = get_notes_by_chat(chat_id)
    
    try:
        index = int(note_index) - 1
        if index < 0 or index >= len(notes):
            return f"❌ Không tìm thấy ghi chú #{note_index}"
        
        note = notes[index]
        record_id = note.get("record_id")
        note_key = note.get("fields", {}).get("note_key", "")
        
        if delete_note(record_id):
            return f"✅ Đã xóa ghi chú: **{note_key}**"
        else:
            return "❌ Không thể xóa ghi chú. Vui lòng thử lại."
    
    except (ValueError, IndexError):
        return f"❌ Số ghi chú không hợp lệ: {note_index}"


def handle_done_note(chat_id: str, identifier: str, identifier_type: str = "id") -> str:
    """
    Mark a note as done (delete it to stop reminders)
    
    NEW in v5.8.0: Fixed "Done" command
    
    identifier_type:
    - "id": Match by note index (1-based)
    - "title": Match by note title (partial match)
    """
    notes = get_notes_by_chat(chat_id)
    
    if not notes:
        return "📝 Bạn chưa có ghi chú nào."
    
    if identifier_type == "id":
        # Match by index
        try:
            index = int(identifier) - 1
            if index < 0 or index >= len(notes):
                return f"❌ Không tìm thấy ghi chú #{identifier}"
            
            note = notes[index]
        except (ValueError, IndexError):
            return f"❌ Số ghi chú không hợp lệ: {identifier}"
    
    else:
        # Match by title (partial match)
        note = None
        identifier_lower = identifier.lower()
        
        for n in notes:
            note_key = n.get("fields", {}).get("note_key", "").lower()
            note_value = n.get("fields", {}).get("note_value", "").lower()
            
            if identifier_lower in note_key or identifier_lower in note_value:
                note = n
                break
        
        if not note:
            return f"❌ Không tìm thấy ghi chú: **{identifier}**"
    
    # Delete the note to mark as done
    record_id = note.get("record_id")
    note_key = note.get("fields", {}).get("note_key", "")
    
    if delete_note(record_id):
        return f"✅ Đã hoàn thành: **{note_key}**\n🔔 Sẽ dừng nhắc nhở về ghi chú này."
    else:
        return "❌ Không thể đánh dấu hoàn thành. Vui lòng thử lại."


def handle_update_note(chat_id: str, note_index: str, note_key: str, note_value: str, deadline: datetime = None) -> str:
    """Update an existing note"""
    notes = get_notes_by_chat(chat_id)
    
    try:
        index = int(note_index) - 1
        if index < 0 or index >= len(notes):
            return f"❌ Không tìm thấy ghi chú #{note_index}"
        
        note = notes[index]
        record_id = note.get("record_id")
        
        fields = {
            "note_key": note_key,
            "note_value": note_value
        }
        if deadline:
            fields["deadline"] = deadline
        
        if update_note(record_id, fields):
            response = f"✅ Đã cập nhật ghi chú #{note_index}: **{note_key}**"
            if deadline:
                response += f"\n📅 Hạn mới: {deadline.strftime('%d/%m/%Y %H:%M')}"
            return response
        else:
            return "❌ Không thể cập nhật ghi chú. Vui lòng thử lại."
    
    except (ValueError, IndexError):
        return f"❌ Số ghi chú không hợp lệ: {note_index}"


# ============================================================================
# REMINDER FUNCTIONS
# ============================================================================

def get_due_reminders(hours: int = 24) -> List[Dict]:
    """Get notes that are due within the next N hours"""
    return get_notes_due_soon(hours)


def format_reminder_message(note: Dict) -> str:
    """Format a reminder notification"""
    fields = note.get("fields", {})
    note_key = fields.get("note_key", "Nhắc nhở")
    note_value = fields.get("note_value", "")
    deadline = fields.get("deadline")
    
    lines = ["🔔 **NHẮC NHỞ**", ""]
    lines.append(f"📌 **{note_key}**")
    
    if note_value and note_value != note_key:
        lines.append(f"📝 {note_value}")
    
    if deadline:
        deadline_dt = datetime.fromtimestamp(deadline / 1000)
        time_str = deadline_dt.strftime('%H:%M %d/%m/%Y')
        
        # Time until deadline
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
    
    lines.append("")
    lines.append("💡 Dùng `Done [tiêu đề]` hoặc `Xong #ID` để hoàn thành")
    
    return "\n".join(lines)


# ============================================================================
# MAIN COMMAND HANDLER
# ============================================================================

def process_note_command(command_info: Dict[str, Any]) -> str:
    """
    Process a parsed note command
    
    command_info from check_note_command()
    """
    action = command_info.get("action")
    chat_id = command_info.get("chat_id")
    
    if action == "add":
        return handle_add_note(
            chat_id,
            command_info.get("note_key", ""),
            command_info.get("note_value", ""),
            command_info.get("deadline")
        )
    
    elif action == "view":
        return handle_view_notes(chat_id)
    
    elif action == "delete":
        return handle_delete_note(chat_id, command_info.get("note_id", "0"))
    
    elif action == "done":
        return handle_done_note(
            chat_id,
            command_info.get("note_id") or command_info.get("note_title", ""),
            command_info.get("identifier_type", "id")
        )
    
    elif action == "update":
        return handle_update_note(
            chat_id,
            command_info.get("note_id", "0"),
            command_info.get("note_key", ""),
            command_info.get("note_value", ""),
            command_info.get("deadline")
        )
    
    return "❌ Lệnh không hợp lệ"


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("Testing notes_manager.py v5.8.0...")
    
    # Test done patterns
    test_messages = [
        "Done #1",
        "done 2",
        "Xong #3",
        "hoàn thành 4",
        "Done Họp team",
        "Xong gọi khách",
        "đã xong báo cáo tuần",
        "#1 done",
        "#2 xong",
    ]
    
    print("\n=== Testing DONE patterns ===")
    for msg in test_messages:
        result = check_note_command(msg, "test_chat_123")
        if result and result.get("action") == "done":
            print(f"✅ '{msg}' -> {result}")
        else:
            print(f"❌ '{msg}' -> Not matched as DONE")
    
    # Test add patterns
    add_messages = [
        "Ghi chú: Họp team deadline 15/12",
        "Nhắc tôi gọi khách hạn 10h30 ngày mai",
        "Note: Review code",
    ]
    
    print("\n=== Testing ADD patterns ===")
    for msg in add_messages:
        result = check_note_command(msg, "test_chat_123")
        if result and result.get("action") == "add":
            print(f"✅ '{msg}' -> {result}")
        else:
            print(f"❌ '{msg}' -> Not matched as ADD")
