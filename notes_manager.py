"""
Notes Manager Module
Quản lý ghi chú người dùng - lưu trữ trong Lark Bitable
Version 5.7.0
"""
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from lark_base import (
    get_notes_by_chat_id,
    get_note_by_key,
    create_note,
    update_note,
    delete_note,
    debug_notes_table
)


def parse_deadline(text: str) -> Optional[str]:
    """
    Parse deadline từ text người dùng
    Returns ISO format datetime string
    
    Examples:
    - "ngày mai" -> tomorrow
    - "thứ 6" -> next Friday
    - "20/12" -> Dec 20
    - "20/12/2024" -> Dec 20, 2024
    """
    now = datetime.now()
    text_lower = text.lower().strip()
    
    # Patterns
    if "hôm nay" in text_lower:
        return now.replace(hour=23, minute=59).isoformat()
    
    if "ngày mai" in text_lower:
        return (now + timedelta(days=1)).replace(hour=23, minute=59).isoformat()
    
    if "ngày kia" in text_lower or "ngày mốt" in text_lower:
        return (now + timedelta(days=2)).replace(hour=23, minute=59).isoformat()
    
    # Thứ trong tuần
    days_map = {
        "thứ 2": 0, "thứ hai": 0,
        "thứ 3": 1, "thứ ba": 1,
        "thứ 4": 2, "thứ tư": 2,
        "thứ 5": 3, "thứ năm": 3,
        "thứ 6": 4, "thứ sáu": 4,
        "thứ 7": 5, "thứ bảy": 5,
        "chủ nhật": 6, "cn": 6,
    }
    
    for day_name, target_weekday in days_map.items():
        if day_name in text_lower:
            current_weekday = now.weekday()
            days_ahead = target_weekday - current_weekday
            if days_ahead <= 0:  # Target day already passed this week
                days_ahead += 7
            target_date = now + timedelta(days=days_ahead)
            return target_date.replace(hour=23, minute=59).isoformat()
    
    # DD/MM format
    match = re.search(r'(\d{1,2})/(\d{1,2})(?:/(\d{4}))?', text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3)) if match.group(3) else now.year
        try:
            target_date = datetime(year, month, day, 23, 59)
            # Nếu ngày đã qua trong năm nay, chuyển sang năm sau
            if target_date < now and not match.group(3):
                target_date = datetime(year + 1, month, day, 23, 59)
            return target_date.isoformat()
        except ValueError:
            pass
    
    return None


def extract_note_key(text: str) -> str:
    """
    Tạo key ngắn gọn từ nội dung ghi chú
    Dùng để identify note sau này
    """
    # Remove common prefixes
    text = re.sub(r'^(nhớ|ghi nhớ|lưu|note|ghi chú|reminder)\s*(rằng|là|:)?\s*', '', text.lower())
    
    # Lấy 30 ký tự đầu hoặc 5 từ đầu
    words = text.split()[:5]
    key = ' '.join(words)
    
    if len(key) > 40:
        key = key[:40]
    
    return key.strip()


class NotesManager:
    """Manager class để xử lý các thao tác với Notes"""
    
    def __init__(self, chat_id: str):
        self.chat_id = chat_id
    
    async def list_notes(self) -> Tuple[str, List[Dict]]:
        """Liệt kê tất cả notes của chat"""
        notes = await get_notes_by_chat_id(self.chat_id)
        
        if not notes:
            return "📝 Bạn chưa có ghi chú nào.", []
        
        lines = ["📝 **Danh sách ghi chú của bạn:**\n"]
        
        for i, note in enumerate(notes, 1):
            key = note.get("note_key", "")
            value = note.get("note_value", "")
            deadline = note.get("deadline")
            
            line = f"{i}. **{key}**"
            if deadline:
                try:
                    dl = datetime.fromisoformat(deadline.replace('Z', '+00:00'))
                    line += f" (⏰ {dl.strftime('%d/%m')})"
                except:
                    pass
            
            line += f"\n   {value[:100]}{'...' if len(value) > 100 else ''}"
            lines.append(line)
        
        return "\n".join(lines), notes
    
    async def add_note(self, content: str, deadline_text: str = None) -> str:
        """Thêm ghi chú mới"""
        note_key = extract_note_key(content)
        
        # Check existing note with same key
        existing = await get_note_by_key(self.chat_id, note_key)
        if existing:
            # Update instead
            deadline = parse_deadline(deadline_text) if deadline_text else None
            await update_note(existing["record_id"], content, deadline)
            return f"✏️ Đã cập nhật ghi chú: **{note_key}**"
        
        # Create new
        deadline = parse_deadline(deadline_text) if deadline_text else None
        result = await create_note(self.chat_id, note_key, content, deadline)
        
        if "error" in result:
            return f"❌ Lỗi khi lưu ghi chú: {result.get('error')}"
        
        response = f"✅ Đã lưu ghi chú: **{note_key}**"
        if deadline:
            try:
                dl = datetime.fromisoformat(deadline)
                response += f"\n⏰ Deadline: {dl.strftime('%d/%m/%Y')}"
            except:
                pass
        
        return response
    
    async def find_note(self, query: str) -> str:
        """Tìm ghi chú theo keyword"""
        notes = await get_notes_by_chat_id(self.chat_id)
        
        if not notes:
            return "📝 Bạn chưa có ghi chú nào."
        
        query_lower = query.lower()
        matches = []
        
        for note in notes:
            key = note.get("note_key", "").lower()
            value = note.get("note_value", "").lower()
            
            if query_lower in key or query_lower in value:
                matches.append(note)
        
        if not matches:
            return f"🔍 Không tìm thấy ghi chú nào chứa '{query}'"
        
        lines = [f"🔍 **Tìm thấy {len(matches)} ghi chú:**\n"]
        
        for i, note in enumerate(matches, 1):
            key = note.get("note_key", "")
            value = note.get("note_value", "")
            lines.append(f"{i}. **{key}**\n   {value[:150]}{'...' if len(value) > 150 else ''}")
        
        return "\n".join(lines)
    
    async def delete_note_by_query(self, query: str) -> str:
        """Xóa ghi chú theo keyword"""
        notes = await get_notes_by_chat_id(self.chat_id)
        
        if not notes:
            return "📝 Bạn chưa có ghi chú nào."
        
        query_lower = query.lower()
        
        for note in notes:
            key = note.get("note_key", "").lower()
            value = note.get("note_value", "").lower()
            
            if query_lower in key or query_lower == key:
                await delete_note(note["record_id"])
                return f"🗑️ Đã xóa ghi chú: **{note.get('note_key')}**"
        
        return f"❌ Không tìm thấy ghi chú '{query}' để xóa"
    
    async def get_upcoming_deadlines(self, days: int = 7) -> str:
        """Lấy các ghi chú có deadline trong N ngày tới"""
        notes = await get_notes_by_chat_id(self.chat_id)
        
        if not notes:
            return "📝 Bạn chưa có ghi chú nào."
        
        now = datetime.now()
        cutoff = now + timedelta(days=days)
        
        upcoming = []
        for note in notes:
            deadline = note.get("deadline")
            if not deadline:
                continue
            try:
                dl = datetime.fromisoformat(deadline.replace('Z', '+00:00'))
                if now <= dl <= cutoff:
                    upcoming.append((dl, note))
            except:
                pass
        
        if not upcoming:
            return f"📅 Không có ghi chú nào có deadline trong {days} ngày tới."
        
        # Sort by deadline
        upcoming.sort(key=lambda x: x[0])
        
        lines = [f"📅 **Ghi chú có deadline trong {days} ngày tới:**\n"]
        
        for dl, note in upcoming:
            key = note.get("note_key", "")
            value = note.get("note_value", "")
            
            # Calculate days remaining
            days_left = (dl - now).days
            if days_left == 0:
                time_str = "⚠️ Hôm nay"
            elif days_left == 1:
                time_str = "⏰ Ngày mai"
            else:
                time_str = f"📆 {dl.strftime('%d/%m')} ({days_left} ngày)"
            
            lines.append(f"• {time_str}: **{key}**\n  {value[:80]}{'...' if len(value) > 80 else ''}")
        
        return "\n".join(lines)


async def handle_notes_intent(chat_id: str, intent: str, message: str) -> str:
    """
    Xử lý intent liên quan đến Notes
    
    Intents:
    - notes_list: Liệt kê ghi chú
    - notes_add: Thêm ghi chú
    - notes_find: Tìm ghi chú
    - notes_delete: Xóa ghi chú
    - notes_upcoming: Xem deadline sắp tới
    """
    manager = NotesManager(chat_id)
    
    if intent == "notes_list":
        result, _ = await manager.list_notes()
        return result
    
    elif intent == "notes_add":
        # Extract deadline nếu có
        deadline_text = None
        deadline_patterns = [
            r'trước\s+(.+?)(?:\.|$)',
            r'deadline\s*[:|]\s*(.+?)(?:\.|$)',
            r'hạn\s+(.+?)(?:\.|$)',
        ]
        
        for pattern in deadline_patterns:
            match = re.search(pattern, message.lower())
            if match:
                deadline_text = match.group(1)
                break
        
        # Clean message để lấy nội dung note
        content = re.sub(r'(nhớ|ghi nhớ|lưu|note|ghi chú)\s*(rằng|là|:)?\s*', '', message, flags=re.IGNORECASE)
        content = re.sub(r'trước\s+.+?(?:\.|$)', '', content)
        content = re.sub(r'deadline\s*[:|]\s*.+?(?:\.|$)', '', content)
        content = content.strip()
        
        if not content:
            return "❓ Bạn muốn ghi chú gì?"
        
        return await manager.add_note(content, deadline_text)
    
    elif intent == "notes_find":
        # Extract query
        query = re.sub(r'(tìm|search|kiếm|tìm kiếm)\s*(ghi chú|note)?\s*(về|có|chứa)?\s*', '', message.lower())
        query = query.strip()
        
        if not query:
            return "❓ Bạn muốn tìm ghi chú gì?"
        
        return await manager.find_note(query)
    
    elif intent == "notes_delete":
        # Extract query
        query = re.sub(r'(xóa|xoá|delete|remove)\s*(ghi chú|note)?\s*(về|có|tên|key)?\s*', '', message.lower())
        query = query.strip()
        
        if not query:
            return "❓ Bạn muốn xóa ghi chú nào?"
        
        return await manager.delete_note_by_query(query)
    
    elif intent == "notes_upcoming":
        # Extract days nếu có
        days = 7
        match = re.search(r'(\d+)\s*(ngày|tuần)', message.lower())
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            days = num * 7 if unit == "tuần" else num
        
        return await manager.get_upcoming_deadlines(days)
    
    else:
        # Default: list notes
        result, _ = await manager.list_notes()
        return result


async def debug_notes():
    """Debug function để test Notes table"""
    return await debug_notes_table()


# ============ COMPATIBILITY FUNCTIONS ============
# Các hàm này để tương thích với main.py cũ

# Global manager instance (for backward compatibility)
_managers: Dict[str, NotesManager] = {}


def get_notes_manager(chat_id: str = "default") -> NotesManager:
    """
    Lấy hoặc tạo NotesManager instance cho chat_id
    (Backward compatibility với code cũ)
    """
    if chat_id not in _managers:
        _managers[chat_id] = NotesManager(chat_id)
    return _managers[chat_id]


def check_note_command(text: str) -> Optional[Dict]:
    """
    Kiểm tra xem text có phải là lệnh note không
    Returns: Dict với action và params, hoặc None
    
    Commands:
    - "note: ..." hoặc "ghi nhớ: ..." -> add
    - "xem note" hoặc "tổng hợp note" -> summary  
    - "hoàn thành #1" hoặc "done #1" -> done
    - "xóa note #1" -> delete
    - "xóa tất cả note" -> clear_all
    - "deadline" hoặc "nhắc nhở" -> upcoming
    """
    text_lower = text.lower().strip()
    
    # Remove @Jarvis prefix
    text_clean = re.sub(r'^@?jarvis\s*', '', text, flags=re.IGNORECASE).strip()
    text_clean_lower = text_clean.lower()
    
    # 1. Add note
    add_patterns = [
        r'^note\s*[:\-]?\s*(.+)',
        r'^ghi\s*nhớ\s*[:\-]?\s*(.+)',
        r'^ghi\s*nho\s*[:\-]?\s*(.+)',
        r'^todo\s*[:\-]?\s*(.+)',
        r'^nhớ\s*[:\-]?\s*(.+)',
        r'^nhắc\s*[:\-]?\s*(.+)',
        r'^vấn\s*đề\s*[:\-]?\s*(.+)',
        r'^van\s*de\s*[:\-]?\s*(.+)',
    ]
    
    for pattern in add_patterns:
        match = re.match(pattern, text_clean_lower)
        if match:
            content = text_clean[match.start(1):].strip()
            return {"action": "add", "content": content}
    
    # 2. Summary/List notes
    summary_keywords = [
        "xem note", "xem ghi chú", "tổng hợp note", "tong hop note",
        "danh sách note", "list note", "các note", "cac note",
        "note của tôi", "note cua toi", "my notes"
    ]
    
    if any(kw in text_clean_lower for kw in summary_keywords):
        return {"action": "summary"}
    
    # 3. Mark done
    done_patterns = [
        r'(?:hoàn thành|hoan thanh|done|xong)\s*#?(\d+)',
        r'#(\d+)\s*(?:hoàn thành|hoan thanh|done|xong)',
    ]
    
    for pattern in done_patterns:
        match = re.search(pattern, text_clean_lower)
        if match:
            return {"action": "done", "note_id": int(match.group(1))}
    
    # 4. Delete note
    delete_patterns = [
        r'(?:xóa|xoa|delete|remove)\s*(?:note)?\s*#?(\d+)',
        r'#(\d+)\s*(?:xóa|xoa|delete)',
    ]
    
    for pattern in delete_patterns:
        match = re.search(pattern, text_clean_lower)
        if match:
            return {"action": "delete", "note_id": int(match.group(1))}
    
    # 5. Clear all
    if any(kw in text_clean_lower for kw in ["xóa tất cả note", "xoa tat ca note", "clear all note", "xóa hết note"]):
        return {"action": "clear_all"}
    
    # 6. Upcoming deadlines
    if any(kw in text_clean_lower for kw in ["deadline", "nhắc nhở", "nhac nho", "sắp tới", "sap toi", "reminder"]):
        return {"action": "upcoming"}
    
    return None


async def handle_note_command(params: Dict, chat_id: str = "default", user_name: str = "") -> str:
    """
    Xử lý lệnh note và trả về response
    (Backward compatibility với main.py cũ)
    """
    action = params.get("action")
    manager = NotesManager(chat_id)
    
    if action == "summary":
        result, _ = await manager.list_notes()
        return result
    
    elif action == "add":
        content = params.get("content", "")
        if not content:
            return "❌ Nội dung ghi chú không được trống"
        
        # Extract deadline nếu có
        deadline_text = None
        deadline_patterns = [
            r'trước\s+(.+?)(?:\.|$)',
            r'deadline\s*[:\-]?\s*(.+?)(?:\.|$)',
            r'hạn\s+(.+?)(?:\.|$)',
        ]
        
        for pattern in deadline_patterns:
            match = re.search(pattern, content.lower())
            if match:
                deadline_text = match.group(1)
                break
        
        return await manager.add_note(content, deadline_text)
    
    elif action == "done":
        note_id = params.get("note_id")
        # Tìm note theo ID (trong key hoặc value)
        return await manager.find_note(f"#{note_id}")
    
    elif action == "delete":
        note_id = params.get("note_id")
        return await manager.delete_note_by_query(f"#{note_id}")
    
    elif action == "clear_all":
        notes = await get_notes_by_chat_id(chat_id)
        count = 0
        for note in notes:
            await delete_note(note["record_id"])
            count += 1
        return f"🗑️ Đã xóa tất cả {count} ghi chú"
    
    elif action == "upcoming":
        return await manager.get_upcoming_deadlines(7)
    
    return "❌ Lệnh không hợp lệ"
