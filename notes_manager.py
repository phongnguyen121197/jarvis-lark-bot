"""
Notes Manager Module
Quản lý ghi chú người dùng - lưu trữ trong Lark Bitable
Version 5.7.14 - Fixed Done command to delete notes by title or record_id
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
    debug_notes_table,
    create_calendar_event,
    get_all_notes
)
from dataclasses import dataclass


@dataclass
class Note:
    """Note object for scheduler compatibility"""
    id: str
    content: str
    chat_id: str
    deadline: datetime
    reminder_sent: bool = False


# In-memory tracking of sent reminders (resets on restart)
_reminder_sent_ids: set = set()


class SchedulerNotesManager:
    """
    Manager class cho scheduler - fetch ALL notes across all chats
    Returns Note objects compatible with main.py scheduler
    """
    
    async def get_notes_due_soon(self, days: int = 1) -> List[Note]:
        """Lấy notes có deadline trong N ngày tới (across all chats)"""
        all_notes = await get_all_notes()
        
        if not all_notes:
            return []
        
        now = datetime.now()
        cutoff = now + timedelta(days=days)
        result = []
        
        for note in all_notes:
            deadline = note.get("deadline")
            if not deadline:
                continue
            try:
                # Handle both timestamp (ms) and ISO format
                if isinstance(deadline, (int, float)):
                    dl = datetime.fromtimestamp(deadline / 1000)
                else:
                    dl = datetime.fromisoformat(str(deadline).replace('Z', '+00:00'))
                
                # Check if within range and not overdue
                if now <= dl <= cutoff:
                    record_id = note.get("record_id", "")
                    result.append(Note(
                        id=record_id,
                        content=note.get("note_value", note.get("note_key", "")),
                        chat_id=note.get("chat_id", ""),
                        deadline=dl,
                        reminder_sent=record_id in _reminder_sent_ids
                    ))
            except Exception as e:
                print(f"⚠️ Error parsing deadline: {e}")
                continue
        
        return result
    
    async def get_overdue_notes(self) -> List[Note]:
        """Lấy notes đã quá hạn (across all chats)"""
        all_notes = await get_all_notes()
        
        if not all_notes:
            return []
        
        now = datetime.now()
        result = []
        
        for note in all_notes:
            deadline = note.get("deadline")
            if not deadline:
                continue
            try:
                # Handle both timestamp (ms) and ISO format
                if isinstance(deadline, (int, float)):
                    dl = datetime.fromtimestamp(deadline / 1000)
                else:
                    dl = datetime.fromisoformat(str(deadline).replace('Z', '+00:00'))
                
                # Check if overdue
                if dl < now:
                    record_id = note.get("record_id", "")
                    result.append(Note(
                        id=record_id,
                        content=note.get("note_value", note.get("note_key", "")),
                        chat_id=note.get("chat_id", ""),
                        deadline=dl,
                        reminder_sent=record_id in _reminder_sent_ids
                    ))
            except Exception as e:
                print(f"⚠️ Error parsing deadline: {e}")
                continue
        
        return result
    
    def mark_reminder_sent(self, note_id: str):
        """Đánh dấu đã gửi reminder cho note (in-memory)"""
        _reminder_sent_ids.add(note_id)
        print(f"✅ Marked reminder sent for note: {note_id}")


# Global scheduler manager instance
_scheduler_manager: SchedulerNotesManager = None


def parse_deadline(text: str) -> Optional[str]:
    """
    Parse deadline từ text người dùng
    Returns ISO format datetime string
    """
    now = datetime.now()
    text_lower = text.lower().strip()
    
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
            if days_ahead <= 0:
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
            if target_date < now and not match.group(3):
                target_date = datetime(year + 1, month, day, 23, 59)
            return target_date.isoformat()
        except ValueError:
            pass
    
    return None


def extract_note_key(text: str) -> str:
    """Tạo key ngắn gọn từ nội dung ghi chú"""
    text = re.sub(r'^(nhớ|ghi nhớ|lưu|note|ghi chú|reminder)\s*(rằng|là|:)?\s*', '', text.lower())
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
            record_id = note.get("record_id", "")
            
            # Show record_id để user có thể dùng Done #record_id
            line = f"{i}. **{key}**"
            if deadline:
                try:
                    if isinstance(deadline, (int, float)):
                        dl = datetime.fromtimestamp(deadline / 1000)
                    else:
                        dl = datetime.fromisoformat(deadline.replace('Z', '+00:00'))
                    line += f" (⏰ {dl.strftime('%d/%m')})"
                except:
                    pass
            
            line += f"\n   {value[:100]}{'...' if len(value) > 100 else ''}"
            line += f"\n   🆔 `{record_id[:15]}...`"
            lines.append(line)
        
        lines.append("\n💡 Hoàn thành: `Done \"tiêu đề\"` hoặc `Finished # record_id`")
        return "\n".join(lines), notes
    
    async def add_note(self, content: str, deadline_text: str = None) -> str:
        """Thêm ghi chú mới"""
        note_key = extract_note_key(content)
        
        existing = await get_note_by_key(self.chat_id, note_key)
        if existing:
            deadline = parse_deadline(deadline_text) if deadline_text else None
            await update_note(existing["record_id"], content, deadline)
            return f"✏️ Đã cập nhật ghi chú: **{note_key}**"
        
        deadline = parse_deadline(deadline_text) if deadline_text else None
        result = await create_note(self.chat_id, note_key, content, deadline)
        
        if "error" in result:
            return f"❌ Lỗi khi lưu ghi chú: {result.get('error')}"
        
        response = f"✅ Đã lưu ghi chú: **{note_key}**"
        
        if deadline:
            try:
                dl = datetime.fromisoformat(deadline)
                response += f"\n⏰ Deadline: {dl.strftime('%d/%m/%Y')}"
            except Exception as e:
                print(f"⚠️ Deadline parse error: {e}")
        
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
    
    async def mark_done_by_title(self, title: str) -> str:
        """
        Đánh dấu hoàn thành (XÓA) note theo tiêu đề
        NEW in v5.7.14: Support Done "tiêu đề"
        """
        notes = await get_notes_by_chat_id(self.chat_id)
        
        if not notes:
            return "📝 Bạn chưa có ghi chú nào."
        
        title_lower = title.lower().strip()
        
        # Tìm note match với title
        for note in notes:
            key = note.get("note_key", "").lower()
            value = note.get("note_value", "").lower()
            
            # Match nếu title chứa trong key hoặc value
            if title_lower in key or title_lower in value or key in title_lower:
                record_id = note.get("record_id")
                note_key = note.get("note_key", "")
                
                # XÓA note để dừng nhắc
                result = await delete_note(record_id)
                
                if result.get("deleted") or result.get("record_id"):
                    # Clear from reminder sent tracking
                    if record_id in _reminder_sent_ids:
                        _reminder_sent_ids.discard(record_id)
                    
                    return f"✅ Đã hoàn thành và xóa ghi chú:\n📝 **{note_key}**\n\n🎉 Tốt lắm! Tiếp tục phát huy nhé!"
                else:
                    return f"❌ Lỗi khi xóa ghi chú: {result.get('error', 'Unknown')}"
        
        return f"❌ Không tìm thấy ghi chú với tiêu đề: \"{title}\"\n💡 Hãy kiểm tra lại tiêu đề hoặc dùng `xem note` để xem danh sách"
    
    async def mark_done_by_record_id(self, record_id: str) -> str:
        """
        Đánh dấu hoàn thành (XÓA) note theo record_id
        NEW in v5.7.14: Support Done # record_id
        """
        notes = await get_notes_by_chat_id(self.chat_id)
        
        if not notes:
            return "📝 Bạn chưa có ghi chú nào."
        
        # Tìm note với record_id
        for note in notes:
            note_record_id = note.get("record_id", "")
            
            # Match nếu record_id khớp (có thể partial match)
            if record_id in note_record_id or note_record_id.startswith(record_id):
                note_key = note.get("note_key", "")
                
                # XÓA note để dừng nhắc
                result = await delete_note(note_record_id)
                
                if result.get("deleted") or result.get("record_id"):
                    # Clear from reminder sent tracking
                    if note_record_id in _reminder_sent_ids:
                        _reminder_sent_ids.discard(note_record_id)
                    
                    return f"✅ Đã hoàn thành và xóa ghi chú:\n📝 **{note_key}**\n🆔 {note_record_id}\n\n🎉 Tốt lắm!"
                else:
                    return f"❌ Lỗi khi xóa ghi chú: {result.get('error', 'Unknown')}"
        
        return f"❌ Không tìm thấy ghi chú với ID: {record_id}\n💡 Dùng `xem note` để xem danh sách và ID"
    
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
                if isinstance(deadline, (int, float)):
                    dl = datetime.fromtimestamp(deadline / 1000)
                else:
                    dl = datetime.fromisoformat(deadline.replace('Z', '+00:00'))
                if now <= dl <= cutoff:
                    upcoming.append((dl, note))
            except:
                pass
        
        if not upcoming:
            return f"📅 Không có ghi chú nào có deadline trong {days} ngày tới."
        
        upcoming.sort(key=lambda x: x[0])
        
        lines = [f"📅 **Ghi chú có deadline trong {days} ngày tới:**\n"]
        
        for dl, note in upcoming:
            key = note.get("note_key", "")
            value = note.get("note_value", "")
            
            days_left = (dl - now).days
            if days_left == 0:
                time_str = "⚠️ Hôm nay"
            elif days_left == 1:
                time_str = "⏰ Ngày mai"
            else:
                time_str = f"📆 {dl.strftime('%d/%m')} ({days_left} ngày)"
            
            lines.append(f"• {time_str}: **{key}**\n  {value[:80]}{'...' if len(value) > 80 else ''}")
        
        return "\n".join(lines)

    async def get_notes_due_soon(self, days: int = 1) -> list:
        """Lấy danh sách notes có deadline sắp đến"""
        notes = await get_notes_by_chat_id(self.chat_id)
        
        if not notes:
            return []
        
        now = datetime.now()
        cutoff = now + timedelta(days=days)
        
        upcoming = []
        for note in notes:
            deadline = note.get("deadline")
            if not deadline:
                continue
            try:
                if isinstance(deadline, (int, float)):
                    dl = datetime.fromtimestamp(deadline / 1000)
                else:
                    dl = datetime.fromisoformat(deadline.replace('Z', '+00:00'))
                if now <= dl <= cutoff:
                    upcoming.append({
                        "note_key": note.get("note_key", ""),
                        "note_value": note.get("note_value", ""),
                        "deadline": dl,
                        "record_id": note.get("record_id", "")
                    })
            except:
                pass
        
        return upcoming


async def handle_notes_intent(chat_id: str, intent: str, message: str) -> str:
    """Xử lý intent liên quan đến Notes"""
    manager = NotesManager(chat_id)
    
    if intent == "notes_list":
        result, _ = await manager.list_notes()
        return result
    
    elif intent == "notes_add":
        deadline_text = None
        deadline_patterns = [
            r'deadline\s*[:\-]?\s*(.+)$',
            r'trước\s+(.+)$',
            r'hạn\s+(.+)$',
        ]
        
        for pattern in deadline_patterns:
            match = re.search(pattern, message.lower())
            if match:
                deadline_text = match.group(1).strip()
                break
        
        content = re.sub(r'(nhớ|ghi nhớ|lưu|note|ghi chú)\s*(rằng|là|:)?\s*', '', message, flags=re.IGNORECASE)
        content = re.sub(r'deadline\s*[:\-]?\s*.+$', '', content, flags=re.IGNORECASE)
        content = re.sub(r'trước\s+.+$', '', content, flags=re.IGNORECASE)
        content = re.sub(r'hạn\s+.+$', '', content, flags=re.IGNORECASE)
        content = content.strip()
        
        if not content:
            return "❓ Bạn muốn ghi chú gì?"
        
        return await manager.add_note(content, deadline_text)
    
    elif intent == "notes_find":
        query = re.sub(r'(tìm|search|kiếm|tìm kiếm)\s*(ghi chú|note)?\s*(về|có|chứa)?\s*', '', message.lower())
        query = query.strip()
        
        if not query:
            return "❓ Bạn muốn tìm ghi chú gì?"
        
        return await manager.find_note(query)
    
    elif intent == "notes_delete":
        query = re.sub(r'(xóa|xoá|delete|remove)\s*(ghi chú|note)?\s*(về|có|tên|key)?\s*', '', message.lower())
        query = query.strip()
        
        if not query:
            return "❓ Bạn muốn xóa ghi chú nào?"
        
        return await manager.delete_note_by_query(query)
    
    elif intent == "notes_upcoming":
        days = 7
        match = re.search(r'(\d+)\s*(ngày|tuần)', message.lower())
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            days = num * 7 if unit == "tuần" else num
        
        return await manager.get_upcoming_deadlines(days)
    
    else:
        result, _ = await manager.list_notes()
        return result


async def debug_notes():
    """Debug function để test Notes table"""
    return await debug_notes_table()


# ============ COMPATIBILITY FUNCTIONS ============

_managers: Dict[str, NotesManager] = {}


def get_notes_manager(chat_id: str = "default"):
    """Lấy hoặc tạo NotesManager instance cho chat_id"""
    global _scheduler_manager
    
    if chat_id == "default":
        if _scheduler_manager is None:
            _scheduler_manager = SchedulerNotesManager()
        return _scheduler_manager
    
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
    - "done tiêu đề" hoặc "finished # record_id" -> done (NEW v5.7.14)
    - "xóa note #1" -> delete
    - "xóa tất cả note" -> clear_all
    - "deadline" hoặc "nhắc nhở" -> upcoming
    """
    text_lower = text.lower().strip()
    
    # Remove @Jarvis prefix
    text_clean = re.sub(r'^@?jarvis\s*', '', text, flags=re.IGNORECASE).strip()
    text_clean_lower = text_clean.lower()
    
    # ===== NEW v5.7.14: DONE BY TITLE =====
    # Pattern: Done "tiêu đề" hoặc Done tiêu đề
    # Examples:
    # - Done "sửa lại quy trình booking"
    # - Finished "Change 20ml packaging"
    # - Xong "modify the booking process"
    # - Hoàn thành "Check the process of pushing order notes"
    done_title_patterns = [
        r'^(?:done|finished|xong|hoàn thành|hoan thanh)\s*["\'](.+?)["\']',  # Done "title"
        r'^(?:done|finished|xong|hoàn thành|hoan thanh)\s+(.+)$',  # Done title (no quotes)
    ]
    
    for pattern in done_title_patterns:
        match = re.match(pattern, text_clean_lower, re.IGNORECASE)
        if match:
            title = match.group(1).strip()
            # Exclude if title looks like a record_id pattern
            if title.startswith('#') or title.startswith('rec'):
                continue
            if title:
                print(f"📝 Detected Done by title: {title}")
                return {"action": "done_title", "title": title}
    
    # ===== NEW v5.7.14: DONE BY RECORD_ID =====
    # Pattern: Done # record_id hoặc Finished # recv6cxNjZL4dF
    done_record_patterns = [
        r'^(?:done|finished|xong|hoàn thành|hoan thanh)\s*#\s*(\w+)',  # Done # record_id
        r'^(?:done|finished|xong|hoàn thành|hoan thanh)\s+(rec\w+)',  # Done recXXX
    ]
    
    for pattern in done_record_patterns:
        match = re.match(pattern, text_clean_lower, re.IGNORECASE)
        if match:
            record_id = match.group(1).strip()
            if record_id:
                print(f"📝 Detected Done by record_id: {record_id}")
                return {"action": "done_record", "record_id": record_id}
    
    # 1. Add note
    add_patterns = [
        r'^note\s*[:\-]?\s*(.+)$',
        r'^ghi\s*nhớ\s*[:\-]?\s*(.+)$',
        r'^ghi\s*nho\s*[:\-]?\s*(.+)$',
        r'^todo\s*[:\-]?\s*(.+)$',
        r'^nhớ\s*[:\-]?\s*(.+)$',
        r'^nhắc\s*[:\-]?\s*(.+)$',
        r'^vấn\s*đề\s*[:\-]?\s*(.+)$',
        r'^van\s*de\s*[:\-]?\s*(.+)$',
    ]
    
    for pattern in add_patterns:
        match = re.match(pattern, text_clean_lower, re.DOTALL)
        if match:
            start_pos = match.start(1)
            content = text_clean[start_pos:].strip()
            return {"action": "add", "content": content}
    
    # 2. Summary/List notes
    summary_keywords = [
        "xem note", "xem ghi chú", "tổng hợp note", "tong hop note",
        "danh sách note", "list note", "các note", "cac note",
        "note của tôi", "note cua toi", "my notes"
    ]
    
    if any(kw in text_clean_lower for kw in summary_keywords):
        return {"action": "summary"}
    
    # 3. OLD Done patterns (by number - deprecated but keep for compatibility)
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
    Updated v5.7.14: Added done_title and done_record actions
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
        
        deadline_text = None
        deadline_patterns = [
            r'deadline\s*[:\-]?\s*(.+)$',
            r'trước\s+(.+)$',
            r'hạn\s+(.+)$',
        ]
        
        for pattern in deadline_patterns:
            match = re.search(pattern, content.lower())
            if match:
                deadline_text = match.group(1).strip()
                content = re.sub(pattern, '', content, flags=re.IGNORECASE).strip()
                break
        
        return await manager.add_note(content, deadline_text)
    
    # ===== NEW v5.7.14: Done by title =====
    elif action == "done_title":
        title = params.get("title", "")
        if not title:
            return "❌ Vui lòng nhập tiêu đề ghi chú cần hoàn thành"
        return await manager.mark_done_by_title(title)
    
    # ===== NEW v5.7.14: Done by record_id =====
    elif action == "done_record":
        record_id = params.get("record_id", "")
        if not record_id:
            return "❌ Vui lòng nhập ID ghi chú cần hoàn thành"
        return await manager.mark_done_by_record_id(record_id)
    
    # OLD done by number (deprecated)
    elif action == "done":
        note_id = params.get("note_id")
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
