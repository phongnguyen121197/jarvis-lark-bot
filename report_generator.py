# report_generator.py - Version 5.8.1
# Fixed: Added ALL missing functions required by main.py
# - generate_content_calendar_text
# - generate_task_summary_text
# - generate_general_summary_text
# - chat_with_gpt

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# OpenAI config
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ============================================================================
# FORMATTING UTILITIES
# ============================================================================

def format_number_vn(num: float, suffix: str = "") -> str:
    """Format number with Vietnamese locale"""
    if num is None:
        return "0" + suffix
    
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B{suffix}"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M{suffix}"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K{suffix}"
    else:
        return f"{num:,.0f}{suffix}".replace(",", ".")


def format_currency_vn(amount: float) -> str:
    """Format currency in Vietnamese dong"""
    return format_number_vn(amount, "")


def generate_progress_bar(percent: float, length: int = 10) -> str:
    """Generate text-based progress bar"""
    percent = min(100, max(0, percent))
    filled = int(percent / 100 * length)
    empty = length - filled
    return f"[{'▓' * filled}{'░' * empty}]"


def format_content_breakdown(content_data: Dict[str, int]) -> str:
    """Format content breakdown from aggregated data"""
    if not content_data:
        return ""
    
    items = []
    for key, count in content_data.items():
        if key not in ("total", "total_cart", "total_text"):
            items.append(f"{count} {key}")
    
    if not items:
        return ""
    
    if len(items) == 1:
        return items[0]
    elif len(items) == 2:
        return f"{items[0]} và {items[1]}"
    else:
        return ", ".join(items[:-1]) + f" và {items[-1]}"


# ============================================================================
# CHAT WITH GPT - REQUIRED BY main.py
# ============================================================================

async def chat_with_gpt(question: str) -> str:
    """
    Chat with OpenAI GPT
    Required by main.py for INTENT_GPT_CHAT
    """
    if not OPENAI_API_KEY:
        return "❌ OpenAI API key chưa được cấu hình. Vui lòng thêm OPENAI_API_KEY vào environment variables."
    
    try:
        import openai
        
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý AI hữu ích, trả lời bằng tiếng Việt."},
                {"role": "user", "content": question}
            ],
            max_tokens=1000,
            temperature=0.7
        )
        
        return response.choices[0].message.content
        
    except ImportError:
        return "❌ Thư viện OpenAI chưa được cài đặt. Vui lòng chạy: pip install openai"
    except Exception as e:
        logger.error(f"GPT error: {e}")
        return f"❌ Lỗi khi gọi GPT: {str(e)}"


# ============================================================================
# KALLE REPORTS
# ============================================================================

async def generate_koc_report_text(summary: Dict[str, Any]) -> str:
    """
    Generate KPI report text for KALLE
    """
    month = summary.get("month", datetime.now().month)
    brand = summary.get("brand", "KALLE")
    staff_list = summary.get("staff_list", [])
    totals = summary.get("totals", {})
    
    lines = [
        f"📊 **BÁO CÁO KOC {brand} - Tháng {month}**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        ""
    ]
    
    # Individual staff reports
    for staff in staff_list:
        name = staff.get("name", "Unknown")
        video_done = staff.get("video_done", 0)
        video_kpi = staff.get("video_kpi", 0)
        video_percent = staff.get("video_percent", 0)
        budget_done = staff.get("budget_done", 0)
        budget_kpi = staff.get("budget_kpi", 0)
        budget_percent = staff.get("budget_percent", 0)
        status = staff.get("status", "")
        progress = staff.get("progress", 0)
        
        # Content breakdown
        content_text = staff.get("content_breakdown_text", "")
        if not content_text:
            content_data = staff.get("content_breakdown", {})
            if content_data:
                content_text = format_content_breakdown(content_data)
        
        progress_bar = generate_progress_bar(progress, 8)
        
        lines.append(f"👤 **{name}** {status}")
        lines.append(f"   📦 Video: {video_done}/{video_kpi} ({video_percent}%)")
        
        if content_text:
            lines.append(f"   📝 Content: {content_text}")
        
        lines.append(f"   💰 Ngân sách: {format_number_vn(budget_done)}/{format_number_vn(budget_kpi)} ({budget_percent}%)")
        lines.append(f"   {progress_bar} {progress}%")
        lines.append("")
    
    # Totals
    lines.extend([
        "───────────────────────────",
        "📈 **TỔNG KẾT:**",
        f"   • Video: {totals.get('video_done', 0)}/{totals.get('video_kpi', 0)} ({totals.get('video_percent', 0)}%)",
        f"   • Ngân sách: {format_number_vn(totals.get('budget_done', 0))}/{format_number_vn(totals.get('budget_kpi', 0))} ({totals.get('budget_percent', 0)}%)",
    ])
    
    return "\n".join(lines)


async def generate_dashboard_report_text(
    data: Dict[str, Any],
    report_type: str = "full",
    nhan_su_filter: str = None
) -> str:
    """
    Generate dashboard report for KALLE staff
    """
    month = data.get("month", datetime.now().month)
    brand = data.get("brand", "KALLE")
    staff_list = data.get("staff_list", [])
    totals = data.get("totals", {})
    
    # Filter by staff if specified
    if nhan_su_filter:
        staff_list = [s for s in staff_list if nhan_su_filter.lower() in s.get("name", "").lower()]
        
        if staff_list:
            staff = staff_list[0]
            content_text = staff.get("content_breakdown_text", "")
            if not content_text:
                content_data = staff.get("content_breakdown", {})
                if content_data:
                    content_text = format_content_breakdown(content_data)
            
            lines = [
                f"🧴 **KPI CÁ NHÂN - {brand}**",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                f"📅 Tháng {month}",
                f"👤 **{staff.get('name')} - PR Booking {brand}**",
                "───────────────────────────",
                "",
                "📦 **SỐ LƯỢNG VIDEO:**",
                f"   • KPI: {staff.get('video_kpi', 0)} video",
                f"   • Đã air: {staff.get('video_done', 0)} video",
                f"   • Tỷ lệ: **{staff.get('video_percent', 0)}%**",
            ]
            
            if content_text:
                lines.append(f"   **Content: {content_text}**")
            
            lines.extend([
                "",
                "💰 **NGÂN SÁCH:**",
                f"   • KPI: {format_number_vn(staff.get('budget_kpi', 0))}",
                f"   • Đã air: {format_number_vn(staff.get('budget_done', 0))}",
                f"   • Tỷ lệ: **{staff.get('budget_percent', 0)}%**",
                "",
                f"📊 **Trạng thái:** {staff.get('status', '')}",
                f"📊 Tiến độ: {generate_progress_bar(staff.get('progress', 0))} {staff.get('progress', 0)}%",
            ])
            
            return "\n".join(lines)
        else:
            return f"❌ Không tìm thấy nhân sự: {nhan_su_filter}"
    
    # Full dashboard report
    lines = [
        f"📊 **DASHBOARD {brand} - Tháng {month}**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        ""
    ]
    
    for staff in staff_list:
        name = staff.get("name", "Unknown")
        video_done = staff.get("video_done", 0)
        video_kpi = staff.get("video_kpi", 0)
        video_percent = staff.get("video_percent", 0)
        budget_done = staff.get("budget_done", 0)
        budget_kpi = staff.get("budget_kpi", 0)
        budget_percent = staff.get("budget_percent", 0)
        status = staff.get("status", "")
        progress = staff.get("progress", 0)
        
        content_text = staff.get("content_breakdown_text", "")
        if not content_text:
            content_data = staff.get("content_breakdown", {})
            if content_data:
                content_text = format_content_breakdown(content_data)
        
        progress_bar = generate_progress_bar(progress, 8)
        
        lines.append(f"👤 **{name}** {status}")
        lines.append(f"   📦 Video: {video_done}/{video_kpi} ({video_percent}%)")
        
        if content_text:
            lines.append(f"   📝 Content: {content_text}")
        
        lines.append(f"   💰 Ngân sách: {format_number_vn(budget_done)}/{format_number_vn(budget_kpi)} ({budget_percent}%)")
        lines.append(f"   {progress_bar} {progress}%")
        lines.append("")
    
    lines.extend([
        "───────────────────────────",
        "📈 **TỔNG KẾT:**",
        f"   • Video: {totals.get('video_done', 0)}/{totals.get('video_kpi', 0)} ({totals.get('video_percent', 0)}%)",
        f"   • Ngân sách: {format_number_vn(totals.get('budget_done', 0))}/{format_number_vn(totals.get('budget_kpi', 0))} ({totals.get('budget_percent', 0)}%)",
    ])
    
    return "\n".join(lines)


# ============================================================================
# CHENG REPORTS
# ============================================================================

async def generate_cheng_report_text(
    summary: Dict[str, Any],
    report_type: str = "full",
    nhan_su_filter: str = None
) -> str:
    """
    Generate KPI report text for CHENG
    """
    month = summary.get("month", datetime.now().month)
    brand = summary.get("brand", "CHENG")
    staff_list = summary.get("staff_list", [])
    totals = summary.get("totals", {})
    
    # Filter by staff if specified
    if nhan_su_filter:
        staff_list = [s for s in staff_list if nhan_su_filter.lower() in s.get("name", "").lower()]
        
        if staff_list:
            staff = staff_list[0]
            content_text = staff.get("content_breakdown_text", "")
            if not content_text:
                content_data = staff.get("content_breakdown", {})
                if content_data:
                    content_text = format_content_breakdown(content_data)
            
            lines = [
                f"💇 **KPI CÁ NHÂN - {brand}**",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                f"📅 Tháng {month}",
                f"👤 **{staff.get('name')} - PR Booking {brand}**",
                "───────────────────────────",
                "",
                "📦 **SỐ LƯỢNG VIDEO:**",
                f"   • KPI: {staff.get('video_kpi', 0)} video",
                f"   • Đã air: {staff.get('video_done', 0)} video",
                f"   • Tỷ lệ: **{staff.get('video_percent', 0)}%**",
            ]
            
            if content_text:
                lines.append(f"   **Content: {content_text}**")
            
            lines.extend([
                "",
                "💰 **GMV (DOANH THU):**",
                f"   • KPI: {format_number_vn(staff.get('gmv_kpi', 0))}",
                f"   • Đã đạt: {format_number_vn(staff.get('gmv_done', 0))}",
                f"   • Tỷ lệ: **{staff.get('gmv_percent', 0)}%**",
                "",
                f"📊 **Trạng thái:** {staff.get('status', '')}",
                f"📊 Tiến độ: {generate_progress_bar(staff.get('progress', 0))} {staff.get('progress', 0)}%",
            ])
            
            return "\n".join(lines)
        else:
            return f"❌ Không tìm thấy nhân sự: {nhan_su_filter}"
    
    # Full report
    lines = [
        f"📊 **BÁO CÁO KOC {brand} - Tháng {month}**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        ""
    ]
    
    for staff in staff_list:
        name = staff.get("name", "Unknown")
        video_done = staff.get("video_done", 0)
        video_kpi = staff.get("video_kpi", 0)
        video_percent = staff.get("video_percent", 0)
        gmv_done = staff.get("gmv_done", 0)
        gmv_kpi = staff.get("gmv_kpi", 0)
        gmv_percent = staff.get("gmv_percent", 0)
        status = staff.get("status", "")
        progress = staff.get("progress", 0)
        
        content_text = staff.get("content_breakdown_text", "")
        if not content_text:
            content_data = staff.get("content_breakdown", {})
            if content_data:
                content_text = format_content_breakdown(content_data)
        
        progress_bar = generate_progress_bar(progress, 8)
        
        lines.append(f"👤 **{name}** {status}")
        lines.append(f"   📦 Video: {video_done}/{video_kpi} ({video_percent}%)")
        
        if content_text:
            lines.append(f"   📝 Content: {content_text}")
        
        lines.append(f"   💰 GMV: {format_number_vn(gmv_done)}/{format_number_vn(gmv_kpi)} ({gmv_percent}%)")
        lines.append(f"   {progress_bar} {progress}%")
        lines.append("")
    
    lines.extend([
        "───────────────────────────",
        "📈 **TỔNG KẾT:**",
        f"   • Video: {totals.get('video_done', 0)}/{totals.get('video_kpi', 0)} ({totals.get('video_percent', 0)}%)",
        f"   • GMV: {format_number_vn(totals.get('gmv_done', 0))}/{format_number_vn(totals.get('gmv_kpi', 0))} ({totals.get('gmv_percent', 0)}%)",
    ])
    
    return "\n".join(lines)


# ============================================================================
# CONTENT CALENDAR TEXT - REQUIRED BY main.py
# ============================================================================

async def generate_content_calendar_text(calendar_data: Dict[str, Any]) -> str:
    """
    Generate content calendar report text
    Required by main.py for INTENT_CONTENT_CALENDAR
    """
    month = calendar_data.get("month")
    year = calendar_data.get("year", datetime.now().year)
    items = calendar_data.get("items", [])
    total = calendar_data.get("total", len(items))
    by_team = calendar_data.get("by_team", {})
    by_status = calendar_data.get("by_status", {})
    team_filter = calendar_data.get("team_filter")
    
    lines = [
        "📅 **LỊCH CONTENT**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    
    if month:
        lines.append(f"📆 Tháng {month}/{year}")
    if team_filter:
        lines.append(f"🏷️ Team: {team_filter}")
    
    lines.append(f"📊 Tổng: {total} công việc")
    lines.append("")
    
    # Summary by team
    if by_team:
        lines.append("📋 **THEO TEAM:**")
        for team, count in by_team.items():
            if team:
                lines.append(f"   • {team}: {count}")
        lines.append("")
    
    # Summary by status
    if by_status:
        lines.append("📊 **THEO TRẠNG THÁI:**")
        for status, count in by_status.items():
            if status:
                lines.append(f"   • {status}: {count}")
        lines.append("")
    
    # List items (max 10)
    if items:
        lines.append("📝 **CHI TIẾT:**")
        for i, item in enumerate(items[:10], 1):
            title = item.get("title", "Không có tiêu đề")
            team = item.get("team", "")
            status = item.get("status", "")
            deadline = item.get("deadline", "")
            
            lines.append(f"{i}. {title}")
            if team:
                lines.append(f"   🏷️ Team: {team}")
            if status:
                lines.append(f"   📊 Trạng thái: {status}")
            if deadline:
                lines.append(f"   📅 Deadline: {deadline}")
        
        if len(items) > 10:
            lines.append(f"   ... và {len(items) - 10} công việc khác")
    else:
        lines.append("📭 Không có công việc nào trong khoảng thời gian này.")
    
    return "\n".join(lines)


# ============================================================================
# TASK SUMMARY TEXT - REQUIRED BY main.py
# ============================================================================

async def generate_task_summary_text(task_data: Dict[str, Any]) -> str:
    """
    Generate task summary report text
    Required by main.py for INTENT_TASK_SUMMARY
    """
    month = task_data.get("month")
    year = task_data.get("year", datetime.now().year)
    tasks = task_data.get("tasks", [])
    total = task_data.get("total", len(tasks))
    overdue = task_data.get("overdue", 0)
    upcoming = task_data.get("upcoming", 0)
    completed = task_data.get("completed", 0)
    by_position = task_data.get("by_position", {})
    vi_tri_filter = task_data.get("vi_tri_filter")
    
    lines = [
        "📋 **PHÂN TÍCH TASK**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    
    if month:
        lines.append(f"📆 Tháng {month}/{year}")
    if vi_tri_filter:
        lines.append(f"🏷️ Vị trí: {vi_tri_filter}")
    
    lines.append("")
    lines.append("📊 **TỔNG QUAN:**")
    lines.append(f"   • Tổng số task: {total}")
    lines.append(f"   • ✅ Hoàn thành: {completed}")
    lines.append(f"   • ⏳ Sắp đến hạn: {upcoming}")
    lines.append(f"   • ⚠️ Quá hạn: {overdue}")
    lines.append("")
    
    # Summary by position
    if by_position:
        lines.append("👥 **THEO VỊ TRÍ:**")
        for position, stats in by_position.items():
            if position:
                pos_total = stats.get("total", 0)
                pos_overdue = stats.get("overdue", 0)
                pos_completed = stats.get("completed", 0)
                
                status_icon = "🔴" if pos_overdue > 0 else "🟢"
                lines.append(f"   {status_icon} {position}: {pos_total} task (✅{pos_completed} | ⚠️{pos_overdue} quá hạn)")
        lines.append("")
    
    # List overdue tasks
    overdue_tasks = [t for t in tasks if t.get("is_overdue")]
    if overdue_tasks:
        lines.append("⚠️ **TASK QUÁ HẠN:**")
        for task in overdue_tasks[:5]:
            title = task.get("title", "Không có tiêu đề")
            position = task.get("position", "")
            lines.append(f"   • {title}")
            if position:
                lines.append(f"     👤 {position}")
        
        if len(overdue_tasks) > 5:
            lines.append(f"   ... và {len(overdue_tasks) - 5} task khác")
    else:
        lines.append("✅ Không có task nào quá hạn!")
    
    return "\n".join(lines)


# ============================================================================
# GENERAL SUMMARY TEXT - REQUIRED BY main.py
# ============================================================================

async def generate_general_summary_text(
    koc_data: Dict[str, Any],
    content_data: Dict[str, Any]
) -> str:
    """
    Generate general summary combining KOC and Content data
    Required by main.py for INTENT_GENERAL_SUMMARY
    """
    month = koc_data.get("month", datetime.now().month)
    year = koc_data.get("year", datetime.now().year)
    
    # KOC totals
    koc_totals = koc_data.get("totals", {})
    video_done = koc_totals.get("video_done", 0)
    video_kpi = koc_totals.get("video_kpi", 0)
    video_percent = koc_totals.get("video_percent", 0)
    budget_done = koc_totals.get("budget_done", 0)
    budget_kpi = koc_totals.get("budget_kpi", 0)
    budget_percent = koc_totals.get("budget_percent", 0)
    
    # Content totals
    content_total = content_data.get("total", 0)
    content_by_status = content_data.get("by_status", {})
    
    lines = [
        f"📊 **TỔNG HỢP KẾT QUẢ - Tháng {month}/{year}**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "🎬 **KOC BOOKING:**",
        f"   • Video: {video_done}/{video_kpi} ({video_percent}%)",
        f"   • Ngân sách: {format_number_vn(budget_done)}/{format_number_vn(budget_kpi)} ({budget_percent}%)",
        "",
        "📅 **CONTENT:**",
        f"   • Tổng công việc: {content_total}",
    ]
    
    # Content status summary
    if content_by_status:
        for status, count in content_by_status.items():
            if status:
                lines.append(f"   • {status}: {count}")
    
    lines.append("")
    
    # Overall status
    overall_percent = (video_percent + budget_percent) / 2 if (video_percent or budget_percent) else 0
    if overall_percent >= 80:
        status_text = "🟢 Tiến độ tốt"
    elif overall_percent >= 50:
        status_text = "🟡 Đang tiến hành"
    else:
        status_text = "🔴 Cần cải thiện"
    
    lines.extend([
        "───────────────────────────",
        f"📈 **ĐÁNH GIÁ CHUNG:** {status_text}",
        f"📊 Tiến độ: {generate_progress_bar(overall_percent)} {overall_percent:.0f}%",
    ])
    
    return "\n".join(lines)


# ============================================================================
# CONTENT DETAIL REPORT
# ============================================================================

def generate_content_detail_report(
    content_by_nhan_su: Dict[str, Dict[str, int]],
    month: int = None,
    brand: str = "KALLE"
) -> str:
    """Generate detailed content report showing breakdown by staff"""
    if month is None:
        month = datetime.now().month
    
    lines = [
        f"📝 **CHI TIẾT NỘI DUNG - {brand}**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 Tháng {month}",
        ""
    ]
    
    if not content_by_nhan_su:
        lines.append("❌ Không có dữ liệu content")
        return "\n".join(lines)
    
    for staff_name, content_data in content_by_nhan_su.items():
        total = content_data.get("total", 0)
        total_cart = content_data.get("total_cart", 0)
        total_text = content_data.get("total_text", 0)
        
        lines.append(f"👤 **{staff_name}**")
        lines.append(f"   📊 Tổng: {total} | Cart: {total_cart} | Text: {total_text}")
        
        for key, count in content_data.items():
            if key not in ("total", "total_cart", "total_text"):
                lines.append(f"      • {key}: {count}")
        
        lines.append("")
    
    # Grand totals
    grand_total = sum(d.get("total", 0) for d in content_by_nhan_su.values())
    grand_cart = sum(d.get("total_cart", 0) for d in content_by_nhan_su.values())
    grand_text = sum(d.get("total_text", 0) for d in content_by_nhan_su.values())
    
    lines.extend([
        "───────────────────────────",
        "📈 **TỔNG KẾT:**",
        f"   • Tổng content: {grand_total}",
        f"   • Cart: {grand_cart}",
        f"   • Text: {grand_text}"
    ])
    
    return "\n".join(lines)


# ============================================================================
# GENERIC REPORT DISPATCHER
# ============================================================================

def generate_report(report_type: str, summary: Dict[str, Any]) -> str:
    """Dispatch to appropriate report generator based on type"""
    generators = {
        "kalle_koc": generate_koc_report_text,
        "kalle_dashboard": generate_dashboard_report_text,
        "cheng_koc": generate_cheng_report_text,
    }
    
    generator = generators.get(report_type)
    if generator:
        import asyncio
        return asyncio.run(generator(summary))
    else:
        logger.warning(f"Unknown report type: {report_type}")
        return f"❌ Không tìm thấy loại báo cáo: {report_type}"


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("Testing report_generator.py v5.8.1...")
    print("Functions available:")
    print("  - generate_koc_report_text()")
    print("  - generate_dashboard_report_text()")
    print("  - generate_cheng_report_text()")
    print("  - generate_content_calendar_text()")
    print("  - generate_task_summary_text()")
    print("  - generate_general_summary_text()")
    print("  - chat_with_gpt()")
