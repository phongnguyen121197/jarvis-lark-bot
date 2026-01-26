# report_generator.py - Version 5.8.0
# Updated: Use content_by_nhan_su from lark_base.py
# Format: KPI reports with content breakdown

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

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
    """
    Format content breakdown from aggregated data
    
    Input: {"Nước hoa,Cart,Dark Beauty": 30, "Nước hoa,Text,Dark Beauty": 10, "total": 40}
    Output: "30 Nước hoa,Cart,Dark Beauty và 10 Nước hoa,Text,Dark Beauty"
    """
    if not content_data:
        return ""
    
    # Filter out total fields
    items = []
    for key, count in content_data.items():
        if key not in ("total", "total_cart", "total_text"):
            items.append(f"{count} {key}")
    
    if not items:
        return ""
    
    # Join with "và" for Vietnamese
    if len(items) == 1:
        return items[0]
    elif len(items) == 2:
        return f"{items[0]} và {items[1]}"
    else:
        return ", ".join(items[:-1]) + f" và {items[-1]}"


# ============================================================================
# KALLE REPORTS
# ============================================================================

def generate_koc_report_text(summary: Dict[str, Any]) -> str:
    """
    Generate KPI report text for KALLE individual staff
    
    Expected summary keys:
    - staff_name, month, year, brand
    - video_kpi, video_done, video_percent
    - budget_kpi, budget_done, budget_percent
    - contact_total, contact_deal, contact_percent
    - content_breakdown (dict) or content_breakdown_text (str)
    - status, progress
    """
    staff_name = summary.get("staff_name", "Unknown")
    month = summary.get("month", datetime.now().month)
    brand = summary.get("brand", "KALLE")
    
    # Video metrics
    video_kpi = summary.get("video_kpi", 0)
    video_done = summary.get("video_done", 0)
    video_percent = summary.get("video_percent", 0)
    
    # Budget metrics
    budget_kpi = summary.get("budget_kpi", 0)
    budget_done = summary.get("budget_done", 0)
    budget_percent = summary.get("budget_percent", 0)
    
    # Contact metrics
    contact_total = summary.get("contact_total", 0)
    contact_deal = summary.get("contact_deal", 0)
    contact_percent = summary.get("contact_percent", 0)
    
    # Content breakdown - NEW in v5.8.0
    content_breakdown_text = summary.get("content_breakdown_text", "")
    if not content_breakdown_text:
        content_breakdown = summary.get("content_breakdown", {})
        if content_breakdown:
            content_breakdown_text = format_content_breakdown(content_breakdown)
    
    # Status and progress
    status = summary.get("status", "🟡 Đang tiến hành")
    progress = summary.get("progress", 0)
    progress_bar = generate_progress_bar(progress)
    
    # Build report
    lines = [
        f"🧴 **KPI CÁ NHÂN - {brand}**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 Tháng {month}",
        f"👤 **{staff_name} - PR Booking {brand}**",
        "───────────────────────────",
        f"📊 **Trạng thái:** {status}",
        "",
        "📦 **SỐ LƯỢNG VIDEO:**",
        f"   • KPI: {video_kpi} video",
        f"   • Đã air: {video_done} video",
        f"   • Tỷ lệ: **{video_percent}%**",
    ]
    
    # Add content breakdown if available
    if content_breakdown_text:
        lines.append(f"   **Content: {content_breakdown_text}**")
    
    lines.extend([
        "",
        "💰 **NGÂN SÁCH:**",
        f"   • KPI: {format_number_vn(budget_kpi)}",
        f"   • Đã air: {format_number_vn(budget_done)}",
        f"   • Tỷ lệ: **{budget_percent}%**",
        "",
        f"📊 Tiến độ: {progress_bar} {progress}%",
    ])
    
    # Add contact stats if available
    if contact_total > 0:
        lines.extend([
            "",
            "📞 **LIÊN HỆ KOC:**",
            f"   • Tổng liên hệ: {contact_total}",
            f"   • Đã deal: {contact_deal} ({contact_percent}%)",
        ])
    
    return "\n".join(lines)


def generate_dashboard_report_text(summary: Dict[str, Any]) -> str:
    """
    Generate dashboard report for all KALLE staff
    
    Expected summary keys:
    - month, year, brand
    - staff_list: List of staff dicts
    - totals: Aggregate totals
    - content_by_nhan_su (optional)
    """
    month = summary.get("month", datetime.now().month)
    brand = summary.get("brand", "KALLE")
    staff_list = summary.get("staff_list", [])
    totals = summary.get("totals", {})
    
    lines = [
        f"📊 **DASHBOARD {brand} - Tháng {month}**",
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
        
        # Add content breakdown if available
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


# ============================================================================
# CHENG REPORTS
# ============================================================================

def generate_cheng_report_text(summary: Dict[str, Any]) -> str:
    """
    Generate KPI report text for CHENG individual staff
    
    Expected summary keys:
    - staff_name, month, year, brand
    - video_kpi, video_done, video_percent
    - gmv_kpi, gmv_done, gmv_percent (CHENG uses GMV instead of budget)
    - contact_total, contact_deal, contact_percent
    - content_breakdown (dict) or content_breakdown_text (str)
    - status, progress
    """
    staff_name = summary.get("staff_name", "Unknown")
    month = summary.get("month", datetime.now().month)
    brand = summary.get("brand", "CHENG")
    
    # Video metrics
    video_kpi = summary.get("video_kpi", 0)
    video_done = summary.get("video_done", 0)
    video_percent = summary.get("video_percent", 0)
    
    # GMV metrics (CHENG specific)
    gmv_kpi = summary.get("gmv_kpi", 0)
    gmv_done = summary.get("gmv_done", 0)
    gmv_percent = summary.get("gmv_percent", 0)
    
    # Contact metrics
    contact_total = summary.get("contact_total", 0)
    contact_deal = summary.get("contact_deal", 0)
    contact_percent = summary.get("contact_percent", 0)
    
    # Content breakdown - NEW in v5.8.0
    content_breakdown_text = summary.get("content_breakdown_text", "")
    if not content_breakdown_text:
        content_breakdown = summary.get("content_breakdown", {})
        if content_breakdown:
            content_breakdown_text = format_content_breakdown(content_breakdown)
    
    # Status and progress
    status = summary.get("status", "🟡 Đang tiến hành")
    progress = summary.get("progress", 0)
    progress_bar = generate_progress_bar(progress)
    
    # Build report
    lines = [
        f"💇 **KPI CÁ NHÂN - {brand}**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 Tháng {month}",
        f"👤 **{staff_name} - PR Booking {brand}**",
        "───────────────────────────",
        f"📊 **Trạng thái:** {status}",
        "",
        "📦 **SỐ LƯỢNG VIDEO:**",
        f"   • KPI: {video_kpi} video",
        f"   • Đã air: {video_done} video",
        f"   • Tỷ lệ: **{video_percent}%**",
    ]
    
    # Add content breakdown if available
    if content_breakdown_text:
        lines.append(f"   **Content: {content_breakdown_text}**")
    
    lines.extend([
        "",
        "💰 **GMV (DOANH THU):**",
        f"   • KPI: {format_number_vn(gmv_kpi)}",
        f"   • Đã đạt: {format_number_vn(gmv_done)}",
        f"   • Tỷ lệ: **{gmv_percent}%**",
        "",
        f"📊 Tiến độ: {progress_bar} {progress}%",
    ])
    
    # Add contact stats if available
    if contact_total > 0:
        lines.extend([
            "",
            "📞 **LIÊN HỆ KOC:**",
            f"   • Tổng liên hệ: {contact_total}",
            f"   • Đã deal: {contact_deal} ({contact_percent}%)",
        ])
    
    return "\n".join(lines)


def generate_cheng_dashboard_report_text(summary: Dict[str, Any]) -> str:
    """
    Generate dashboard report for all CHENG staff
    """
    month = summary.get("month", datetime.now().month)
    brand = summary.get("brand", "CHENG")
    staff_list = summary.get("staff_list", [])
    totals = summary.get("totals", {})
    
    lines = [
        f"📊 **DASHBOARD {brand} - Tháng {month}**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        ""
    ]
    
    # Individual staff reports
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
        
        # Content breakdown
        content_text = staff.get("content_breakdown_text", "")
        if not content_text:
            content_data = staff.get("content_breakdown", {})
            if content_data:
                content_text = format_content_breakdown(content_data)
        
        progress_bar = generate_progress_bar(progress, 8)
        
        lines.append(f"👤 **{name}** {status}")
        lines.append(f"   📦 Video: {video_done}/{video_kpi} ({video_percent}%)")
        
        # Add content breakdown if available
        if content_text:
            lines.append(f"   📝 Content: {content_text}")
        
        lines.append(f"   💰 GMV: {format_number_vn(gmv_done)}/{format_number_vn(gmv_kpi)} ({gmv_percent}%)")
        lines.append(f"   {progress_bar} {progress}%")
        lines.append("")
    
    # Totals
    lines.extend([
        "───────────────────────────",
        "📈 **TỔNG KẾT:**",
        f"   • Video: {totals.get('video_done', 0)}/{totals.get('video_kpi', 0)} ({totals.get('video_percent', 0)}%)",
        f"   • GMV: {format_number_vn(totals.get('gmv_done', 0))}/{format_number_vn(totals.get('gmv_kpi', 0))} ({totals.get('gmv_percent', 0)}%)",
    ])
    
    return "\n".join(lines)


# ============================================================================
# GENERIC REPORT DISPATCHER
# ============================================================================

def generate_report(report_type: str, summary: Dict[str, Any]) -> str:
    """
    Dispatch to appropriate report generator based on type
    
    report_type:
    - "kalle_koc": Individual KALLE staff
    - "kalle_dashboard": All KALLE staff  
    - "cheng_koc": Individual CHENG staff
    - "cheng_dashboard": All CHENG staff
    """
    generators = {
        "kalle_koc": generate_koc_report_text,
        "kalle_dashboard": generate_dashboard_report_text,
        "cheng_koc": generate_cheng_report_text,
        "cheng_dashboard": generate_cheng_dashboard_report_text
    }
    
    generator = generators.get(report_type)
    if generator:
        return generator(summary)
    else:
        logger.warning(f"Unknown report type: {report_type}")
        return f"❌ Không tìm thấy loại báo cáo: {report_type}"


# ============================================================================
# CONTENT DETAIL REPORT - NEW in v5.8.0
# ============================================================================

def generate_content_detail_report(
    content_by_nhan_su: Dict[str, Dict[str, int]],
    month: int = None,
    brand: str = "KALLE"
) -> str:
    """
    Generate detailed content report showing breakdown by staff
    
    Input:
    {
        "Như Mai": {"Nước hoa,Cart,Dark Beauty": 30, "Nước hoa,Text,Dark Beauty": 10, "total": 40},
        "Lan Anh": {"Nước hoa,Cart,Coco": 20, "total": 20}
    }
    """
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
    
    # Summary by staff
    for staff_name, content_data in content_by_nhan_su.items():
        total = content_data.get("total", 0)
        total_cart = content_data.get("total_cart", 0)
        total_text = content_data.get("total_text", 0)
        
        lines.append(f"👤 **{staff_name}**")
        lines.append(f"   📊 Tổng: {total} | Cart: {total_cart} | Text: {total_text}")
        
        # Detail breakdown
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
# TESTING
# ============================================================================

if __name__ == "__main__":
    # Test report generation
    print("Testing report_generator.py v5.8.0...")
    
    # Mock summary with content breakdown
    mock_summary = {
        "staff_name": "Như Mai",
        "month": 12,
        "brand": "KALLE",
        "video_kpi": 85,
        "video_done": 78,
        "video_percent": 91.8,
        "budget_kpi": 14500000,
        "budget_done": 8900000,
        "budget_percent": 61.4,
        "contact_total": 129,
        "contact_deal": 27,
        "contact_percent": 20.9,
        "content_breakdown": {
            "Nước hoa,Cart,Dark Beauty 30ml": 30,
            "Nước hoa,Text,Dark Beauty 30ml": 10,
            "total": 40,
            "total_cart": 30,
            "total_text": 10
        },
        "content_breakdown_text": "",
        "status": "🟢 Gần đạt",
        "progress": 80
    }
    
    report = generate_koc_report_text(mock_summary)
    print(report)
    print("\n" + "="*50 + "\n")
    
    # Test content detail report
    mock_content = {
        "Như Mai": {
            "Nước hoa,Cart,Dark Beauty 30ml": 30,
            "Nước hoa,Text,Dark Beauty 30ml": 10,
            "total": 40,
            "total_cart": 30,
            "total_text": 10
        },
        "Lan Anh": {
            "Nước hoa,Cart,Coco 50ml": 20,
            "Sữa tắm,Text,Lavender": 15,
            "total": 35,
            "total_cart": 20,
            "total_text": 15
        }
    }
    
    detail_report = generate_content_detail_report(mock_content, month=12, brand="KALLE")
    print(detail_report)
