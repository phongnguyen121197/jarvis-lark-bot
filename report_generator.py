"""
Report Generator Module - Version 5.7.2
"""
import os
import json
from typing import Dict, Any, Optional
from openai import AsyncOpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = AsyncOpenAI(api_key=OPENAI_API_KEY)

def format_currency(value):
    if value >= 1_000_000_000:
        return f"{value/1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.0f}K"
    else:
        return f"{value:,.0f}"

async def generate_koc_report_text(summary_data: Dict[str, Any]) -> str:
    summary = summary_data.get("summary", {})
    by_group = summary_data.get("by_group", {})
    group_label = summary_data.get("group_label", "sản phẩm")
    month = summary_data.get("month")
    week = summary_data.get("week")
    
    tong_chi_phi = summary.get("tong_chi_phi_deal", 0)
    chi_phi_formatted = f"{int(tong_chi_phi):,}".replace(",", ".")
    
    total = summary.get("total", 0)
    da_air = summary.get("da_air", 0)
    ty_le_air = round((da_air / total * 100), 1) if total > 0 else 0
    
    week_text = f" tuần {week}" if week else ""
    
    text = f"""📊 Tóm tắt KOC tháng {month}{week_text}:

• Tổng: {total} KOC đã deal
• Đã air: {da_air} KOC ({ty_le_air}%)
• Chưa air: {summary.get('chua_air', 0)} KOC
• Đã air nhưng chưa có link: {summary.get('da_air_chua_link', 0)} KOC
• Đã air nhưng chưa gắn giỏ: {summary.get('da_air_chua_gan_gio', 0)} KOC

💰 Tổng chi phí deal: {chi_phi_formatted} VNĐ"""
    
    if by_group:
        text += f"\n\n📦 Theo {group_label}:"
        sorted_g = sorted(by_group.items(), key=lambda x: x[1].get("count", 0), reverse=True)
        for name, stats in sorted_g[:8]:
            chi_phi_g = stats.get("chi_phi", 0)
            chi_phi_g_fmt = f"{int(chi_phi_g):,}".replace(",", ".")
            text += f"\n• {name}: {stats.get('count', 0)} KOC ({chi_phi_g_fmt} VNĐ)"
    
    return text

async def generate_content_calendar_text(calendar_data: Dict[str, Any]) -> str:
    summary = calendar_data.get("summary", {})
    date_range = calendar_data.get("date_range", "tuần này")
    
    return f"""📅 Lịch content {date_range}:

• Tổng: {summary.get('total_tasks', 0)} task
• Overdue: {summary.get('total_overdue', 0)} task
• Số ngày có content: {summary.get('days_with_content', 0)}"""

async def generate_task_summary_text(task_data: Dict[str, Any]) -> str:
    summary = task_data.get("summary", {})
    by_vi_tri = task_data.get("by_vi_tri", {})
    month = task_data.get("month")
    
    month_text = f" tháng {month}" if month else ""
    
    text = f"""📋 Phân tích Task{month_text}:

• Tổng: {summary.get('total_tasks', 0)} task
• Quá hạn: {summary.get('total_overdue', 0)} task ⚠️
• Sắp deadline: {summary.get('total_sap_deadline', 0)} task

👥 Theo vị trí:"""
    
    sorted_vt = sorted(by_vi_tri.items(), key=lambda x: x[1].get("total", 0), reverse=True)
    for vt, stats in sorted_vt[:5]:
        overdue = stats.get("overdue", 0)
        overdue_text = f" ({overdue} overdue)" if overdue > 0 else ""
        text += f"\n• {vt}: {stats.get('total', 0)} task{overdue_text}"
    
    return text

async def generate_general_summary_text(koc_data: Dict, content_data: Dict) -> str:
    koc_sum = koc_data.get("summary", {})
    content_sum = content_data.get("summary", {})
    
    return f"""🗓️ Tổng hợp tuần này:

📢 KOC/PR:
• Tổng: {koc_sum.get('total', 0)} KOC
• Đã air: {koc_sum.get('da_air', 0)}
• Chưa air: {koc_sum.get('chua_air', 0)}

📝 Content:
• Tổng task: {content_sum.get('total_tasks', 0)}
• Overdue: {content_sum.get('total_overdue', 0)}"""

async def chat_with_gpt(question: str) -> str:
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là Jarvis - trợ lý AI thông minh. Trả lời bằng tiếng Việt, ngắn gọn."},
                {"role": "user", "content": question}
            ],
            temperature=0.8,
            max_tokens=1000
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"❌ Lỗi: {str(e)}"

async def generate_dashboard_report_text(data: dict, report_type: str = "full", nhan_su_filter: str = None) -> str:
    from datetime import datetime
    
    month = data.get("month")
    week = data.get("week")
    tong_quan = data.get("tong_quan", {})
    kpi_nhan_su = data.get("kpi_nhan_su", {})
    top_koc = data.get("top_koc", [])
    lien_he_nhan_su = data.get("lien_he_nhan_su", {})
    
    current_day = datetime.now().day
    current_month = datetime.now().month
    is_after_15 = current_day > 15
    is_current_month = (month == current_month)
    
    time_label = f"Tháng {month}" if month else "Tổng hợp"
    if week:
        time_label += f" - {week}"
    
    lines = []
    
    if report_type == "kpi_ca_nhan" and nhan_su_filter:
        lines.append(f"👤 **KPI CÁ NHÂN - {time_label.upper()}**\n")
        
        matched_ns = None
        matched_kpi = None
        matched_lh = None
        
        for ns, kpi in kpi_nhan_su.items():
            if nhan_su_filter.lower() in ns.lower() or ns.lower() in nhan_su_filter.lower():
                matched_ns = ns
                matched_kpi = kpi
                break
        
        for ns, lh in lien_he_nhan_su.items():
            if nhan_su_filter.lower() in ns.lower() or ns.lower() in nhan_su_filter.lower():
                matched_lh = lh
                break
        
        if not matched_ns:
            lines.append(f"❌ Không tìm thấy nhân sự: {nhan_su_filter}")
            lines.append("\n📋 Danh sách nhân sự có sẵn:")
            for ns in kpi_nhan_su.keys():
                if ns != "Không xác định":
                    lines.append(f"  • {ns}")
            return "\n".join(lines)
        
        lines.append(f"═══════════════════════════")
        lines.append(f"📊 **{matched_ns}**")
        lines.append(f"═══════════════════════════\n")
        
        if matched_kpi:
            pct_sl = matched_kpi.get("pct_kpi_so_luong", 0)
            sl_air = matched_kpi.get("so_luong_air", 0)
            kpi_sl = matched_kpi.get("kpi_so_luong", 0)
            
            if pct_sl >= 50:
                status = "🟢 Đang trên tiến độ"
            elif pct_sl >= 20:
                status = "🟡 Cần cố gắng thêm"
            else:
                status = "🔴 Dưới tiến độ"
            
            lines.append(f"**Trạng thái:** {status}\n")
            lines.append(f"📦 **KPI Số lượng:** {sl_air}/{kpi_sl} ({pct_sl}%)")
        
        return "\n".join(lines)
    
    lines.append(f"📊 **DASHBOARD {time_label.upper()}**\n")
    
    kpi_sl = tong_quan.get("kpi_so_luong", 0)
    sl_air = tong_quan.get("so_luong_air", 0)
    pct_sl = tong_quan.get("pct_kpi_so_luong", 0)
    lines.append(f"📦 Số lượng Air: {sl_air}/{kpi_sl} ({pct_sl}%)")
    
    total_gmv = tong_quan.get("total_gmv", 0)
    if total_gmv > 0:
        lines.append(f"🏆 Tổng GMV: {format_currency(total_gmv)}")
    
    if kpi_nhan_su:
        lines.append("\n👥 **KPI NHÂN SỰ:**")
        sorted_ns = sorted(kpi_nhan_su.items(), key=lambda x: x[1].get("pct_kpi_so_luong", 0), reverse=True)
        for nhan_su, kpi in sorted_ns[:8]:
            if nhan_su == "Không xác định":
                continue
            pct = kpi.get("pct_kpi_so_luong", 0)
            emoji = "🟢" if pct >= 50 else "🟡" if pct >= 20 else "🔴"
            lines.append(f"{emoji} {nhan_su}: {pct}%")
    
    if top_koc:
        lines.append("\n🏅 **TOP KOC:**")
        medals = ["🥇", "🥈", "🥉"]
        for i, (koc_id, gmv) in enumerate(top_koc[:5]):
            prefix = medals[i] if i < 3 else f"{i+1}."
            lines.append(f"{prefix} @{koc_id}: {format_currency(gmv)}")
    
    return "\n".join(lines)

async def generate_cheng_report_text(summary_data: Dict[str, Any], report_type: str = "full", nhan_su_filter: str = None) -> str:
    from datetime import datetime
    
    tong_quan = summary_data.get("tong_quan", {})
    kpi_nhan_su = summary_data.get("kpi_nhan_su", {})
    lien_he_nhan_su = summary_data.get("lien_he_nhan_su", {})
    top_koc = summary_data.get("top_koc", [])
    month = summary_data.get("month")
    week = summary_data.get("week")
    
    lines = []
    
    # KPI CÁ NHÂN cho CHENG
    if report_type == "kpi_ca_nhan" and nhan_su_filter:
        lines.append("🧴 **KPI CÁ NHÂN - CHENG**")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        week_text = f" - Tuần {week}" if week else ""
        lines.append(f"📅 Tháng {month}{week_text}")
        lines.append("")
        
        found_kpi = None
        for ns, data in kpi_nhan_su.items():
            if nhan_su_filter.lower() in ns.lower() or ns.lower() in nhan_su_filter.lower():
                found_kpi = (ns, data)
                break
        
        found_lh = None
        for ns, data in lien_he_nhan_su.items():
            if nhan_su_filter.lower() in ns.lower() or ns.lower() in nhan_su_filter.lower():
                found_lh = (ns, data)
                break
        
        if not found_kpi and not found_lh:
            lines.append(f"❌ Không tìm thấy nhân sự CHENG: {nhan_su_filter}")
            lines.append("")
            lines.append("💡 Nhân sự CHENG: Phương, Linh, Trang, Hằng")
            return "\n".join(lines)
        
        if found_kpi:
            ns, data = found_kpi
            lines.append(f"👤 **{ns}**")
            lines.append("───────────────────────────")
            
            sl_air = data.get("so_luong_air", 0)
            kpi_sl = data.get("kpi_so_luong", 0)
            pct_sl = data.get("pct_kpi_so_luong", 0)
            
            ns_air = data.get("ngan_sach_air", 0)
            kpi_ns = data.get("kpi_ngan_sach", 0)
            pct_ns = data.get("pct_kpi_ngan_sach", 0)
            
            if pct_sl >= 100:
                status = "🏆 Đã đạt KPI!"
            elif pct_sl >= 70:
                status = "🟢 Gần đạt"
            elif pct_sl >= 50:
                status = "🟡 Đang tiến triển"
            else:
                status = "🔴 Cần cố gắng"
            
            lines.append(f"📊 **Trạng thái:** {status}")
            lines.append("")
            lines.append("📦 **SỐ LƯỢNG VIDEO:**")
            lines.append(f"   • KPI: {kpi_sl} video")
            lines.append(f"   • Đã air: {sl_air} video")
            lines.append(f"   • Tỷ lệ: **{pct_sl}%**")
            lines.append("")
            lines.append("💰 **NGÂN SÁCH:**")
            lines.append(f"   • KPI: {format_currency(kpi_ns)}")
            lines.append(f"   • Đã air: {format_currency(ns_air)}")
            lines.append(f"   • Tỷ lệ: **{pct_ns}%**")
            
            progress_filled = int(pct_sl / 10)
            progress_empty = 10 - progress_filled
            progress_bar = "▓" * progress_filled + "░" * progress_empty
            lines.append(f"\n📊 Tiến độ: [{progress_bar}] {pct_sl}%")
        
        if found_lh:
            ns, data = found_lh
            lines.append("")
            lines.append("📞 **LIÊN HỆ KOC:**")
            lines.append(f"   • Tổng liên hệ: {data.get('tong_lien_he', 0)}")
            lines.append(f"   • Đã deal: {data.get('da_deal', 0)} ({data.get('ty_le_deal', 0)}%)")
        
        return "\n".join(lines)
    
    # FULL REPORT
    lines.append("🧴 **BÁO CÁO KOC - CHENG LOVE HAIR**")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    week_text = f" - Tuần {week}" if week else ""
    lines.append(f"📅 Tháng {month}{week_text}")
    lines.append(f"🕐 Cập nhật: {datetime.now().strftime('%H:%M %d/%m/%Y')}")
    lines.append("")
    
    kpi_sl = tong_quan.get("kpi_so_luong", 0)
    sl_air = tong_quan.get("so_luong_air", 0)
    pct_sl = tong_quan.get("pct_kpi_so_luong", 0)
    
    lines.append("📊 **TỔNG QUAN KPI**")
    lines.append("───────────────────────────")
    lines.append(f"📦 **Số lượng:** {sl_air}/{kpi_sl} video ({pct_sl}%)")
    
    total_gmv = tong_quan.get("total_gmv", 0)
    if total_gmv > 0:
        lines.append(f"📈 **GMV KOC:** {format_currency(total_gmv)}")
    
    progress_filled = int(pct_sl / 10)
    progress_empty = 10 - progress_filled
    progress_bar = "▓" * progress_filled + "░" * progress_empty
    lines.append(f"📊 [{progress_bar}] {pct_sl}%")
    lines.append("")
    
    if kpi_nhan_su:
        lines.append("👥 **KPI THEO NHÂN SỰ**")
        lines.append("───────────────────────────")
        
        sorted_nhan_su = sorted(kpi_nhan_su.items(), key=lambda x: x[1].get("pct_kpi_so_luong", 0), reverse=True)
        
        for nhan_su, data in sorted_nhan_su:
            if not nhan_su or nhan_su == "Không xác định":
                continue
            
            sl_air = data.get("so_luong_air", 0)
            kpi_sl = data.get("kpi_so_luong", 0)
            pct_sl = data.get("pct_kpi_so_luong", 0)
            pct_ns = data.get("pct_kpi_ngan_sach", 0)
            
            if pct_sl >= 100:
                emoji = "🏆"
            elif pct_sl >= 70:
                emoji = "🟢"
            elif pct_sl >= 50:
                emoji = "🟡"
            else:
                emoji = "🔴"
            
            short_name = nhan_su.split(" - ")[0] if " - " in nhan_su else nhan_su
            lines.append(f"{emoji} **{short_name}**: {sl_air}/{kpi_sl} ({pct_sl}%) | NS: {pct_ns}%")
        
        lines.append("")
    
    if top_koc:
        lines.append("🌟 **TOP KOC DOANH SỐ**")
        lines.append("───────────────────────────")
        
        medals = ["🥇", "🥈", "🥉"]
        for i, (koc_id, gmv) in enumerate(top_koc[:5]):
            prefix = medals[i] if i < 3 else f"{i+1}."
            lines.append(f"{prefix} @{koc_id}: {format_currency(gmv)}")
        
        lines.append("")
    
    lines.append("───────────────────────────")
    lines.append("🧴 **Cheng Love Hair** | Jarvis v5.7.2")
    lines.append("💡 Tip: Hỏi \"KPI của Phương\" để xem chi tiết")
    
    return "\n".join(lines)
