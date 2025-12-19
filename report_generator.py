"""
Report Generator Module
Sử dụng OpenAI để sinh báo cáo đẹp từ dữ liệu
Version 5.7.0 - Improved CHENG report formatting
"""
import os
import json
from typing import Dict, Any, Optional
from openai import AsyncOpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# ============ PROMPTS ============
KOC_REPORT_PROMPT = """Bạn là một Brand Manager có 10 năm kinh nghiệm trong ngành mỹ phẩm/nước hoa.

Dựa trên dữ liệu KOC dưới đây, hãy viết báo cáo và đưa ra nhận xét chuyên môn.

Dữ liệu:
{data}

Yêu cầu báo cáo gồm 4 phần:

**PHẦN 1 - TỔNG QUAN SỐ LIỆU:**
- Tổng KOC, đã air, chưa air, chưa có link, chưa gắn giỏ
- Tổng chi phí deal (format: X.XXX.XXX VNĐ)

**PHẦN 2 - THEO GROUP (xem field group_label để biết nhóm theo gì):**
- Liệt kê TẤT CẢ items trong theo_group với số lượng KOC và chi phí

**PHẦN 3 - NHẬN XÉT TỪ BRAND MANAGER:**
Với kinh nghiệm 10 năm, hãy nhận xét:
- Đánh giá hiệu quả chiến dịch KOC (tỷ lệ air, chi phí/KOC)
- Phân tích vấn đề tồn đọng (KOC chưa air, chưa gắn giỏ...)
- So sánh hiệu quả giữa các nhóm
- Cảnh báo rủi ro nếu có (ví dụ: chi phí cao nhưng tỷ lệ air thấp)

**PHẦN 4 - ĐỀ XUẤT HÀNH ĐỘNG:**
Đưa ra 3-5 đề xuất CỤ THỂ với:
- Tên/ID KOC cần action (nếu có trong dữ liệu)
- Deadline đề xuất (trong 24h, 48h, tuần này...)
- Người/team nên phụ trách

Format output:
📊 Tóm tắt KOC tháng X:
• [số liệu]

📦 Theo [group_label]:
• [tên]: X KOC (Y VNĐ)

💼 Nhận xét từ Brand Manager:
• [nhận xét chuyên môn]

🎯 Đề xuất hành động:
• [hành động cụ thể với tên KOC, deadline]

Lưu ý:
- Viết bằng tiếng Việt
- KHÔNG dùng markdown headers (#)
- Giọng văn chuyên nghiệp nhưng thực tế
- Nhận xét phải dựa trên DATA, không suy đoán
"""

TASK_SUMMARY_PROMPT = """Dựa trên dữ liệu task dưới đây, hãy viết báo cáo phân tích task theo vị trí.

Dữ liệu:
{data}

Yêu cầu:
- Tổng quan: tổng task, quá hạn, sắp deadline
- Phân tích theo từng vị trí (HR, Content Creator, Ecommerce, etc.)
- Highlight các task quá hạn và sắp đến deadline (trong 3 ngày)
- Giọng văn ngắn gọn, chuyên nghiệp
- Viết bằng tiếng Việt
- KHÔNG sử dụng markdown headers (#), chỉ dùng bullet points (•)

Ví dụ format:
📋 Phân tích Task:

• Tổng: X task
• Quá hạn (overdue): Y task ⚠️
• Sắp đến deadline (3 ngày): Z task

👥 Theo vị trí:
• HR: X task (Y overdue)
• Content Creator: X task (Y overdue)
• Ecommerce: X task (Y overdue)

⚠️ Task quá hạn cần xử lý:
• [Tên dự án] - [Người phụ trách]

⏰ Task sắp deadline:
• [Tên dự án] - deadline [ngày]

🎯 Đề xuất:
• [hành động ưu tiên]
"""

CONTENT_CALENDAR_PROMPT = """Dựa trên danh sách task content dưới đây, hãy viết tóm tắt lịch content.

Dữ liệu:
{data}

Yêu cầu:
- Nhóm theo team/người phụ trách
- Highlight ngày có nhiều content
- Highlight các task overdue/trễ deadline
- Tối đa 6-8 bullet points
- Viết bằng tiếng Việt
- KHÔNG sử dụng markdown headers (#), chỉ dùng bullet points (•)

Ví dụ format:
📅 Lịch content tuần này:

• Tổng X task trong tuần
• Team Content: Y task
• Team Design: Z task
• A task đang overdue - cần ưu tiên
• Ngày B có nhiều content nhất (C bài)

⚠️ Cần chú ý:
• [task quan trọng]
"""

GENERAL_SUMMARY_PROMPT = """Dựa trên dữ liệu KOC và Content dưới đây, hãy viết báo cáo tổng hợp tuần.

Dữ liệu KOC:
{koc_data}

Dữ liệu Content:
{content_data}

Yêu cầu:
- Tổng hợp cả 2 mảng KOC và Content
- Viết ngắn gọn, dễ đọc
- Highlight các điểm cần chú ý
- Viết bằng tiếng Việt
- KHÔNG sử dụng markdown headers (#), chỉ dùng bullet points (•)
- Tối đa 10 bullet points

Format:
🗓️ Tổng hợp tuần {week}:

📢 KOC/PR:
• ...

📝 Content:
• ...

🎯 Action items:
• ...
"""

# ============ GENERATORS ============
async def generate_koc_report_text(summary_data: Dict[str, Any]) -> str:
    """Sinh báo cáo KOC từ dữ liệu summary (bao gồm chi phí và phân loại sản phẩm)"""
    
    summary = summary_data.get("summary", {})
    missing_link = summary_data.get("missing_link_kocs", [])
    missing_gio = summary_data.get("missing_gio_kocs", [])
    by_group = summary_data.get("by_group", {})
    group_label = summary_data.get("group_label", "sản phẩm")
    
    tong_chi_phi = summary.get("tong_chi_phi_deal", 0)
    chi_phi_formatted = f"{int(tong_chi_phi):,}".replace(",", ".") if tong_chi_phi else "0"
    
    group_stats = []
    for name, stats in by_group.items():
        chi_phi_g = stats.get("chi_phi", 0)
        chi_phi_g_formatted = f"{int(chi_phi_g):,}".replace(",", ".") if chi_phi_g else "0"
        
        kocs_in_g = stats.get("kocs", [])
        kocs_chua_air = [k.get("id_koc") for k in kocs_in_g if not k.get("da_air")][:3]
        
        group_stats.append({
            "ten": name,
            "count": stats.get("count", 0),
            "da_air": stats.get("da_air", 0),
            "chua_air": stats.get("chua_air", 0),
            "chi_phi": chi_phi_g_formatted,
            "kocs_chua_air": kocs_chua_air
        })
    
    group_stats.sort(key=lambda x: x["count"], reverse=True)
    
    kocs_can_link = [k.get("id_koc") or k.get("id_kenh") for k in missing_link[:5] if k.get("id_koc") or k.get("id_kenh")]
    kocs_can_gio = [k.get("id_koc") or k.get("id_kenh") for k in missing_gio[:5] if k.get("id_koc") or k.get("id_kenh")]
    
    total = summary.get("total", 0)
    da_air = summary.get("da_air", 0)
    ty_le_air = round((da_air / total * 100), 1) if total > 0 else 0
    chi_phi_trung_binh = round(tong_chi_phi / total) if total > 0 else 0
    chi_phi_tb_formatted = f"{int(chi_phi_trung_binh):,}".replace(",", ".") if chi_phi_trung_binh else "0"
    
    data_for_prompt = {
        "month": summary_data.get("month"),
        "week": summary_data.get("week"),
        "group_label": group_label,
        "total": total,
        "da_air": da_air,
        "chua_air": summary.get("chua_air", 0),
        "da_air_chua_link": summary.get("da_air_chua_link", 0),
        "da_air_chua_gan_gio": summary.get("da_air_chua_gan_gio", 0),
        "tong_chi_phi_deal": chi_phi_formatted,
        "ty_le_air_percent": ty_le_air,
        "chi_phi_trung_binh_per_koc": chi_phi_tb_formatted,
        "theo_group": group_stats,
        "kocs_can_cap_nhat_link": kocs_can_link,
        "kocs_can_gan_gio": kocs_can_gio,
    }
    
    prompt = KOC_REPORT_PROMPT.format(data=json.dumps(data_for_prompt, ensure_ascii=False, indent=2))
    
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là Brand Manager có 10 năm kinh nghiệm trong ngành mỹ phẩm/nước hoa. Bạn có khả năng phân tích dữ liệu KOC sâu sắc, nhận ra các vấn đề tiềm ẩn và đưa ra đề xuất thực tế, cụ thể. Khi đề xuất, LUÔN nêu rõ TÊN/ID KOC, deadline và người phụ trách."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1500
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ OpenAI Error: {e}")
        return format_koc_report_simple(summary_data)

async def generate_content_calendar_text(calendar_data: Dict[str, Any]) -> str:
    """Sinh báo cáo lịch content"""
    
    summary = calendar_data.get("summary", {})
    by_vi_tri = calendar_data.get("by_vi_tri", {})
    overdue = calendar_data.get("overdue_tasks", [])
    date_range = calendar_data.get("date_range", "tuần này")
    
    data_for_prompt = {
        "date_range": date_range,
        "total_tasks": summary.get("total_tasks", 0),
        "total_overdue": summary.get("total_overdue", 0),
        "vi_tri": {vt: len(tasks) for vt, tasks in by_vi_tri.items()},
        "overdue_samples": [t.get("du_an") for t in overdue[:5]],
    }
    
    prompt = CONTENT_CALENDAR_PROMPT.format(
        data=json.dumps(data_for_prompt, ensure_ascii=False, indent=2)
    )
    
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý AI chuyên viết báo cáo lịch content ngắn gọn."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=800
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ OpenAI Error: {e}")
        return format_content_calendar_simple(calendar_data)


async def generate_task_summary_text(task_data: Dict[str, Any]) -> str:
    """Sinh báo cáo phân tích task theo vị trí"""
    
    summary = task_data.get("summary", {})
    by_vi_tri = task_data.get("by_vi_tri", {})
    overdue_tasks = task_data.get("overdue_tasks", [])
    sap_deadline_tasks = task_data.get("sap_deadline_tasks", [])
    month = task_data.get("month")
    
    vi_tri_stats = []
    for vt, stats in by_vi_tri.items():
        vi_tri_stats.append({
            "ten": vt,
            "total": stats.get("total", 0),
            "overdue": stats.get("overdue", 0),
            "sap_deadline": stats.get("sap_deadline", 0)
        })
    
    vi_tri_stats.sort(key=lambda x: x["total"], reverse=True)
    
    data_for_prompt = {
        "month": month,
        "total_tasks": summary.get("total_tasks", 0),
        "total_overdue": summary.get("total_overdue", 0),
        "total_sap_deadline": summary.get("total_sap_deadline", 0),
        "theo_vi_tri": vi_tri_stats,
        "overdue_samples": [
            {"du_an": t.get("du_an"), "nguoi": t.get("nguoi_phu_trach"), "vi_tri": t.get("vi_tri")}
            for t in overdue_tasks[:5]
        ],
        "sap_deadline_samples": [
            {"du_an": t.get("du_an"), "deadline": t.get("deadline"), "nguoi": t.get("nguoi_phu_trach")}
            for t in sap_deadline_tasks[:5]
        ],
    }
    
    prompt = TASK_SUMMARY_PROMPT.format(data=json.dumps(data_for_prompt, ensure_ascii=False, indent=2))
    
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý AI chuyên phân tích và báo cáo task management."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1000
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ OpenAI Error: {e}")
        return format_task_summary_simple(task_data)

async def generate_general_summary_text(koc_data: Dict, content_data: Dict) -> str:
    """Sinh báo cáo tổng hợp"""
    
    prompt = GENERAL_SUMMARY_PROMPT.format(
        koc_data=json.dumps(koc_data.get("summary", {}), ensure_ascii=False),
        content_data=json.dumps(content_data.get("summary", {}), ensure_ascii=False),
        week="này"
    )
    
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý AI chuyên viết báo cáo tổng hợp marketing."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1000
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ OpenAI Error: {e}")
        return "Không thể tạo báo cáo. Vui lòng thử lại."

# ============ FALLBACK FORMATTERS ============
def format_koc_report_simple(summary_data: Dict[str, Any]) -> str:
    """Format báo cáo KOC đơn giản (fallback)"""
    summary = summary_data.get("summary", {})
    month = summary_data.get("month")
    week = summary_data.get("week")
    by_group = summary_data.get("by_group", {})
    group_label = summary_data.get("group_label", "sản phẩm")
    missing_link = summary_data.get("missing_link_kocs", [])
    missing_gio = summary_data.get("missing_gio_kocs", [])
    
    week_text = f" tuần {week}" if week else ""
    
    tong_chi_phi = summary.get("tong_chi_phi_deal", 0)
    chi_phi_formatted = f"{int(tong_chi_phi):,}".replace(",", ".") if tong_chi_phi else "0"
    
    total = summary.get('total', 0)
    da_air = summary.get('da_air', 0)
    ty_le_air = round((da_air / total * 100), 1) if total > 0 else 0
    chi_phi_tb = round(tong_chi_phi / total) if total > 0 else 0
    chi_phi_tb_fmt = f"{int(chi_phi_tb):,}".replace(",", ".") if chi_phi_tb else "0"
    
    text = f"""📊 Tóm tắt KOC tháng {month}{week_text}:

• Tổng: {total} KOC đã deal
• Đã air: {da_air} KOC ({ty_le_air}%)
• Chưa air: {summary.get('chua_air', 0)} KOC
• Đã air nhưng chưa có link: {summary.get('da_air_chua_link', 0)} KOC
• Đã air nhưng chưa gắn giỏ: {summary.get('da_air_chua_gan_gio', 0)} KOC

💰 Chi phí:
• Tổng chi phí deal: {chi_phi_formatted} VNĐ
• Chi phí trung bình/KOC: {chi_phi_tb_fmt} VNĐ"""
    
    if by_group:
        text += f"\n\n📦 Theo {group_label}:"
        sorted_g = sorted(by_group.items(), key=lambda x: x[1].get("count", 0), reverse=True)
        for name, stats in sorted_g:
            chi_phi_g = stats.get("chi_phi", 0)
            chi_phi_g_fmt = f"{int(chi_phi_g):,}".replace(",", ".") if chi_phi_g else "0"
            text += f"\n• {name}: {stats.get('count', 0)} KOC ({chi_phi_g_fmt} VNĐ)"
    
    text += "\n\n💼 Nhận xét từ Brand Manager:"
    if ty_le_air >= 90:
        text += f"\n• Tỷ lệ air {ty_le_air}% rất tốt, chiến dịch đang đi đúng hướng"
    elif ty_le_air >= 70:
        text += f"\n• Tỷ lệ air {ty_le_air}% ở mức khá, cần đẩy nhanh tiến độ"
    else:
        text += f"\n• ⚠️ Tỷ lệ air {ty_le_air}% thấp, cần review lại quy trình"
    
    chua_gan_gio = summary.get('da_air_chua_gan_gio', 0)
    if chua_gan_gio > 0:
        text += f"\n• ⚠️ {chua_gan_gio} KOC chưa gắn giỏ = mất cơ hội chuyển đổi"
    
    if summary.get('da_air_chua_link', 0) > 0 or summary.get('da_air_chua_gan_gio', 0) > 0:
        text += "\n\n🎯 Đề xuất hành động:"
        
        if missing_link:
            koc_names = [k.get("id_koc") or k.get("id_kenh") for k in missing_link[:3] if k.get("id_koc") or k.get("id_kenh")]
            if koc_names:
                text += f"\n• [Trong 24h] Cập nhật link air: {', '.join(koc_names)}"
        
        if missing_gio:
            koc_names = [k.get("id_koc") or k.get("id_kenh") for k in missing_gio[:3] if k.get("id_koc") or k.get("id_kenh")]
            if koc_names:
                text += f"\n• [Trong 48h] Follow-up gắn giỏ: {', '.join(koc_names)}"
    
    return text

def format_content_calendar_simple(calendar_data: Dict[str, Any]) -> str:
    """Format lịch content đơn giản (fallback)"""
    summary = calendar_data.get("summary", {})
    date_range = calendar_data.get("date_range", "tuần này")
    
    text = f"""📅 Lịch content {date_range}:

• Tổng: {summary.get('total_tasks', 0)} task
• Overdue: {summary.get('total_overdue', 0)} task
• Số ngày có content: {summary.get('days_with_content', 0)}
• Số vị trí: {summary.get('vi_tri_count', 0)}"""
    
    return text

def format_task_summary_simple(task_data: Dict[str, Any]) -> str:
    """Format báo cáo task đơn giản (fallback)"""
    summary = task_data.get("summary", {})
    by_vi_tri = task_data.get("by_vi_tri", {})
    month = task_data.get("month")
    
    month_text = f" tháng {month}" if month else ""
    
    text = f"""📋 Phân tích Task{month_text}:

• Tổng: {summary.get('total_tasks', 0)} task
• Quá hạn (overdue): {summary.get('total_overdue', 0)} task ⚠️
• Sắp đến deadline (3 ngày): {summary.get('total_sap_deadline', 0)} task

👥 Theo vị trí:"""
    
    sorted_vt = sorted(by_vi_tri.items(), key=lambda x: x[1].get("total", 0), reverse=True)
    for vt, stats in sorted_vt[:5]:
        overdue = stats.get("overdue", 0)
        overdue_text = f" ({overdue} overdue)" if overdue > 0 else ""
        text += f"\n• {vt}: {stats.get('total', 0)} task{overdue_text}"
    
    if summary.get('total_overdue', 0) > 0:
        text += "\n\n⚠️ Cần xử lý các task quá hạn ngay!"
    
    return text


# ============ GPT CHAT ============
async def chat_with_gpt(question: str) -> str:
    """Gửi câu hỏi trực tiếp đến ChatGPT"""
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": """Bạn là Jarvis - trợ lý AI thông minh của team marketing. 
Bạn có thể trả lời mọi câu hỏi một cách thân thiện và hữu ích.
Trả lời bằng tiếng Việt.
Giữ câu trả lời ngắn gọn, súc tích (tối đa 500 từ).
Sử dụng emoji phù hợp để làm nội dung sinh động hơn."""
                },
                {"role": "user", "content": question}
            ],
            temperature=0.8,
            max_tokens=1000
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ GPT Chat Error: {e}")
        return f"❌ Xin lỗi, tôi không thể xử lý câu hỏi này lúc này. Lỗi: {str(e)}"


# ============ DASHBOARD REPORT ============

def format_currency(value):
    """Format số tiền thành dạng đọc được (VD: 77.4M, 5.5M, 850K)"""
    if value >= 1_000_000_000:
        return f"{value/1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.0f}K"
    else:
        return f"{value:,.0f}"


async def generate_dashboard_report_text(data: dict, report_type: str = "full", nhan_su_filter: str = None) -> str:
    """
    Sinh báo cáo Dashboard dạng text
    report_type: "full", "top_koc", "lien_he", "kpi_nhan_su", "kpi_ca_nhan", "canh_bao"
    nhan_su_filter: Tên nhân sự cụ thể (cho report_type = "kpi_ca_nhan")
    """
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
    
    # ===== KPI CÁ NHÂN =====
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
            pct_ns = matched_kpi.get("pct_kpi_ngan_sach", 0)
            sl_air = matched_kpi.get("so_luong_air", 0)
            kpi_sl = matched_kpi.get("kpi_so_luong", 0)
            ns_air = matched_kpi.get("ngan_sach_air", 0)
            kpi_ns = matched_kpi.get("kpi_ngan_sach", 0)
            
            if pct_sl >= 50:
                status = "🟢 Đang trên tiến độ"
            elif pct_sl >= 20:
                status = "🟡 Cần cố gắng thêm"
            else:
                status = "🔴 Dưới tiến độ"
            
            lines.append(f"**Trạng thái:** {status}\n")
            lines.append(f"📦 **KPI Số lượng:**")
            lines.append(f"   • Đã air: {sl_air}/{kpi_sl} video")
            lines.append(f"   • Tiến độ: {pct_sl}%")
            lines.append(f"   • Còn thiếu: {max(0, kpi_sl - sl_air)} video\n")
            
            lines.append(f"💰 **KPI Ngân sách:**")
            lines.append(f"   • Đã air: {format_currency(ns_air)}/{format_currency(kpi_ns)}")
            lines.append(f"   • Tiến độ: {pct_ns}%\n")
            
            if is_after_15 and is_current_month and pct_sl < 50:
                lines.append(f"⚠️ **CẢNH BÁO:** Đã qua ngày 15, KPI mới đạt {pct_sl}%!")
                remaining_days = 30 - current_day
                videos_needed = kpi_sl - sl_air
                if remaining_days > 0:
                    daily_target = round(videos_needed / remaining_days, 1)
                    lines.append(f"📌 Cần air thêm ~{daily_target} video/ngày để đạt KPI\n")
        
        if matched_lh:
            tong = matched_lh.get("tong_lien_he", 0)
            ty_le_deal = matched_lh.get("ty_le_deal", 0)
            ty_le_trao_doi = matched_lh.get("ty_le_trao_doi", 0)
            ty_le_tu_choi = matched_lh.get("ty_le_tu_choi", 0)
            da_deal = matched_lh.get("da_deal", 0)
            
            lines.append(f"📞 **Liên hệ KOC:**")
            lines.append(f"   • Tổng liên hệ: {tong}")
            lines.append(f"   • ✅ Đã deal: {da_deal} ({ty_le_deal}%)")
            lines.append(f"   • 💬 Đang trao đổi: {ty_le_trao_doi}%")
            lines.append(f"   • ❌ Từ chối: {ty_le_tu_choi}%")
        
        return "\n".join(lines)
    
    # ===== BÁO CÁO CẢNH BÁO =====
    if report_type == "canh_bao":
        lines.append(f"⚠️ **CẢNH BÁO KPI - {time_label.upper()}**\n")
        
        if not is_after_15:
            lines.append(f"📅 Hôm nay là ngày {current_day}, chưa đến mốc kiểm tra (ngày 15).")
            lines.append("Hệ thống sẽ cảnh báo khi qua ngày 15 mà KPI < 50%.")
            return "\n".join(lines)
        
        lines.append(f"📅 Hôm nay là ngày {current_day}, đã qua mốc kiểm tra ngày 15.\n")
        
        warning_list = []
        ok_list = []
        
        for nhan_su, kpi in kpi_nhan_su.items():
            if nhan_su == "Không xác định":
                continue
            pct_sl = kpi.get("pct_kpi_so_luong", 0)
            sl_air = kpi.get("so_luong_air", 0)
            kpi_sl = kpi.get("kpi_so_luong", 0)
            
            if pct_sl < 50:
                warning_list.append((nhan_su, pct_sl, sl_air, kpi_sl))
            else:
                ok_list.append((nhan_su, pct_sl))
        
        if warning_list:
            lines.append("🔴 **NHÂN SỰ CẦN CHÚ Ý:**")
            lines.append("═══════════════════════════")
            for ns, pct, done, target in sorted(warning_list, key=lambda x: x[1]):
                remaining = target - done
                lines.append(f"⚠️ **{ns}**: {pct}% ({done}/{target})")
                lines.append(f"   → Cần thêm {remaining} video")
            lines.append("")
        else:
            lines.append("✅ Tất cả nhân sự đều đạt >= 50% KPI!")
        
        if ok_list:
            lines.append("\n🟢 **NHÂN SỰ ĐẠT TIẾN ĐỘ:**")
            for ns, pct in sorted(ok_list, key=lambda x: x[1], reverse=True):
                lines.append(f"✅ {ns}: {pct}%")
        
        return "\n".join(lines)
    
    # ===== BÁO CÁO THÔNG THƯỜNG =====
    lines.append(f"📊 **DASHBOARD {time_label.upper()}**\n")
    
    # ===== TỔNG QUAN =====
    if report_type in ["full", "kpi_nhan_su"]:
        lines.append("═══════════════════════════")
        lines.append("📈 **TỔNG QUAN KPI**")
        lines.append("═══════════════════════════")
        
        kpi_sl = tong_quan.get("kpi_so_luong", 0)
        sl_air = tong_quan.get("so_luong_air", 0)
        pct_sl = tong_quan.get("pct_kpi_so_luong", 0)
        lines.append(f"📦 Số lượng Air: {sl_air}/{kpi_sl} ({pct_sl}%)")
        
        kpi_ns = tong_quan.get("kpi_ngan_sach", 0)
        ns_air = tong_quan.get("ngan_sach_air", 0)
        pct_ns = tong_quan.get("pct_kpi_ngan_sach", 0)
        lines.append(f"💰 Ngân sách Air: {format_currency(ns_air)}/{format_currency(kpi_ns)} ({pct_ns}%)")
        
        total_gmv = tong_quan.get("total_gmv", 0)
        if total_gmv > 0:
            lines.append(f"🏆 Tổng GMV KOC: {format_currency(total_gmv)}")
        
        if is_after_15 and is_current_month and pct_sl < 50:
            lines.append(f"\n⚠️ **CẢNH BÁO:** Đã qua ngày 15, KPI tổng mới đạt {pct_sl}%!")
        
        lines.append("")
    
    # ===== KPI NHÂN SỰ =====
    if report_type in ["full", "kpi_nhan_su"] and kpi_nhan_su:
        lines.append("═══════════════════════════")
        lines.append("👥 **KPI THEO NHÂN SỰ**")
        lines.append("═══════════════════════════")
        
        sorted_ns = sorted(kpi_nhan_su.items(), key=lambda x: x[1].get("pct_kpi_so_luong", 0), reverse=True)
        
        warning_count = 0
        for nhan_su, kpi in sorted_ns:
            if nhan_su == "Không xác định":
                continue
            pct_sl = kpi.get("pct_kpi_so_luong", 0)
            pct_ns = kpi.get("pct_kpi_ngan_sach", 0)
            sl_air = kpi.get("so_luong_air", 0)
            kpi_sl = kpi.get("kpi_so_luong", 0)
            
            if pct_sl >= 50:
                emoji = "🟢"
            elif pct_sl >= 20:
                emoji = "🟡"
            else:
                emoji = "🔴"
                if is_after_15 and is_current_month:
                    warning_count += 1
            
            lines.append(f"{emoji} **{nhan_su}**: {sl_air}/{kpi_sl} ({pct_sl}% SL | {pct_ns}% NS)")
        
        if warning_count > 0 and is_after_15 and is_current_month:
            lines.append(f"\n⚠️ Có {warning_count} nhân sự KPI < 20% sau ngày 15!")
        
        lines.append("")
    
    # ===== TOP KOC DOANH SỐ =====
    if report_type in ["full", "top_koc"] and top_koc:
        lines.append("═══════════════════════════")
        lines.append("🏅 **TOP 10 KOC DOANH SỐ**")
        lines.append("═══════════════════════════")
        
        medals = ["🥇", "🥈", "🥉"]
        for i, (koc_id, gmv) in enumerate(top_koc[:10]):
            if i < 3:
                prefix = medals[i]
            else:
                prefix = f"{i+1}."
            lines.append(f"{prefix} @{koc_id}: {format_currency(gmv)}")
        
        lines.append("")
    
    # ===== LIÊN HỆ NHÂN SỰ =====
    if report_type in ["full", "lien_he"] and lien_he_nhan_su:
        lines.append("═══════════════════════════")
        lines.append("📞 **TỶ LỆ LIÊN HỆ NHÂN SỰ**")
        lines.append("═══════════════════════════")
        
        sorted_lh = sorted(lien_he_nhan_su.items(), key=lambda x: x[1].get("tong_lien_he", 0), reverse=True)
        
        for nhan_su, lh in sorted_lh:
            if nhan_su == "Không xác định":
                continue
            tong = lh.get("tong_lien_he", 0)
            if tong == 0:
                continue
            
            ty_le_deal = lh.get("ty_le_deal", 0)
            ty_le_trao_doi = lh.get("ty_le_trao_doi", 0)
            ty_le_tu_choi = lh.get("ty_le_tu_choi", 0)
            
            lines.append(f"👤 **{nhan_su}** ({tong} liên hệ)")
            lines.append(f"   ✅ Deal: {ty_le_deal}% | 💬 Trao đổi: {ty_le_trao_doi}% | ❌ Từ chối: {ty_le_tu_choi}%")
        
        lines.append("")
    
    lines.append("───────────────────────────")
    lines.append("💡 Tip: Hỏi \"KPI của Mai\" hoặc \"Cảnh báo KPI\" để xem chi tiết")
    
    return "\n".join(lines)


# ============ CHENG REPORT (Updated v5.7.0) ============

async def generate_cheng_report_text(summary_data: Dict[str, Any], report_type: str = "full", nhan_su_filter: str = None) -> str:
    """
    Sinh báo cáo KOC cho CHENG từ dữ liệu summary
    Updated v5.7.1: Added nhan_su_filter support for individual KPI reports
    
    report_type: "full", "kpi_ca_nhan"
    nhan_su_filter: Tên nhân sự cụ thể (cho report_type = "kpi_ca_nhan")
    """
    from datetime import datetime
    
    tong_quan = summary_data.get("tong_quan", {})
    kpi_nhan_su = summary_data.get("kpi_nhan_su", {})
    lien_he_nhan_su = summary_data.get("lien_he_nhan_su", {})
    top_koc = summary_data.get("top_koc", [])
    month = summary_data.get("month")
    week = summary_data.get("week")
    
    lines = []
    
    # === KPI CÁ NHÂN (CHENG) ===
    if report_type == "kpi_ca_nhan" and nhan_su_filter:
        lines.append("🧴 **KPI CÁ NHÂN - CHENG**")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        week_text = f" - Tuần {week}" if week else ""
        lines.append(f"📅 Tháng {month}{week_text}")
        lines.append("")
        
        # Tìm nhân sự trong KPI data
        found_kpi = None
        for ns, data in kpi_nhan_su.items():
            if nhan_su_filter.lower() in ns.lower() or ns.lower() in nhan_su_filter.lower():
                found_kpi = (ns, data)
                break
        
        # Tìm nhân sự trong liên hệ data
        found_lh = None
        for ns, data in lien_he_nhan_su.items():
            if nhan_su_filter.lower() in ns.lower() or ns.lower() in nhan_su_filter.lower():
                found_lh = (ns, data)
                break
        
        if not found_kpi and not found_lh:
            lines.append(f"❌ Không tìm thấy nhân sự CHENG: {nhan_su_filter}")
            lines.append("")
            lines.append("💡 Gợi ý: Kiểm tra lại tên hoặc dùng '@Jarvis báo cáo CHENG tháng X' để xem danh sách nhân sự")
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
            
            # Status emoji
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
            lines.append("")
            
            # Progress bar
            progress_filled = int(pct_sl / 10)
            progress_empty = 10 - progress_filled
            progress_bar = "▓" * progress_filled + "░" * progress_empty
            lines.append(f"📊 Tiến độ: [{progress_bar}] {pct_sl}%")
        
        if found_lh:
            ns, data = found_lh
            lines.append("")
            lines.append("📞 **LIÊN HỆ KOC:**")
            lines.append(f"   • Tổng liên hệ: {data.get('tong_lien_he', 0)}")
            lines.append(f"   • Đã deal: {data.get('da_deal', 0)} ({data.get('ty_le_deal', 0)}%)")
            lines.append(f"   • Đang trao đổi: {data.get('dang_trao_doi', 0)} ({data.get('ty_le_trao_doi', 0)}%)")
            lines.append(f"   • Từ chối: {data.get('tu_choi', 0)} ({data.get('ty_le_tu_choi', 0)}%)")
        
        return "\n".join(lines)
    
    # === FULL REPORT (default) ===
    # Header
    lines.append("🧴 **BÁO CÁO KOC - CHENG LOVE HAIR**")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    week_text = f" - Tuần {week}" if week else ""
    lines.append(f"📅 Tháng {month}{week_text}")
    lines.append(f"🕐 Cập nhật: {datetime.now().strftime('%H:%M %d/%m/%Y')}")
    lines.append("")
    
    # === TỔNG QUAN KPI ===
    lines.append("📊 **TỔNG QUAN KPI**")
    lines.append("───────────────────────────")
    
    kpi_sl = tong_quan.get("kpi_so_luong", 0)
    sl_air = tong_quan.get("so_luong_air", 0)
    pct_sl = tong_quan.get("pct_kpi_so_luong", 0)
    
    kpi_ns = tong_quan.get("kpi_ngan_sach", 0)
    ns_air = tong_quan.get("ngan_sach_air", 0)
    pct_ns = tong_quan.get("pct_kpi_ngan_sach", 0)
    
    total_gmv = tong_quan.get("total_gmv", 0)
    
    lines.append(f"📦 **Số lượng:** {sl_air}/{kpi_sl} video ({pct_sl}%)")
    lines.append(f"💰 **Ngân sách:** {format_currency(ns_air)}/{format_currency(kpi_ns)} ({pct_ns}%)")
    
    if total_gmv > 0:
        lines.append(f"📈 **GMV KOC:** {format_currency(total_gmv)}")
    
    # Progress bar visual
    progress_filled = int(pct_sl / 10)
    progress_empty = 10 - progress_filled
    progress_bar = "▓" * progress_filled + "░" * progress_empty
    lines.append(f"📊 [{progress_bar}] {pct_sl}%")
    lines.append("")
    
    # === KPI THEO NHÂN SỰ ===
    if kpi_nhan_su:
        lines.append("👥 **KPI THEO NHÂN SỰ**")
        lines.append("───────────────────────────")
        
        # Sort theo % KPI số lượng giảm dần
        sorted_nhan_su = sorted(
            kpi_nhan_su.items(), 
            key=lambda x: x[1].get("pct_kpi_so_luong", 0), 
            reverse=True
        )
        
        for nhan_su, data in sorted_nhan_su:
            if not nhan_su or nhan_su == "Không xác định":
                continue
                
            sl_air = data.get("so_luong_air", 0)
            kpi_sl = data.get("kpi_so_luong", 0)
            pct_sl = data.get("pct_kpi_so_luong", 0)
            pct_ns = data.get("pct_kpi_ngan_sach", 0)
            
            # Emoji theo tiến độ
            if pct_sl >= 100:
                emoji = "🏆"
            elif pct_sl >= 70:
                emoji = "🟢"
            elif pct_sl >= 50:
                emoji = "🟡"
            elif pct_sl >= 20:
                emoji = "🟠"
            else:
                emoji = "🔴"
            
            # Rút gọn tên nếu quá dài
            short_name = nhan_su.split(" - ")[0] if " - " in nhan_su else nhan_su
            if len(short_name) > 20:
                short_name = short_name[:17] + "..."
            
            lines.append(f"{emoji} **{short_name}**: {sl_air}/{kpi_sl} ({pct_sl}%) | NS: {pct_ns}%")
        
        lines.append("")
    
    # === LIÊN HỆ THEO NHÂN SỰ ===
    if lien_he_nhan_su:
        lines.append("📞 **LIÊN HỆ KOC**")
        lines.append("───────────────────────────")
        
        # Sort theo tổng liên hệ
        sorted_lh = sorted(
            lien_he_nhan_su.items(),
            key=lambda x: x[1].get("tong_lien_he", 0),
            reverse=True
        )
        
        for nhan_su, data in sorted_lh:
            if not nhan_su or nhan_su == "Không xác định":
                continue
            
            tong = data.get("tong_lien_he", 0)
            if tong == 0:
                continue
            
            da_deal = data.get("da_deal", 0)
            ty_le_deal = data.get("ty_le_deal", 0)
            ty_le_trao_doi = data.get("ty_le_trao_doi", 0)
            ty_le_tu_choi = data.get("ty_le_tu_choi", 0)
            
            short_name = nhan_su.split(" - ")[0] if " - " in nhan_su else nhan_su
            if len(short_name) > 20:
                short_name = short_name[:17] + "..."
            
            lines.append(f"👤 **{short_name}** ({tong} liên hệ)")
            lines.append(f"   ✅ Deal: {da_deal} ({ty_le_deal}%) | 💬 Trao đổi: {ty_le_trao_doi}% | ❌ Từ chối: {ty_le_tu_choi}%")
        
        lines.append("")
    
    # === TOP KOC DOANH SỐ ===
    if top_koc:
        lines.append("🌟 **TOP KOC DOANH SỐ**")
        lines.append("───────────────────────────")
        
        medals = ["🥇", "🥈", "🥉"]
        for i, (koc_id, gmv) in enumerate(top_koc[:5]):
            if i < 3:
                prefix = medals[i]
            else:
                prefix = f"{i+1}."
            
            gmv_fmt = format_currency(gmv)
            lines.append(f"{prefix} @{koc_id}: {gmv_fmt}")
        
        lines.append("")
    
    # Footer
    lines.append("───────────────────────────")
    lines.append("🧴 **Cheng Love Hair** | Báo cáo tự động bởi Jarvis")
    lines.append("💡 Tip: Hỏi \"KPI của [tên]\" để xem chi tiết cá nhân")
    
    return "\n".join(lines)
