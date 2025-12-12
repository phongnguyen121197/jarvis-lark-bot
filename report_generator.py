"""
Report Generator Module
Sử dụng OpenAI để sinh báo cáo đẹp từ dữ liệu
"""
import os
import json
from typing import Dict, Any
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

**PHẦN 2 - THEO PHÂN LOẠI SẢN PHẨM:**
- Liệt kê TẤT CẢ phân loại với số lượng KOC và chi phí

**PHẦN 3 - NHẬN XÉT TỪ BRAND MANAGER:**
Với kinh nghiệm 10 năm, hãy nhận xét:
- Đánh giá hiệu quả chiến dịch KOC (tỷ lệ air, chi phí/KOC)
- Phân tích vấn đề tồn đọng (KOC chưa air, chưa gắn giỏ...)
- So sánh hiệu quả giữa các phân loại sản phẩm
- Cảnh báo rủi ro nếu có (ví dụ: chi phí cao nhưng tỷ lệ air thấp)

**PHẦN 4 - ĐỀ XUẤT HÀNH ĐỘNG:**
Đưa ra 3-5 đề xuất CỤ THỂ với:
- Tên/ID KOC cần action (nếu có trong dữ liệu)
- Deadline đề xuất (trong 24h, 48h, tuần này...)
- Người/team nên phụ trách

Format output:
📊 Tóm tắt KOC tháng X:
• [số liệu]

📦 Theo phân loại sản phẩm:
• [phân loại]: X KOC (Y VNĐ)

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
    
    # Chuẩn bị data cho prompt
    summary = summary_data.get("summary", {})
    missing_link = summary_data.get("missing_link_kocs", [])
    missing_gio = summary_data.get("missing_gio_kocs", [])
    by_phan_loai = summary_data.get("by_phan_loai", {})
    
    # Format chi phí
    tong_chi_phi = summary.get("tong_chi_phi_deal", 0)
    chi_phi_formatted = f"{int(tong_chi_phi):,}".replace(",", ".") if tong_chi_phi else "0"
    
    # Format theo phân loại sản phẩm
    phan_loai_stats = []
    for pl, stats in by_phan_loai.items():
        chi_phi_pl = stats.get("chi_phi", 0)
        chi_phi_pl_formatted = f"{int(chi_phi_pl):,}".replace(",", ".") if chi_phi_pl else "0"
        
        # Lấy danh sách KOC chưa air hoặc cần follow-up trong phân loại này
        kocs_in_pl = stats.get("kocs", [])
        kocs_chua_air = [k.get("id_koc") for k in kocs_in_pl if not k.get("da_air")][:3]
        
        phan_loai_stats.append({
            "ten": pl,
            "count": stats.get("count", 0),
            "da_air": stats.get("da_air", 0),
            "chua_air": stats.get("chua_air", 0),
            "chi_phi": chi_phi_pl_formatted,
            "kocs_chua_air": kocs_chua_air  # KOC cụ thể cần follow
        })
    
    # Sort by count descending
    phan_loai_stats.sort(key=lambda x: x["count"], reverse=True)
    
    # Lấy danh sách KOC cụ thể cần follow-up
    kocs_can_link = [k.get("id_koc") or k.get("id_kenh") for k in missing_link[:5] if k.get("id_koc") or k.get("id_kenh")]
    kocs_can_gio = [k.get("id_koc") or k.get("id_kenh") for k in missing_gio[:5] if k.get("id_koc") or k.get("id_kenh")]
    
    # Tính toán metrics cho Brand Manager phân tích
    total = summary.get("total", 0)
    da_air = summary.get("da_air", 0)
    ty_le_air = round((da_air / total * 100), 1) if total > 0 else 0
    chi_phi_trung_binh = round(tong_chi_phi / total) if total > 0 else 0
    chi_phi_tb_formatted = f"{int(chi_phi_trung_binh):,}".replace(",", ".") if chi_phi_trung_binh else "0"
    
    data_for_prompt = {
        "month": summary_data.get("month"),
        "week": summary_data.get("week"),
        "total": total,
        "da_air": da_air,
        "chua_air": summary.get("chua_air", 0),
        "da_air_chua_link": summary.get("da_air_chua_link", 0),
        "da_air_chua_gan_gio": summary.get("da_air_chua_gan_gio", 0),
        "tong_chi_phi_deal": chi_phi_formatted,
        # Metrics cho Brand Manager
        "ty_le_air_percent": ty_le_air,
        "chi_phi_trung_binh_per_koc": chi_phi_tb_formatted,
        "theo_phan_loai": phan_loai_stats,  # Tất cả phân loại
        # Danh sách KOC CỤ THỂ cần action
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
            max_tokens=1500  # Tăng để có đủ chỗ cho nhận xét
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ OpenAI Error: {e}")
        # Fallback to simple format
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
    
    # Format data theo vị trí
    vi_tri_stats = []
    for vt, stats in by_vi_tri.items():
        vi_tri_stats.append({
            "ten": vt,
            "total": stats.get("total", 0),
            "overdue": stats.get("overdue", 0),
            "sap_deadline": stats.get("sap_deadline", 0)
        })
    
    # Sort by total descending
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
    by_phan_loai = summary_data.get("by_phan_loai", {})
    missing_link = summary_data.get("missing_link_kocs", [])
    missing_gio = summary_data.get("missing_gio_kocs", [])
    
    week_text = f" tuần {week}" if week else ""
    
    # Format chi phí
    tong_chi_phi = summary.get("tong_chi_phi_deal", 0)
    chi_phi_formatted = f"{int(tong_chi_phi):,}".replace(",", ".") if tong_chi_phi else "0"
    
    # Tính metrics
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
    
    # Thêm thống kê theo phân loại sản phẩm
    if by_phan_loai:
        text += "\n\n📦 Theo phân loại sản phẩm:"
        sorted_pl = sorted(by_phan_loai.items(), key=lambda x: x[1].get("count", 0), reverse=True)
        for pl, stats in sorted_pl:
            chi_phi_pl = stats.get("chi_phi", 0)
            chi_phi_pl_fmt = f"{int(chi_phi_pl):,}".replace(",", ".") if chi_phi_pl else "0"
            text += f"\n• {pl}: {stats.get('count', 0)} KOC ({chi_phi_pl_fmt} VNĐ)"
    
    # Nhận xét Brand Manager (simple version)
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
    
    # Đề xuất CỤ THỂ với tên KOC
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
