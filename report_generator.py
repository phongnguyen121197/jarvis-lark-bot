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
KOC_REPORT_PROMPT = """Dựa trên dữ liệu KOC dưới đây, hãy viết báo cáo ngắn gọn cho Marketing Manager.

Dữ liệu:
{data}

Yêu cầu:
- Viết 5-8 bullet points chính
- Nhấn mạnh số lượng: tổng KOC, đã air, chưa air, chưa có link, chưa gắn giỏ
- Tổng chi phí deal (format: X,XXX,XXX VNĐ)
- Nếu có thống kê theo sản phẩm, liệt kê top 3 sản phẩm
- Giọng văn ngắn gọn, chuyên nghiệp
- Nếu có vấn đề cần follow-up, đề xuất 2-3 hành động ưu tiên
- Viết bằng tiếng Việt
- KHÔNG sử dụng markdown headers (#), chỉ dùng bullet points (•)

Ví dụ format:
📊 Tóm tắt KOC tháng X:

• Tổng X KOC đã deal
• Y KOC đã air (Z%)
• A KOC chưa air
• B KOC đã air nhưng chưa có link - cần follow up
• C KOC đã air nhưng chưa gắn giỏ

💰 Chi phí:
• Tổng chi phí deal: X,XXX,XXX VNĐ

📦 Theo sản phẩm:
• Sản phẩm A: X KOC (Y VNĐ)
• Sản phẩm B: X KOC (Y VNĐ)

🎯 Đề xuất:
• [hành động 1]
• [hành động 2]
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
    """Sinh báo cáo KOC từ dữ liệu summary (bao gồm chi phí và sản phẩm)"""
    
    # Chuẩn bị data cho prompt
    summary = summary_data.get("summary", {})
    missing_link = summary_data.get("missing_link_kocs", [])
    missing_gio = summary_data.get("missing_gio_kocs", [])
    by_san_pham = summary_data.get("by_san_pham", {})
    
    # Format chi phí
    tong_chi_phi = summary.get("tong_chi_phi_deal", 0)
    chi_phi_formatted = f"{int(tong_chi_phi):,}".replace(",", ".") if tong_chi_phi else "0"
    
    # Format theo sản phẩm
    san_pham_stats = []
    for sp, stats in by_san_pham.items():
        chi_phi_sp = stats.get("chi_phi", 0)
        chi_phi_sp_formatted = f"{int(chi_phi_sp):,}".replace(",", ".") if chi_phi_sp else "0"
        san_pham_stats.append({
            "ten": sp,
            "count": stats.get("count", 0),
            "da_air": stats.get("da_air", 0),
            "chi_phi": chi_phi_sp_formatted
        })
    
    # Sort by count descending
    san_pham_stats.sort(key=lambda x: x["count"], reverse=True)
    
    data_for_prompt = {
        "month": summary_data.get("month"),
        "week": summary_data.get("week"),
        "total": summary.get("total", 0),
        "da_air": summary.get("da_air", 0),
        "chua_air": summary.get("chua_air", 0),
        "da_air_chua_link": summary.get("da_air_chua_link", 0),
        "da_air_chua_gan_gio": summary.get("da_air_chua_gan_gio", 0),
        "tong_chi_phi_deal": chi_phi_formatted,
        "theo_san_pham": san_pham_stats[:5],  # Top 5 sản phẩm
        "sample_missing_link": [k.get("id_koc") for k in missing_link[:5]],
        "sample_missing_gio": [k.get("id_koc") for k in missing_gio[:5]],
    }
    
    prompt = KOC_REPORT_PROMPT.format(data=json.dumps(data_for_prompt, ensure_ascii=False, indent=2))
    
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý AI chuyên viết báo cáo marketing ngắn gọn, chuyên nghiệp."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1000
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
    by_san_pham = summary_data.get("by_san_pham", {})
    
    week_text = f" tuần {week}" if week else ""
    
    # Format chi phí
    tong_chi_phi = summary.get("tong_chi_phi_deal", 0)
    chi_phi_formatted = f"{int(tong_chi_phi):,}".replace(",", ".") if tong_chi_phi else "0"
    
    text = f"""📊 Tóm tắt KOC tháng {month}{week_text}:

• Tổng: {summary.get('total', 0)} KOC đã deal
• Đã air: {summary.get('da_air', 0)} KOC
• Chưa air: {summary.get('chua_air', 0)} KOC
• Đã air nhưng chưa có link: {summary.get('da_air_chua_link', 0)} KOC
• Đã air nhưng chưa gắn giỏ: {summary.get('da_air_chua_gan_gio', 0)} KOC

💰 Chi phí:
• Tổng chi phí deal: {chi_phi_formatted} VNĐ"""
    
    # Thêm thống kê theo sản phẩm nếu có
    if by_san_pham:
        text += "\n\n📦 Theo sản phẩm:"
        sorted_sp = sorted(by_san_pham.items(), key=lambda x: x[1].get("count", 0), reverse=True)
        for sp, stats in sorted_sp[:5]:
            chi_phi_sp = stats.get("chi_phi", 0)
            chi_phi_sp_fmt = f"{int(chi_phi_sp):,}".replace(",", ".") if chi_phi_sp else "0"
            text += f"\n• {sp}: {stats.get('count', 0)} KOC ({chi_phi_sp_fmt} VNĐ)"
    
    if summary.get('da_air_chua_link', 0) > 0 or summary.get('da_air_chua_gan_gio', 0) > 0:
        text += "\n\n🎯 Đề xuất:\n• Follow up các KOC chưa có link\n• Nhắc KOC gắn giỏ hàng"
    
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
