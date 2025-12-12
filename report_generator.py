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
- Viết 4-6 bullet points chính
- Nhấn mạnh số lượng: tổng KOC, đã air, chưa air, chưa có link, chưa gắn giỏ
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

🎯 Đề xuất:
• [hành động 1]
• [hành động 2]
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
    """Sinh báo cáo KOC từ dữ liệu summary"""
    
    # Chuẩn bị data cho prompt
    summary = summary_data.get("summary", {})
    missing_link = summary_data.get("missing_link_kocs", [])
    missing_gio = summary_data.get("missing_gio_kocs", [])
    
    data_for_prompt = {
        "month": summary_data.get("month"),
        "week": summary_data.get("week"),
        "total": summary.get("total", 0),
        "da_air": summary.get("da_air", 0),
        "chua_air": summary.get("chua_air", 0),
        "da_air_chua_link": summary.get("da_air_chua_link", 0),
        "da_air_chua_gan_gio": summary.get("da_air_chua_gan_gio", 0),
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
            max_tokens=800
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ OpenAI Error: {e}")
        # Fallback to simple format
        return format_koc_report_simple(summary_data)

async def generate_content_calendar_text(calendar_data: Dict[str, Any]) -> str:
    """Sinh báo cáo lịch content"""
    
    summary = calendar_data.get("summary", {})
    by_team = calendar_data.get("by_team", {})
    overdue = calendar_data.get("overdue_tasks", [])
    date_range = calendar_data.get("date_range", "tuần này")
    
    data_for_prompt = {
        "date_range": date_range,
        "total_tasks": summary.get("total_tasks", 0),
        "total_overdue": summary.get("total_overdue", 0),
        "teams": {team: len(tasks) for team, tasks in by_team.items()},
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
    
    week_text = f" tuần {week}" if week else ""
    
    text = f"""📊 Tóm tắt KOC tháng {month}{week_text}:

• Tổng: {summary.get('total', 0)} KOC
• Đã air: {summary.get('da_air', 0)} KOC
• Chưa air: {summary.get('chua_air', 0)} KOC
• Đã air nhưng chưa có link: {summary.get('da_air_chua_link', 0)} KOC
• Đã air nhưng chưa gắn giỏ: {summary.get('da_air_chua_gan_gio', 0)} KOC"""
    
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
• Số team tham gia: {summary.get('teams_involved', 0)}"""
    
    return text
