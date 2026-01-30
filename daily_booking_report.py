"""
Daily Booking Report Module
v5.7.25 - Feature: Thông báo cá nhân + Báo cáo team hàng ngày

Features:
1. Thông báo cá nhân: Video air hôm qua, KPI cần hôm nay (cộng dồn)
2. Báo cáo team: Tình hình booking tháng hiện tại gửi vào nhóm 9h
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import httpx

# ============ STAFF MAPPING ============
# Map từ User ID Lark -> Tên trong Dashboard
BOOKING_STAFF = {
    "7ad1g7b9": {
        "name": "Nguyễn Như Mai",
        "dashboard_names": ["Nguyễn Như Mai - PR Bookingg", "Nguyễn Như Mai"],
        "short_name": "Mai"
    },
    "bbc7c22c": {
        "name": "Lê Thuỳ Dương",
        "dashboard_names": ["Lê Thuỳ Dương", "Lê Thuỳ Dương (vịt)"],
        "short_name": "Dương"
    },
    "f987ca64": {
        "name": "Quân Nguyễn",
        "dashboard_names": ["Quân Nguyễn - Booking Remote", "Quân Nguyễn"],
        "short_name": "Quân"
    },
    "29545d7g": {
        "name": "Châu Đặng",
        "dashboard_names": ["Bảo Châu - Booking Remote", "Châu Đặng - Booking Remote", "Châu Đặng"],
        "short_name": "Châu"
    },
    "2ccaca2e": {
        "name": "Huyền Trang",
        "dashboard_names": ["Huyền Trang - Booking Kalle Remote", "Huyền Trang"],
        "short_name": "Trang"
    },
    "9g9634c2": {
        "name": "Phương Thảo",
        "dashboard_names": ["Phương Thảo - Intern Booking", "Phương Thảo intern Booking", "Phương Thảo"],
        "short_name": "Thảo"
    },
    "d2294g8g": {
        "name": "Trà Mi",
        "dashboard_names": ["Trà Mi - Intern Booking", "Trà Mi"],
        "short_name": "Mi"
    },
}

# ============ CONFIG ============
BOOKING_GROUP_CHAT_ID = "oc_7356c37c72891ea5314507d78ab2e937"  # Nhóm "Kalle - Booking k sếp"
DAILY_KPI = 2  # KPI: 2 video/ngày

# Lark API
LARK_APP_ID = os.getenv("LARK_APP_ID")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET")
LARK_API_BASE = "https://open.larksuite.com/open-apis"


async def get_tenant_access_token() -> str:
    """Get Lark tenant access token"""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{LARK_API_BASE}/auth/v3/tenant_access_token/internal",
            json={
                "app_id": LARK_APP_ID,
                "app_secret": LARK_APP_SECRET
            }
        )
        data = response.json()
        return data.get("tenant_access_token", "")


async def send_message_to_user(user_id: str, message: str) -> bool:
    """
    Gửi tin nhắn đến user qua user_id
    """
    try:
        token = await get_tenant_access_token()
        
        # Escape special characters for JSON
        escaped_message = message.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{LARK_API_BASE}/im/v1/messages?receive_id_type=user_id",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={
                    "receive_id": user_id,
                    "msg_type": "text",
                    "content": f'{{"text": "{escaped_message}"}}'
                }
            )
            result = response.json()
            
            if result.get("code") == 0:
                print(f"✅ Sent message to user {user_id}")
                return True
            else:
                print(f"❌ Failed to send to {user_id}: {result}")
                return False
    except Exception as e:
        print(f"❌ Error sending to {user_id}: {e}")
        return False


async def send_message_to_chat(chat_id: str, message: str) -> bool:
    """Gửi tin nhắn đến group chat"""
    try:
        token = await get_tenant_access_token()
        
        # Escape special characters for JSON
        escaped_message = message.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{LARK_API_BASE}/im/v1/messages?receive_id_type=chat_id",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={
                    "receive_id": chat_id,
                    "msg_type": "text",
                    "content": f'{{"text": "{escaped_message}"}}'
                }
            )
            result = response.json()
            
            if result.get("code") == 0:
                print(f"✅ Sent message to chat {chat_id}")
                return True
            else:
                print(f"❌ Failed to send to chat {chat_id}: {result}")
                return False
    except Exception as e:
        print(f"❌ Error sending to chat {chat_id}: {e}")
        return False


def normalize_name(name: str) -> str:
    """Normalize tên để so sánh"""
    import re
    # Lấy phần trước " - "
    name = name.split(" - ")[0].strip()
    # Loại bỏ phần trong ngoặc
    name = re.sub(r'\s*\([^)]*\)', '', name).strip()
    return name.lower()


def match_staff_name(search_name: str, dashboard_names: List[str]) -> bool:
    """
    Kiểm tra tên có match không
    """
    search_normalized = normalize_name(search_name)
    
    for dash_name in dashboard_names:
        dash_normalized = normalize_name(dash_name)
        
        # Exact match
        if search_normalized == dash_normalized:
            return True
        
        # Partial match - có ít nhất 2 từ giống nhau
        search_parts = set(search_normalized.split())
        dash_parts = set(dash_normalized.split())
        common = search_parts & dash_parts
        if len(common) >= 2:
            return True
        
        # Contains match
        if search_normalized in dash_normalized or dash_normalized in search_normalized:
            return True
    
    return False


async def get_video_air_by_date(target_date: datetime) -> Dict[str, Dict]:
    """
    Lấy số video air theo ngày từ Booking table
    
    Returns:
        Dict[nhan_su_name, {"count": int, "cart": int, "text": int}]
    """
    from lark_base import get_all_records, BOOKING_BASE, safe_extract_person_name
    
    target_date_str = target_date.strftime("%Y/%m/%d")
    print(f"📅 Getting video air for date: {target_date_str}")
    
    records = await get_all_records(
        app_token=BOOKING_BASE["app_token"],
        table_id=BOOKING_BASE["table_id"],
        max_records=2000
    )
    
    result = {}
    
    for record in records:
        fields = record.get("fields", {})
        
        # Chỉ đếm records đã air (có Link air bài)
        link_air = fields.get("Link air bài") or fields.get("link_air_bai") or fields.get("Link air")
        if not link_air:
            continue
        
        # Check thời gian air
        thoi_gian_air = fields.get("Thời gian air") or fields.get("thoi_gian_air")
        if not thoi_gian_air:
            continue
        
        # Parse date - format: yyyy/mm/dd
        air_date_str = None
        if isinstance(thoi_gian_air, str):
            # Format: "2025/10/09"
            air_date_str = thoi_gian_air.strip()[:10]
        elif isinstance(thoi_gian_air, (int, float)):
            # Timestamp
            try:
                dt = datetime.fromtimestamp(thoi_gian_air / 1000)
                air_date_str = dt.strftime("%Y/%m/%d")
            except:
                continue
        
        if not air_date_str or air_date_str != target_date_str:
            continue
        
        # Lấy nhân sự
        nhan_su = safe_extract_person_name(fields.get("Nhân sự book"))
        if not nhan_su:
            continue
        nhan_su = nhan_su.strip()
        
        # Lấy loại content (Cart/Text/Video)
        content_type = fields.get("Content") or "Video"
        if isinstance(content_type, list) and len(content_type) > 0:
            content_type = content_type[0] if isinstance(content_type[0], str) else content_type[0].get("text", "Video")
        content_type = str(content_type).strip().lower() if content_type else "video"
        
        # Aggregate
        if nhan_su not in result:
            result[nhan_su] = {"count": 0, "cart": 0, "text": 0}
        
        result[nhan_su]["count"] += 1
        if "cart" in content_type:
            result[nhan_su]["cart"] += 1
        elif "text" in content_type:
            result[nhan_su]["text"] += 1
    
    print(f"📊 Video air on {target_date_str}: {result}")
    return result


async def get_monthly_stats() -> Dict:
    """
    Lấy thống kê tháng hiện tại từ Dashboard
    """
    from lark_base import generate_dashboard_summary
    
    now = datetime.now()
    month = now.month
    
    print(f"📊 Getting monthly stats for month {month}...")
    
    data = await generate_dashboard_summary(month=month)
    
    if not data:
        print("❌ Failed to get dashboard data")
        return None
    
    totals = data.get("totals", {})
    staff_list = data.get("staff_list", [])
    
    print(f"📊 Monthly stats: KPI={totals.get('video_kpi', 0)}, Done={totals.get('video_done', 0)}")
    print(f"📊 Staff count: {len(staff_list)}")
    
    return {
        "month": month,
        "total_kpi": totals.get("video_kpi", 0),
        "total_done": totals.get("video_done", 0),
        "total_percent": totals.get("video_percent", 0),
        "content_text": totals.get("content_text", 0),
        "content_cart": totals.get("content_cart", 0),
        "staff_list": staff_list
    }


def get_status_emoji(percent: float) -> str:
    """
    Lấy emoji trạng thái theo phần trăm
    10%-40%: 🔴
    41%-69%: 🟡  
    70%-100%: 🟢
    """
    if percent >= 70:
        return "🟢"
    elif percent >= 41:
        return "🟡"
    else:
        return "🔴"


def find_staff_data(staff_info: Dict, data_dict: Dict, monthly_list: List[Dict]) -> tuple:
    """
    Tìm data của nhân sự từ yesterday_data và monthly_stats
    """
    dashboard_names = staff_info.get("dashboard_names", [])
    
    # Tìm trong yesterday_data
    yesterday_stats = None
    for key, value in data_dict.items():
        if match_staff_name(key, dashboard_names):
            yesterday_stats = value
            break
    
    if not yesterday_stats:
        yesterday_stats = {"count": 0, "cart": 0, "text": 0}
    
    # Tìm trong monthly_list
    monthly_personal = None
    for staff in monthly_list:
        staff_name = staff.get("name", "")
        if match_staff_name(staff_name, dashboard_names):
            monthly_personal = staff
            break
    
    return yesterday_stats, monthly_personal


async def generate_personal_report(user_id: str, staff_info: Dict, yesterday_data: Dict, monthly_stats: Dict) -> str:
    """
    Tạo báo cáo cá nhân cho 1 nhân sự
    """
    name = staff_info["short_name"]
    
    # Tìm data của nhân sự
    yesterday_stats, monthly_personal = find_staff_data(
        staff_info, 
        yesterday_data, 
        monthly_stats.get("staff_list", [])
    )
    
    # Tính toán
    yesterday = datetime.now() - timedelta(days=1)
    today = datetime.now()
    days_passed = today.day - 1  # Số ngày đã qua (không tính hôm nay)
    
    video_yesterday = yesterday_stats["count"]
    cart_yesterday = yesterday_stats["cart"]
    text_yesterday = yesterday_stats["text"]
    
    # Thiếu hôm qua
    deficit_yesterday = max(0, DAILY_KPI - video_yesterday)
    
    # Tổng đã air trong tháng
    total_done = monthly_personal.get("video_done", 0) if monthly_personal else 0
    
    # Tổng thiếu cộng dồn = (số ngày đã qua * KPI) - tổng đã air
    expected_total = days_passed * DAILY_KPI
    total_deficit = max(0, expected_total - total_done)
    
    # Cần air hôm nay = KPI ngày + thiếu cộng dồn
    need_today = DAILY_KPI + total_deficit
    
    # Status emoji
    status = "✅ Đạt KPI!" if video_yesterday >= DAILY_KPI else f"⚠️ Thiếu {deficit_yesterday} video"
    
    # Format message
    message = f"""🔔 Chào {name}, báo cáo booking ngày {today.strftime('%d/%m')}:

📊 HÔM QUA ({yesterday.strftime('%d/%m')}):
• Đã air: {video_yesterday} video (KPI: {DAILY_KPI}/ngày)
• Phân loại: {cart_yesterday} Cart, {text_yesterday} Text
• {status}

📌 HÔM NAY ({today.strftime('%d/%m')}):
• Cần air: {need_today} video ({DAILY_KPI} KPI + {total_deficit} thiếu cộng dồn)

💪 Cố lên {name}!"""
    
    return message


async def generate_team_report(monthly_stats: Dict) -> str:
    """
    Tạo báo cáo team cho nhóm
    """
    if not monthly_stats:
        return "❌ Không thể lấy dữ liệu báo cáo team"
    
    month = monthly_stats["month"]
    total_kpi = monthly_stats["total_kpi"]
    total_done = monthly_stats["total_done"]
    total_percent = monthly_stats["total_percent"]
    staff_list = monthly_stats["staff_list"]
    
    today = datetime.now()
    
    # Sort staff by percent descending
    sorted_staff = sorted(staff_list, key=lambda x: x.get("video_percent", 0), reverse=True)
    
    # Build staff details
    staff_lines = []
    for staff in sorted_staff:
        name = staff.get("name", "N/A")
        done = staff.get("video_done", 0)
        kpi = staff.get("video_kpi", 0)
        percent = staff.get("video_percent", 0)
        emoji = get_status_emoji(percent)
        staff_lines.append(f"   {emoji} {name}: {done}/{kpi} ({percent}%)")
    
    staff_details = "\n".join(staff_lines)
    
    message = f"""🧴 **BÁO CÁO TEAM BOOKING - KALLE**
📅 Tháng {month} - Cập nhật {today.strftime('%d/%m')}

👥 **TEAM PR Booking KALLE** ({len(sorted_staff)} nhân sự)

📦 **SỐ LƯỢNG VIDEO:**
• KPI: {total_kpi} video
• Đã air: {total_done} video
• Tỷ lệ: **{total_percent}%**

👤 **CHI TIẾT TỪNG NHÂN SỰ:**
{staff_details}

📊 Chú thích: 🟢 ≥70% | 🟡 41-69% | 🔴 ≤40%"""
    
    return message


async def send_daily_booking_reports():
    """
    Main function: Gửi báo cáo hàng ngày
    1. Gửi thông báo cá nhân cho từng nhân sự
    2. Gửi báo cáo team vào nhóm
    """
    print(f"\n{'='*50}")
    print(f"📊 Starting daily booking reports at {datetime.now()}")
    print(f"{'='*50}")
    
    try:
        # Lấy data
        yesterday = datetime.now() - timedelta(days=1)
        print(f"📅 Getting data for yesterday: {yesterday.strftime('%Y/%m/%d')}")
        
        yesterday_data = await get_video_air_by_date(yesterday)
        monthly_stats = await get_monthly_stats()
        
        if not monthly_stats:
            print("❌ Failed to get monthly stats, aborting...")
            return
        
        # 1. Gửi thông báo cá nhân
        print(f"\n📤 Sending personal reports to {len(BOOKING_STAFF)} staff members...")
        success_count = 0
        for user_id, staff_info in BOOKING_STAFF.items():
            try:
                message = await generate_personal_report(user_id, staff_info, yesterday_data, monthly_stats)
                result = await send_message_to_user(user_id, message)
                if result:
                    success_count += 1
                    print(f"   ✅ {staff_info['name']}")
                else:
                    print(f"   ❌ {staff_info['name']} - Failed to send")
            except Exception as e:
                print(f"   ❌ {staff_info['name']} - Error: {e}")
        
        print(f"📊 Personal reports sent: {success_count}/{len(BOOKING_STAFF)}")
        
        # 2. Gửi báo cáo team
        print(f"\n📤 Sending team report to group...")
        try:
            team_message = await generate_team_report(monthly_stats)
            result = await send_message_to_chat(BOOKING_GROUP_CHAT_ID, team_message)
            if result:
                print("   ✅ Team report sent successfully")
            else:
                print("   ❌ Failed to send team report")
        except Exception as e:
            print(f"   ❌ Team report error: {e}")
        
        print(f"\n{'='*50}")
        print(f"📊 Daily booking reports completed at {datetime.now()}")
        print(f"{'='*50}\n")
        
    except Exception as e:
        print(f"❌ Error in daily booking reports: {e}")
        import traceback
        traceback.print_exc()


# For testing
async def test_daily_report():
    """Test function - có thể gọi manual"""
    print("🧪 Testing daily booking report...")
    await send_daily_booking_reports()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_daily_report())
