# JARVIS v5.7.25 - Daily Booking Report

## 🆕 NEW FEATURES

### Feature 1: Thông báo cá nhân hàng ngày (9:00)
Gửi tin nhắn riêng cho từng nhân sự với nội dung:
- Video air hôm qua (so với KPI 2/ngày)
- Phân loại content: Cart, Text
- Video cần air hôm nay (KPI + thiếu cộng dồn)

**Format:**
```
🔔 Chào Mai, báo cáo booking ngày 30/01:

📊 HÔM QUA (29/01):
• Đã air: 3 video (KPI: 2/ngày)
• Phân loại: 2 Cart, 1 Text
• ✅ Đạt KPI!

📌 HÔM NAY (30/01):
• Cần air: 2 video (2 KPI + 0 thiếu cộng dồn)

💪 Cố lên Mai!
```

### Feature 2: Báo cáo team vào nhóm (9:00)
Gửi báo cáo tình hình booking tháng hiện tại vào nhóm "Kalle - Booking k sếp"

**Format:**
```
🧴 **BÁO CÁO TEAM BOOKING - KALLE**
📅 Tháng 1 - Cập nhật 30/01

👥 **TEAM PR Booking KALLE** (7 nhân sự)

📦 **SỐ LƯỢNG VIDEO:**
• KPI: 597 video
• Đã air: 244 video
• Tỷ lệ: **40.9%**

👤 **CHI TIẾT TỪNG NHÂN SỰ:**
   🟢 Nguyễn Như Mai: 83/85 (97.6%)
   🟡 Phương Thảo: 59/74 (79.7%)
   🟡 Bảo Châu: 44/84 (52.4%)
   ...

📊 Chú thích: 🟢 ≥70% | 🟡 41-69% | 🔴 ≤40%
```

## 📋 STAFF MAPPING

| User ID | Nhân sự | Tên Dashboard |
|---------|---------|---------------|
| 7ad1g7b9 | Nguyễn Như Mai | Nguyễn Như Mai - PR Bookingg |
| bbc7c22c | Lê Thuỳ Dương | Lê Thuỳ Dương |
| f987ca64 | Quân Nguyễn | Quân Nguyễn - Booking Remote |
| 29545d7g | Châu Đặng | Bảo Châu - Booking Remote |
| 2ccaca2e | Huyền Trang | Huyền Trang - Booking Kalle Remote |
| 9g9634c2 | Phương Thảo | Phương Thảo - Intern Booking |
| d2294g8g | Trà Mi | Trà Mi - Intern Booking |

## 📁 FILES

| File | Description |
|------|-------------|
| `main.py` | Added scheduler job + import |
| `daily_booking_report.py` | **NEW** - Module báo cáo hàng ngày |

## ⚙️ CONFIG

```python
BOOKING_GROUP_CHAT_ID = "oc_7356c37c72891ea5314507d78ab2e937"  # Nhóm "Kalle - Booking k sếp"
DAILY_KPI = 2  # KPI: 2 video/ngày
```

## 🧪 TEST ENDPOINT

```bash
# Test manual gửi báo cáo
curl https://your-jarvis-url/test/daily-booking
```

## 🚀 DEPLOYMENT

```powershell
cd D:\jarvis-lark-bot

# Copy files
copy main_v5.7.25.py main.py
copy daily_booking_report.py daily_booking_report.py

# Deploy
git add main.py daily_booking_report.py
git commit -m "v5.7.25: Daily booking report - personal + team"
git push origin main
```

## 📊 SCHEDULER JOBS

| Job ID | Time | Description |
|--------|------|-------------|
| daily_reminder | 9:00 | Note reminders |
| periodic_reminder | 0,6,12,18h | Periodic note reminders |
| tiktok_ads_warning | 9:00, 17:00 | TikTok Ads debt check |
| **daily_booking_report** | **9:00** | **NEW: Booking report** |

## 📝 LOGIC TÍNH TOÁN

### Thiếu cộng dồn
```python
# Số ngày đã qua trong tháng (không tính hôm nay)
days_passed = today.day - 1

# Tổng video lẽ ra phải air
expected_total = days_passed * DAILY_KPI  # days_passed * 2

# Thiếu cộng dồn = expected - đã air
total_deficit = max(0, expected_total - total_done)

# Cần air hôm nay
need_today = DAILY_KPI + total_deficit  # 2 + thiếu
```

### Status Emoji
```python
🟢 ≥70%    # Tốt
🟡 41-69%  # Trung bình  
🔴 ≤40%   # Cần cải thiện
```

---
**Version**: 5.7.25
**Date**: 2026-01-30
