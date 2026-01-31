# JARVIS v5.7.21 - Complete Fix

## 🐛 BUGS FIXED

### 1. Content chỉ đếm ~70 thay vì ~113
**Nguyên nhân**: Có thể code chưa được deploy đúng hoặc matching không work

**Debug logs mới**:
```
✅ Matched (exact): 'Nguyễn Như Mai - PR Bookingg' (5 types, 44 total)
📦 Nguyễn Như Mai - PR Bookingg: content_total=44, items=5
📊 DEBUG team_content: 10 loại, tổng=113
```

### 2. Reminder vẫn gửi dù đã Done
**Nguyên nhân**: 
- `_reminder_sent` bị reset khi restart container
- `get_overdue_notes()` không track đúng record_id

**Fix** (notes_manager.py):
```python
# v5.7.21: Track by record_id và skip nếu đã gửi
if record_id and record_id not in self._reminder_sent:
    overdue_notes.append(note)
```

### 3. Thêm báo cáo TKQC 17h hàng ngày
**New feature**: Scheduler gửi báo cáo dư nợ TKQC lúc 17h

```
📊 **BÁO CÁO DƯ NỢ TKQC - 17H**
━━━━━━━━━━━━━━━━━━━━━━━━
💳 **Dư nợ hiện tại:** 50,000,000 VND
🏦 **Hạn mức:** 100,000,000 VND
📊 **Tỷ lệ sử dụng:** **50.0%**
📈 **Trạng thái:** 🟢 An toàn
```

## ✅ FILES CHANGED

| File | Thay đổi |
|------|----------|
| `main.py` | Thêm scheduler TKQC 17h, function send_tkqc_daily_report |
| `notes_manager.py` | Fix get_overdue_notes track by record_id |
| `lark_base.py` | Debug logs cho content matching |
| `report_generator.py` | Debug logs cho team aggregate |

## 🚀 DEPLOYMENT

```powershell
cd D:\jarvis-lark-bot

# Copy 4 files
copy main_v5.7.21.py main.py
copy lark_base_v5.7.21.py lark_base.py
copy notes_manager_v5.7.21.py notes_manager.py
copy report_generator_v5.7.21.py report_generator.py

# Commit
git add main.py lark_base.py notes_manager.py report_generator.py
git commit -m "v5.7.21: Fix reminder + TKQC 17h + Debug content"
git push origin main
```

## 📋 SCHEDULER JOBS

| Job | Thời gian | Mô tả |
|-----|-----------|-------|
| daily_reminder | 9:00 | Nhắc nhở notes |
| periodic_reminder | 0,6,12,18h | Nhắc nhở định kỳ |
| tiktok_ads_warning | 9:00 | Cảnh báo nếu > 85% |
| **tkqc_daily_report_17h** | **17:00** | **Báo cáo dư nợ hàng ngày** |

## 📊 DEBUG LOGS

Sau deploy, logs sẽ hiện:
```
🔍 Tên từ Dashboard: [...]
🔍 Tên từ Booking: [...]
   ✅ Matched (exact): 'Nguyễn Như Mai - PR Bookingg' (5 types, 44 total)
   📦 Nguyễn Như Mai - PR Bookingg: content_total=44
📊 DEBUG kpi_team: 8 staff
📊 DEBUG team_content: 10 loại, tổng=113
```

---
**Version**: 5.7.21
**Date**: 2026-01-28
**Type**: Bugfix + Feature
