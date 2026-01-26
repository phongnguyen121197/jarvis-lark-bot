# Jarvis v5.7.13 - KPI Routing Fix

## 🐛 Bug Fixed: CHENG Staff KPI Routing

**Problem:** Khi hỏi "KPI của Phương" (nhân sự CHENG), hệ thống route sai đến DASHBOARD thay vì CHENG_REPORT, trả về data của tất cả nhân sự CHENG thay vì filter theo tên.

**Root Cause:** 
- `is_dashboard` flag chỉ được set `True` khi detect KALLE staff
- CHENG routing logic yêu cầu `is_dashboard = True` nhưng không được set cho CHENG staff

**Fix (intent_classifier.py, line ~399-401):**
```python
# BEFORE (bug):
cheng_nhan_su_detected = full_name
break

# AFTER (fixed):
cheng_nhan_su_detected = full_name
# FIX v5.7.13: Set is_dashboard=True for CHENG staff KPI queries
is_dashboard = True
break
```

## 📦 Deployment Instructions

1. **Thay thế file** `intent_classifier.py` trong project Railway bằng file trong zip
2. **Commit & Push** lên GitHub/Railway
3. Railway sẽ tự động redeploy

## ✅ Test Cases

Sau khi deploy, test các query:
- "KPI của Phương" → Phải trả về KPI riêng của Phương (CHENG)
- "KPI của Hương" → Phải trả về KPI riêng của Hương (KALLE)
- "Báo cáo CHENG" → Phải trả về báo cáo tổng CHENG

## 📋 Pending Tasks

| Task | Priority | Status |
|------|----------|--------|
| TikTok Scheduler | ✅ | Fixed (v5.7.6 - CronTrigger) |
| KPI Routing | ✅ | **Fixed (v5.7.13)** |
| Calendar Integration | 🟡 | invalid calendar_id - cần verify |
| Content Statistics | ⏳ | Chờ user input requirements |
| Group Notification | ⏳ | Chờ user input scenarios |
