# JARVIS v5.7.23 - Content từ Dashboard Tháng

## 🔄 THAY ĐỔI CHÍNH

### 1. Content lấy từ Dashboard Tháng (thay vì Booking)
**Trước:** Đếm records từ Booking table có "Link air bài"
**Sau:** Lấy trực tiếp từ 2 cột mới trong Dashboard Tháng:
- `Content Text` - số lượng content text
- `Content cart` - số lượng content gắn giỏ

```python
# v5.7.23: Lấy từ Dashboard Tháng
content_text = int(fields.get("Content Text") or 0)
content_cart = int(fields.get("Content cart") or 0)
```

### 2. TikTok Ads Check: 9h VÀ 17h
```python
scheduler.add_job(
    check_tiktok_ads_warning,
    CronTrigger(hour="9,17", minute=0, timezone=TIMEZONE),
    id="tiktok_ads_warning",
    replace_existing=True
)
```

### 3. Hiển thị tổng Content trong báo cáo
```
Content (211): 43 cart Nước hoa, 27 cart Box quà và 5 loại khác
```

## ✅ FILES CHANGED

| File | Thay đổi |
|------|----------|
| `main.py` | TikTok check 9h+17h |
| `lark_base.py` | Content từ Dashboard Tháng (Content Text + Content cart) |
| `report_generator.py` | Hiển thị tổng số content |

## 📊 DATA SOURCE

| Metric | Source | Cột |
|--------|--------|-----|
| KPI Số lượng | Dashboard Tháng | KPI Số lượng |
| Số lượng Air | Dashboard Tháng | Số lượng tổng - Air |
| KPI Ngân sách | Dashboard Tháng | KPI ngân sách |
| Ngân sách Air | Dashboard Tháng | Ngân sách tổng - Air |
| **Content Text** | **Dashboard Tháng** | **Content Text** |
| **Content Cart** | **Dashboard Tháng** | **Content cart** |

## 🚀 DEPLOYMENT

```powershell
cd D:\jarvis-lark-bot

copy main_v5.7.23.py main.py
copy lark_base_v5.7.23.py lark_base.py
copy report_generator_v5.7.23.py report_generator.py

git add main.py lark_base.py report_generator.py
git commit -m "v5.7.23: Content from Dashboard + TikTok 9h+17h"
git push origin main
```

## 📋 EXPECTED OUTPUT

```
📝 KALLE Content (from Dashboard): 8 nhân sự, Cart=150, Text=61, Tổng=211
   Nguyễn Như Mai: [{'san_pham': 'DARK BEAUTY - 30ML', 'loai': 'Cart', 'so_luong': 42}]

📊 BÁO CÁO TEAM BOOKING - KALLE
📦 SỐ LƯỢNG VIDEO:
   • KPI: 597 video
   • Đã air: 211 video
   • Tỷ lệ: 35.3%
   **Content (211): 42 cart DARK BEAUTY - 30ML, 25 cart Dark beauty 30ml + sữa... và 10 loại khác**
```

---
**Version**: 5.7.23
**Date**: 2026-01-28
