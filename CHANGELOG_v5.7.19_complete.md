# JARVIS v5.7.19 - Team Booking Report + Content from Booking

## 🔄 MAJOR CHANGES

### 1. Content breakdown lấy từ Booking thay vì Dashboard
- **Trước**: Lấy từ Dashboard Tháng (không có field Content, Phân loại gửi hàng)
- **Sau**: Lấy từ bảng Booking (`KALLE - PR - Data list booking`)

### 2. Thêm báo cáo Team Booking
- Trigger: "báo cáo team booking", "booking tháng X", "tình hình team"
- Format tương tự báo cáo cá nhân nhưng tổng hợp toàn team

---

## ✅ FILES CHANGED

### 1. intent_classifier.py

**Thêm keywords DASHBOARD:**
```python
DASHBOARD_KEYWORDS = [
    ...existing...,
    # v5.7.19: Team booking keywords
    "team booking", "tình hình team", "tinh hinh team",
    "báo cáo team", "bao cao team", "booking tháng"
]
```

**Thêm logic detect report_type = kpi_team:**
```python
if "team" in text_lower or ("booking" in text_lower and "tháng" in text_lower):
    if not kalle_nhan_su_detected:
        report_type = "kpi_team"
```

### 2. lark_base.py

**Content breakdown từ Booking:**
```python
# Loop qua booking_records thay vì dashboard_records
for record in booking_records:
    # Chỉ đếm records đã air
    link_air = fields.get("Link air bài")
    if not link_air:
        continue
    
    # Filter theo tháng
    if month and thang_air != month:
        continue
    
    # Extract fields từ Booking
    content_type = fields.get("Content")  # Cart/Text
    san_pham = fields.get("Sản phẩm")
    phan_loai_gh = find_phan_loai_field(fields)  # Dark Beauty 30ml...
```

### 3. report_generator.py

**Thêm format cho kpi_team:**
```python
if report_type == "kpi_team":
    # KPI tổng từ Dashboard Tháng
    total_video_kpi = totals.get("video_kpi", 0)
    total_video_done = totals.get("video_done", 0)
    ...
    
    # Content tổng từ Booking (aggregated)
    team_content = {}
    for staff in staff_list:
        content_data = staff.get("content_breakdown", {})
        ...
```

---

## 📊 EXPECTED OUTPUT

### Input: "báo cáo tình hình team booking tháng 1"

### Output:
```
🧴 **BÁO CÁO TEAM BOOKING - KALLE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 Tháng 1

👥 **TEAM PR Booking KALLE** (6 nhân sự)
───────────────────────────
📊 **Trạng thái:** 🟢 Gần đạt

📦 **SỐ LƯỢNG VIDEO:**
   • KPI: 337 video
   • Đã air: 290 video
   • Tỷ lệ: **86.1%**
   **Content: 80 cart Nước hoa,Cart,Dark Beauty 30ml, 45 cart Box quà 30ml,Cart,Dark Beauty 30ml và 3 loại khác**

💰 **NGÂN SÁCH:**
   • KPI: 120.5M
   • Đã air: 98.2M
   • Tỷ lệ: **81.5%**

📊 Tiến độ: [████████░░] 83.8%

📞 **LIÊN HỆ KOC:**
   • Tổng liên hệ: 456
   • Đã deal: 189 (41.4%)

───────────────────────────
👤 **CHI TIẾT TỪNG NHÂN SỰ:**
   🟢 Nguyễn Như Mai - PR Bookingg: 78/85 (91.8%)
   🟡 Bảo Châu - Booking Remote: 41/53 (77.4%)
   🟡 Phương Thảo - Intern Booking: 51/74 (68.9%)
   ...
```

---

## 🚀 DEPLOYMENT

```bash
cd D:\jarvis-lark-bot

# Copy all 3 files
copy lark_base_v5.7.19_fixed.py lark_base.py
copy intent_classifier_v5.7.19_fixed.py intent_classifier.py
copy report_generator_v5.7.19_fixed.py report_generator.py

# Commit
git add lark_base.py intent_classifier.py report_generator.py
git commit -m "v5.7.19: Team booking report + Content from Booking table"
git push origin main
```

---

## 📋 TEST CASES

| Query | Expected report_type |
|-------|---------------------|
| "KPI của Mai" | kpi_ca_nhan |
| "báo cáo team booking tháng 1" | kpi_team |
| "booking tháng 1" | kpi_team |
| "tình hình team booking" | kpi_team |
| "dashboard kalle" | full |

---
**Version**: 5.7.19
**Date**: 2026-01-27
**Type**: Feature + Fix
