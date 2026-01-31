# JARVIS v5.7.19 - Content Breakdown from Booking

## 🔄 MAJOR CHANGE

**Content breakdown giờ lấy từ bảng Booking thay vì Dashboard Tháng**

### Lý do:
- Dashboard Tháng không có field "Phân loại sp gửi hàng"
- Dashboard Tháng là bảng tổng hợp từ Booking
- Booking có đầy đủ thông tin chi tiết

### Bảng Booking có:
- ✅ **Content** (Cart/Text)
- ✅ **Sản phẩm** (Nước hoa, Box quà 30ml...)
- ✅ **Phân loại sp gửi hàng** (Dark Beauty 30ml, LadyKiller...)
- ✅ **Nhân sự book**
- ✅ **Tháng air** / **Thời gian air**
- ✅ **Link air bài** (để biết đã air chưa)

## ✅ CHANGES

### lark_base.py - generate_dashboard_summary()

```python
# OLD: Lấy từ dashboard_records
content_by_nhan_su = {}
for r in dashboard_records:
    ...

# NEW: Lấy từ booking_records
content_by_nhan_su = {}
for record in booking_records:
    fields = record.get("fields", {})
    
    # Chỉ đếm records đã air
    link_air = fields.get("Link air bài")
    if not link_air:
        continue
    
    # Filter theo tháng air
    thang_air = ... (parse từ Thời gian air hoặc Tháng dự kiến)
    if month and thang_air != month:
        continue
    
    # Extract fields từ Booking
    content_type = fields.get("Content")  # Cart/Text
    san_pham = fields.get("Sản phẩm")
    phan_loai_gh = find_phan_loai_field(fields)  # Dark Beauty 30ml...
    
    # Aggregate
    ...
```

## 📊 EXPECTED OUTPUT

**Trước (v5.7.18)**:
```
Content: 60 video Nước hoa,Video và 18 video Box quà 30ml,Video
```

**Sau (v5.7.19)**:
```
Content: 30 cart Nước hoa,Cart,Dark Beauty 30ml và 10 cart Box quà 30ml,Cart,Dark Beauty 30ml
```

## 🚀 DEPLOYMENT

```bash
cd D:\jarvis-lark-bot
# Copy lark_base_v5.7.19_fixed.py → lark_base.py
git add lark_base.py
git commit -m "v5.7.19: Content breakdown from Booking table"
git push origin main
```

## 📋 DEBUG LOGS

Sau deploy, check logs để verify:
```
📦 Booking fields sample: Content=Cart, Sản phẩm=Nước hoa, Phân loại=Dark Beauty 30ml
📝 KALLE Content breakdown (from Booking, tháng 1): 6 nhân sự
   Nguyễn Như Mai - PR Bookingg: [{'san_pham': 'Nước hoa', 'loai': 'Cart', 'phan_loai': 'Dark Beauty 30ml', 'so_luong': 30}, ...]
```

---
**Version**: 5.7.19
**Date**: 2026-01-27
**Type**: Feature Change
