# Jarvis v5.7.1 - Bug Fixes

## Lỗi đã Fix trong `lark_base.py`

### 1. ✅ Note lỗi - `NameError: name 'create_record' is not defined`
**Nguyên nhân**: Thiếu các hàm CRUD cho Bitable  
**Fix**: Thêm 3 hàm mới (lines 199-268):
- `create_record(app_token, table_id, fields)` - Tạo record mới
- `update_record(app_token, table_id, record_id, fields)` - Cập nhật record
- `delete_record(app_token, table_id, record_id)` - Xóa record

### 2. ✅ GMV tính sai - Đang dùng bảng KOC chi tiết thay vì bảng Tổng
**Nguyên nhân**: GMV được cộng từ bảng "CHENG - PR - Data doanh thu Koc (tuần)" thay vì bảng "CHENG - PR - Data doanh thu tổng Cheng (tuần)"

**Fix**:
- Thêm hàm `get_cheng_doanh_thu_tong_records()` (lines 757-849)
- Sửa `generate_cheng_koc_summary()` để lấy GMV từ bảng Doanh thu tổng

**Trước**: `total_gmv = sum(koc_gmv.values())` (từ KOC chi tiết)  
**Sau**: `total_gmv = sum(r.get("gmv", 0) for r in doanh_thu_tong_records)` (từ bảng Tổng)

---

## Lỗi chưa fix (cần phân tích thêm)

### 3. ⚠️ Calendar không hoạt động
**Cần kiểm tra**: 
- Calendar ID đã được config đúng: `7585485663517069021`
- Cần xem log chi tiết khi gọi calendar command

### 4. ⚠️ Data mixing giữa KALLE và CHENG
**Nguyên nhân**: Khi user hỏi "KPI Mai tháng 12" (không nói brand), system route đến `DASHBOARD` intent → KALLE data

**Giải pháp đề xuất**:
- Option A: Cải thiện intent_classifier để nhận diện nhân sự thuộc brand nào
- Option B: Khi `DASHBOARD` intent, check tên nhân sự có trong CHENG không → route lại

---

## Files cần deploy

1. **lark_base.py** (2121 lines) - Đã fix Note + GMV

---

## Test commands sau khi deploy

```
# Test Note
@Jarvis note: họp team lúc 3h chiều
@Jarvis xem note

# Test GMV CHENG 
@Jarvis báo cáo CHENG tháng 12
→ Kiểm tra GMV có khớp với bảng "CHENG - PR - Data doanh thu tổng Cheng (tuần)" không

# Test Calendar (nếu cần)
@Jarvis lịch tuần này
```

---

## Changelog v5.7.1
- Added: `create_record()`, `update_record()`, `delete_record()` CRUD functions
- Added: `get_cheng_doanh_thu_tong_records()` function for correct GMV
- Fixed: GMV calculation in `generate_cheng_koc_summary()` 
- Fixed: Note commands should now work
