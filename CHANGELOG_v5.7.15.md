# JARVIS v5.7.15 - Content Breakdown for KPI Reports

## 🎯 Mục tiêu
Thêm chi tiết số lượng content theo sản phẩm và loại (Cart/Text/Video) vào báo cáo KPI.

## ✅ Đã hoàn thành

### 1. Thêm function `extract_loai_video()` (line 390-412)
```python
def extract_loai_video(record: Dict) -> Optional[str]:
    """
    Trích xuất field "Loại video" từ record
    Các giá trị: Cart, Text, Video
    """
```
- Hỗ trợ nhiều tên field: "Loại video", "Loai video", "Loại Video", "Content Type", "Type"
- Sử dụng `safe_extract_text()` để handle nhiều kiểu dữ liệu Lark

### 2. Cập nhật `get_cheng_dashboard_records()` (line 629-638)
- Thêm `loai_video` extraction trước khi append record
- Thêm `"loai_video": loai_video` vào parsed dict

### 3. Cập nhật `get_dashboard_thang_records()` (line 1731-1739)  
- Thêm `loai_video` extraction cho KALLE dashboard
- Thêm `"loai_video": loai_video` vào result dict

### 4. Cập nhật `generate_cheng_koc_summary()` (line 1067-1132)
- Thêm aggregation `content_by_nhan_su` cho CHENG
- Format output:
```python
content_by_nhan_su = {
    "Như Mai": [
        {"san_pham": "Dark Beauty 30ml", "loai": "Cart", "so_luong": 30},
        {"san_pham": "Midnight Rose", "loai": "Text", "so_luong": 15}
    ]
}
```
- Sort theo số lượng giảm dần
- Thêm vào return dict: `"content_by_nhan_su": content_by_nhan_su`

### 5. Cập nhật `generate_dashboard_summary()` (line 2012-2082)
- Thêm aggregation `content_by_nhan_su` cho KALLE (logic tương tự CHENG)
- Thêm vào return dict: `"content_by_nhan_su": content_by_nhan_su`

## 📊 Output Format

```python
# Trong generate_cheng_koc_summary() và generate_dashboard_summary()
return {
    ...
    "content_by_nhan_su": {
        "Nhân viên A": [
            {"san_pham": "Sản phẩm X", "loai": "Cart", "so_luong": 30},
            {"san_pham": "Sản phẩm Y", "loai": "Text", "so_luong": 15},
            {"san_pham": "Sản phẩm Z", "loai": "Video", "so_luong": 10}
        ],
        "Nhân viên B": [...]
    }
}
```

## 🔗 Integration với report_generator.py
File `report_generator.py` đã có sẵn function `format_content_breakdown()` (line 50-68) để format data này thành text hiển thị.

## 📝 Notes
- Default loại video là "Video" nếu field không có giá trị
- Chỉ aggregate records có `so_luong_air > 0`
- Sort theo số lượng giảm dần cho mỗi nhân sự
