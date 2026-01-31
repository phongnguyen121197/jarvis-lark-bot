# JARVIS v5.7.20 - Fix Content Matching + Notes

## 🐛 BUGS FIXED

### 1. Content chỉ đếm 70 thay vì ~235
**Nguyên nhân**: Tên nhân sự từ Dashboard **không match** với tên từ Booking

Ví dụ:
| Dashboard | Booking |
|-----------|---------|
| `"Nguyễn Như Mai - PR Bookingg"` | `"Nguyễn Như Mai - PR Booking"` |
| `"Lê Thuỳ Dương"` | `"Lê Thuỳ Dương (vịt) - PR Booking"` |
| `"Bảo Châu - Booking Remote"` | `"Châu Đặng - Booking Remote"` |

**Fix**: 3 bước matching:

```python
# 1. Exact match
content_items = content_by_nhan_su.get(nhan_su_name, [])

# 2. Normalized match (loại bỏ suffix, ngoặc)
# "Lê Thuỳ Dương (vịt) - PR Booking" → "lê thuỳ dương"
normalized = normalize_name(nhan_su_name)

# 3. Partial match (ít nhất 2 từ giống nhau)
# "nguyễn như mai" ∩ "nguyễn như mai" = 3 từ ✅
```

### 2. Notes "Done # 1" không nhận diện
**Fix**: Regex thêm `\s*` để match khoảng trắng

## 📊 DEBUG LOGS MỚI

```
🔍 Tên từ Dashboard: ['Nguyễn Như Mai - PR Bookingg', 'Bảo Châu - Booking Remote', ...]
🔍 Tên từ Booking: ['Nguyễn Như Mai - PR Booking', 'Châu Đặng - Booking Remote', ...]
   ✅ Matched (normalized): 'Nguyễn Như Mai - PR Bookingg' → 'nguyễn như mai' (15 items)
   ✅ Matched (partial): 'Lê Thuỳ Dương' → 'Lê Thuỳ Dương (vịt) - PR Booking' (8 items)
   ⚠️ No content match for: 'Bảo Châu - Booking Remote'
```

## 🚀 DEPLOYMENT

```powershell
cd D:\jarvis-lark-bot

copy lark_base_v5.7.20_fixed.py lark_base.py
copy notes_manager_v5.7.20_fixed.py notes_manager.py

git add lark_base.py notes_manager.py
git commit -m "v5.7.20: Fix content name matching with normalize + partial match"
git push origin main
```

## ✅ EXPECTED RESULT

**Trước**: Content: 43 cart Nước hoa... (chỉ 1 người = 70)
**Sau**: Content: ~150 cart Nước hoa... (tất cả nhân sự được match)

---
**Version**: 5.7.20
**Date**: 2026-01-27
**Type**: Bugfix
