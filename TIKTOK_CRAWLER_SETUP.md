# TikTok Ads Crawler Setup

## 🎯 Tính năng

Jarvis tự động crawl dư nợ TikTok Ads Manager và cảnh báo khi đạt ngưỡng.

## 🚀 Setup lần đầu

### Bước 1: Đăng nhập TikTok Ads Manager

1. Mở trình duyệt (Chrome/Edge)
2. Đăng nhập https://ads.tiktok.com
3. Vào trang Payment: https://ads.tiktok.com/i18n/account/payment?aadvid=7089362853240553474

### Bước 2: Xuất Cookies

**Cách 1: EditThisCookie Extension**
1. Cài extension: https://chrome.google.com/webstore/detail/editthiscookie/fngmhnnpilhplaeedifhccceomclgfbg
2. Click vào icon extension
3. Click biểu tượng "Export" (mũi tên xuống)
4. Copy JSON

**Cách 2: DevTools**
1. F12 → Console tab
2. Paste code:
```javascript
copy(document.cookie)
```
3. Ctrl+V để paste cookies

### Bước 3: Tạo file cookies.json

Tạo file `/tmp/tiktok_ads_cookies.json` với nội dung:

```json
[
  {
    "name": "cookie_name",
    "value": "cookie_value",
    "domain": ".tiktok.com",
    "path": "/"
  }
]
```

### Bước 4: Upload lên Railway

**Option A: Railway Volume**
1. Railway Dashboard → Service → Data → Volumes
2. Tạo volume mount tại `/tmp`
3. Upload file `tiktok_ads_cookies.json`

**Option B: Environment Variable**
1. Railway Dashboard → Variables
2. Thêm `TIKTOK_COOKIES` = `<paste cookies JSON>`
3. Code sẽ tự động load từ env

### Bước 5: Deploy

```bash
git add .
git commit -m "Add TikTok Ads Crawler"
git push railway main
```

## 📝 Sử dụng

**Xem dư nợ:**
```
@Jarvis dư nợ TikTok
@Jarvis TKQC
```

**Làm mới data (bypass cache):**
```
@Jarvis TKQC refresh
@Jarvis dư nợ TikTok làm mới
```

## 🔧 Cấu hình

Environment variables:

```env
TIKTOK_PRIMARY_ADVERTISER_ID=7089362853240553474
TIKTOK_CREDIT_LIMIT=163646248
TIKTOK_WARNING_THRESHOLD=85
```

## 🐛 Troubleshooting

### Login Required

Nếu gặp lỗi "Cần đăng nhập lại":
1. Cookies đã hết hạn
2. Làm lại Bước 2-4

### Page Not Found

Check URL có đúng advertiser ID không:
```
https://ads.tiktok.com/i18n/account/payment?aadvid=YOUR_ID
```

### Chromium Crash

Railway memory issue:
1. Tăng RAM instance
2. Hoặc giảm số concurrent requests

## 📊 Cache

- Data cache 1 giờ
- Force refresh bằng keyword "refresh" hoặc "làm mới"
- Auto-refresh mỗi ngày lúc 9:00 AM

## 🔐 Bảo mật

- Cookies được lưu trong `/tmp` (ephemeral storage)
- Railway restart = mất cookies
- Cân nhắc dùng Railway Volume cho persistent storage
