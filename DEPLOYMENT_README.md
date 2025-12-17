# Jarvis v5.5.0 - TikTok Ads Crawler

## 🎯 Tính năng mới

✅ **Tự động crawl dư nợ TikTok Ads Manager**
- Sử dụng Playwright để crawl trực tiếp từ trang web
- Cache data 1 giờ để tránh crawl quá nhiều
- Tự động cảnh báo khi đạt 85% hạn mức
- Không cần API key hay OAuth

## 📦 Files mới

```
jarvis-v3/
├── tiktok_ads_crawler.py       # Core crawler logic
├── nixpacks.toml               # Railway build config
├── Aptfile                     # Chromium dependencies
├── TIKTOK_CRAWLER_SETUP.md     # Hướng dẫn chi tiết
└── convert_cookies.py          # Helper script convert cookies
```

## 🚀 Quick Start

### 1. Deploy Railway

```bash
git add .
git commit -m "Add TikTok Ads Crawler v5.5.0"
git push railway main
```

Railway sẽ tự động:
- Install Chromium
- Install Playwright
- Setup dependencies

### 2. Setup Cookies

**Cách nhanh nhất:**

1. Đăng nhập https://ads.tiktok.com/i18n/account/payment?aadvid=7089362853240553474

2. F12 → Console → Paste code:
```javascript
copy(JSON.stringify(document.cookie.split('; ').map(c => {
  const [name, value] = c.split('=');
  return {name, value, domain: '.tiktok.com', path: '/'};
})))
```

3. Railway Dashboard → Environment Variables
   - Tên: `TIKTOK_COOKIES_JSON`
   - Value: Paste cookies (Ctrl+V)

4. Redeploy

### 3. Update code để load cookies từ env

Thêm vào `tiktok_ads_crawler.py`:

```python
def load_cookies() -> Optional[list]:
    """Load cookies from env or file"""
    # Try env first
    cookies_json = os.getenv("TIKTOK_COOKIES_JSON")
    if cookies_json:
        try:
            cookies = json.loads(cookies_json)
            print(f"✅ Loaded {len(cookies)} cookies from env")
            return cookies
        except:
            pass
    
    # Try file
    try:
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE, 'r') as f:
                cookies = json.load(f)
                print(f"✅ Loaded {len(cookies)} cookies from file")
                return cookies
    except Exception as e:
        print(f"❌ Error loading cookies: {e}")
    return None
```

## 💬 Sử dụng

**Xem dư nợ:**
```
@Jarvis dư nợ TikTok
@Jarvis TKQC
```

**Làm mới (bypass cache):**
```
@Jarvis TKQC refresh
```

## 📊 Response

```
💰 BÁO CÁO TÀI KHOẢN TIKTOK ADS
📅 Cập nhật: 23:15 16/12/2024

✅ Chenglovehair0422
🆔 ID: 7089362853240553474

💳 Dư nợ hiện tại: 105,672,606 / 163,646,248 VND
📊 Tỷ lệ sử dụng: 64.6%
📆 Thanh toán tiếp theo: Jan 1, 2026

✅ Mức sử dụng an toàn

📦 Dữ liệu từ cache
🕐 Cập nhật lúc: 23:10 16/12
```

## ⚙️ Environment Variables

```env
TIKTOK_PRIMARY_ADVERTISER_ID=7089362853240553474
TIKTOK_CREDIT_LIMIT=163646248
TIKTOK_WARNING_THRESHOLD=85
TIKTOK_COOKIES_JSON=<JSON cookies>
```

## 🔧 Troubleshooting

### "Cần đăng nhập lại"

→ Cookies hết hạn, làm lại bước 2

### "Chromium crash"

→ Railway memory thấp:
1. Settings → Resources
2. Tăng RAM (ít nhất 1GB)

### "Cannot find chromium"

→ Build failed:
1. Check nixpacks.toml có trong repo
2. Redeploy

### Debug logs

Railway Dashboard → Deployments → View Logs:
- `📸 Screenshot saved` = Crawl thành công
- `📝 Page content length` = Đã load trang
- `❌ Crawler error` = Có lỗi

## 📁 Debug Files

Khi crawl, Jarvis tạo files để debug:
- `/tmp/tiktok_ads_page.png` - Screenshot trang
- `/tmp/tiktok_ads_content.html` - HTML content

Railway Volume để xem files:
1. Settings → Volumes
2. Mount `/tmp` volume
3. Download files để check

## 🎯 Features

✅ Auto crawl with cache (1 hour)
✅ Force refresh on demand
✅ Warning alerts at 85% threshold
✅ Screenshot for debugging
✅ Cookie persistence
✅ Error handling
✅ Fallback mechanisms

## 📝 Notes

- Cookies cần refresh ~30 ngày
- Cache TTL: 1 giờ
- Crawl time: ~5-10 giây
- Memory usage: ~200-300MB

## 🔄 Upgrade từ v5.4.0

Old version (manual input) → New version (crawler):
1. Remove manual debt commands
2. Deploy v5.5.0
3. Setup cookies
4. Test: `@Jarvis TKQC`

## 📞 Support

Issues? Check:
1. TIKTOK_CRAWLER_SETUP.md (hướng dẫn chi tiết)
2. Railway logs
3. Debug files in `/tmp`

---

**Version:** 5.5.0
**Date:** December 17, 2025
**Author:** Claude + Phong
