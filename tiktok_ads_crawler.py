"""
TikTok Ads Web Crawler
Crawl spending data từ TikTok Ads Manager bằng Playwright
"""
import os
import re
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeout

# Config
TIKTOK_ADS_URL = "https://ads.tiktok.com/i18n/account/payment"
PRIMARY_ADVERTISER_ID = os.getenv("TIKTOK_PRIMARY_ADVERTISER_ID", "7089362853240553474")
COOKIES_FILE = "/tmp/tiktok_ads_cookies.json"

# Hạn mức tín dụng
CREDIT_LIMIT = float(os.getenv("TIKTOK_CREDIT_LIMIT", "163646248"))
WARNING_THRESHOLD = float(os.getenv("TIKTOK_WARNING_THRESHOLD", "85"))

# Cache
_cached_data: Dict[str, Any] = {
    "spending": 0,
    "credit_limit": CREDIT_LIMIT,
    "next_billing_date": None,
    "account_name": None,
    "updated_at": None,
    "cache_ttl": 3600,  # 1 hour
}


def save_cookies(cookies: list):
    """Lưu cookies vào file"""
    try:
        with open(COOKIES_FILE, 'w') as f:
            json.dump(cookies, f)
        print(f"✅ Saved {len(cookies)} cookies")
    except Exception as e:
        print(f"❌ Error saving cookies: {e}")


def load_cookies() -> Optional[list]:
    """Load cookies từ env hoặc file"""
    cookies = None
    
    # Try env first (Railway friendly)
    cookies_json = os.getenv("TIKTOK_COOKIES_JSON")
    if cookies_json:
        try:
            cookies = json.loads(cookies_json)
            print(f"✅ Loaded {len(cookies)} cookies from env")
        except Exception as e:
            print(f"⚠️ Failed to parse TIKTOK_COOKIES_JSON: {e}")
    
    # Try file fallback
    if not cookies:
        try:
            if os.path.exists(COOKIES_FILE):
                with open(COOKIES_FILE, 'r') as f:
                    cookies = json.load(f)
                    print(f"✅ Loaded {len(cookies)} cookies from file")
        except Exception as e:
            print(f"❌ Error loading cookies from file: {e}")
    
    if not cookies:
        print("⚠️ No cookies found (env or file)")
        return None
    
    # Normalize cookies for Playwright
    return normalize_cookies(cookies)


def normalize_cookies(cookies: list) -> list:
    """
    Normalize cookies để tương thích với Playwright
    - Fix sameSite values
    - Remove invalid fields
    """
    normalized = []
    
    for cookie in cookies:
        # Chỉ giữ các field cần thiết
        clean_cookie = {
            "name": cookie.get("name", ""),
            "value": cookie.get("value", ""),
            "domain": cookie.get("domain", ".tiktok.com"),
            "path": cookie.get("path", "/"),
        }
        
        # Skip cookies without name or value
        if not clean_cookie["name"] or not clean_cookie["value"]:
            continue
        
        # Handle sameSite - Playwright requires: Strict, Lax, or None
        same_site = str(cookie.get("sameSite", "")).lower()
        if same_site == "strict":
            clean_cookie["sameSite"] = "Strict"
        elif same_site == "none":
            clean_cookie["sameSite"] = "None"
        else:
            # Default to Lax for unspecified/invalid/lax
            clean_cookie["sameSite"] = "Lax"
        
        # Handle secure
        if cookie.get("secure"):
            clean_cookie["secure"] = True
        
        # Handle httpOnly
        if cookie.get("httpOnly"):
            clean_cookie["httpOnly"] = True
        
        # Handle expires (Playwright uses expires as seconds since epoch)
        if cookie.get("expirationDate"):
            try:
                clean_cookie["expires"] = float(cookie["expirationDate"])
            except:
                pass
        elif cookie.get("expires") and cookie.get("expires") != -1:
            try:
                clean_cookie["expires"] = float(cookie["expires"])
            except:
                pass
        
        normalized.append(clean_cookie)
    
    print(f"🔧 Normalized {len(normalized)} cookies")
    return normalized


def is_cache_valid() -> bool:
    """Kiểm tra cache còn valid không"""
    if not _cached_data["updated_at"]:
        return False
    
    try:
        updated = datetime.fromisoformat(_cached_data["updated_at"])
        now = datetime.now()
        delta = (now - updated).total_seconds()
        return delta < _cached_data["cache_ttl"]
    except:
        return False


def get_cached_data() -> Dict[str, Any]:
    """Lấy data từ cache"""
    return _cached_data.copy()


async def crawl_spending_data(advertiser_id: str = None) -> Dict[str, Any]:
    """
    Crawl spending data từ TikTok Ads Manager
    """
    target_id = advertiser_id or PRIMARY_ADVERTISER_ID
    url = f"{TIKTOK_ADS_URL}?aadvid={target_id}"
    
    print(f"🔍 Crawling TikTok Ads: {url}")
    
    result = {
        "success": False,
        "spending": 0,
        "credit_limit": CREDIT_LIMIT,
        "next_billing_date": None,
        "account_name": None,
        "error": None,
        "login_required": False
    }
    
    browser = None
    
    try:
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-software-rasterizer',
                    '--disable-extensions',
                    '--single-process',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-US'
            )
            
            # Load cookies nếu có
            cookies = load_cookies()
            if cookies:
                await context.add_cookies(cookies)
                print("🍪 Added cookies to context")
            
            page = await context.new_page()
            
            # Set extra headers
            await page.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            })
            
            # Navigate to payment page
            try:
                response = await page.goto(url, wait_until='networkidle', timeout=30000)
                print(f"📄 Page loaded: {page.url}")
            except PlaywrightTimeout:
                print("⏱️ Timeout waiting for networkidle, continuing...")
            
            # Check if redirected to login
            current_url = page.url
            if 'login' in current_url.lower() or 'signin' in current_url.lower():
                result["error"] = "Cần đăng nhập lại TikTok Ads Manager"
                result["login_required"] = True
                print("🔐 Login required")
                await browser.close()
                return result
            
            # Wait for content to load - try to find spending text
            try:
                await page.wait_for_selector('text=Spending so far', timeout=10000)
                print("✅ Found 'Spending so far' text")
            except:
                print("⚠️ 'Spending so far' text not found, waiting longer...")
                await page.wait_for_timeout(5000)
            
            # Scroll down to trigger lazy loading
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
            await page.wait_for_timeout(2000)
            
            # Take screenshot for debugging
            try:
                await page.screenshot(path='/tmp/tiktok_ads_page.png', full_page=True)
                print("📸 Screenshot saved to /tmp/tiktok_ads_page.png")
            except:
                pass
            
            # Get page content
            content = await page.content()
            
            print(f"📝 Page content length: {len(content)} chars")
            
            # Parse spending data - multiple patterns
            # HTML structure: "Spending so far..." <span>129,265,101</span> "VND"
            spending_patterns = [
                # Pattern cho số trong <span> tag sau "Spending so far"
                r'Spending\s+so\s+far[^<]*<span[^>]*>([\d,]+)</span>',
                # Pattern cho số sau "current billing cycle"
                r'current\s+billing\s+cycle[^<]*<span[^>]*>([\d,]+)</span>',
                # Pattern đơn giản - số trong span
                r'billing\s+cycle[^<]*<span[^>]*>([\d,]+)</span>',
            ]
            
            found_spending = False
            for pattern in spending_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    spending_str = match.group(1).replace(',', '').replace('.', '')
                    print(f"🔎 Pattern matched: {pattern[:40]}... → raw: {match.group(1)} → clean: {spending_str}")
                    try:
                        spending = float(spending_str)
                        # Sanity check: số phải > 1 triệu (1,000,000)
                        if spending > 1000000:
                            result["spending"] = spending
                            result["success"] = True
                            found_spending = True
                            print(f"✅ Found spending: {result['spending']:,.0f} VND")
                            break
                        else:
                            print(f"⚠️ Spending too small: {spending}")
                    except ValueError as e:
                        print(f"⚠️ ValueError: {e}")
                        continue
            
            # Fallback: tìm spending và credit limit riêng biệt
            if not found_spending:
                print("⚠️ Could not find spending with patterns, trying smart fallback...")
                
                # 1. Tìm Spending: số trong <span> sau "Spending so far"
                spending_match = re.search(
                    r'Spending\s+so\s+far[^<]*<span[^>]*>([\d,]+)</span>',
                    content, re.IGNORECASE | re.DOTALL
                )
                if spending_match:
                    spending_str = spending_match.group(1).replace(',', '')
                    try:
                        result["spending"] = float(spending_str)
                        result["success"] = True
                        found_spending = True
                        print(f"✅ Found spending in span: {result['spending']:,.0f}")
                    except:
                        pass
                
                # 2. Tìm Credit Limit: số trong <span> sau "spending reaches"
                credit_match = re.search(
                    r'spending\s+reaches[^<]*<span[^>]*>([\d,]+)</span>',
                    content, re.IGNORECASE | re.DOTALL
                )
                if not credit_match:
                    # Try simpler pattern
                    credit_match = re.search(
                        r'spending\s+reaches[^0-9]*([\d,]+)',
                        content, re.IGNORECASE
                    )
                if credit_match:
                    limit_str = credit_match.group(1).replace(',', '')
                    try:
                        result["credit_limit"] = float(limit_str)
                        print(f"✅ Found credit limit: {result['credit_limit']:,.0f}")
                    except:
                        pass
                
                # 3. Nếu vẫn không tìm được spending, thử tìm tất cả số trong <span> tags
                if not found_spending:
                    print("⚠️ Smart fallback failed, trying span number scan...")
                    
                    # Tìm tất cả số trong <span> tags
                    span_numbers = re.findall(r'<span[^>]*>([\d,]+)</span>', content)
                    large_numbers = []
                    seen = set()
                    for num_str in span_numbers:
                        if num_str not in seen and ',' in num_str:  # Chỉ lấy số có comma (lớn)
                            seen.add(num_str)
                            clean = num_str.replace(',', '')
                            try:
                                num = float(clean)
                                if num > 1000000:
                                    large_numbers.append((num_str, num))
                            except:
                                pass
                    
                    print(f"📊 Found {len(large_numbers)} numbers in span: {large_numbers[:5]}")
                    
                    # Nếu có ít nhất 2 số, số lớn nhất = credit limit, số nhỏ hơn = spending
                    if len(large_numbers) >= 2:
                        sorted_nums = sorted(large_numbers, key=lambda x: x[1], reverse=True)
                        result["credit_limit"] = sorted_nums[0][1]
                        result["spending"] = sorted_nums[1][1]
                        result["success"] = True
                        found_spending = True
                        print(f"✅ Span scan: spending={sorted_nums[1][1]:,.0f}, limit={sorted_nums[0][1]:,.0f}")
                
                # Save content for debugging
                with open('/tmp/tiktok_ads_content.html', 'w', encoding='utf-8') as f:
                    f.write(content)
                print("📝 Saved page content to /tmp/tiktok_ads_content.html")
            
            # Parse credit limit (nếu chưa có từ fallback)
            if result.get("credit_limit", 0) == CREDIT_LIMIT:
                limit_patterns = [
                    # Pattern cho số trong span sau "spending reaches"
                    r'spending\s+reaches[^<]*<span[^>]*>([\d,]+)</span>',
                    r'spending\s+reaches[^0-9]*([\d,]+)',
                    r'Or\s+when\s+ad\s+spending\s+reaches[^0-9]*([\d,]+)',
                ]
                
                for pattern in limit_patterns:
                    match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                    if match:
                        limit_str = match.group(1).replace(',', '')
                        try:
                            limit = float(limit_str)
                            if limit > 100000000:  # > 100M VND
                                result["credit_limit"] = limit
                                print(f"✅ Found credit limit: {result['credit_limit']:,.0f} VND")
                                break
                        except ValueError:
                            continue
            
            # Parse next billing date
            date_patterns = [
                r'Next\s+billing\s+date.*?([A-Z][a-z]{2}\s+\d{1,2},?\s+\d{4})',
                r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}',
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    result["next_billing_date"] = match.group(1) if match.lastindex else match.group(0)
                    print(f"✅ Found next billing: {result['next_billing_date']}")
                    break
            
            # Parse account name
            name_patterns = [
                r'Chenglovehair\d*',
                r'KALLE\s+FEUM',
            ]
            
            for pattern in name_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    result["account_name"] = match.group(0)
                    print(f"✅ Found account name: {result['account_name']}")
                    break
            
            # Save cookies for next time
            cookies = await context.cookies()
            save_cookies(cookies)
            
            await browser.close()
            
            # Update cache
            if result["success"]:
                _cached_data["spending"] = result["spending"]
                _cached_data["credit_limit"] = result["credit_limit"]
                _cached_data["next_billing_date"] = result["next_billing_date"]
                _cached_data["account_name"] = result["account_name"]
                _cached_data["updated_at"] = datetime.now().isoformat()
                print(f"💾 Updated cache: {_cached_data['spending']:,.0f} VND")
            
    except Exception as e:
        print(f"❌ Crawler error: {e}")
        import traceback
        traceback.print_exc()
        result["error"] = str(e)
        
        if browser:
            try:
                await browser.close()
            except:
                pass
    
    return result


async def get_spending_data(force_refresh: bool = False) -> Dict[str, Any]:
    """
    Lấy spending data (từ cache hoặc crawl mới)
    """
    # Check cache
    if not force_refresh and is_cache_valid():
        print("📦 Using cached data")
        return {
            "success": True,
            "spending": _cached_data["spending"],
            "credit_limit": _cached_data["credit_limit"],
            "next_billing_date": _cached_data["next_billing_date"],
            "account_name": _cached_data["account_name"],
            "updated_at": _cached_data["updated_at"],
            "from_cache": True
        }
    
    # Crawl new data
    print("🔄 Cache expired or force refresh, crawling...")
    return await crawl_spending_data()


def format_spending_report(data: Dict[str, Any]) -> str:
    """Format báo cáo chi tiêu"""
    if not data.get("success"):
        error = data.get("error", "Không thể lấy dữ liệu")
        
        if data.get("login_required"):
            return (
                "❌ **Cần đăng nhập TikTok Ads Manager**\n\n"
                "💡 **Hướng dẫn:**\n"
                "1. Đăng nhập TikTok Ads Manager trên trình duyệt\n"
                "2. Xuất cookies (dùng extension như EditThisCookie)\n"
                "3. Lưu cookies vào `/tmp/tiktok_ads_cookies.json`\n"
                "4. Deploy lại Railway\n\n"
                f"📝 Chi tiết lỗi: {error}"
            )
        
        return f"❌ {error}\n\n💡 Kiểm tra logs để debug"
    
    spending = data["spending"]
    credit_limit = data["credit_limit"]
    percentage = (spending / credit_limit * 100) if credit_limit > 0 else 0
    
    lines = [
        "💰 **BÁO CÁO TÀI KHOẢN TIKTOK ADS**",
        f"📅 Cập nhật: {datetime.now().strftime('%H:%M %d/%m/%Y')}",
        "",
    ]
    
    if data.get("account_name"):
        lines.append(f"✅ **{data['account_name']}**")
    else:
        lines.append(f"✅ **Chenglovehair0422**")
    
    lines.append(f"🆔 ID: `{PRIMARY_ADVERTISER_ID}`")
    lines.append("")
    lines.append(f"💳 **Dư nợ hiện tại: {spending:,.0f} / {credit_limit:,.0f} VND**")
    lines.append(f"📊 Tỷ lệ sử dụng: **{percentage:.1f}%**")
    
    if data.get("next_billing_date"):
        lines.append(f"📆 Thanh toán tiếp theo: {data['next_billing_date']}")
    
    lines.append("")
    
    # Warning
    if percentage >= WARNING_THRESHOLD:
        lines.append("🚨" * 5)
        lines.append(f"⚠️ **CẢNH BÁO: Dư nợ đã đạt {percentage:.1f}% hạn mức!**")
        lines.append(f"💡 Hạn mức còn lại: {credit_limit - spending:,.0f} VND")
        lines.append("🚨" * 5)
    elif percentage >= 70:
        lines.append(f"⚠️ Lưu ý: Đã sử dụng {percentage:.1f}% hạn mức")
    else:
        lines.append("✅ Mức sử dụng an toàn")
    
    # Cache info
    if data.get("from_cache"):
        lines.append("")
        lines.append("📦 Dữ liệu từ cache")
    
    if data.get("updated_at"):
        try:
            dt = datetime.fromisoformat(data["updated_at"])
            lines.append(f"🕐 Cập nhật lúc: {dt.strftime('%H:%M %d/%m')}")
        except:
            pass
    
    return "\n".join(lines)


def check_warning_threshold() -> Optional[str]:
    """Kiểm tra cảnh báo"""
    if not _cached_data["spending"]:
        return None
    
    spending = _cached_data["spending"]
    credit_limit = _cached_data["credit_limit"]
    percentage = (spending / credit_limit * 100) if credit_limit > 0 else 0
    
    if percentage >= WARNING_THRESHOLD:
        return (
            "🚨 **CẢNH BÁO TIKTOK ADS** 🚨\n\n"
            f"💳 Dư nợ: **{spending:,.0f} / {credit_limit:,.0f} VND**\n"
            f"📊 Đã sử dụng: **{percentage:.1f}%** hạn mức\n"
            f"💡 Còn lại: {credit_limit - spending:,.0f} VND\n\n"
            "⚠️ Vui lòng kiểm tra và thanh toán sớm!"
        )
    
    return None


def is_tiktok_ads_query(text: str) -> bool:
    """Kiểm tra query TikTok Ads"""
    keywords = [
        "số dư tiktok", "so du tiktok", 
        "tiktok ads", "tkqc", 
        "quảng cáo tiktok", "quang cao tiktok",
        "tiền quảng cáo", "tien quang cao",
        "chi tiêu tiktok", "chi tieu tiktok",
        "dư nợ tiktok", "du no tiktok",
        "dư nợ ads", "du no ads",
        "balance tiktok", "spending tiktok"
    ]
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)
