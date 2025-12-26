"""
TikTok Ads Web Crawler - Version 5.7.7
Fixed: Increased wait time + retry logic for content loading
"""
import os
import re
import json
from datetime import datetime
from typing import Optional, Dict, Any

# Config from env
PRIMARY_ADVERTISER_ID = os.getenv("TIKTOK_PRIMARY_ADVERTISER_ID", "7089362853240553474")
CREDIT_LIMIT = float(os.getenv("TIKTOK_CREDIT_LIMIT", "163646248"))
WARNING_THRESHOLD = float(os.getenv("TIKTOK_WARNING_THRESHOLD", "85"))
COOKIES_FILE = "/tmp/tiktok_ads_cookies.json"

# Cache
_cached_data: Dict[str, Any] = {
    "spending": 0,
    "credit_limit": CREDIT_LIMIT,
    "next_billing_date": None,
    "account_name": "Chenglovehair0422",
    "updated_at": None,
    "cache_ttl": 3600,  # 1 hour
}


def is_cache_valid() -> bool:
    """Check if cached data is still valid"""
    if not _cached_data["updated_at"]:
        return False
    try:
        updated = datetime.fromisoformat(_cached_data["updated_at"])
        delta = (datetime.now() - updated).total_seconds()
        return delta < _cached_data["cache_ttl"]
    except:
        return False


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
        
        # Handle sameSite - Playwright requires: Strict, Lax, None
        same_site = cookie.get("sameSite", "Lax")
        if isinstance(same_site, str):
            same_site = same_site.lower()
            if same_site in ["strict", "lax", "none"]:
                clean_cookie["sameSite"] = same_site.capitalize()
            elif same_site == "no_restriction":
                clean_cookie["sameSite"] = "None"
            else:
                clean_cookie["sameSite"] = "Lax"
        else:
            clean_cookie["sameSite"] = "Lax"
        
        # Handle secure flag
        if cookie.get("secure"):
            clean_cookie["secure"] = True
        
        # Handle httpOnly
        if cookie.get("httpOnly"):
            clean_cookie["httpOnly"] = True
        
        normalized.append(clean_cookie)
    
    print(f"✅ Normalized {len(normalized)} cookies")
    return normalized


async def crawl_tiktok_ads() -> Dict[str, Any]:
    """
    Crawl TikTok Ads Manager để lấy thông tin dư nợ
    Sử dụng Playwright để render JavaScript
    """
    cookies = load_cookies()
    
    if not cookies:
        return {
            "success": False,
            "error": "You need to set cookies to crawl TikTok ads",
            "help": "Set TIKTOK_COOKIES_JSON env variable with cookies from TikTok Ads Manager"
        }
    
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return {
            "success": False,
            "error": "Playwright not installed. Run: pip install playwright && playwright install chromium"
        }
    
    url = f"https://ads.tiktok.com/i18n/account/payment?aadvid={PRIMARY_ADVERTISER_ID}"
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            # Add cookies
            await context.add_cookies(cookies)
            
            page = await context.new_page()
            
            print(f"🌐 Navigating to {url}")
            # v5.7.8: Use networkidle for better content loading
            try:
                await page.goto(url, wait_until='networkidle', timeout=30000)
                print("✅ Navigation complete (networkidle)")
            except Exception as nav_err:
                print(f"⚠️ networkidle timeout, trying domcontentloaded: {nav_err}")
                await page.goto(url, wait_until='domcontentloaded', timeout=15000)
            
            # v5.7.8: Wait longer for JS to render spending data - 15s
            print("⏳ Waiting 15s for JS to render...")
            await page.wait_for_timeout(15000)
            
            # v5.7.8: Check content with 5 attempts, 5s each
            spending_text = ""
            for attempt in range(5):
                spending_text = await page.evaluate('() => document.body.innerText')
                content_len = len(spending_text)
                has_spending = 'Spending' in spending_text
                has_vnd = 'VND' in spending_text
                
                print(f"📊 Attempt {attempt + 1}: len={content_len}, has_Spending={has_spending}, has_VND={has_vnd}")
                
                if has_spending or has_vnd or content_len > 1000:
                    print(f"✅ Content loaded after {attempt + 1} attempt(s), {content_len} chars")
                    break
                    
                print(f"⏳ Content not ready, waiting 5s more...")
                await page.wait_for_timeout(5000)
            
            # Check if login required
            content = await page.content()
            if 'login' in content.lower() and 'sign in' in content.lower():
                await browser.close()
                return {
                    "success": False,
                    "error": "Cookies expired - need to re-login and update cookies"
                }
            
            # v5.7.7: Log content info for debugging
            page_title = await page.title()
            print(f"📄 Page title: {page_title}")
            print(f"📏 Content length: {len(spending_text)} chars")
            
            # Log first 300 chars to see what we got
            print(f"📝 Content preview: {spending_text[:300]}")
            
            # Parse spending from text - v5.7.6 multi-strategy parsing
            spending = 0
            
            # Strategy 1: Look for "Spending so far" pattern
            spending_match = re.search(r'Spending so far[^\d]*(\d[\d,\.]+)', spending_text, re.IGNORECASE)
            if spending_match:
                num_str = spending_match.group(1).replace(',', '').replace('.', '')
                try:
                    spending = float(num_str)
                    print(f"✅ Strategy 1 - Found 'Spending so far': {spending:,.0f}")
                except:
                    pass
            
            # Strategy 2: Look for "billing cycle" + number + VND
            if spending == 0:
                billing_match = re.search(r'billing cycle[^\d]*(\d[\d,\.]+)\s*VND', spending_text, re.IGNORECASE)
                if billing_match:
                    num_str = billing_match.group(1).replace(',', '').replace('.', '')
                    try:
                        spending = float(num_str)
                        print(f"✅ Strategy 2 - Found 'billing cycle': {spending:,.0f}")
                    except:
                        pass
            
            # Strategy 3: Find all VND amounts and pick the largest > 1M
            if spending == 0:
                vnd_matches = re.findall(r'(\d{1,3}(?:,\d{3})+|\d+)\s*VND', spending_text)
                print(f"🔍 Found {len(vnd_matches)} VND amounts: {vnd_matches[:5]}")
                for num_str in vnd_matches:
                    try:
                        val = float(num_str.replace(',', ''))
                        if val > spending and 1000000 < val < 500000000:
                            spending = val
                    except:
                        pass
                if spending > 0:
                    print(f"✅ Strategy 3 - Largest VND amount: {spending:,.0f}")
            
            # Strategy 4: Find any large number in reasonable range
            if spending == 0:
                numbers = re.findall(r'(\d{1,3}(?:,\d{3})+)', spending_text)
                for num_str in numbers:
                    try:
                        val = float(num_str.replace(',', ''))
                        if 1000000 < val < 500000000:
                            spending = val
                            print(f"✅ Strategy 4 - Large number: {spending:,.0f}")
                            break
                    except:
                        pass
            
            if spending == 0:
                print(f"⚠️ Could not parse spending. Content sample: {spending_text[:300]}")
            
            # Save screenshot for debug
            try:
                await page.screenshot(path='/tmp/tiktok_ads_page.png')
                print("📸 Screenshot saved")
            except:
                pass
            
            await browser.close()
            
            # Update cache
            _cached_data["spending"] = spending
            _cached_data["updated_at"] = datetime.now().isoformat()
            
            return {
                "success": True,
                "spending": spending,
                "credit_limit": CREDIT_LIMIT,
                "account_name": "Chenglovehair0422",
                "updated_at": _cached_data["updated_at"]
            }
            
    except Exception as e:
        print(f"❌ Crawler error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": f"Crawler error: {str(e)}"
        }


async def get_spending_data(force_refresh: bool = False) -> Dict[str, Any]:
    """Get TikTok Ads spending data"""
    
    # Return cache if valid
    if not force_refresh and is_cache_valid():
        return {
            "success": True,
            "spending": _cached_data["spending"],
            "credit_limit": _cached_data["credit_limit"],
            "next_billing_date": _cached_data["next_billing_date"],
            "account_name": _cached_data["account_name"],
            "updated_at": _cached_data["updated_at"],
            "from_cache": True
        }
    
    # Check if cookies exist
    cookies = load_cookies()
    if not cookies:
        return {
            "success": False,
            "error": "You need to set cookies to crawl TikTok ads",
            "help": "Set TIKTOK_COOKIES_JSON env variable"
        }
    
    # Try to crawl
    result = await crawl_tiktok_ads()
    return result


def format_spending_report(data: Dict[str, Any]) -> str:
    """Format spending data into readable report"""
    if not data.get("success"):
        error = data.get("error", "Không có dữ liệu")
        help_text = data.get("help", "")
        msg = f"❌ {error}"
        if help_text:
            msg += f"\n💡 {help_text}"
        return msg
    
    spending = data.get("spending", 0)
    credit_limit = data.get("credit_limit", CREDIT_LIMIT)
    percentage = (spending / credit_limit * 100) if credit_limit > 0 else 0
    
    lines = [
        "💰 **BÁO CÁO TIKTOK ADS**",
        f"📅 Cập nhật: {datetime.now().strftime('%H:%M %d/%m/%Y')}",
        "",
        f"💳 Dư nợ: {spending:,.0f} / {credit_limit:,.0f} VND",
        f"📊 Tỷ lệ: {percentage:.1f}%",
    ]
    
    if percentage >= WARNING_THRESHOLD:
        lines.append(f"\n⚠️ **CẢNH BÁO: Dư nợ đạt {percentage:.1f}%!**")
    else:
        lines.append("\n✅ Mức sử dụng an toàn")
    
    if data.get("from_cache"):
        lines.append(f"\n📦 _Dữ liệu từ cache_")
    
    return "\n".join(lines)


def is_tiktok_ads_query(text: str) -> bool:
    """Check if text is asking about TikTok Ads"""
    keywords = ["số dư tiktok", "tiktok ads", "tkqc", "dư nợ tiktok", "dư nợ ads", "tài khoản quảng cáo"]
    return any(kw in text.lower() for kw in keywords)
