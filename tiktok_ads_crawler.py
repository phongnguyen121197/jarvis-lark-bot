"""
TikTok Ads Web Crawler - Version 5.7.5
Fixed: Improved selector to find correct spending amount (not "Preview")
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
            await page.goto(url, wait_until='networkidle', timeout=30000)
            
            # Wait for page to load
            await page.wait_for_timeout(3000)
            
            # Check if login required
            content = await page.content()
            if 'login' in content.lower() and 'sign in' in content.lower():
                await browser.close()
                return {
                    "success": False,
                    "error": "Cookies expired - need to re-login and update cookies"
                }
            
            # Try to find spending info - v5.7.5 improved selector
            spending_text = await page.evaluate('''
                () => {
                    const bodyText = document.body.innerText;
                    
                    // Strategy 1: Look for "Spending so far" pattern (exact match from screenshot)
                    const spendingMatch = bodyText.match(/Spending so far[^\\d]*(\\d[\\d,\\.]+)/i);
                    if (spendingMatch) {
                        console.log("Found via 'Spending so far':", spendingMatch[0]);
                        return spendingMatch[0];
                    }
                    
                    // Strategy 2: Look for "billing cycle" pattern
                    const billingMatch = bodyText.match(/billing cycle[^\\d]*(\\d[\\d,\\.]+)/i);
                    if (billingMatch) {
                        console.log("Found via 'billing cycle':", billingMatch[0]);
                        return billingMatch[0];
                    }
                    
                    // Strategy 3: Look for large VND amounts (spending is usually millions)
                    const vndMatches = bodyText.match(/(\\d{1,3}(?:,\\d{3})+(?:\\.\\d+)?)\\s*VND/g);
                    if (vndMatches && vndMatches.length > 0) {
                        // Return the largest VND amount (most likely spending)
                        let maxAmount = 0;
                        let maxMatch = "";
                        for (const m of vndMatches) {
                            const numStr = m.replace(/[^\\d]/g, '');
                            const val = parseInt(numStr);
                            if (val > maxAmount && val > 1000000) {
                                maxAmount = val;
                                maxMatch = m;
                            }
                        }
                        if (maxMatch) {
                            console.log("Found via VND pattern:", maxMatch);
                            return maxMatch;
                        }
                    }
                    
                    // Strategy 4: Element-based search (skip small text like "Preview")
                    const elements = document.querySelectorAll('[class*="balance"], [class*="spending"], [class*="amount"], [class*="money"], [class*="price"]');
                    for (let el of elements) {
                        const text = el.innerText.trim();
                        // Must have digits, be reasonably sized, and not be "Preview" or similar
                        if (text && /\\d{2,}/.test(text) && text.length > 5 && text.length < 200) {
                            if (!text.toLowerCase().includes('preview')) {
                                console.log("Found via element:", text);
                                return text;
                            }
                        }
                    }
                    
                    // Fallback: return body text for parsing
                    console.log("Fallback: returning body text");
                    return bodyText.substring(0, 8000);
                }
            ''')
            
            print(f"✅ Got spending text: {spending_text[:100]}...")
            
            # Parse spending from text - v5.7.5 improved parsing
            spending = 0
            
            # First try to find VND-specific pattern
            vnd_match = re.search(r'(\d{1,3}(?:,\d{3})+|\d+)\s*VND', spending_text)
            if vnd_match:
                num_str = vnd_match.group(1).replace(',', '')
                try:
                    spending = float(num_str)
                    print(f"✅ Found VND amount: {spending:,.0f}")
                except:
                    pass
            
            # Fallback: find numbers in reasonable range
            if spending == 0:
                numbers = re.findall(r'[\d,]+(?:\.\d+)?', spending_text)
                for num in numbers:
                    try:
                        val = float(num.replace(',', ''))
                        if 1000000 < val < 500000000:
                            spending = val
                            print(f"✅ Found spending via fallback: {spending:,.0f}")
                            break
                    except:
                        pass
            
            if spending == 0:
                print(f"⚠️ Could not parse spending from: {spending_text[:200]}")
            
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
