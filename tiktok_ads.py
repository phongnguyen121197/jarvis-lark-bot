"""
TikTok Ads API Integration
Theo dõi chi tiêu quảng cáo TikTok
"""
import os
import json
import httpx
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ============ CONFIG ============
TIKTOK_APP_ID = os.getenv("TIKTOK_APP_ID", "7584349619291684880")
TIKTOK_APP_SECRET = os.getenv("TIKTOK_APP_SECRET", "")
TIKTOK_REDIRECT_URI = os.getenv("TIKTOK_REDIRECT_URI", "https://jarvis-lark-bot-production.up.railway.app/tiktok/callback")

# Advertiser ID chính
PRIMARY_ADVERTISER_ID = os.getenv("TIKTOK_PRIMARY_ADVERTISER_ID", "7089362853240553474")

# Hạn mức tín dụng (Credit Limit) - có thể set từ env hoặc hardcode
CREDIT_LIMIT = float(os.getenv("TIKTOK_CREDIT_LIMIT", "163646248"))

# Ngưỡng cảnh báo (%)
WARNING_THRESHOLD = float(os.getenv("TIKTOK_WARNING_THRESHOLD", "85"))

# API Base URLs
TIKTOK_AUTH_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"
TIKTOK_API_BASE = "https://business-api.tiktok.com/open_api/v1.3"

# Storage for tokens
_token_storage: Dict[str, Any] = {}


def get_authorization_url(state: str = "jarvis_auth") -> str:
    """Tạo URL để user authorize app"""
    return (
        f"https://business-api.tiktok.com/portal/auth"
        f"?app_id={TIKTOK_APP_ID}"
        f"&state={state}"
        f"&redirect_uri={TIKTOK_REDIRECT_URI}"
    )


async def exchange_code_for_token(auth_code: str) -> Dict[str, Any]:
    """Đổi authorization code lấy access token"""
    if not TIKTOK_APP_SECRET:
        return {"error": "TIKTOK_APP_SECRET not configured"}
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            TIKTOK_AUTH_URL,
            json={
                "app_id": TIKTOK_APP_ID,
                "secret": TIKTOK_APP_SECRET,
                "auth_code": auth_code,
            },
            headers={"Content-Type": "application/json"}
        )
        
        result = response.json()
        print(f"🔐 Token exchange response: {result}")
        
        if result.get("code") == 0:
            data = result.get("data", {})
            _token_storage["access_token"] = data.get("access_token")
            _token_storage["advertiser_ids"] = data.get("advertiser_ids", [])
            _token_storage["scope"] = data.get("scope", [])
            _token_storage["token_time"] = datetime.now().isoformat()
            
            return {
                "success": True,
                "access_token": data.get("access_token"),
                "advertiser_ids": data.get("advertiser_ids", []),
                "scope": data.get("scope", [])
            }
        else:
            return {
                "success": False,
                "error": result.get("message", "Unknown error"),
                "code": result.get("code")
            }


def get_stored_token() -> Optional[str]:
    return _token_storage.get("access_token")


def get_stored_advertiser_ids() -> List[str]:
    return _token_storage.get("advertiser_ids", [])


async def get_advertiser_info(access_token: str, advertiser_id: str) -> Dict[str, Any]:
    """Lấy thông tin advertiser"""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{TIKTOK_API_BASE}/advertiser/info/",
            params={
                "advertiser_ids": json.dumps([advertiser_id]),
            },
            headers={
                "Access-Token": access_token,
                "Content-Type": "application/json"
            }
        )
        
        result = response.json()
        print(f"📊 Advertiser info for {advertiser_id}: {result}")
        return result


async def get_report_spending(access_token: str, advertiser_id: str) -> Dict[str, Any]:
    """
    Lấy báo cáo chi tiêu từ Report API
    Tính tổng spend trong billing cycle (từ đầu tháng)
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    first_of_month = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"{TIKTOK_API_BASE}/report/integrated/get/",
            params={
                "advertiser_id": advertiser_id,
                "report_type": "BASIC",
                "dimensions": json.dumps(["advertiser_id"]),
                "data_level": "AUCTION_ADVERTISER",
                "start_date": first_of_month,
                "end_date": end_date,
                "metrics": json.dumps(["spend", "cash_spend", "voucher_spend"]),
            },
            headers={
                "Access-Token": access_token,
                "Content-Type": "application/json"
            }
        )
        
        result = response.json()
        print(f"📈 Report spending for {advertiser_id}: {result}")
        return result


async def get_account_spending(access_token: str, advertiser_id: str) -> Dict[str, Any]:
    """
    Lấy thông tin chi tiêu của tài khoản
    """
    result = {
        "advertiser_id": advertiser_id,
        "success": False,
        "name": "Unknown",
        "status": "Unknown",
        "currency": "VND",
        "spending": 0,
        "credit_limit": CREDIT_LIMIT,
    }
    
    # 1. Lấy thông tin cơ bản
    info_response = await get_advertiser_info(access_token, advertiser_id)
    if info_response.get("code") == 0:
        accounts = info_response.get("data", {}).get("list", [])
        if accounts:
            acc = accounts[0]
            result["name"] = acc.get("name", "Unknown")
            result["status"] = acc.get("status", "Unknown")
            result["currency"] = acc.get("currency", "VND")
    
    # 2. Lấy chi tiêu từ Report API
    report_response = await get_report_spending(access_token, advertiser_id)
    if report_response.get("code") == 0:
        report_list = report_response.get("data", {}).get("list", [])
        if report_list:
            metrics = report_list[0].get("metrics", {})
            spend = float(metrics.get("spend", 0))
            result["spending"] = spend
            result["success"] = True
    
    # Đánh dấu success nếu có info
    if result["name"] != "Unknown":
        result["success"] = True
    
    return result


async def get_all_balances(advertiser_id: str = None) -> Dict[str, Any]:
    """
    Lấy thông tin chi tiêu từ tài khoản TikTok Ads
    """
    access_token = get_stored_token()
    if not access_token:
        return {
            "success": False,
            "error": "Chưa kết nối TikTok Ads.\n\n💡 Vui lòng authorize tại:\n" + get_authorization_url()
        }
    
    target_id = advertiser_id or PRIMARY_ADVERTISER_ID
    
    account_info = await get_account_spending(access_token, target_id)
    
    if account_info.get("success"):
        return {
            "success": True,
            "accounts": [account_info],
            "total_spending": account_info.get("spending", 0),
            "count": 1
        }
    else:
        return {
            "success": False,
            "error": "Không thể lấy thông tin tài khoản. Token có thể đã hết hạn.\n\n💡 Authorize lại tại:\n" + get_authorization_url(),
            "raw": account_info
        }


def format_balance_report(balance_data: Dict[str, Any]) -> str:
    """Format báo cáo dư nợ"""
    if not balance_data.get("success"):
        error = balance_data.get('error', 'Unknown error')
        return f"❌ {error}"
    
    accounts = balance_data.get("accounts", [])
    
    lines = [
        "💰 **BÁO CÁO TÀI KHOẢN TIKTOK ADS**",
        f"📅 Cập nhật: {datetime.now().strftime('%H:%M %d/%m/%Y')}",
        "",
    ]
    
    for acc in accounts:
        status = acc.get("status", "Unknown")
        status_emoji = "✅" if "ENABLE" in status.upper() else "⚠️"
        currency = acc.get("currency", "VND")
        spending = acc.get("spending", 0)
        credit_limit = acc.get("credit_limit", CREDIT_LIMIT)
        
        # Tính phần trăm
        percentage = (spending / credit_limit * 100) if credit_limit > 0 else 0
        
        lines.append(f"{status_emoji} **{acc.get('name', 'Tài khoản')}**")
        lines.append(f"🆔 ID: `{acc.get('advertiser_id', 'N/A')}`")
        lines.append("")
        lines.append(f"💳 **Dư nợ hiện tại: {spending:,.0f} / {credit_limit:,.0f} {currency}**")
        lines.append(f"📊 Tỷ lệ sử dụng: **{percentage:.1f}%**")
        lines.append("")
        
        # Cảnh báo nếu đạt ngưỡng
        if percentage >= WARNING_THRESHOLD:
            lines.append("🚨" * 5)
            lines.append(f"⚠️ **CẢNH BÁO: Dư nợ đã đạt {percentage:.1f}% hạn mức!**")
            lines.append(f"💡 Hạn mức còn lại: {credit_limit - spending:,.0f} {currency}")
            lines.append("🚨" * 5)
        elif percentage >= 70:
            lines.append(f"⚠️ Lưu ý: Đã sử dụng {percentage:.1f}% hạn mức")
        
        lines.append("")
    
    # Thông tin billing cycle
    lines.append(f"📆 Billing cycle: Ngày 1/{datetime.now().month} - Ngày 1/{datetime.now().month + 1 if datetime.now().month < 12 else 1}")
    
    return "\n".join(lines)


def check_warning_threshold(spending: float) -> bool:
    """Kiểm tra xem có cần cảnh báo không"""
    percentage = (spending / CREDIT_LIMIT * 100) if CREDIT_LIMIT > 0 else 0
    return percentage >= WARNING_THRESHOLD


# ============ TOKEN PERSISTENCE ============
def load_tokens_from_env():
    """Load tokens from environment"""
    token = os.getenv("TIKTOK_ACCESS_TOKEN")
    if token:
        _token_storage["access_token"] = token
    
    advertiser_ids = os.getenv("TIKTOK_ADVERTISER_IDS")
    if advertiser_ids:
        _token_storage["advertiser_ids"] = advertiser_ids.split(",")


# Auto-load on import
load_tokens_from_env()
