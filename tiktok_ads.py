"""
TikTok Ads API Integration
Theo dõi số dư tài khoản quảng cáo TikTok
"""
import os
import httpx
from typing import Optional, Dict, List, Any
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ============ CONFIG ============
TIKTOK_APP_ID = os.getenv("TIKTOK_APP_ID", "7584349619291684880")
TIKTOK_APP_SECRET = os.getenv("TIKTOK_APP_SECRET", "")
TIKTOK_REDIRECT_URI = os.getenv("TIKTOK_REDIRECT_URI", "https://jarvis-lark-bot-production.up.railway.app/tiktok/callback")

# API Base URLs
TIKTOK_AUTH_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"
TIKTOK_API_BASE = "https://business-api.tiktok.com/open_api/v1.3"

# Storage for tokens (in production, use database)
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
    """
    Đổi authorization code lấy access token
    """
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
            # Success - save token
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
    """Lấy access token đã lưu"""
    return _token_storage.get("access_token")


def get_stored_advertiser_ids() -> List[str]:
    """Lấy danh sách advertiser IDs đã lưu"""
    return _token_storage.get("advertiser_ids", [])


async def get_advertiser_info(access_token: str, advertiser_ids: List[str]) -> Dict[str, Any]:
    """
    Lấy thông tin advertiser
    API: GET /advertiser/info/
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{TIKTOK_API_BASE}/advertiser/info/",
            params={
                "advertiser_ids": advertiser_ids,
            },
            headers={
                "Access-Token": access_token,
                "Content-Type": "application/json"
            }
        )
        
        return response.json()


async def get_bc_balance(access_token: str, bc_id: str) -> Dict[str, Any]:
    """
    Lấy số dư Business Center
    API: GET /bc/balance/get/
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{TIKTOK_API_BASE}/bc/balance/get/",
            params={
                "bc_id": bc_id,
            },
            headers={
                "Access-Token": access_token,
                "Content-Type": "application/json"
            }
        )
        
        result = response.json()
        print(f"💰 BC Balance response: {result}")
        return result


async def get_advertiser_balance(access_token: str, advertiser_ids: List[str]) -> Dict[str, Any]:
    """
    Lấy số dư tài khoản quảng cáo
    API: GET /advertiser/fund/get/ (nếu có quyền)
    Hoặc dùng /advertiser/info/ để lấy balance
    """
    async with httpx.AsyncClient() as client:
        # Try to get balance info
        response = await client.get(
            f"{TIKTOK_API_BASE}/advertiser/info/",
            params={
                "advertiser_ids": str(advertiser_ids),
                "fields": '["balance", "name", "status", "currency"]'
            },
            headers={
                "Access-Token": access_token,
                "Content-Type": "application/json"
            }
        )
        
        result = response.json()
        print(f"💰 Advertiser info response: {result}")
        return result


async def get_all_balances() -> Dict[str, Any]:
    """
    Lấy tất cả số dư từ các tài khoản đã kết nối
    """
    access_token = get_stored_token()
    if not access_token:
        return {
            "success": False,
            "error": "Chưa kết nối TikTok Ads. Vui lòng authorize tại: " + get_authorization_url()
        }
    
    advertiser_ids = get_stored_advertiser_ids()
    if not advertiser_ids:
        return {
            "success": False,
            "error": "Không có advertiser ID nào được lưu"
        }
    
    result = await get_advertiser_balance(access_token, advertiser_ids)
    
    if result.get("code") == 0:
        accounts = result.get("data", {}).get("list", [])
        
        formatted_accounts = []
        total_balance = 0
        
        for acc in accounts:
            balance = float(acc.get("balance", 0))
            total_balance += balance
            
            formatted_accounts.append({
                "id": acc.get("advertiser_id"),
                "name": acc.get("name", "Unknown"),
                "balance": balance,
                "currency": acc.get("currency", "VND"),
                "status": acc.get("status", "Unknown")
            })
        
        return {
            "success": True,
            "accounts": formatted_accounts,
            "total_balance": total_balance,
            "count": len(formatted_accounts)
        }
    else:
        return {
            "success": False,
            "error": result.get("message", "Unknown error"),
            "code": result.get("code")
        }


def format_balance_report(balance_data: Dict[str, Any]) -> str:
    """
    Format báo cáo số dư thành text đẹp
    """
    if not balance_data.get("success"):
        return f"❌ Lỗi: {balance_data.get('error', 'Unknown error')}"
    
    accounts = balance_data.get("accounts", [])
    total = balance_data.get("total_balance", 0)
    
    lines = [
        "💰 **SỐ DƯ TÀI KHOẢN TIKTOK ADS**",
        f"📅 Cập nhật: {datetime.now().strftime('%H:%M %d/%m/%Y')}",
        "",
        "-" * 30,
    ]
    
    for acc in accounts:
        status_emoji = "✅" if acc.get("status") == "STATUS_ENABLE" else "⚠️"
        balance = acc.get("balance", 0)
        currency = acc.get("currency", "VND")
        
        # Format balance với separator
        if currency == "VND":
            balance_str = f"{balance:,.0f} VND"
        else:
            balance_str = f"{balance:,.2f} {currency}"
        
        lines.append(f"{status_emoji} {acc.get('name', 'Unknown')}")
        lines.append(f"   💵 Số dư: {balance_str}")
        lines.append("")
    
    lines.append("-" * 30)
    
    # Format total
    if accounts and accounts[0].get("currency") == "VND":
        total_str = f"{total:,.0f} VND"
    else:
        total_str = f"{total:,.2f}"
    
    lines.append(f"📊 **Tổng số dư: {total_str}**")
    
    # Warning if low balance
    if total < 1000000:  # Less than 1M VND
        lines.append("")
        lines.append("⚠️ **CẢNH BÁO: Số dư thấp! Cần nạp thêm tiền.**")
    
    return "\n".join(lines)


# ============ TOKEN PERSISTENCE ============
# In production, use Redis or Database

def save_tokens_to_env():
    """Save tokens để không mất khi restart (workaround)"""
    # This is a simple approach - in production use database
    pass


def load_tokens_from_env():
    """Load tokens from environment if available"""
    token = os.getenv("TIKTOK_ACCESS_TOKEN")
    if token:
        _token_storage["access_token"] = token
    
    advertiser_ids = os.getenv("TIKTOK_ADVERTISER_IDS")
    if advertiser_ids:
        _token_storage["advertiser_ids"] = advertiser_ids.split(",")
