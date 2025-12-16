"""
TikTok Ads Integration
Hỗ trợ: Manual Input + Web Crawler (future)
"""
import os
import re
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# ============ CONFIG ============
PRIMARY_ADVERTISER_ID = os.getenv("TIKTOK_PRIMARY_ADVERTISER_ID", "7089362853240553474")
CREDIT_LIMIT = float(os.getenv("TIKTOK_CREDIT_LIMIT", "163646248"))
WARNING_THRESHOLD = float(os.getenv("TIKTOK_WARNING_THRESHOLD", "85"))

# ============ STORAGE ============
_spending_data: Dict[str, Any] = {
    "spending": 0,
    "credit_limit": CREDIT_LIMIT,
    "updated_at": None,
    "source": None,
    "account_name": "Chenglovehair0422"
}


# ============ MANUAL INPUT ============

def update_manual_debt(amount: float, updated_by: str = "user") -> Dict[str, Any]:
    """Cập nhật dư nợ thủ công"""
    _spending_data["spending"] = amount
    _spending_data["updated_at"] = datetime.now().isoformat()
    _spending_data["source"] = "manual"
    
    percentage = (amount / CREDIT_LIMIT * 100) if CREDIT_LIMIT > 0 else 0
    warning = percentage >= WARNING_THRESHOLD
    
    print(f"💾 Updated debt: {amount:,.0f} VND ({percentage:.1f}%) by {updated_by}")
    
    return {
        "success": True,
        "spending": amount,
        "credit_limit": CREDIT_LIMIT,
        "percentage": percentage,
        "warning": warning,
        "updated_at": _spending_data["updated_at"]
    }


def parse_debt_command(text: str) -> Optional[float]:
    """
    Parse lệnh cập nhật dư nợ
    Formats:
    - "cập nhật dư nợ: 105672606"
    - "dư nợ: 105,672,606"
    - "debt: 105672606"
    - "tkqc: 105672606"
    """
    patterns = [
        r'(?:cập nhật|update)?\s*(?:dư nợ|debt|du no|nợ)\s*[:\s]*([\d,\.]+)',
        r'(?:set|đặt)\s*(?:dư nợ|debt|du no|nợ)\s*[:\s]*([\d,\.]+)',
        r'tkqc\s*[:\s]*([\d,\.]+)',
        r'tiktok\s*ads?\s*[:\s]*([\d,\.]+)',
    ]
    
    text_lower = text.lower()
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            amount_str = match.group(1).replace(',', '').replace('.', '')
            try:
                amount = float(amount_str)
                if amount > 1000:
                    return amount
            except ValueError:
                continue
    return None


def format_debt_update_response(result: Dict[str, Any]) -> str:
    """Format response sau khi cập nhật dư nợ"""
    if not result.get("success"):
        return f"❌ {result.get('error', 'Không thể cập nhật dư nợ')}"
    
    spending = result["spending"]
    credit_limit = result["credit_limit"]
    percentage = result["percentage"]
    
    lines = [
        "✅ **Đã cập nhật dư nợ TikTok Ads**",
        "",
        f"💳 **Dư nợ hiện tại: {spending:,.0f} / {credit_limit:,.0f} VND**",
        f"📊 Tỷ lệ sử dụng: **{percentage:.1f}%**",
        "",
    ]
    
    if result.get("warning"):
        lines.append("🚨" * 5)
        lines.append(f"⚠️ **CẢNH BÁO: Dư nợ đã đạt {percentage:.1f}% hạn mức!**")
        lines.append(f"💡 Hạn mức còn lại: {credit_limit - spending:,.0f} VND")
        lines.append("🚨" * 5)
    elif percentage >= 70:
        lines.append(f"⚠️ Lưu ý: Đã sử dụng {percentage:.1f}% hạn mức")
    else:
        lines.append("✅ Mức sử dụng an toàn")
    
    return "\n".join(lines)


# ============ GET BALANCE ============

async def get_all_balances(advertiser_id: str = None) -> Dict[str, Any]:
    """Lấy thông tin dư nợ"""
    if _spending_data["spending"] > 0:
        return {
            "success": True,
            "accounts": [{
                "advertiser_id": PRIMARY_ADVERTISER_ID,
                "name": _spending_data["account_name"],
                "status": "STATUS_ENABLE",
                "currency": "VND",
                "spending": _spending_data["spending"],
                "credit_limit": _spending_data["credit_limit"],
                "updated_at": _spending_data["updated_at"],
                "source": _spending_data["source"],
            }],
            "total_spending": _spending_data["spending"],
            "count": 1
        }
    
    return {
        "success": False,
        "error": (
            "Chưa có dữ liệu dư nợ.\n\n"
            "💡 **Cập nhật dư nợ:**\n"
            "`Jarvis dư nợ: 105672606`\n\n"
            "Hoặc: `Jarvis TKQC: 105,672,606`"
        )
    }


def format_balance_report(balance_data: Dict[str, Any]) -> str:
    """Format báo cáo dư nợ"""
    if not balance_data.get("success"):
        return balance_data.get('error', '❌ Không có dữ liệu')
    
    accounts = balance_data.get("accounts", [])
    if not accounts:
        return "❌ Không có thông tin tài khoản"
    
    acc = accounts[0]
    spending = acc.get("spending", 0)
    credit_limit = acc.get("credit_limit", CREDIT_LIMIT)
    percentage = (spending / credit_limit * 100) if credit_limit > 0 else 0
    
    lines = [
        "💰 **BÁO CÁO TÀI KHOẢN TIKTOK ADS**",
        f"📅 Cập nhật: {datetime.now().strftime('%H:%M %d/%m/%Y')}",
        "",
        f"✅ **{acc.get('name', 'Chenglovehair0422')}**",
        f"🆔 ID: `{acc.get('advertiser_id', PRIMARY_ADVERTISER_ID)}`",
        "",
        f"💳 **Dư nợ hiện tại: {spending:,.0f} / {credit_limit:,.0f} VND**",
        f"📊 Tỷ lệ sử dụng: **{percentage:.1f}%**",
        "",
    ]
    
    if percentage >= WARNING_THRESHOLD:
        lines.append("🚨" * 5)
        lines.append(f"⚠️ **CẢNH BÁO: Dư nợ đã đạt {percentage:.1f}% hạn mức!**")
        lines.append(f"💡 Hạn mức còn lại: {credit_limit - spending:,.0f} VND")
        lines.append("🚨" * 5)
    elif percentage >= 70:
        lines.append(f"⚠️ Lưu ý: Đã sử dụng {percentage:.1f}% hạn mức")
    else:
        lines.append("✅ Mức sử dụng an toàn")
    
    if acc.get("updated_at"):
        update_time = acc["updated_at"]
        if isinstance(update_time, str):
            try:
                dt = datetime.fromisoformat(update_time)
                update_time = dt.strftime("%H:%M %d/%m")
            except:
                pass
        lines.append("")
        lines.append(f"📝 Dữ liệu cập nhật lúc: {update_time}")
    
    return "\n".join(lines)


# ============ CHECK WARNING ============

def check_warning_threshold() -> Optional[str]:
    """Kiểm tra và trả về cảnh báo nếu đạt ngưỡng"""
    if _spending_data["spending"] == 0:
        return None
    
    spending = _spending_data["spending"]
    percentage = (spending / CREDIT_LIMIT * 100) if CREDIT_LIMIT > 0 else 0
    
    if percentage >= WARNING_THRESHOLD:
        return (
            "🚨 **CẢNH BÁO TIKTOK ADS** 🚨\n\n"
            f"💳 Dư nợ: **{spending:,.0f} / {CREDIT_LIMIT:,.0f} VND**\n"
            f"📊 Đã sử dụng: **{percentage:.1f}%** hạn mức\n"
            f"💡 Còn lại: {CREDIT_LIMIT - spending:,.0f} VND\n\n"
            "⚠️ Vui lòng kiểm tra và thanh toán sớm!"
        )
    
    return None


# ============ KEYWORDS ============

def is_tiktok_ads_query(text: str) -> bool:
    """Kiểm tra xem có phải query TikTok Ads không"""
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


def is_debt_update_command(text: str) -> bool:
    """Kiểm tra xem có phải lệnh cập nhật dư nợ không"""
    patterns = [
        r'(?:dư nợ|du no|debt|nợ|tkqc)\s*[:\s]+\d',
        r'(?:cập nhật|update|set|đặt)\s+(?:dư nợ|debt)',
    ]
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in patterns)
