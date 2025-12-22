"""
TikTok Ads Web Crawler - Version 5.7.2
"""
import os
import re
import json
from datetime import datetime
from typing import Optional, Dict, Any

PRIMARY_ADVERTISER_ID = os.getenv("TIKTOK_PRIMARY_ADVERTISER_ID", "7089362853240553474")
CREDIT_LIMIT = float(os.getenv("TIKTOK_CREDIT_LIMIT", "163646248"))
WARNING_THRESHOLD = float(os.getenv("TIKTOK_WARNING_THRESHOLD", "85"))

_cached_data: Dict[str, Any] = {
    "spending": 0,
    "credit_limit": CREDIT_LIMIT,
    "next_billing_date": None,
    "account_name": "Chenglovehair0422",
    "updated_at": None,
    "cache_ttl": 3600,
}

def is_cache_valid() -> bool:
    if not _cached_data["updated_at"]:
        return False
    try:
        updated = datetime.fromisoformat(_cached_data["updated_at"])
        delta = (datetime.now() - updated).total_seconds()
        return delta < _cached_data["cache_ttl"]
    except:
        return False

async def get_spending_data(force_refresh: bool = False) -> Dict[str, Any]:
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
    
    return {
        "success": False,
        "error": "Cần cấu hình cookies để crawl TikTok Ads",
        "spending": 0,
        "credit_limit": CREDIT_LIMIT
    }

def format_spending_report(data: Dict[str, Any]) -> str:
    if not data.get("success"):
        return f"❌ {data.get('error', 'Không có dữ liệu')}"
    
    spending = data["spending"]
    credit_limit = data["credit_limit"]
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
    
    return "\n".join(lines)

def is_tiktok_ads_query(text: str) -> bool:
    keywords = ["số dư tiktok", "tiktok ads", "tkqc", "dư nợ tiktok", "dư nợ ads"]
    return any(kw in text.lower() for kw in keywords)
