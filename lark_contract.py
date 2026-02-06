"""
==========================================================================
LARK CONTRACT - Sync API client for contract record updates
==========================================================================
Pattern tham khảo từ script filter_koc_thang10.py (đã chạy thành công)
Dùng requests (sync) thay vì httpx (async) để đảm bảo ổn định
trong background tasks.
==========================================================================
"""

import os
import requests
import time

# ╔════════════════════════════════════════════════════════════════╗
# ║                         CẤU HÌNH                              ║
# ╚════════════════════════════════════════════════════════════════╝

LARK_APP_ID = os.getenv("LARK_APP_ID", "cli_a816ab3b94b8d010")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET", "Xyyqd95i9xbNeVSGNo48jb7ADDD8lC2u")
LARK_API = "https://open.larksuite.com/open-apis"

# ╔════════════════════════════════════════════════════════════════╗
# ║                     TOKEN MANAGEMENT                           ║
# ╚════════════════════════════════════════════════════════════════╝

_token_cache = {"token": None, "expires": 0}


def get_token() -> str:
    """Lấy tenant access token (sync, có cache)"""
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires"]:
        return _token_cache["token"]

    resp = requests.post(
        f"{LARK_API}/auth/v3/tenant_access_token/internal",
        json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET},
        timeout=10,
    )
    data = resp.json()

    if data.get("code") != 0:
        raise Exception(f"❌ Lark auth failed: {data}")

    _token_cache["token"] = data["tenant_access_token"]
    _token_cache["expires"] = now + 5400  # cache ~90 phút
    return _token_cache["token"]


def headers() -> dict:
    """Headers với authorization"""
    return {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type": "application/json",
    }


# ╔════════════════════════════════════════════════════════════════╗
# ║                     BITABLE OPERATIONS                         ║
# ╚════════════════════════════════════════════════════════════════╝

def update_record(app_token: str, table_id: str, record_id: str, fields: dict) -> dict:
    """
    Cập nhật 1 record trong Bitable (SYNC).
    Trả về response data từ Lark API.
    """
    url = f"{LARK_API}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

    resp = requests.put(
        url,
        headers=headers(),
        json={"fields": fields},
        timeout=15,
    )
    data = resp.json()

    if data.get("code") != 0:
        print(f"❌ Lark update_record error: code={data.get('code')} msg={data.get('msg')}")
        return {"error": data.get("msg", "Unknown"), "code": data.get("code")}

    print(f"✅ Lark record updated: {record_id}")
    return data.get("data", {}).get("record", {})


def get_record(app_token: str, table_id: str, record_id: str) -> dict:
    """Đọc 1 record từ Bitable (SYNC)."""
    url = f"{LARK_API}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

    resp = requests.get(url, headers=headers(), timeout=15)
    data = resp.json()

    if data.get("code") != 0:
        print(f"❌ Lark get_record error: code={data.get('code')} msg={data.get('msg')}")
        return {"error": data.get("msg", "Unknown"), "code": data.get("code")}

    return data.get("data", {}).get("record", {})
