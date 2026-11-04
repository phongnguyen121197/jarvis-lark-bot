"""
Nhanh.vn Open API v3.0 client
==============================
- OAuth: tạo link uỷ quyền, đổi accessCode → accessToken
- Gọi API POST JSON (appId/businessId trên query, Authorization = accessToken)
- Xử lý rate limit ERR_429 (150 req / 30s mỗi appId+businessId+URL)
- Chuẩn hoá đơn hàng từ cả API order/list lẫn payload webhook

Tài liệu: https://apidocs.nhanh.vn/v3  •  https://developers.nhanh.group/app/access-token/

ENV:
  NHANH_APP_ID                 (bắt buộc)  appId của app trên open.nhanh.vn
  NHANH_SECRET_KEY             (bắt buộc)  secretKey của app
  NHANH_ACCESS_TOKEN           token sau khi uỷ quyền (sống 1 năm, không có refresh)
  NHANH_BUSINESS_ID            businessId trả về cùng token
  NHANH_WEBHOOK_VERIFY_TOKEN   verify token đã khai trên app (header Authorization của webhook)
  NHANH_REDIRECT_URL           returnLink đã khai trên app
"""

import os
import re
import time
import threading
from datetime import datetime
from typing import Dict, Iterator, List, Optional

import requests

NHANH_APP_ID = os.getenv("NHANH_APP_ID", "78743")
NHANH_SECRET_KEY = os.getenv("NHANH_SECRET_KEY", "")
NHANH_WEBHOOK_VERIFY_TOKEN = os.getenv("NHANH_WEBHOOK_VERIFY_TOKEN", "")
NHANH_REDIRECT_URL = os.getenv(
    "NHANH_REDIRECT_URL",
    "https://jarvis-lark-bot-production.up.railway.app/nhanh/oauth/callback",
)

POS_BASE = "https://pos.open.nhanh.vn"
OAUTH_URL = "https://nhanh.vn/oauth"

# Token giữ trong bộ nhớ; khởi tạo từ env. Sau khi uỷ quyền qua /nhanh/oauth/callback
# token mới được set vào đây, đồng thời in ra log để bạn dán vào Railway env
# (container Railway không giữ file sau khi redeploy).
_token = {
    "access_token": os.getenv("NHANH_ACCESS_TOKEN", ""),
    "business_id": os.getenv("NHANH_BUSINESS_ID", ""),
    "expired_at": int(os.getenv("NHANH_TOKEN_EXPIRED_AT", "0") or 0),
    "permissions": [],
    "depot_ids": [],
}
_token_lock = threading.Lock()

# Rate limit: khoá theo URL tới unlockedAt khi bị ERR_429
_locked_until: Dict[str, float] = {}


# ╔════════════════════════════════════════════════════════════════╗
# ║                       ORDER CONSTANTS                          ║
# ╚════════════════════════════════════════════════════════════════╝

# https://apidocs.nhanh.vn/v3/modelconstant  (Order Status)
ORDER_STATUS = {
    54: "Đơn mới",
    55: "Đang xác nhận",
    56: "Đã xác nhận",
    57: "Chờ khách xác nhận",
    42: "Đang đóng gói",
    40: "Đã đóng gói",
    43: "Chờ thu gom",
    73: "Đổi kho xuất hàng",
    59: "Đang chuyển",
    60: "Thành công",
    61: "Thất bại",
    58: "Hãng vận chuyển hủy đơn",
    63: "Khách hủy",
    64: "Hệ thống hủy",
    68: "Hết hàng",
    71: "Đang chuyển hoàn",
    74: "Xác nhận hoàn",
    72: "Đã chuyển hoàn",
}

# Nhóm trạng thái gọn để hiển thị / lọc
STATUS_GROUP = {
    54: "Mới", 55: "Mới", 56: "Đã xác nhận", 57: "Mới",
    42: "Đang đóng gói", 40: "Đang đóng gói", 43: "Đang đóng gói", 73: "Đang đóng gói",
    59: "Đang giao",
    60: "Thành công",
    61: "Thất bại", 58: "Hủy", 63: "Hủy", 64: "Hủy", 68: "Hủy",
    71: "Đang hoàn", 74: "Đang hoàn", 72: "Đã hoàn",
}

CARRIERS = {
    2: "Viettel Post", 5: "GHN", 8: "GHTK", 12: "Tự vận chuyển", 18: "Ahamove",
    22: "Vietnam Post", 24: "J&T Express", 25: "EMS", 26: "Best Express",
    27: "NinjaVan", 28: "SuperShip", 29: "SPX", 30: "LEX", 31: "Grab",
}

SALE_CHANNELS = {
    1: "Admin", 2: "Website", 10: "API", 20: "Facebook", 21: "Instagram",
    41: "Lazada", 42: "Shopee", 43: "Sendo", 45: "Tiki", 48: "TikTok Shop",
    49: "Zalo OA", 50: "Shopee chat", 51: "Lazada chat", 52: "Zalo cá nhân",
}


class NhanhError(Exception):
    def __init__(self, message: str, error_code: str = "", payload: dict = None):
        super().__init__(message)
        self.error_code = error_code
        self.payload = payload or {}


# ╔════════════════════════════════════════════════════════════════╗
# ║                         TOKEN / OAUTH                          ║
# ╚════════════════════════════════════════════════════════════════╝

def is_configured() -> bool:
    return bool(NHANH_APP_ID and NHANH_SECRET_KEY)


def has_token() -> bool:
    return bool(_token["access_token"] and _token["business_id"])


def token_info() -> dict:
    """Thông tin token (không lộ token đầy đủ)."""
    tok = _token["access_token"] or ""
    exp = _token["expired_at"] or 0
    return {
        "app_id": NHANH_APP_ID,
        "configured": is_configured(),
        "has_token": has_token(),
        "business_id": _token["business_id"],
        "token_prefix": (tok[:8] + "...") if tok else "",
        "expired_at": exp,
        "expired_at_text": datetime.fromtimestamp(exp).strftime("%d/%m/%Y %H:%M") if exp else "",
        "days_left": int((exp - time.time()) / 86400) if exp else None,
        "permissions": _token.get("permissions", []),
        "depot_ids": _token.get("depot_ids", []),
    }


def set_token(access_token: str, business_id, expired_at: int = 0,
              permissions: list = None, depot_ids: list = None):
    with _token_lock:
        _token["access_token"] = access_token or ""
        _token["business_id"] = str(business_id or "")
        _token["expired_at"] = int(expired_at or 0)
        _token["permissions"] = permissions or []
        _token["depot_ids"] = depot_ids or []


def oauth_url(return_link: str = None) -> str:
    """Link để Giám đốc/chủ tài khoản Nhanh.vn bấm uỷ quyền cho app."""
    link = return_link or NHANH_REDIRECT_URL
    return f"{OAUTH_URL}?appId={NHANH_APP_ID}&returnLink={requests.utils.quote(link, safe='')}"


def exchange_access_code(access_code: str) -> dict:
    """
    Đổi accessCode (sống 10 phút, dùng 1 lần) → accessToken (sống 1 năm).
    POST https://pos.open.nhanh.vn/v3.0/app/getaccesstoken?appId=...
    """
    if not is_configured():
        raise NhanhError("NHANH_APP_ID / NHANH_SECRET_KEY chưa được cấu hình")
    resp = requests.post(
        f"{POS_BASE}/v3.0/app/getaccesstoken",
        params={"appId": NHANH_APP_ID},
        json={"accessCode": access_code, "secretKey": NHANH_SECRET_KEY},
        timeout=20,
    )
    data = _parse_response(resp)
    d = data.get("data", {})
    set_token(
        d.get("accessToken", ""), d.get("businessId", ""), d.get("expiredAt", 0),
        d.get("permissions", []), d.get("depotIds", []),
    )
    print(f"✅ [Nhanh] Access token obtained: business={d.get('businessId')} "
          f"expiredAt={d.get('expiredAt')} permissions={d.get('permissions')}")
    return d


def check_token() -> dict:
    """Kiểm tra token hiện tại còn hợp lệ không."""
    if not has_token():
        raise NhanhError("Chưa có access token (uỷ quyền qua /nhanh/oauth/start)")
    resp = requests.post(
        f"{POS_BASE}/v3.0/app/checkaccesstoken",
        params={"appId": NHANH_APP_ID, "businessId": _token["business_id"]},
        headers={"Authorization": _token["access_token"], "Content-Type": "application/json"},
        json={"secretKey": NHANH_SECRET_KEY},
        timeout=20,
    )
    data = _parse_response(resp)
    d = data.get("data", {})
    if d.get("expiredAt"):
        _token["expired_at"] = int(d["expiredAt"])
    if d.get("permissions") is not None:
        _token["permissions"] = d.get("permissions") or []
    return d


# ╔════════════════════════════════════════════════════════════════╗
# ║                          API CALLS                             ║
# ╚════════════════════════════════════════════════════════════════╝

def _parse_response(resp: requests.Response) -> dict:
    try:
        data = resp.json()
    except ValueError:
        raise NhanhError(f"Nhanh trả về không phải JSON (HTTP {resp.status_code}): {resp.text[:200]}")
    if data.get("code") != 1:
        err = data.get("errorCode", "") or str(data.get("code"))
        msg = data.get("message") or data.get("messages") or data
        raise NhanhError(f"Nhanh API error {err}: {msg}", error_code=err, payload=data)
    return data


def api_post(path: str, body: dict = None, retries: int = 2) -> dict:
    """
    POST https://pos.open.nhanh.vn/v3.0/<path>?appId=..&businessId=..
    Tự chờ khi bị khoá rate limit (ERR_429) tối đa 1 lần.
    """
    if not has_token():
        raise NhanhError("Chưa có access token Nhanh.vn")
    url = f"{POS_BASE}/v3.0/{path.lstrip('/')}"

    for attempt in range(retries + 1):
        locked = _locked_until.get(url, 0)
        if locked > time.time():
            wait = min(locked - time.time(), 35)
            print(f"⏳ [Nhanh] {path} rate-limited, waiting {wait:.0f}s")
            time.sleep(wait)
        try:
            resp = requests.post(
                url,
                params={"appId": NHANH_APP_ID, "businessId": _token["business_id"]},
                headers={"Authorization": _token["access_token"], "Content-Type": "application/json"},
                json=body or {},
                timeout=30,
            )
            return _parse_response(resp)
        except NhanhError as e:
            if e.error_code in ("ERR_429", "ERR_EXCEEDED_RATE_LIMIT") and attempt < retries:
                unlocked = (e.payload.get("data") or {}).get("unlockedAt")
                _locked_until[url] = float(unlocked) if unlocked else time.time() + 10
                continue
            raise
        except requests.RequestException as e:
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
                continue
            raise NhanhError(f"Nhanh request failed: {e}")


def list_orders(filters: dict = None, size: int = 100, max_pages: int = 50) -> Iterator[dict]:
    """
    Duyệt đơn hàng theo filters (xem https://apidocs.nhanh.vn/v3/order/list).
    Yield từng đơn (raw). Khoảng createdAt/updatedAt tối đa 31 ngày.
    """
    body = {"filters": filters or {}, "paginator": {"size": size}, "dataOptions": {}}
    for _ in range(max_pages):
        data = api_post("order/list", body)
        for order in data.get("data", []) or []:
            yield order
        nxt = (data.get("paginator") or {}).get("next")
        if not nxt:
            break
        body["paginator"] = {"size": size, "next": nxt}


def list_orders_updated_since(from_ts: int, to_ts: int = None) -> Iterator[dict]:
    flt = {"updatedAtFrom": int(from_ts), "updatedAtTo": int(to_ts or time.time())}
    return list_orders(flt)


def list_orders_by_ids(ids: List[int]) -> Iterator[dict]:
    return list_orders({"ids": [int(i) for i in ids]})


# ╔════════════════════════════════════════════════════════════════╗
# ║                     CHUẨN HOÁ ĐƠN HÀNG                        ║
# ╚════════════════════════════════════════════════════════════════╝

def normalize_phone(value) -> str:
    """'+84 915 815 658' / '84915815658' / '0915.815.658' → '0915815658'."""
    if value is None:
        return ""
    digits = re.sub(r"\D", "", str(value))
    if digits.startswith("84") and len(digits) == 11:
        digits = "0" + digits[2:]
    if len(digits) == 9 and not digits.startswith("0"):
        digits = "0" + digits
    return digits if 10 <= len(digits) <= 11 else ""


def extract_phones(text) -> List[str]:
    """Tách mọi SĐT Việt Nam trong một đoạn text (vd cột 'Thông tin nhận hàng')."""
    if not text:
        return []
    s = str(text)
    found = []
    for m in re.finditer(r"(?:\+?84|0)[\s.\-]?(?:\d[\s.\-]?){8,9}\d", s):
        p = normalize_phone(m.group(0))
        if p and p not in found:
            found.append(p)
    return found


def _first(d: dict, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, ""):
            return d.get(k)
    return default


def _ts(value) -> Optional[int]:
    """Lark/Nhanh thời gian: unix giây, unix ms, hoặc 'YYYY-mm-dd HH:MM:SS'."""
    if value in (None, "", 0):
        return None
    try:
        v = float(value)
        return int(v / 1000) if v > 1e12 else int(v)
    except (TypeError, ValueError):
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(str(value), fmt).timestamp())
        except ValueError:
            continue
    return None


def normalize_order(raw: dict) -> dict:
    """
    Đưa đơn từ order/list hoặc webhook (orderAdd/orderUpdate/orderPartialReturn)
    về một dict phẳng. Hai nguồn có cấu trúc gần giống nhau:
      info.{id,status,createdAt,updatedAt,trackingUrl,type}
      shippingAddress.{name,mobile,address}
      carrier.{id,name,carrierCode}
      channel.{saleChannel,appOrderId}
      products[].{name,quantity,price|totalAmount}
    """
    info = raw.get("info") or {}
    ship = raw.get("shippingAddress") or {}
    carrier = raw.get("carrier") or {}
    channel = raw.get("channel") or {}
    payment = raw.get("payment") or {}
    customer = raw.get("customer") or {}

    order_id = _first(info, "id", "orderId") or raw.get("id") or raw.get("orderId")
    status_code = info.get("status", raw.get("status"))
    try:
        status_code = int(status_code) if status_code not in (None, "") else None
    except (TypeError, ValueError):
        status_code = None

    mobile = normalize_phone(_first(ship, "mobile", "phone") or _first(customer, "mobile", "phone") or raw.get("customerMobile"))
    carrier_id = carrier.get("id")
    try:
        carrier_id = int(carrier_id) if carrier_id not in (None, "") else None
    except (TypeError, ValueError):
        carrier_id = None
    carrier_name = carrier.get("name") or CARRIERS.get(carrier_id, "") if carrier else ""

    sale_channel = channel.get("saleChannel")
    try:
        sale_channel = int(sale_channel) if sale_channel not in (None, "") else None
    except (TypeError, ValueError):
        sale_channel = None

    products = []
    total = 0.0
    for p in raw.get("products") or []:
        if not isinstance(p, dict):
            continue
        qty = p.get("quantity") or 0
        price = p.get("price") or 0
        try:
            line_total = float(p.get("totalAmount") or (float(qty) * float(price)))
        except (TypeError, ValueError):
            line_total = 0.0
        total += line_total
        products.append({"name": p.get("name", ""), "code": p.get("code", ""), "quantity": qty, "price": price})

    return {
        "order_id": str(order_id) if order_id is not None else "",
        "status_code": status_code,
        "status": ORDER_STATUS.get(status_code, f"Mã {status_code}" if status_code is not None else ""),
        "status_group": STATUS_GROUP.get(status_code, ""),
        "customer_name": _first(ship, "name") or _first(customer, "name") or "",
        "mobile": mobile,
        "address": ship.get("address", ""),
        "carrier_id": carrier_id,
        "carrier_name": carrier_name or "",
        "carrier_code": carrier.get("carrierCode") or carrier.get("fullCarrierCode") or "",
        "tracking_url": info.get("trackingUrl") or "",
        "sale_channel": SALE_CHANNELS.get(sale_channel, str(sale_channel or "")),
        "app_order_id": channel.get("appOrderId") or "",
        "created_at": _ts(info.get("createdAt")),
        "updated_at": _ts(info.get("updatedAt")),
        "description": info.get("description") or "",
        "cod_amount": payment.get("codAmount"),
        "total_amount": total,
        "products": products,
        "type": info.get("type"),
    }
