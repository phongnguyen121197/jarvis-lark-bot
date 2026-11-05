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
import threading
import time

# ╔════════════════════════════════════════════════════════════════╗
# ║                         CẤU HÌNH                              ║
# ╚════════════════════════════════════════════════════════════════╝

LARK_APP_ID = os.getenv("LARK_APP_ID", "cli_a816ab3b94b8d010")
LARK_APP_SECRET = os.getenv("LARK_APP_SECRET", "")
LARK_API = "https://open.larksuite.com/open-apis"

# Mã lỗi Lark báo token không còn dùng được → lấy token mới rồi gọi lại
TOKEN_INVALID_CODES = {99991661, 99991663, 99991668, 99991677}
# Mã lỗi quá tải / rate limit → chờ rồi gọi lại
RATE_LIMIT_CODES = {99991400, 1254290, 1254291}

# ╔════════════════════════════════════════════════════════════════╗
# ║                     TOKEN MANAGEMENT                           ║
# ╚════════════════════════════════════════════════════════════════╝

_token_cache = {"token": None, "expires": 0}
_token_lock = threading.Lock()


def _fetch_token() -> str:
    """Gọi Lark lấy tenant_access_token mới (thử tối đa 3 lần)."""
    if not LARK_APP_SECRET:
        raise Exception("❌ Lark auth failed: thiếu biến môi trường LARK_APP_SECRET")
    last_err = None
    for attempt in range(3):
        try:
            resp = requests.post(
                f"{LARK_API}/auth/v3/tenant_access_token/internal",
                json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET},
                timeout=10,
            )
            data = resp.json()
        except Exception as e:
            last_err = Exception(f"❌ Lark auth request failed: {e}")
            time.sleep(1 + attempt)
            continue
        if data.get("code") == 0 and data.get("tenant_access_token"):
            # Lark trả expire (giây, thường 7200). Cache ngắn hơn 10 phút cho an toàn.
            expire = data.get("expire") or 7200
            ttl = max(300, min(5400, int(expire) - 600))
            _token_cache["token"] = data["tenant_access_token"]
            _token_cache["expires"] = time.time() + ttl
            return _token_cache["token"]
        last_err = Exception(f"❌ Lark auth failed: {data}")
        time.sleep(1 + attempt)
    raise last_err


def get_token() -> str:
    """Lấy tenant access token (sync, có cache, an toàn khi nhiều thread gọi cùng lúc)."""
    if _token_cache["token"] and time.time() < _token_cache["expires"]:
        return _token_cache["token"]
    with _token_lock:
        # Thread khác có thể vừa làm mới xong trong lúc chờ lock
        if _token_cache["token"] and time.time() < _token_cache["expires"]:
            return _token_cache["token"]
        return _fetch_token()


def refresh_token(stale: str = None) -> str:
    """
    Bỏ token đang cache và lấy token mới.
    stale: token vừa bị Lark từ chối – nếu cache đã đổi sang token khác rồi
    (thread khác làm mới trước) thì dùng luôn token đó, không gọi Lark thêm.
    """
    with _token_lock:
        if stale is None or _token_cache["token"] in (None, stale):
            _token_cache["token"] = None
            _token_cache["expires"] = 0
            return _fetch_token()
        return _token_cache["token"]


def headers() -> dict:
    """Headers với authorization"""
    return {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type": "application/json",
    }


def lark_request(method: str, url: str, timeout: int = 15, **kwargs) -> dict:
    """
    Gọi Lark API và trả về JSON (dict có 'code').
    - Token bị từ chối (99991663...) → lấy token mới, gọi lại (tối đa 2 lần).
    - Rate limit (HTTP 429 / 99991400) → chờ rồi gọi lại.
    - Lỗi mạng → thử lại 1 lần.
    Không raise: caller tự kiểm tra data['code'].
    """
    data = {}
    for attempt in range(3):
        token = get_token()
        h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        try:
            resp = requests.request(method, url, headers=h, timeout=timeout, **kwargs)
        except Exception as e:
            data = {"code": -1, "msg": f"request error: {e}"}
            if attempt < 2:
                time.sleep(1 + attempt)
                continue
            return data
        try:
            data = resp.json()
        except ValueError:
            data = {"code": resp.status_code or -1, "msg": (resp.text or "")[:200]}

        code = data.get("code")
        if code == 0:
            return data
        if code in TOKEN_INVALID_CODES and attempt < 2:
            print(f"⚠️ Lark token bị từ chối (code={code}) → làm mới token và gọi lại ({method} {url.split('/open-apis/')[-1]})")
            refresh_token(stale=token)
            time.sleep(0.3 * (attempt + 1))
            continue
        if (code in RATE_LIMIT_CODES or resp.status_code == 429) and attempt < 2:
            wait = 1.5 * (attempt + 1)
            print(f"⚠️ Lark rate limit (code={code}) → chờ {wait}s rồi gọi lại")
            time.sleep(wait)
            continue
        return data
    return data


# ╔════════════════════════════════════════════════════════════════╗
# ║                     BITABLE OPERATIONS                         ║
# ╚════════════════════════════════════════════════════════════════╝

def update_record(app_token: str, table_id: str, record_id: str, fields: dict) -> dict:
    """
    Cập nhật 1 record trong Bitable (SYNC).
    Trả về response data từ Lark API.
    """
    url = f"{LARK_API}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

    data = lark_request("PUT", url, json={"fields": fields}, timeout=15)

    if data.get("code") != 0:
        print(f"❌ Lark update_record error: code={data.get('code')} msg={data.get('msg')}")
        return {"error": data.get("msg", "Unknown"), "code": data.get("code")}

    print(f"✅ Lark record updated: {record_id}")
    return data.get("data", {}).get("record", {})


def get_record(app_token: str, table_id: str, record_id: str) -> dict:
    """Đọc 1 record từ Bitable (SYNC)."""
    url = f"{LARK_API}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

    data = lark_request("GET", url, timeout=15)

    if data.get("code") != 0:
        print(f"❌ Lark get_record error: code={data.get('code')} msg={data.get('msg')}")
        return {"error": data.get("msg", "Unknown"), "code": data.get("code")}

    return data.get("data", {}).get("record", {})


def download_attachment(file_token: str, save_path: str) -> bool:
    """
    Download a Lark Base attachment file by file_token.
    Uses Drive media download API.
    Returns True if successful.
    """
    url = f"{LARK_API}/drive/v1/medias/{file_token}/download"

    try:
        token = get_token()
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30, stream=True)
        if resp.status_code in (401, 403):
            # Token có thể vừa bị thu hồi → lấy token mới, thử lại 1 lần
            token = refresh_token(stale=token)
            resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30, stream=True)
        if resp.status_code == 200:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            fsize = os.path.getsize(save_path)
            # Log first bytes for format detection
            with open(save_path, "rb") as f:
                header = f.read(16)
            print(f"✅ Downloaded attachment: {file_token} → {save_path} ({fsize} bytes, header={header[:8]})")
            return True
        else:
            print(f"❌ Download attachment failed: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"❌ Download attachment error: {e}")
        return False


def fetch_cccd_images(app_token: str, table_id: str, record_id: str, tmp_dir: str, record: dict = None) -> dict:
    """
    Fetch CCCD front/back images from a Lark Base record.

    record: record đã get_record() sẵn (tránh gọi API 2 lần); None → tự gọi.
    Returns dict with paths:
        {"cccd_truoc_path": "/path/to/front.jpg", "cccd_sau_path": "/path/to/back.jpg"}
    """
    result = {"cccd_truoc_path": "", "cccd_sau_path": ""}

    try:
        if record is None:
            record = get_record(app_token, table_id, record_id)
        if not record or "error" in record:
            return result
        
        fields = record.get("fields", {})
        
        # Lark Base attachment fields are arrays of objects with file_token
        for field_name, key in [("Mặt trước CCCD", "cccd_truoc_path"), ("Mặt sau CCCD", "cccd_sau_path")]:
            attachments = fields.get(field_name, [])
            if attachments and isinstance(attachments, list) and len(attachments) > 0:
                att = attachments[0]  # Take first attachment
                file_token = att.get("file_token", "")
                name = att.get("name", "cccd.jpg")
                if file_token:
                    save_path = os.path.join(tmp_dir, f"{key}_{name}")
                    if download_attachment(file_token, save_path):
                        result[key] = save_path
    except Exception as e:
        print(f"⚠️ fetch_cccd_images error: {e}")
    
    return result


# ╔════════════════════════════════════════════════════════════════╗
# ║                   FIELD OPTIONS MANAGEMENT                     ║
# ╚════════════════════════════════════════════════════════════════╝

def list_fields(app_token: str, table_id: str) -> list:
    """List all fields in a Bitable table."""
    url = f"{LARK_API}/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    data = lark_request("GET", url, timeout=15)
    if data.get("code") != 0:
        print(f"❌ list_fields error: code={data.get('code')} msg={data.get('msg')}")
        return []
    return data.get("data", {}).get("items", [])


def list_records(app_token: str, table_id: str, page_size: int = 500, max_pages: int = 40) -> list:
    """Liệt kê toàn bộ record của 1 bảng (phân trang bằng page_token)."""
    url = f"{LARK_API}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    records, page_token = [], ""
    for _ in range(max_pages):
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        data = lark_request("GET", url, params=params, timeout=30)
        if data.get("code") != 0:
            raise Exception(f"Lark list_records error: code={data.get('code')} msg={data.get('msg')}")
        d = data.get("data", {})
        records.extend(d.get("items", []) or [])
        if not d.get("has_more"):
            break
        page_token = d.get("page_token", "")
        if not page_token:
            break
    return records


def create_field(app_token: str, table_id: str, field_name: str, field_type: int = 1, property: dict = None) -> dict:
    """
    Tạo cột mới. field_type: 1 text, 2 number, 3 single select, 5 datetime, 15 url.
    """
    url = f"{LARK_API}/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    body = {"field_name": field_name, "type": field_type}
    if property:
        body["property"] = property
    data = lark_request("POST", url, json=body, timeout=15)
    if data.get("code") != 0:
        raise Exception(f"Lark create_field '{field_name}' error: code={data.get('code')} msg={data.get('msg')}")
    return data.get("data", {}).get("field", {})


_option_map_cache: dict = {}   # (app_token, table_id) → {"ts": float, "map": {opt_id: name}}
OPTION_MAP_TTL_SEC = 600


def get_option_map(app_token: str, table_id: str, force: bool = False) -> dict:
    """
    Map {option_id → option_name} của TẤT CẢ cột single/multi-select trong bảng.
    Dùng để dịch giá trị 'optXXXX' mà Automation/API đôi khi trả về thay vì tên.
    Cache 10 phút cho mỗi bảng.
    """
    import time as _time
    key = (app_token, table_id)
    cached = _option_map_cache.get(key)
    if cached and not force and _time.time() - cached["ts"] < OPTION_MAP_TTL_SEC:
        return cached["map"]

    opt_map = {}
    try:
        for f in list_fields(app_token, table_id):
            for opt in (f.get("property") or {}).get("options") or []:
                oid, oname = opt.get("id"), opt.get("name")
                if oid and oname:
                    opt_map[oid] = oname
        _option_map_cache[key] = {"ts": _time.time(), "map": opt_map}
    except Exception as e:
        print(f"⚠️ get_option_map error: {e}")
        if cached:
            return cached["map"]
    return opt_map


def get_field_options(app_token: str, table_id: str, field_name: str) -> dict:
    """
    Get a single_select field's info including current options.
    Returns: {"field_id": "...", "options": ["HDKOC", ...], "type": 3} or empty dict
    """
    fields = list_fields(app_token, table_id)
    for f in fields:
        if f.get("field_name") == field_name:
            field_id = f.get("field_id", "")
            field_type = f.get("type", -1)
            options = []
            prop = f.get("property", {})
            if prop and prop.get("options"):
                options = [opt.get("name", "") for opt in prop["options"]]
            print(f"🔍 Field '{field_name}': id={field_id}, type={field_type}, options={options}")
            return {"field_id": field_id, "options": options, "property": prop, "type": field_type}
    return {}


def add_field_options(app_token: str, table_id: str, field_id: str, existing_options: list, new_options: list, field_name: str = "Template", field_type: int = 3) -> dict:
    """
    Add new options to a single_select field while keeping existing ones.
    existing_options: raw option objects from field property (with id, name, color)
    new_options: list of new option name strings to add
    """
    url = f"{LARK_API}/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}"
    
    # Keep existing options AS-IS (with their id/color) + add new with name only
    all_options = []
    existing_names = set()
    for opt in existing_options:
        if isinstance(opt, dict) and opt.get("name"):
            all_options.append(opt)  # keep original id, color, name
            existing_names.add(opt["name"])
    
    for name in new_options:
        if name not in existing_names:
            all_options.append({"name": name})
    
    body = {
        "field_name": field_name,
        "type": field_type,
        "property": {
            "options": all_options
        }
    }
    
    print(f"🔧 [UpdateField] URL: {url}")
    print(f"🔧 [UpdateField] Body: {body}")
    
    data = lark_request("PUT", url, json=body, timeout=15)

    if data.get("code") != 0:
        print(f"❌ add_field_options error: {data.get('msg')} (code={data.get('code')})")
        print(f"   Full response: {data}")
        return {"error": data.get("msg")}
    
    print(f"✅ Field options updated: +{len(new_options)} → {[o for o in new_options]}")
    return data.get("data", {})
