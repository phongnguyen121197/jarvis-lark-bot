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


def download_attachment(file_token: str, save_path: str) -> bool:
    """
    Download a Lark Base attachment file by file_token.
    Uses Drive media download API.
    Returns True if successful.
    """
    url = f"{LARK_API}/drive/v1/medias/{file_token}/download"
    h = {"Authorization": f"Bearer {get_token()}"}
    
    try:
        resp = requests.get(url, headers=h, timeout=30, stream=True)
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


def fetch_cccd_images(app_token: str, table_id: str, record_id: str, tmp_dir: str) -> dict:
    """
    Fetch CCCD front/back images from a Lark Base record.
    
    Returns dict with paths:
        {"cccd_truoc_path": "/path/to/front.jpg", "cccd_sau_path": "/path/to/back.jpg"}
    """
    result = {"cccd_truoc_path": "", "cccd_sau_path": ""}
    
    try:
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
    resp = requests.get(url, headers=headers(), timeout=15)
    data = resp.json()
    if data.get("code") != 0:
        print(f"❌ list_fields error: {data.get('msg')}")
        return []
    return data.get("data", {}).get("items", [])


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


def add_field_options(app_token: str, table_id: str, field_id: str, existing_options: list, new_options: list, field_name: str = "Template") -> dict:
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
        "type": 3,
        "property": {
            "options": all_options
        }
    }
    
    print(f"🔧 [UpdateField] URL: {url}")
    print(f"🔧 [UpdateField] Body: {body}")
    
    resp = requests.put(url, headers=headers(), json=body, timeout=15)
    data = resp.json()
    
    if data.get("code") != 0:
        print(f"❌ add_field_options error: {data.get('msg')} (code={data.get('code')})")
        print(f"   Full response: {data}")
        return {"error": data.get("msg")}
    
    print(f"✅ Field options updated: +{len(new_options)} → {[o for o in new_options]}")
    return data.get("data", {})
