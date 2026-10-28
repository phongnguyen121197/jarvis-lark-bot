"""
Google Drive Client - Upload contracts using OAuth2 (personal Google account)
Version 2.0.0

Uses OAuth2 refresh token so files are owned by YOUR account (has storage quota).
Service Accounts don't have storage quota on personal Google Drive.
"""

import os
import json
import logging
import threading
import time
from typing import Optional, Dict
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

logger = logging.getLogger(__name__)

# Env vars
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")
GOOGLE_DRIVE_TEMPLATE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_TEMPLATE_FOLDER_ID", "")

SCOPES = ["https://www.googleapis.com/auth/drive"]

# googleapiclient/httplib2 KHÔNG thread-safe: 2 webhook chạy song song dùng chung
# 1 kết nối sẽ gây "[SSL] record layer failure". Mọi lời gọi Drive đi qua lock này.
_drive_lock = threading.RLock()


class GoogleDriveClient:
    """Google Drive client using OAuth2 refresh token."""
    
    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        refresh_token: str = None,
        folder_id: str = None,
    ):
        self.folder_id = folder_id or GOOGLE_DRIVE_FOLDER_ID
        
        cid = client_id or GOOGLE_CLIENT_ID
        csecret = client_secret or GOOGLE_CLIENT_SECRET
        rtoken = refresh_token or GOOGLE_REFRESH_TOKEN
        
        if not all([cid, csecret, rtoken]):
            raise ValueError(
                "Missing Google OAuth2 credentials. "
                "Set GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN env vars."
            )
        
        creds = Credentials(
            token=None,
            refresh_token=rtoken,
            client_id=cid,
            client_secret=csecret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=SCOPES,
        )
        creds.refresh(Request())
        
        self.service = build("drive", "v3", credentials=creds)
        logger.info("✅ Google Drive client initialized (OAuth2)")
    
    def upload_docx_as_gdoc(
        self,
        file_path: str,
        file_name: str = None,
        folder_id: str = None,
        set_permission: bool = True,
    ) -> Dict[str, str]:
        """Upload .docx → Google Docs, optionally set anyone with link can edit."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        target_folder = folder_id or self.folder_id
        name = file_name or os.path.basename(file_path).replace(".docx", "")
        
        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.document",
        }
        if target_folder:
            file_metadata["parents"] = [target_folder]
        
        media = MediaFileUpload(
            file_path,
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            resumable=False,
        )
        
        with _drive_lock:
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id, webViewLink",
            ).execute()
        
        file_id = file.get("id")
        logger.info(f"📄 Uploaded to Google Drive: {name} (ID: {file_id})")
        
        if set_permission:
            self.set_anyone_edit(file_id)
        
        web_view_link = file.get("webViewLink", f"https://docs.google.com/document/d/{file_id}/edit")
        
        return {
            "file_id": file_id,
            "web_view_link": web_view_link,
        }
    
    def set_anyone_edit(self, file_id: str):
        """Set permission: anyone with link can edit."""
        with _drive_lock:
            self.service.permissions().create(
                fileId=file_id,
                body={"type": "anyone", "role": "writer"},
                fields="id",
            ).execute()
        logger.info(f"🔓 Permission set: anyone with link can edit")

    def find_template(self, template_name: str, folder_id: str = None) -> Optional[Dict]:
        """
        Find a template file by name in Drive folder.
        Returns: {"id": ..., "name": ..., "modifiedTime": ...} or None
        """
        target_folder = folder_id or GOOGLE_DRIVE_TEMPLATE_FOLDER_ID
        if not target_folder:
            return None
        
        query = (
            f"'{target_folder}' in parents and trashed = false "
            f"and name contains '{template_name}'"
        )
        with _drive_lock:
            result = self.service.files().list(
                q=query,
                fields="files(id, name, modifiedTime, mimeType)",
                orderBy="modifiedTime desc",
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()

        files = result.get("files", [])
        return files[0] if files else None

    def download_file(self, file_id: str, save_path: str) -> bool:
        """Download a file from Drive to local path."""
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with _drive_lock:
                request = self.service.files().get_media(fileId=file_id)
                with open(save_path, "wb") as f:
                    downloader = MediaIoBaseDownload(f, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
            logger.info(f"📥 Downloaded template: {save_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Template download failed: {e}")
            return False


_drive_client: Optional[GoogleDriveClient] = None

# Template cache: {"template_name": {"modified_time": "...", "local_path": "..."}}
_template_cache: Dict[str, Dict] = {}
TEMPLATE_CACHE_DIR = os.path.join("/tmp", "drive_templates")


def get_drive_client(force_new: bool = False) -> Optional[GoogleDriveClient]:
    global _drive_client
    if _drive_client is not None and not force_new:
        return _drive_client
    try:
        _drive_client = GoogleDriveClient()
        return _drive_client
    except Exception as e:
        logger.warning(f"⚠️ Google Drive client not available: {e}")
        return None


TEMPLATE_FETCH_RETRIES = 3


def _fetch_template_once(client: GoogleDriveClient, template_name: str) -> str:
    """
    1 lần: tìm template trên Drive → nếu cache còn mới thì dùng cache,
    không thì tải về. Raise nếu lỗi (caller sẽ retry).
    """
    file_info = client.find_template(template_name)
    if not file_info:
        raise FileNotFoundError(f"Template '{template_name}' not found on Drive")

    file_id = file_info["id"]
    modified_time = file_info["modifiedTime"]
    file_name = file_info["name"]

    # Check cache
    cached = _template_cache.get(template_name)
    if cached and cached["modified_time"] == modified_time:
        local_path = cached["local_path"]
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logger.info(f"📁 Template cache hit: {template_name} ({file_name})")
            return local_path

    # Download new version (ghi ra file tạm rồi rename để không bao giờ
    # để lại file cache dở dang nếu tải lỗi giữa chừng)
    os.makedirs(TEMPLATE_CACHE_DIR, exist_ok=True)
    local_path = os.path.join(TEMPLATE_CACHE_DIR, f"{template_name}.docx")
    tmp_path = local_path + ".part"

    mime = file_info.get("mimeType", "")
    print(f"📄 Template file: {file_name} (mime={mime})")
    with _drive_lock:
        if mime == "application/vnd.google-apps.document":
            # Google Docs native → export as docx
            request = client.service.files().export_media(
                fileId=file_id,
                mimeType="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
        else:
            # Uploaded .docx or other files → download directly
            request = client.service.files().get_media(fileId=file_id)

        with open(tmp_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
    os.replace(tmp_path, local_path)

    _template_cache[template_name] = {
        "modified_time": modified_time,
        "local_path": local_path,
        "file_name": file_name,
    }

    fsize = os.path.getsize(local_path)
    print(f"📥 Template downloaded: {file_name} → {local_path} ({fsize} bytes)")
    return local_path


def get_template_path(template_name: str = "HDKOC", fallback_path: str = None) -> str:
    """
    Get template path from Drive with caching.
    Downloads from Drive template folder, caches locally.
    Only re-downloads when file modifiedTime changes.

    Thread-safe + retry. Nếu Drive lỗi tạm thời: thử lại 3 lần (làm mới kết nối),
    rồi dùng bản cache cũ nếu có. KHÔNG âm thầm dùng template local không có
    placeholder — thà báo lỗi để Lark hiện "Failed" còn hơn sinh hợp đồng trống.

    Args:
        template_name: Template name to search (e.g. "HDKOC", "HDDV")
        fallback_path: Local fallback — chỉ dùng khi Drive CHƯA được cấu hình

    Returns:
        Path to local .docx template file
    """
    global _template_cache

    client = get_drive_client()
    if not client or not GOOGLE_DRIVE_TEMPLATE_FOLDER_ID:
        logger.info("📁 Drive template folder not configured, using local fallback")
        if fallback_path and os.path.exists(fallback_path):
            return fallback_path
        raise FileNotFoundError(f"No template available for: {template_name}")

    last_err = None
    for attempt in range(1, TEMPLATE_FETCH_RETRIES + 1):
        try:
            return _fetch_template_once(client, template_name)
        except FileNotFoundError:
            raise  # template thật sự không có trên Drive → không retry
        except Exception as e:
            last_err = e
            print(f"⚠️ Template fetch attempt {attempt}/{TEMPLATE_FETCH_RETRIES} failed: {e}")
            if attempt < TEMPLATE_FETCH_RETRIES:
                time.sleep(1.5 * attempt)
                client = get_drive_client(force_new=True) or client

    # Hết retry: dùng bản cache cũ nếu còn
    cached = _template_cache.get(template_name)
    if cached and os.path.exists(cached["local_path"]) and os.path.getsize(cached["local_path"]) > 0:
        print(f"⚠️ Drive unreachable, using cached template: {cached.get('file_name')}")
        return cached["local_path"]

    logger.error(f"❌ Template fetch error after {TEMPLATE_FETCH_RETRIES} attempts: {last_err}")
    raise RuntimeError(f"Cannot fetch template '{template_name}' from Drive: {last_err}")
