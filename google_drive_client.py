"""
Google Drive Client - Upload contracts using OAuth2 (personal Google account)
Version 2.0.0

Uses OAuth2 refresh token so files are owned by YOUR account (has storage quota).
Service Accounts don't have storage quota on personal Google Drive.
"""

import os
import json
import logging
from typing import Optional, Dict
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

# Env vars
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

SCOPES = ["https://www.googleapis.com/auth/drive"]


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
    ) -> Dict[str, str]:
        """Upload .docx → Google Docs, set anyone with link can edit."""
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
            resumable=True,
        )
        
        file = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink, webContentLink",
        ).execute()
        
        file_id = file.get("id")
        logger.info(f"📄 Uploaded to Google Drive: {name} (ID: {file_id})")
        
        self.service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "writer"},
            fields="id",
        ).execute()
        logger.info(f"🔓 Permission set: anyone with link can edit")
        
        web_view_link = file.get("webViewLink", f"https://docs.google.com/document/d/{file_id}/edit")
        
        return {
            "file_id": file_id,
            "web_view_link": web_view_link,
            "web_content_link": file.get("webContentLink", ""),
        }


_drive_client: Optional[GoogleDriveClient] = None


def get_drive_client() -> Optional[GoogleDriveClient]:
    global _drive_client
    if _drive_client is not None:
        return _drive_client
    try:
        _drive_client = GoogleDriveClient()
        return _drive_client
    except Exception as e:
        logger.warning(f"⚠️ Google Drive client not available: {e}")
        return None
