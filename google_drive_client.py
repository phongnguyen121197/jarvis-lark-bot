"""
Google Drive Client - Upload contracts and get shareable links
Version 1.0.0

Uploads .docx files to Google Drive with:
- Conversion to Google Docs format (so link opens as editable Google Doc)
- Permission: anyone with link can edit
- Organized in a configurable folder
"""

import os
import json
import logging
from typing import Optional, Dict
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

# Google Drive scopes
SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

# Env vars
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")


class GoogleDriveClient:
    """Google Drive client for uploading contract files."""
    
    def __init__(self, credentials_json: str = None, folder_id: str = None):
        """
        Initialize Google Drive client.
        
        Args:
            credentials_json: JSON string of service account credentials
            folder_id: Google Drive folder ID to upload files to
        """
        self.folder_id = folder_id or GOOGLE_DRIVE_FOLDER_ID
        creds_json = credentials_json or GOOGLE_CREDENTIALS_JSON
        
        if not creds_json:
            raise ValueError(
                "Google credentials not found. "
                "Set GOOGLE_CREDENTIALS_JSON env var with service account JSON."
            )
        
        # Parse credentials
        if isinstance(creds_json, str):
            creds_dict = json.loads(creds_json)
        else:
            creds_dict = creds_json
        
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        self.service = build("drive", "v3", credentials=creds)
        logger.info("✅ Google Drive client initialized")
    
    def upload_docx_as_gdoc(
        self,
        file_path: str,
        file_name: str = None,
        folder_id: str = None,
    ) -> Dict[str, str]:
        """
        Upload a .docx file to Google Drive, converting to Google Docs format.
        Sets permission to 'anyone with link can edit'.
        
        Args:
            file_path: Path to the .docx file
            file_name: Display name in Google Drive (default: original filename)
            folder_id: Override folder ID
        
        Returns:
            Dict with:
                - file_id: Google Drive file ID
                - web_view_link: Link to view/edit in Google Docs
                - web_content_link: Direct download link
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        target_folder = folder_id or self.folder_id
        name = file_name or os.path.basename(file_path).replace(".docx", "")
        
        # File metadata
        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.document",  # Convert to Google Docs
        }
        
        if target_folder:
            file_metadata["parents"] = [target_folder]
        
        # Upload with conversion
        media = MediaFileUpload(
            file_path,
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            resumable=True,
        )
        
        file = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink, webContentLink",
            supportsAllDrives=True,
        ).execute()
        
        file_id = file.get("id")
        logger.info(f"📄 Uploaded to Google Drive: {name} (ID: {file_id})")
        
        # Set permission: anyone with link can edit
        self.service.permissions().create(
            fileId=file_id,
            body={
                "type": "anyone",
                "role": "writer",
            },
            fields="id",
        ).execute()
        
        logger.info(f"🔓 Permission set: anyone with link can edit")
        
        web_view_link = file.get("webViewLink", f"https://docs.google.com/document/d/{file_id}/edit")
        
        return {
            "file_id": file_id,
            "web_view_link": web_view_link,
            "web_content_link": file.get("webContentLink", ""),
        }
    
    def delete_file(self, file_id: str) -> bool:
        """Delete a file from Google Drive."""
        try:
            self.service.files().delete(
                fileId=file_id,
                supportsAllDrives=True,
            ).execute()
            logger.info(f"🗑️ Deleted file: {file_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Delete failed: {e}")
            return False


# Singleton instance
_drive_client: Optional[GoogleDriveClient] = None


def get_drive_client() -> Optional[GoogleDriveClient]:
    """Get or create singleton Google Drive client."""
    global _drive_client
    
    if _drive_client is not None:
        return _drive_client
    
    try:
        _drive_client = GoogleDriveClient()
        return _drive_client
    except Exception as e:
        logger.warning(f"⚠️ Google Drive client not available: {e}")
        return None
