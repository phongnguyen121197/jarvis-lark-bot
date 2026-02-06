# Contract Generator - Setup Guide
## Tích hợp tạo Hợp đồng KOC tự động vào Jarvis Bot

### Tổng quan Flow
```
Lark Base → Click "Generate" button
    → Lark Automation trigger
    → POST /webhook/contract (Jarvis API)
    → Fill Word template (Bên B info)
    → Upload Google Drive (convert → Google Docs)
    → Update Lark record (Status=Done, OutputWord=link)
```

---

### Bước 1: Tạo OAuth2 Client trên Google Cloud

1. Vào [Google Cloud Console](https://console.cloud.google.com) → project "KOC Contract"
2. **APIs & Services → OAuth consent screen**:
   - User Type: **External** → Create
   - App name: `Jarvis Contract Bot`
   - User support email: chọn email của bạn
   - Developer contact: email của bạn → Save
   - Scopes: bỏ qua → Save
   - Test users: **Add Users** → thêm email Google của bạn → Save
3. **APIs & Services → Credentials → + Create Credentials → OAuth client ID**:
   - Application type: **Desktop app**
   - Name: `Jarvis Contract`
   - Click **Create**
   - **Copy lại Client ID và Client Secret**

---

### Bước 2: Lấy Refresh Token (chạy 1 lần trên máy local)

```bash
pip install google-auth-oauthlib google-api-python-client
```

M�� file `get_refresh_token.py`, paste **Client ID** và **Client Secret** vào:
```python
CLIENT_CONFIG = {
    "installed": {
        "client_id": "PASTE_CLIENT_ID_HERE",
        "client_secret": "PASTE_CLIENT_SECRET_HERE",
        ...
    }
}
```

Chạy:
```bash
python get_refresh_token.py
```

Browser mở ra → đăng nhập Google → cho phép quyền → terminal hiện:
```
GOOGLE_CLIENT_ID=xxx.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=GOCSPX-xxx
GOOGLE_REFRESH_TOKEN=1//0xxx
```

---

### Bước 3: Thêm Environment Variables trên Railway

Vào Railway Dashboard → Jarvis project → Variables, thêm 4 biến:

```
GOOGLE_CLIENT_ID=xxx.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=GOCSPX-xxx
GOOGLE_REFRESH_TOKEN=1//0xxx
GOOGLE_DRIVE_FOLDER_ID=1MJ_PHIU973h_PJ5RlkhvDeJQAh0h9jgk
```

> Có thể xóa biến `GOOGLE_CREDENTIALS_JSON` cũ (không dùng nữa).

---

### Bước 4: Push code và Test

```bash
cd jarvis-lark-bot
git add .
git commit -m "feat: switch to OAuth2 for Google Drive upload"
git push origin main
```

Test sau khi deploy:
```powershell
Invoke-RestMethod -Method POST -Uri "https://jarvis-lark-bot-production.up.railway.app/test/contract"
```

Kết quả thành công:
```json
{
  "success": true,
  "google_docs_link": "https://docs.google.com/document/d/.../edit",
  "drive_configured": true
}
```

---

### Bước 5: Setup Lark Automation

1. Mở Lark Base → Bảng Hợp đồng KOC
2. Vào **Automation** (⚡) → Tạo mới

**Trigger:** "When button is clicked" → Button: "Generate"

**Action:** "Send HTTP Request"
- Method: **POST**
- URL: `https://jarvis-lark-bot-production.up.railway.app/webhook/contract`
- Headers: `Content-Type: application/json`
- Body:
```json
{
  "record_id": "{{Record ID}}",
  "fields": {
    "ID KOC": "{{ID KOC}}",
    "Họ và Tên Bên B": "{{Họ và Tên Bên B}}",
    "Địa chỉ Bên B": "{{Địa chỉ Bên B}}",
    "MST Bên B": "{{MST Bên B}}",
    "SDT Bên B": "{{SDT Bên B}}",
    "CCCD Bên B": "{{CCCD Bên B}}",
    "CCCD Ngày Cấp": "{{CCCD Ngày Cấp}}",
    "CCCD Nơi Cấp": "{{CCCD Nơi Cấp}}",
    "Gmail Bên B": "{{Gmail Bên B}}",
    "STK bên B": "{{STK bên B}}"
  }
}
```

Enable → Save

---

### Cách sử dụng

1. Điền thông tin Bên B trong Lark Base
2. Click **Generate** → đợi ~5 giây
3. **Status** → "Done" ✅
4. **OutputWord** → Link Google Docs (anyone with link can edit)

---

### Troubleshooting

| Vấn đề | Giải pháp |
|--------|-----------|
| Status = "Failed" | Check Railway logs |
| Token expired | Refresh token tự động renew, không cần làm gì |
| 403 storage quota | Đảm bảo dùng OAuth2, không phải Service Account |
| Automation không trigger | Button type = "Run Automation" |
