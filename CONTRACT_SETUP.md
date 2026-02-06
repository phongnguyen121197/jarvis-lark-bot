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

### Bước 1: Chuẩn bị Google Service Account

1. Vào [Google Cloud Console](https://console.cloud.google.com)
2. Tạo project mới hoặc chọn project có sẵn
3. Enable **Google Drive API**:
   - APIs & Services → Library → search "Google Drive API" → Enable
4. Tạo Service Account:
   - APIs & Services → Credentials → Create Credentials → Service Account
   - Đặt tên (VD: `jarvis-contract-bot`)
   - Tạo key JSON: click vào Service Account → Keys → Add Key → JSON
   - **Download file JSON** → giữ lại nội dung
5. Tạo Google Drive Folder:
   - Tạo folder mới trên Google Drive (VD: "Hợp đồng KOC")
   - Copy **Folder ID** từ URL: `https://drive.google.com/drive/folders/{FOLDER_ID}`
   - **Share folder** với email Service Account (có dạng `xxx@xxx.iam.gserviceaccount.com`) → quyền **Editor**

---

### Bước 2: Thêm Environment Variables trên Railway

Vào Railway Dashboard → Jarvis project → Variables, thêm:

```
GOOGLE_CREDENTIALS_JSON={"type":"service_account","project_id":"...","private_key":"...","client_email":"..."}
GOOGLE_DRIVE_FOLDER_ID=1AbCdEfGhIjKlMnOpQrStUvWxYz
```

> ⚠️ `GOOGLE_CREDENTIALS_JSON` paste **TOÀN BỘ nội dung file JSON** thành 1 dòng.

Env vars mặc định (đã hardcode, chỉ cần set nếu muốn thay đổi):
```
CONTRACT_BASE_APP_TOKEN=W4trb7H8FaxrbbsjWLXlxru2gUe
CONTRACT_BASE_TABLE_ID=tblWZAmV3MfFsJpo
```

---

### Bước 3: Push code lên GitHub

```bash
cd jarvis-lark-bot
git add .
git commit -m "feat: add KOC contract generator v1.0.0 - Google Drive integration"
git push origin main
```

Files mới:
- `contract_generator.py` - Fill Word template
- `google_drive_client.py` - Upload Google Drive
- `templates/Mau_hop_dong_KOC.docx` - File mẫu hợp đồng
- Updated: `main.py`, `requirements.txt`, `.env.example`

Railway sẽ tự động deploy khi push.

---

### Bước 4: Test endpoint

Sau khi deploy xong, test bằng POST request:

```bash
curl -X POST https://jarvis-lark-bot-production.up.railway.app/test/contract
```

Response thành công:
```json
{
  "success": true,
  "local_path": "/tmp/contract_.../HD_KOC_TEST001_Nguyen_Van_Test.docx",
  "google_docs_link": "https://docs.google.com/document/d/.../edit",
  "drive_configured": true
}
```

---

### Bước 5: Setup Lark Automation

1. Mở Lark Base → Bảng Hợp đồng KOC
2. Vào **Automation** (biểu tượng ⚡ góc trên phải)
3. Tạo Automation mới:

**Trigger:**
- Chọn: "When button is clicked"
- Button field: "Generate"

**Action:**
- Chọn: "Send HTTP Request"
- Method: **POST**
- URL: `https://jarvis-lark-bot-production.up.railway.app/webhook/contract`
- Headers:
  ```
  Content-Type: application/json
  ```
- Body (JSON):
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

4. **Enable** Automation → Save

---

### Cách sử dụng

1. Điền đầy đủ thông tin Bên B trong Lark Base
2. Click nút **Generate** ở cột cuối
3. Đợi ~5 giây
4. Cột **Status** → "Done" ✅
5. Cột **OutputWord** → Link Google Docs (anyone with link can edit)

---

### Troubleshooting

| Vấn đề | Giải pháp |
|--------|-----------|
| Status = "Failed" | Check log trên Railway: `railway logs` |
| Google Drive error | Kiểm tra GOOGLE_CREDENTIALS_JSON đúng format |
| Folder permission | Share folder Drive với email Service Account |
| Automation không trigger | Kiểm tra button type = "Run Automation" |
| Missing fields | Đảm bảo tất cả field names trong Automation body khớp với tên cột |
