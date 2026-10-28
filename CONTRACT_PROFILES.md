# Contract Profiles – tạo hợp đồng cho nhiều Lark Base

Từ phiên bản này, Jarvis có thể tạo hợp đồng KOC cho **nhiều Lark Base khác nhau** mà
không cần deploy riêng. Mỗi Base là một **profile**. Base cũ vẫn chạy nguyên như trước.

| Profile   | Lark Base                                   | URL webhook tạo hợp đồng          | URL sync template                       |
|-----------|---------------------------------------------|-----------------------------------|-----------------------------------------|
| `default` | Base KOC cũ (`CONTRACT_BASE_*`)             | `POST /webhook/contract`          | `POST /webhook/sync-templates`          |
| `cheng`   | `V89SbotPpakF3DsJbyAljveXgcg` / `tblMaoRSTQHWBQQY` (chenglovehair) | `POST /webhook/contract/cheng` | `POST /webhook/sync-templates/cheng` |

Tất cả profile dùng **chung** Google Drive: cùng folder output (`GOOGLE_DRIVE_FOLDER_ID`)
và cùng folder template (`GOOGLE_DRIVE_TEMPLATE_FOLDER_ID`). File hợp đồng tạo ra đặt tên
`<ID KOC> <dd-mm-yyyy>` như cũ.

---

## 1. Biến môi trường trên Railway

Profile `cheng` đã có giá trị mặc định trong code, nhưng nên khai báo rõ trên Railway để dễ đổi:

```
CONTRACT_CHENG_APP_TOKEN=V89SbotPpakF3DsJbyAljveXgcg
CONTRACT_CHENG_TABLE_ID=tblMaoRSTQHWBQQY
```

Muốn thêm profile mới (hoặc Base có tên cột Status / Kết quả / Template khác) thì dùng
`CONTRACT_PROFILES_JSON` – không cần sửa code:

```
CONTRACT_PROFILES_JSON={"kalle2":{"app_token":"XXXX","table_id":"tblYYYY"},"mkt":{"app_token":"V89SbotPpakF3DsJbyAljveXgcg","table_id":"tblMaoRSTQHWBQQY","status_field":"Trạng thái (MKT)","result_field":"Kết quả (MKT)"}}
```

Key hợp lệ trong 1 profile:

| Key              | Bắt buộc | Mặc định   | Ý nghĩa                                   |
|------------------|----------|------------|-------------------------------------------|
| `app_token`      | ✅       |            | Base ID – phần sau `/base/` trong URL      |
| `table_id`       | ✅       |            | Table ID – tham số `table=` trong URL      |
| `status_field`   |          | `Status`   | Cột bot ghi `Done` / `Failed`             |
| `result_field`   |          | `Kết quả`  | Cột (kiểu Link/URL) nhận link Google Docs |
| `template_field` |          | `Template` | Cột single-select chọn mẫu hợp đồng       |

Profile khai báo trong JSON trùng tên với profile có sẵn sẽ **ghi đè** profile đó.

---

## 2. Yêu cầu trên Lark Base mới

1. **Add app Jarvis vào Base**: mở Base → `...` → *Add collaborator / Thêm thành viên* →
   tìm app **Jarvis** → quyền *Can edit*. Không có bước này bot sẽ báo
   `NoPermission` / `RecordIdNotFound`.
2. Các cột bot **đọc** (tên phải khớp chính xác, kể cả hoa/thường):
   `ID KOC`, `Họ và Tên Bên B`, `Địa chỉ Bên B`, `MST Bên B`, `SDT Bên B`, `CCCD Bên B`,
   `CCCD Ngày Cấp`, `CCCD Nơi Cấp`, `Gmail Bên B`, `STK bên B`, `Ngân hàng`,
   `Chi phí`, `Thành tiền`, `Số lượng clip`, `% thuế TNCN`, `Thuế TNCN`, `Tổng giá trị sau thuế`,
   `Thương hiệu`, `Ngày dự kiến air`, `% Hoa hồng tự nhiên`, `% Hoa hồng chạy ads`, `Template`.
3. Cột **ảnh đính kèm** (bot tự tải từ record, không cần gửi trong body): `Mặt trước CCCD`, `Mặt sau CCCD`.
4. Cột bot **ghi**: `Status` (single select, phải có sẵn 2 option `Done` và `Failed`) và
   `Kết quả` (kiểu *Link*).

---

## 3. Lark Automation cho Base mới

**Trigger:** *When button is clicked* → nút **Magic**

**Action:** *Send HTTP Request*

- Method: `POST`
- URL: `https://jarvis-lark-bot-production.up.railway.app/webhook/contract/cheng`
- Headers: `Content-Type: application/json`
- Body:

```json
{
  "record_id": "{{Record ID}}",
  "fields": {
    "ID KOC": "{{ID KOC}}",
    "Template": "{{Template}}",
    "Họ và Tên Bên B": "{{Họ và Tên Bên B}}",
    "Địa chỉ Bên B": "{{Địa chỉ Bên B}}",
    "MST Bên B": "{{MST Bên B}}",
    "SDT Bên B": "{{SDT Bên B}}",
    "CCCD Bên B": "{{CCCD Bên B}}",
    "CCCD Ngày Cấp": "{{CCCD Ngày Cấp}}",
    "CCCD Nơi Cấp": "{{CCCD Nơi Cấp}}",
    "Gmail Bên B": "{{Gmail Bên B}}",
    "STK bên B": "{{STK bên B}}",
    "Ngân hàng": "{{Ngân hàng}}",
    "Thương hiệu": "{{Thương hiệu}}",
    "Ngày dự kiến air": "{{Ngày dự kiến air}}",
    "% Hoa hồng tự nhiên": "{{% Hoa hồng tự nhiên}}",
    "% Hoa hồng chạy ads": "{{% Hoa hồng chạy ads}}",
    "Chi phí": "{{Chi phí}}",
    "Số lượng clip": "{{Số lượng clip}}",
    "Thành tiền": "{{Thành tiền}}",
    "% thuế TNCN": "{{% thuế TNCN}}",
    "Thuế TNCN": "{{Thuế TNCN}}",
    "Tổng giá trị sau thuế": "{{Tổng giá trị sau thuế}}"
  }
}
```

Nút **Thêm mẫu hợp đồng** (quét Drive → thêm option vào cột `Template`):

- Method: `POST`
- URL: `https://jarvis-lark-bot-production.up.railway.app/webhook/sync-templates/cheng`

---

## 4. Placeholder mới trong file Word

Thêm `{NGAN_HANG}` – điền từ cột `Ngân hàng`. Danh sách đầy đủ placeholder xem đầu file
`contract_generator.py`.

---

## 5. Kiểm tra sau khi deploy

```bash
# Xem bot đã nạp profile nào
curl https://jarvis-lark-bot-production.up.railway.app/test/contract-profiles

# Kiểm tra quyền + danh sách cột của Base mới (phải thấy target_table_found: true)
curl "https://jarvis-lark-bot-production.up.railway.app/test/lark-debug?profile=cheng"

# Thử ghi Status = "Test" vào 1 record của Base mới
curl -X POST "https://jarvis-lark-bot-production.up.railway.app/test/lark-update/<record_id>?profile=cheng"

# Chạy sync template cho Base mới và xem kết quả chi tiết
curl "https://jarvis-lark-bot-production.up.railway.app/test/sync-templates?profile=cheng"
```

Log Railway sẽ gắn tag `[BG:cheng]` / `[Sync:cheng]` cho mọi request của profile này.
