# Hướng Dẫn Setup Seeding Notification

## Tổng quan

Tính năng này cho phép gửi thông báo tự động vào nhóm Lark (kể cả external groups) khi có video TikTok mới cần seeding, bao gồm:
- Thông tin KOC, kênh, sản phẩm
- Button xem video và link bản ghi

**Lưu ý:** Vì nhóm "Gấp 2H" là external group, không thể add Jarvis Bot vào. Thay vào đó sử dụng **Custom Bot (Webhook)**.

---

## Bước 1: Tạo Custom Bot trong nhóm

Bạn đã có webhook URL:
```
https://open.larksuite.com/open-apis/bot/v2/hook/59f0b874-ea0f-4011-aad8-be4a58b2db62
```

Nếu cần tạo mới:
1. Mở nhóm trong Lark
2. Click tên nhóm → **Settings** → **Bots**
3. Click **Add Bot** → **Custom Bot**
4. Đặt tên (ví dụ: "Seeding Alert")
5. Copy **Webhook URL**

---

## Bước 2: Thêm biến môi trường trên Railway

1. Vào Railway Dashboard → Service Jarvis Bot
2. Tab **Variables** → Add variable:

```
SEEDING_WEBHOOK_URL = https://open.larksuite.com/open-apis/bot/v2/hook/59f0b874-ea0f-4011-aad8-be4a58b2db62
```

3. Railway sẽ tự động redeploy

---

## Bước 3: Test thủ công

Gọi API test để đảm bảo mọi thứ hoạt động:

```bash
curl -X POST "https://your-jarvis.railway.app/test/seeding-card"
```

---

## Bước 4: Setup Lark Base Automation

### 4.1. Vào Automation của Lark Base

1. Mở Base "Báo cáo chỉ số MKT - Cheng"
2. Click **⚡ Automation** → **Create automation**

### 4.2. Chọn Trigger

- **Option A:** "When record is created" (khi có record mới)
- **Option B:** "When record matches conditions" (khi record thỏa điều kiện)

### 4.3. Thêm Action "Send HTTP request"

**URL:**
```
https://your-jarvis.railway.app/webhook/seeding
```

**Method:** `POST`

**Headers:**
| Key | Value |
|-----|-------|
| Content-Type | application/json |

**Body (JSON):**
```json
{
  "koc_name": "{{Tên KOC}}",
  "channel_id": "{{ID kênh}}",
  "tiktok_url": "{{Link air video}}",
  "product": "{{Sản phẩm}}",
  "product_type": "{{Phân loại sp}}",
  "record_url": "{{Record URL}}"
}
```

> **Lưu ý:** Thay `{{...}}` bằng field reference thực từ Base (click icon ⊕ để chọn)

### 4.4. Save và bật Automation

---

## Các Endpoints

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `/webhook/seeding` | POST | Nhận webhook từ Lark Base |
| `/test/seeding-card` | POST | Test gửi card với dữ liệu mẫu |
| `/test/tiktok-thumbnail` | GET | Test crawl thumbnail từ TikTok URL |
| `/send/seeding` | POST | Gửi seeding card thủ công |

---

## Troubleshooting

### "Missing SEEDING_WEBHOOK_URL"
- Chưa set biến môi trường trên Railway
- Hoặc set sai tên biến

### "Failed to send via webhook"
- Webhook URL sai
- Bot đã bị xóa khỏi nhóm
- Xem log chi tiết trên Railway

### Webhook không được gọi
- Kiểm tra URL Jarvis đúng chưa
- Automation có được bật không
- Condition có đúng không

---

## Message Card Preview

```
┌─────────────────────────────────────────┐
│  🔥 SOS VIDEO ĐÃ AIR SEEDING GẤP        │
├─────────────────────────────────────────┤
│                                         │
│  **Tên KOC:** Hai người yêu nhau 💕     │
│  **ID kênh:** hainguoiiunhau9           │
│  **Sản phẩm:** Box quà "YÊU"            │
│  **Link video:** https://tiktok.com/... │
│                                         │
│  Check gấp triển khai công việc...      │
│─────────────────────────────────────────│
│  [🎬 XEM VIDEO]  [📋 LINK BẢN GHI]      │
└─────────────────────────────────────────┘
```

**Lưu ý:** Webhook không hỗ trợ hiển thị thumbnail như khi paste link thủ công.
