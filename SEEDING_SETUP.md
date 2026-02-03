# Hướng Dẫn Setup Seeding Notification

## Tổng quan

Tính năng này cho phép gửi thông báo tự động vào nhóm Lark khi có video TikTok mới cần seeding, bao gồm:
- Thumbnail video (tự động crawl từ TikTok)
- Thông tin KOC, kênh, sản phẩm
- Button xem video và link bản ghi

---

## Bước 1: Lấy Chat ID của nhóm

1. Mở nhóm cần nhận thông báo (ví dụ: "Gấp 2H") trong Lark
2. Gửi tin nhắn mention @Jarvis (ví dụ: "@Jarvis test")
3. Xem log trên Railway → tìm dòng `📍 Chat ID: oc_xxxxx`
4. Copy chat_id này

---

## Bước 2: Thêm biến môi trường trên Railway

1. Vào Railway Dashboard → Service Jarvis Bot
2. Tab **Variables** → Add variable:

```
GAP_2H_CHAT_ID = oc_xxxxx (chat_id từ bước 1)
```

3. Railway sẽ tự động redeploy

---

## Bước 3: Test thủ công

Gọi API test để đảm bảo mọi thứ hoạt động:

```bash
curl -X POST "https://your-jarvis.railway.app/test/seeding-card"
```

Hoặc mở trình duyệt:
```
https://your-jarvis.railway.app/test/seeding-card?tiktok_url=https://www.tiktok.com/@test/video/123&koc_name=Test%20KOC&product=Test%20Product
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

### "Missing GAP_2H_CHAT_ID"
- Chưa set biến môi trường trên Railway
- Hoặc set sai tên biến

### "Failed to send seeding card"
- Bot chưa được add vào nhóm
- Chat ID sai
- Xem log chi tiết trên Railway

### Thumbnail không hiển thị
- TikTok có thể block request
- Video có thể đã bị xóa
- Card vẫn được gửi, chỉ không có ảnh

### Webhook không được gọi
- Kiểm tra URL đúng chưa
- Automation có được bật không
- Condition có đúng không

---

## Message Card Preview

```
┌─────────────────────────────────────────┐
│  🔥 SOS VIDEO ĐÃ AIR SEEDING GẤP        │
├─────────────────────────────────────────┤
│  ┌─────────────────────────┐            │
│  │                         │            │
│  │    [Video Thumbnail]    │            │
│  │                         │            │
│  └─────────────────────────┘            │
│                                         │
│  • Tên KOC: Hai người yêu nhau 💕       │
│  • ID kênh: hainguoiiunhau9             │
│  • Sản phẩm: Box quà "YÊU"              │
│                                         │
│  Check gấp triển khai công việc...      │
│─────────────────────────────────────────│
│  [🎬 XEM VIDEO]  [📋 LINK BẢN GHI]      │
└─────────────────────────────────────────┘
```
