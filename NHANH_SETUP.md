# Nhanh.vn ↔ Jarvis – Tracking trạng thái đơn booking

Jarvis theo dõi đơn hàng trên Nhanh.vn và ghi trạng thái về bảng booking trên Lark Base
(`PR - Data list booking Ken Perfume`, Base Cheng). Đơn ↔ booking được khớp bằng **SĐT người nhận
trên đơn** so với SĐT tách từ cột `Thông tin nhận hàng` của record booking.

Bot nhận dữ liệu bằng 2 đường song song: **webhook** (Nhanh đẩy ngay khi đơn thêm/sửa/xóa/trả hàng)
và **chu kỳ 10 phút** qua API: (1) với record booking chưa có mã đơn → hỏi Nhanh các đơn của SĐT đó và gắn
đơn phù hợp nhất (ưu tiên đơn tạo sau *Ngày deal*); (2) với record đã gắn → tra lại theo mã đơn và cập nhật.
Lưu ý: đơn từ sàn (Shopee/TikTok) bị Nhanh che SĐT nên **không khớp được theo SĐT**, chỉ theo dõi được khi
đơn được tạo tay trên Nhanh (kênh Admin) hoặc đã gắn mã đơn. Các cột sau được **tự tạo** trong bảng booking
nếu chưa có: `Nhanh - Mã đơn`, `Nhanh - Trạng thái`, `Nhanh - Mã vận đơn`, `Nhanh - Hãng VC`, `Nhanh - Cập nhật`.

---

## 1. App trên open.nhanh.vn (đã tạo)

| | |
|---|---|
| App | **Jarvis Booking Tracker** – App ID `78743` |
| Redirect URL | `https://jarvis-lark-bot-production.up.railway.app/nhanh/oauth/callback` |
| Webhook callback | `https://jarvis-lark-bot-production.up.railway.app/webhook/nhanh` |
| Webhook version | 3.0 |
| Sự kiện | Thêm / Sửa / Xóa đơn hàng, Trả hàng 1 phần |

## 2. Biến môi trường trên Railway

```
NHANH_APP_ID=78743
NHANH_SECRET_KEY=<Secret key trong trang chi tiết app>
NHANH_WEBHOOK_VERIFY_TOKEN=<verify token đã nhập trên app>

# Có sau bước uỷ quyền (mục 3):
NHANH_ACCESS_TOKEN=
NHANH_BUSINESS_ID=
NHANH_TOKEN_EXPIRED_AT=

# Tuỳ chọn (mặc định đã đúng cho Base Cheng):
# NHANH_BOOKING_APP_TOKEN=V89SbotPpakF3DsJbyAljveXgcg
# NHANH_BOOKING_TABLE_ID=tblyf1KR7lDjoEzz
# NHANH_BOOKING_PHONE_FIELDS=Thông tin nhận hàng
```

## 3. Uỷ quyền (làm 1 lần, token sống 1 năm)

1. Đảm bảo Nhanh.vn đã bật *Cài đặt → Cài đặt Open API → Cho phép kết nối Open API* (quyền Giám đốc).
2. Đăng nhập nhanh.vn bằng tài khoản Giám đốc, rồi mở:
   `https://jarvis-lark-bot-production.up.railway.app/nhanh/oauth/start`
3. Chọn doanh nghiệp, kho, quyền (cần ít nhất **Đơn hàng – xem**), bấm Đồng ý.
4. Nhanh đưa về trang callback của Jarvis: trang này hiện `NHANH_ACCESS_TOKEN`, `NHANH_BUSINESS_ID`,
   `NHANH_TOKEN_EXPIRED_AT` → dán 3 biến đó vào Railway → Variables → redeploy.
   (Token đã nằm trong bộ nhớ bot nên dùng được ngay, nhưng sẽ mất khi container restart nếu chưa lưu env.)
5. Kiểm tra: `https://jarvis-lark-bot-production.up.railway.app/test/nhanh-status?check=true`
   – phải thấy `has_token: true`, `check_token` không lỗi, `booking_index.records > 0`.

Token hết hạn sau 1 năm, không có refresh token → tới gần ngày `expired_at_text` làm lại bước 2–4.

## 4. Bật webhook trên app

Sau khi Jarvis đã deploy code này: open.nhanh.vn → app → **Sửa** → bật *Bật webhooks* → Lưu.
Nhanh sẽ gửi thử sự kiện `webhooksEnabled` tới callback; endpoint trả 200 là lưu được.

## 5. Vận hành

| Việc | Cách làm |
|---|---|
| **Gắn đơn cho booking theo SĐT** (chạy đầu tiên) | `GET /nhanh/link` – chỉ record chưa có mã đơn; `?all=true` gắn lại toàn bộ |
| Làm mới trạng thái các đơn đã gắn | `GET /nhanh/refresh` |
| Soi: mỗi SĐT booking có bao nhiêu đơn trên Nhanh | `GET /nhanh/debug-match?limit=5` |
| Quét theo thời gian cập nhật (ít dùng) | `GET /nhanh/sync?minutes=43200` (tối đa 31 ngày) |
| Xem 1 đơn khớp record nào | `GET /nhanh/order/<id đơn Nhanh>` |
| Tình trạng kết nối | `GET /test/nhanh-status?check=true` |
| Log | Railway, các dòng gắn `[Nhanh]`, `[Nhanh:webhook]`, `[Nhanh:poll]` |

Quy tắc khớp đơn → booking:
1. Record đã gắn đúng `Nhanh - Mã đơn` → cập nhật record đó.
2. Chưa có → record có SĐT trùng và **chưa** gắn mã đơn (nếu nhiều, lấy record cuối bảng = mới nhất).
3. Vẫn không có → record có SĐT trùng mới nhất (ghi đè – KOC lên đơn lại).
4. Không có SĐT trùng → bỏ qua (log `unmatched`), không tạo record mới.

Chỉ xử lý đơn kiểu *Giao hàng tận nhà*, *Khách trả lại hàng*, *Đơn hoàn một phần*; bỏ đơn POS/chuyển kho.

Mã trạng thái Nhanh → chữ: 54 Đơn mới · 56 Đã xác nhận · 42 Đang đóng gói · 43 Chờ thu gom ·
59 Đang chuyển · 60 Thành công · 61 Thất bại · 63 Khách hủy · 64 Hệ thống hủy · 71 Đang chuyển hoàn ·
72 Đã chuyển hoàn (đầy đủ trong `nhanh_client.ORDER_STATUS`).

Giới hạn API: 150 request / 30 giây mỗi URL; bot tự chờ khi bị `ERR_429`.
