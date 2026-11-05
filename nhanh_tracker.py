"""
Nhanh.vn → Lark Base booking tracker
====================================
Theo dõi trạng thái đơn hàng Nhanh.vn của các KOC đã booking và ghi về
bảng booking trên Lark Base.

Cách khớp đơn ↔ booking: SĐT người nhận trên đơn Nhanh == SĐT tách ra từ cột
"Thông tin nhận hàng" (hoặc cột khác cấu hình qua env) của record booking.

Bot tự tạo các cột sau trong bảng booking nếu chưa có:
  Nhanh - Mã đơn        (text)      id đơn trên Nhanh.vn
  Nhanh - Trạng thái    (text)      vd "Đang chuyển", "Thành công", "Đã chuyển hoàn"
  Nhanh - Mã vận đơn    (text)      carrierCode
  Nhanh - Hãng VC       (text)      GHN / GHTK / Viettel Post ...
  Nhanh - Cập nhật      (datetime)  lần cập nhật gần nhất

ENV:
  NHANH_BOOKING_APP_TOKEN     Base chứa bảng booking (mặc định Base Cheng)
  NHANH_BOOKING_TABLE_ID      bảng booking (mặc định "PR - Data list booking Ken Perfume")
  NHANH_BOOKING_PHONE_FIELDS  các cột chứa SĐT, phân cách bằng dấu ; (mặc định "Thông tin nhận hàng")
"""

import os
import re
import time
import threading
from typing import Dict, List, Optional, Tuple

import nhanh_client as nc
from lark_contract import list_records, list_fields, create_field, update_record

BOOKING_APP_TOKEN = os.getenv("NHANH_BOOKING_APP_TOKEN", "V89SbotPpakF3DsJbyAljveXgcg")
BOOKING_TABLE_ID = os.getenv("NHANH_BOOKING_TABLE_ID", "tblyf1KR7lDjoEzz")
PHONE_FIELDS = [s.strip() for s in os.getenv("NHANH_BOOKING_PHONE_FIELDS", "Thông tin nhận hàng").split(";") if s.strip()]

F_ORDER_ID = "Nhanh - Mã đơn"
F_STATUS = "Nhanh - Trạng thái"
F_CARRIER_CODE = "Nhanh - Mã vận đơn"
F_CARRIER = "Nhanh - Hãng VC"
F_UPDATED = "Nhanh - Cập nhật"

TRACKING_FIELDS = {
    F_ORDER_ID: (1, None),
    F_STATUS: (1, None),
    F_CARRIER_CODE: (1, None),
    F_CARRIER: (1, None),
    F_UPDATED: (5, {"date_formatter": "yyyy/MM/dd HH:mm"}),
}

INDEX_TTL_SEC = 300

_lock = threading.Lock()
_fields_ready = {"ts": 0.0, "ok": False}
_index_cache = {"ts": 0.0, "by_phone": {}, "by_order": {}, "count": 0}
# Chống ghi trùng: order_id → (status_code, carrier_code)
_last_written: Dict[str, Tuple] = {}


# ╔════════════════════════════════════════════════════════════════╗
# ║                     LARK: CỘT & INDEX SĐT                     ║
# ╚════════════════════════════════════════════════════════════════╝

def ensure_tracking_fields(force: bool = False) -> dict:
    """Tạo các cột Nhanh-* nếu bảng booking chưa có. Kết quả cache 1 giờ."""
    if _fields_ready["ok"] and not force and time.time() - _fields_ready["ts"] < 3600:
        return {"ok": True, "created": [], "cached": True}
    existing = {f.get("field_name") for f in list_fields(BOOKING_APP_TOKEN, BOOKING_TABLE_ID)}
    created = []
    for name, (ftype, prop) in TRACKING_FIELDS.items():
        if name not in existing:
            create_field(BOOKING_APP_TOKEN, BOOKING_TABLE_ID, name, ftype, prop)
            created.append(name)
    if created:
        print(f"🆕 [Nhanh] Created booking tracking fields: {created}")
    _fields_ready.update({"ts": time.time(), "ok": True})
    missing_phone_fields = [f for f in PHONE_FIELDS if f not in existing]
    return {"ok": True, "created": created, "missing_phone_fields": missing_phone_fields}


def _cell_text(value) -> str:
    """Giá trị cell Lark (str / list segment / dict) → chuỗi."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return str(value.get("text", value.get("value", "")))
    if isinstance(value, (list, tuple)):
        return " ".join(_cell_text(v) for v in value)
    return str(value)


def build_index(force: bool = False) -> dict:
    """
    Đọc toàn bộ bảng booking, lập index:
      by_phone: sđt → [ {record_id, koc, order_id, idx} ... ] theo thứ tự trong bảng
      by_order: nhanh order id → record_id
    Cache 5 phút.
    """
    with _lock:
        if not force and _index_cache["by_phone"] and time.time() - _index_cache["ts"] < INDEX_TTL_SEC:
            return _index_cache
        records = list_records(BOOKING_APP_TOKEN, BOOKING_TABLE_ID)
        by_phone: Dict[str, List[dict]] = {}
        by_order: Dict[str, str] = {}
        for idx, rec in enumerate(records):
            fields = rec.get("fields", {}) or {}
            rid = rec.get("record_id", "")
            phones = []
            for pf in PHONE_FIELDS:
                for p in nc.extract_phones(_cell_text(fields.get(pf))):
                    if p not in phones:
                        phones.append(p)
            entry = {
                "record_id": rid,
                "idx": idx,
                "koc": _cell_text(fields.get("ID KOC")),
                "order_id": _cell_text(fields.get(F_ORDER_ID)).strip(),
                "phones": phones,
            }
            for p in phones:
                by_phone.setdefault(p, []).append(entry)
            if entry["order_id"]:
                by_order[entry["order_id"]] = rid
        _index_cache.update({"ts": time.time(), "by_phone": by_phone, "by_order": by_order, "count": len(records)})
        print(f"📇 [Nhanh] Booking index: {len(records)} records, {len(by_phone)} phones, {len(by_order)} linked orders")
        return _index_cache


def find_booking_for_order(order: dict) -> Optional[dict]:
    """
    Tìm record booking cho 1 đơn:
      1. record đã gắn đúng mã đơn này
      2. record có SĐT trùng và CHƯA gắn mã đơn (lấy record mới nhất - cuối bảng)
      3. record có SĐT trùng (mới nhất), kể cả đã gắn đơn khác → ghi đè (KOC lên đơn lại)
    """
    index = build_index()
    oid = order.get("order_id", "")
    if oid and oid in index["by_order"]:
        return {"record_id": index["by_order"][oid], "reason": "order_id"}
    phone = order.get("mobile", "")
    if not phone:
        return None
    candidates = index["by_phone"].get(phone) or []
    if not candidates:
        return None
    free = [c for c in candidates if not c["order_id"]]
    pick = (free or candidates)[-1]
    return {"record_id": pick["record_id"], "koc": pick["koc"], "reason": "phone_free" if free else "phone_overwrite"}


# ╔════════════════════════════════════════════════════════════════╗
# ║                        XỬ LÝ ĐƠN HÀNG                         ║
# ╚════════════════════════════════════════════════════════════════╝

def _is_booking_order(order: dict) -> bool:
    """Chỉ quan tâm đơn giao hàng tận nhà (type 1) và đơn hoàn; bỏ đơn POS/chuyển kho."""
    t = order.get("type")
    try:
        t = int(t) if t not in (None, "") else None
    except (TypeError, ValueError):
        t = None
    return t in (None, 1, 14, 16)


def process_order(order: dict, source: str = "webhook") -> dict:
    """
    Khớp 1 đơn (đã normalize) với booking và ghi trạng thái về Lark.
    Trả về dict mô tả kết quả (matched / skipped / error).
    """
    oid = order.get("order_id", "")
    if not oid:
        return {"order_id": "", "result": "skipped", "reason": "no_order_id"}
    if not _is_booking_order(order):
        return {"order_id": oid, "result": "skipped", "reason": f"order_type_{order.get('type')}"}

    key = (order.get("status_code"), order.get("carrier_code"))
    if _last_written.get(oid) == key:
        return {"order_id": oid, "result": "skipped", "reason": "unchanged"}

    match = find_booking_for_order(order)
    if not match:
        return {"order_id": oid, "result": "unmatched", "mobile": order.get("mobile", "")}

    ensure_tracking_fields()
    fields = {
        F_ORDER_ID: oid,
        F_STATUS: order.get("status") or "",
        F_CARRIER_CODE: order.get("carrier_code") or "",
        F_CARRIER: order.get("carrier_name") or "",
        F_UPDATED: int(time.time() * 1000),
    }
    update_record(BOOKING_APP_TOKEN, BOOKING_TABLE_ID, match["record_id"], fields)
    _last_written[oid] = key
    # cập nhật index để lần sau khớp theo mã đơn
    _index_cache["by_order"][oid] = match["record_id"]
    for entries in _index_cache["by_phone"].values():
        for e in entries:
            if e["record_id"] == match["record_id"]:
                e["order_id"] = oid
    print(f"📦 [Nhanh:{source}] Order {oid} ({order.get('status')}) → booking {match.get('koc') or match['record_id']} [{match['reason']}]")
    return {"order_id": oid, "result": "updated", "record_id": match["record_id"],
            "koc": match.get("koc"), "status": order.get("status"), "reason": match["reason"]}


def handle_webhook_event(payload: dict) -> dict:
    """Xử lý 1 sự kiện webhook v3 của Nhanh."""
    event = payload.get("event", "")
    data = payload.get("data")
    if event in ("orderAdd", "orderUpdate", "orderPartialReturn"):
        if isinstance(data, dict):
            order = nc.normalize_order(data)
            if not order.get("mobile") and order.get("order_id"):
                # webhook thiếu SĐT → đọc lại đơn qua API
                try:
                    for raw in nc.list_orders_by_ids([order["order_id"]]):
                        order = nc.normalize_order(raw)
                        break
                except Exception as e:
                    print(f"⚠️ [Nhanh] Could not reload order {order['order_id']}: {e}")
            return process_order(order, source=event)
        return {"result": "skipped", "reason": "no_data"}
    if event == "orderDelete":
        ids = data if isinstance(data, list) else [data]
        results = []
        index = build_index()
        for oid in ids:
            rid = index["by_order"].get(str(oid))
            if rid:
                update_record(BOOKING_APP_TOKEN, BOOKING_TABLE_ID, rid,
                              {F_STATUS: "Đơn đã bị xóa", F_UPDATED: int(time.time() * 1000)})
                results.append({"order_id": str(oid), "result": "marked_deleted", "record_id": rid})
            else:
                results.append({"order_id": str(oid), "result": "unmatched"})
        return {"result": "ok", "items": results}
    return {"result": "ignored", "event": event}


def poll_recent_orders(minutes: int = 30) -> dict:
    """
    Quét các đơn cập nhật trong N phút gần nhất qua API (bù cho webhook rớt,
    và dùng để đồng bộ lần đầu). Tối đa 31 ngày theo giới hạn Nhanh.
    """
    minutes = max(1, min(int(minutes), 31 * 24 * 60))
    since = int(time.time()) - minutes * 60
    stats = {"scanned": 0, "updated": 0, "unmatched": 0, "skipped": 0, "errors": 0, "details": []}
    build_index(force=True)
    for raw in nc.list_orders_updated_since(since):
        stats["scanned"] += 1
        try:
            r = process_order(nc.normalize_order(raw), source="poll")
        except Exception as e:
            stats["errors"] += 1
            stats["details"].append({"error": str(e)[:200]})
            continue
        res = r.get("result")
        if res == "updated":
            stats["updated"] += 1
            stats["details"].append(r)
        elif res == "unmatched":
            stats["unmatched"] += 1
        else:
            stats["skipped"] += 1
    print(f"🔁 [Nhanh] Poll {minutes}m: {stats['scanned']} scanned, {stats['updated']} updated, "
          f"{stats['unmatched']} unmatched, {stats['skipped']} skipped, {stats['errors']} errors")
    stats["details"] = stats["details"][:50]
    return stats


def _record_deal_ts(fields: dict) -> Optional[int]:
    """Mốc thời gian booking (giây) từ 'Ngày deal' / 'Ngày gửi seeding' nếu có."""
    for name in ("Ngày deal", "Ngày gửi seeding", "Ngày deal gần nhất"):
        v = fields.get(name)
        ts = nc._ts(v) if not isinstance(v, (list, dict)) else None
        if ts:
            return ts
    return None


def _pick_order_for_booking(orders: List[dict], deal_ts: Optional[int]) -> Optional[dict]:
    """
    Chọn đơn đại diện cho booking trong danh sách đơn (mới nhất trước) của cùng SĐT:
    ưu tiên đơn giao hàng (type 1) tạo sau ngày deal - 7 ngày; không có thì đơn mới nhất.
    """
    cands = [nc.normalize_order(r) for r in orders]
    cands = [o for o in cands if _is_booking_order(o) and o.get("order_id")]
    if not cands:
        return None
    if deal_ts:
        after = [o for o in cands if (o.get("created_at") or 0) >= deal_ts - 7 * 86400]
        if after:
            return after[0]
    return cands[0]


def _write_order_to_record(record_id: str, order: dict, source: str, koc: str = "") -> dict:
    ensure_tracking_fields()
    oid = order["order_id"]
    update_record(BOOKING_APP_TOKEN, BOOKING_TABLE_ID, record_id, {
        F_ORDER_ID: oid,
        F_STATUS: order.get("status") or "",
        F_CARRIER_CODE: order.get("carrier_code") or "",
        F_CARRIER: order.get("carrier_name") or "",
        F_UPDATED: int(time.time() * 1000),
    })
    _last_written[oid] = (order.get("status_code"), order.get("carrier_code"))
    _index_cache["by_order"][oid] = record_id
    print(f"📦 [Nhanh:{source}] Order {oid} ({order.get('status')}) → booking {koc or record_id}")
    return {"record_id": record_id, "koc": koc, "order_id": oid, "status": order.get("status")}


def link_bookings_by_phone(only_unlinked: bool = True, limit: int = 200) -> dict:
    """
    Đi từ bảng booking: với mỗi record có SĐT (và chưa gắn mã đơn nếu only_unlinked),
    hỏi Nhanh các đơn của SĐT đó và gắn đơn phù hợp nhất.
    """
    records = list_records(BOOKING_APP_TOKEN, BOOKING_TABLE_ID)
    stats = {"records": len(records), "checked": 0, "linked": 0, "no_phone": 0,
             "no_order": 0, "already_linked": 0, "errors": 0, "details": []}
    seen_phone_cache: Dict[str, List[dict]] = {}
    for rec in records:
        if stats["checked"] >= limit:
            break
        fields = rec.get("fields", {}) or {}
        rid = rec.get("record_id", "")
        koc = _cell_text(fields.get("ID KOC"))
        current_oid = _cell_text(fields.get(F_ORDER_ID)).strip()
        if only_unlinked and current_oid:
            stats["already_linked"] += 1
            continue
        phones = []
        for pf in PHONE_FIELDS:
            for p in nc.extract_phones(_cell_text(fields.get(pf))):
                if p not in phones:
                    phones.append(p)
        if not phones:
            stats["no_phone"] += 1
            continue
        stats["checked"] += 1
        try:
            orders: List[dict] = []
            for p in phones:
                if p not in seen_phone_cache:
                    seen_phone_cache[p] = nc.list_orders_by_phone(p)
                orders.extend(seen_phone_cache[p])
            order = _pick_order_for_booking(orders, _record_deal_ts(fields))
            if not order:
                stats["no_order"] += 1
                stats["details"].append({"koc": koc, "phones": phones, "result": "no_order"})
                continue
            r = _write_order_to_record(rid, order, "link", koc)
            stats["linked"] += 1
            stats["details"].append({**r, "orders_found": len(orders), "result": "linked"})
        except Exception as e:
            stats["errors"] += 1
            stats["details"].append({"koc": koc, "phones": phones, "error": str(e)[:200]})
    print(f"🔗 [Nhanh] Link by phone: {stats['linked']} linked / {stats['checked']} checked "
          f"({stats['no_order']} no order, {stats['no_phone']} no phone, {stats['errors']} errors)")
    stats["details"] = stats["details"][:100]
    build_index(force=True)
    return stats


def refresh_linked_orders() -> dict:
    """Tra lại trạng thái các đơn đã gắn (theo mã đơn, 50 mã/lần) và cập nhật nếu đổi."""
    index = build_index(force=True)
    by_order = dict(index["by_order"])
    stats = {"linked": len(by_order), "updated": 0, "unchanged": 0, "missing": 0, "errors": 0, "details": []}
    ids = [int(x) for x in by_order.keys() if str(x).isdigit()]
    for i in range(0, len(ids), 50):
        chunk = ids[i:i + 50]
        try:
            found = set()
            for raw in nc.list_orders_by_ids(chunk):
                order = nc.normalize_order(raw)
                oid = order.get("order_id")
                found.add(oid)
                key = (order.get("status_code"), order.get("carrier_code"))
                if _last_written.get(oid) == key:
                    stats["unchanged"] += 1
                    continue
                r = _write_order_to_record(by_order[oid], order, "refresh")
                stats["updated"] += 1
                stats["details"].append(r)
            stats["missing"] += len([c for c in chunk if str(c) not in found])
        except Exception as e:
            stats["errors"] += 1
            stats["details"].append({"error": str(e)[:200]})
    print(f"🔄 [Nhanh] Refresh linked: {stats['updated']} updated, {stats['unchanged']} unchanged, "
          f"{stats['missing']} missing, {stats['errors']} errors")
    stats["details"] = stats["details"][:100]
    return stats


def scheduled_cycle() -> dict:
    """Chu kỳ 10 phút: gắn booking mới theo SĐT + làm mới đơn đã gắn."""
    out = {}
    try:
        out["link"] = link_bookings_by_phone(only_unlinked=True)
    except Exception as e:
        out["link"] = {"error": str(e)}
    try:
        out["refresh"] = refresh_linked_orders()
    except Exception as e:
        out["refresh"] = {"error": str(e)}
    return out


def reset_caches():
    _index_cache.update({"ts": 0.0, "by_phone": {}, "by_order": {}, "count": 0})
    _fields_ready.update({"ts": 0.0, "ok": False})
    _last_written.clear()
