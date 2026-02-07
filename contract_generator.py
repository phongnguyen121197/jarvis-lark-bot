"""
Contract Generator - Fill Word template with KOC data from Lark Base
Version 1.1.0

Fills Table 1 (Bên B info) in the contract template:
  Row 0: Tên Bên B
  Row 1: Địa chỉ
  Row 2: MST
  Row 3: SĐT
  Row 4: Gmail
  Row 5: CCCD + ngày cấp + nơi cấp
  Row 6: STK

v1.1: Thêm _normalize_formatting() - chuẩn hóa lề, spacing, indent.
"""

import os
import copy
import tempfile
from datetime import datetime
from typing import Dict, Optional
from docx import Document
from docx.shared import Pt, Cm, Emu
from docx.enum.text import WD_ALIGN_PARAGRAPH
import logging

logger = logging.getLogger(__name__)

# Template path - local fallback
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
TEMPLATE_HDKOC = os.path.join(TEMPLATE_DIR, "Mau_hop_dong_KOC.docx")

# Template name mapping: Lark "Template" field value → Drive search name
TEMPLATE_MAP = {
    "HDKOC": "HDKOC",
    # Add more: "HDDV": "HDDV", "HDHTM": "HDHTM"
}


# ╔════════════════════════════════════════════════════════════════╗
# ║              FORMAT CHUẨN HỢP ĐỒNG VIỆT NAM                   ║
# ╚════════════════════════════════════════════════════════════════╝

# Lề trang chuẩn hợp đồng VN (Nghị định 30/2020/NĐ-CP)
MARGIN_TOP = Cm(2)        # Lề trên: 2cm
MARGIN_BOTTOM = Cm(2)     # Lề dưới: 2cm
MARGIN_LEFT = Cm(2.5)     # Lề trái: 2.5cm (đóng gáy)
MARGIN_RIGHT = Cm(1.5)    # Lề phải: 1.5cm

# Spacing chuẩn
LINE_SPACING = 1.15                 # Khoảng cách dòng
SPACE_AFTER_NORMAL = Pt(3)          # Sau đoạn thường: 3pt
SPACE_AFTER_HEADING = Pt(6)         # Sau tiêu đề điều: 6pt
SPACE_AFTER_MAX = Pt(8)             # Tối đa cho phép: 8pt
SPACE_BEFORE_HEADING = Pt(6)        # Trước tiêu đề điều: 6pt

# Thresholds để phát hiện
BIG_SPACE_THRESHOLD = Pt(10)        # > 10pt coi là quá lớn
HEADING_KEYWORDS = ["ĐIỀU", "CỘNG HOÀ", "HỢP ĐỒNG", "ĐẠI DIỆN"]


def _is_heading_paragraph(text: str) -> bool:
    """Kiểm tra paragraph có phải tiêu đề/heading không."""
    stripped = text.strip()
    if not stripped:
        return False
    for kw in HEADING_KEYWORDS:
        if stripped.startswith(kw):
            return True
    return False


def _normalize_formatting(doc: Document):
    """
    Chuẩn hóa formatting toàn bộ document theo tiêu chuẩn hợp đồng VN.
    
    Fixes:
    1. Lề trang → chuẩn VN (2.5 / 1.5 / 2 / 2 cm)
    2. space_after quá lớn → cap lại 3-6pt
    3. Indent âm → 0
    4. Line spacing → 1.15
    5. Ngày tháng → căn phải, bỏ tab thừa
    """
    
    # === 1. Chuẩn hóa lề trang ===
    for section in doc.sections:
        section.top_margin = MARGIN_TOP
        section.bottom_margin = MARGIN_BOTTOM
        section.left_margin = MARGIN_LEFT
        section.right_margin = MARGIN_RIGHT
    
    # === 2. Chuẩn hóa paragraphs ===
    for i, para in enumerate(doc.paragraphs):
        pf = para.paragraph_format
        text = para.text.strip()
        
        # --- Fix indent âm ---
        if pf.first_line_indent is not None and pf.first_line_indent < 0:
            pf.first_line_indent = 0
        
        # --- Fix space_after quá lớn ---
        if pf.space_after is not None and pf.space_after > BIG_SPACE_THRESHOLD:
            if _is_heading_paragraph(text):
                pf.space_after = SPACE_AFTER_HEADING
            else:
                pf.space_after = SPACE_AFTER_NORMAL
        
        # --- Chuẩn hóa line spacing ---
        # Chỉ fix nếu gần 1.15 (template dùng 1.07-1.15)
        if pf.line_spacing is not None and isinstance(pf.line_spacing, float):
            if 1.0 <= pf.line_spacing <= 1.2:
                pf.line_spacing = LINE_SPACING
        
        # --- Fix dòng ngày tháng: bỏ tab, căn phải ---
        if "Hà Nội, ngày" in text:
            # Xóa tab thừa, giữ text gọn
            clean_date = text.replace("\t", "").strip()
            if para.runs:
                for run in para.runs:
                    run.text = ""
                para.runs[0].text = clean_date
            pf.alignment = WD_ALIGN_PARAGRAPH.RIGHT
            pf.first_line_indent = 0
            pf.space_after = SPACE_AFTER_HEADING
        
        # --- Fix dòng "(Về việc...)" ---
        if text.startswith("(Về việc"):
            pf.space_after = SPACE_AFTER_HEADING
        
        # --- Fix tiêu đề ĐIỀU: thêm space_before ---
        if text.startswith("ĐIỀU"):
            pf.space_before = SPACE_BEFORE_HEADING
            pf.space_after = SPACE_AFTER_HEADING
    
    # === 3. Chuẩn hóa bảng (table cells) ===
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for para in cell.paragraphs:
                    pf = para.paragraph_format
                    # Fix indent âm trong bảng
                    if pf.first_line_indent is not None and pf.first_line_indent < 0:
                        pf.first_line_indent = 0
                    # Fix space_after trong bảng
                    if pf.space_after is not None and pf.space_after > BIG_SPACE_THRESHOLD:
                        pf.space_after = Pt(2)
                    # Chuẩn hóa line spacing trong bảng
                    if pf.line_spacing is not None and isinstance(pf.line_spacing, float):
                        if 1.0 <= pf.line_spacing <= 1.2:
                            pf.line_spacing = LINE_SPACING


def _set_cell_text(cell, text: str, bold: bool = False, font_size: float = None):
    """Set text in a table cell, preserving basic formatting."""
    # Clear existing paragraphs
    for para in cell.paragraphs:
        for run in para.runs:
            run.text = ""
    
    # Set text in first paragraph's first run, or create new run
    if cell.paragraphs and cell.paragraphs[0].runs:
        run = cell.paragraphs[0].runs[0]
        run.text = text
        run.bold = bold
        if font_size:
            run.font.size = Pt(font_size)
    else:
        run = cell.paragraphs[0].add_run(text)
        run.bold = bold
        if font_size:
            run.font.size = Pt(font_size)


def _format_currency_vn(amount) -> str:
    """
    Format number as Vietnamese currency: 11700000 → '11.700.000'
    Handles: int, float, string with commas/dots.
    """
    if not amount and amount != 0:
        return ""
    try:
        # Clean string input
        s = str(amount).replace(",", "").replace(".", "").strip()
        num = int(float(s)) if s else 0
        if num == 0:
            return "0"
        return f"{num:,}".replace(",", ".")
    except (ValueError, TypeError):
        return str(amount)


def _format_date_field(value) -> str:
    """
    Convert Lark date field to dd/mm/yyyy string.
    Handles: timestamp in ms (e.g. 1738886400000), date string, or dd/mm/yyyy.
    """
    if not value:
        return ""
    s = str(value).strip()
    
    # Already in dd/mm/yyyy format
    if "/" in s and len(s) <= 10:
        return s
    
    # Timestamp in milliseconds
    try:
        ts = float(s)
        if ts > 1e12:  # ms
            ts = ts / 1000
        from datetime import datetime as _dt
        dt = _dt.fromtimestamp(ts)
        return dt.strftime("%d/%m/%Y")
    except (ValueError, TypeError, OSError):
        pass
    
    return s


def _format_percent(value) -> str:
    """
    Convert percentage value to display string.
    Handles: 0.05 → '5', '5%' → '5', '5' → '5', 0.1 → '10'
    """
    if not value and value != 0:
        return ""
    s = str(value).replace("%", "").strip()
    try:
        num = float(s)
        # If value < 1, it's decimal format (0.05 = 5%)
        if num < 1:
            num = num * 100
        # Remove trailing .0
        result = f"{num:g}"
        return result
    except (ValueError, TypeError):
        return s


def _insert_cccd_image(doc: Document, para_index: int, image_path: str):
    """
    Insert CCCD image after the title paragraph (e.g. 'Mặt trước CCCD').
    Converts image to JPEG via Pillow first to handle formats docx can't read.
    """
    try:
        from docx.shared import Cm
        from PIL import Image as PILImage
        import io, traceback
        
        # Convert image to JPEG bytes via Pillow (handles HEIC, WebP, corrupted headers)
        img = PILImage.open(image_path)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90)
        buf.seek(0)
        
        print(f"📐 CCCD image {image_path}: {img.size[0]}x{img.size[1]}, converted to JPEG ({buf.getbuffer().nbytes} bytes)")
        
        # Find first empty paragraph after the title to insert image
        target_para = None
        for idx in range(para_index + 1, min(para_index + 4, len(doc.paragraphs))):
            p = doc.paragraphs[idx]
            if not p.text.strip():
                target_para = p
                break
        
        if target_para is None:
            target_para = doc.paragraphs[para_index]
        
        # Clear paragraph and add image
        for run in target_para.runs:
            run.text = ""
        
        run = target_para.add_run()
        run.add_picture(buf, width=Cm(14))
        print(f"✅ Inserted CCCD image at para {para_index}")
    except Exception as e:
        print(f"⚠️ Failed to insert CCCD image at para {para_index}: {type(e).__name__}: {e}")
        traceback.print_exc()


def _number_to_vietnamese_words(number) -> str:
    """
    Chuyển số thành chữ tiếng Việt.
    Ví dụ: 10530000 → 'Mười triệu năm trăm ba mươi nghìn đồng'
    """
    try:
        n = int(float(str(number).replace(",", "").replace(".", "").strip()))
    except (ValueError, TypeError):
        return ""
    
    if n == 0:
        return "Không đồng"
    
    ones = ["", "một", "hai", "ba", "bốn", "năm", "sáu", "bảy", "tám", "chín"]
    
    def _read_group_3(num):
        """Đọc nhóm 3 chữ số."""
        if num == 0:
            return ""
        
        h = num // 100
        t = (num % 100) // 10
        u = num % 10
        
        parts = []
        
        if h > 0:
            parts.append(f"{ones[h]} trăm")
            if t == 0 and u > 0:
                parts.append("lẻ")
        
        if t > 1:
            parts.append(f"{ones[t]} mươi")
            if u == 1:
                parts.append("mốt")
            elif u == 4:
                parts.append("tư")
            elif u == 5:
                parts.append("lăm")
            elif u > 0:
                parts.append(ones[u])
        elif t == 1:
            parts.append("mười")
            if u == 5:
                parts.append("lăm")
            elif u > 0:
                parts.append(ones[u])
        elif t == 0 and h > 0 and u > 0:
            parts.append(ones[u])
        elif t == 0 and h == 0 and u > 0:
            parts.append(ones[u])
        
        return " ".join(parts)
    
    units = ["", "nghìn", "triệu", "tỷ"]
    
    groups = []
    temp = n
    while temp > 0:
        groups.append(temp % 1000)
        temp //= 1000
    
    result_parts = []
    for i in range(len(groups) - 1, -1, -1):
        if groups[i] > 0:
            text = _read_group_3(groups[i])
            if text:
                if i < len(units):
                    result_parts.append(f"{text} {units[i]}".strip())
                else:
                    result_parts.append(text)
        elif i > 0 and any(groups[j] > 0 for j in range(i)):
            # Nhóm = 0 nhưng còn nhóm sau có giá trị → thêm "không trăm"
            pass
    
    result = " ".join(result_parts).strip()
    # Capitalize first letter
    result = result[0].upper() + result[1:] if result else ""
    return f"{result} đồng."


def _format_date_vn(date_value) -> str:
    """
    Convert date value to Vietnamese format dd/MM/yyyy.
    Handles: timestamp (ms), ISO string, dd/MM/yyyy string, datetime object.
    """
    if not date_value:
        return ""
    
    # Lark Date field returns timestamp in milliseconds
    if isinstance(date_value, (int, float)):
        dt = datetime.fromtimestamp(date_value / 1000)
        return dt.strftime("%d/%m/%Y")
    
    if isinstance(date_value, datetime):
        return date_value.strftime("%d/%m/%Y")
    
    if isinstance(date_value, str):
        # Already in dd/MM/yyyy format
        if len(date_value) == 10 and date_value[2] == "/" and date_value[5] == "/":
            return date_value
        # ISO format
        try:
            dt = datetime.fromisoformat(date_value.replace("Z", "+00:00"))
            return dt.strftime("%d/%m/%Y")
        except ValueError:
            pass
        # Try yyyy-MM-dd
        try:
            dt = datetime.strptime(date_value, "%Y-%m-%d")
            return dt.strftime("%d/%m/%Y")
        except ValueError:
            pass
    
    return str(date_value)


def _get_current_date_vn() -> str:
    """Get current date in Vietnamese format: 'ngày DD tháng MM năm YYYY'"""
    now = datetime.now()
    return f"ngày {now.day:02d} tháng {now.month:02d} năm {now.year}"


def generate_contract(data: Dict, template_path: str = None, output_path: str = None) -> str:
    """
    Generate a KOC contract by filling the Word template.
    
    Args:
        data: Dictionary with contract fields from Lark Base:
            - ho_ten: Họ và Tên Bên B
            - dia_chi: Địa chỉ Bên B
            - mst: MST Bên B
            - sdt: SDT Bên B
            - cccd: CCCD Bên B
            - cccd_ngay_cap: CCCD Ngày Cấp (timestamp ms or date string)
            - cccd_noi_cap: CCCD Nơi Cấp
            - gmail: Gmail Bên B
            - stk: STK bên B
            - id_koc: ID KOC (for filename)
        template_path: Path to Word template (default: templates/Mau_hop_dong_KOC.docx)
        output_path: Path for output file (default: temp file)
    
    Returns:
        Path to generated .docx file
    """
    template = template_path
    if not template:
        # Try to get template from Drive (with cache)
        template_key = data.get("template", "HDKOC")
        drive_name = TEMPLATE_MAP.get(template_key, template_key)
        try:
            from google_drive_client import get_template_path
            template = get_template_path(drive_name, fallback_path=TEMPLATE_HDKOC)
        except Exception as e:
            print(f"⚠️ Drive template fetch failed, using local: {e}")
            template = TEMPLATE_HDKOC
    
    if not os.path.exists(template):
        raise FileNotFoundError(f"Template not found: {template}")
    
    doc = Document(template)
    
    # === 1. Update contract date (Paragraph 3) ===
    date_para = doc.paragraphs[3]
    date_text = f"\t\t\t\t\t\t\t Hà Nội, {_get_current_date_vn()}"
    if date_para.runs:
        # Clear all runs and set new text
        for run in date_para.runs:
            run.text = ""
        date_para.runs[0].text = date_text
    else:
        date_para.text = date_text
    
    # === 2. Fill Table 1 - Bên B Information ===
    table_b = doc.tables[1]
    
    ho_ten = data.get("ho_ten", "")
    dia_chi = data.get("dia_chi", "")
    mst = data.get("mst", "")
    sdt = data.get("sdt", "")
    gmail = data.get("gmail", "")
    cccd = data.get("cccd", "")
    id_koc = data.get("id_koc", "")
    cccd_ngay_cap = _format_date_vn(data.get("cccd_ngay_cap", ""))
    cccd_noi_cap = data.get("cccd_noi_cap", "")
    stk = data.get("stk", "")
    
    # Row 0: BÊN B: [Tên]
    # Merged cells - set on first cell, others will follow
    row0_cell = table_b.rows[0].cells[0]
    _set_cell_text(row0_cell, f"BÊN B: {ho_ten}", bold=True)
    
    # Row 1: Địa chỉ: [value in cell 1]
    if dia_chi:
        _set_cell_text(table_b.rows[1].cells[1], dia_chi)
    
    # Row 2: MST: [value in cell 1]
    if mst:
        _set_cell_text(table_b.rows[2].cells[1], mst)
    
    # Row 3: SĐT: [value in cell 1]
    if sdt:
        _set_cell_text(table_b.rows[3].cells[1], str(sdt))
    
    # Row 4: Gmail: [value in cell 1]
    if gmail:
        _set_cell_text(table_b.rows[4].cells[1], gmail)
    
    # Row 5: CCCD: [number] | cấp ngày [date] tại [place]
    if cccd:
        _set_cell_text(table_b.rows[5].cells[0], f"CCCD: {cccd}", bold=True)
    
    cccd_detail = f"cấp ngày {cccd_ngay_cap} tại {cccd_noi_cap}"
    _set_cell_text(table_b.rows[5].cells[1], cccd_detail)
    
    # Row 6: STK: [value in cell 1]
    if stk:
        _set_cell_text(table_b.rows[6].cells[1], str(stk))
    
    # === 3. Fill Paragraph-level data (Điều 2, 3) ===
    thuong_hieu = data.get("thuong_hieu", "")
    ngay_du_kien_air = data.get("ngay_du_kien_air", "")
    hoa_hong_tu_nhien = data.get("hoa_hong_tu_nhien", "")
    hoa_hong_chay_ads = data.get("hoa_hong_chay_ads", "")
    
    # Para 24: "Kallé Feum" → Thương hiệu
    if thuong_hieu:
        p24 = doc.paragraphs[24]
        if len(p24.runs) > 18:
            p24.runs[16].text = str(thuong_hieu)  # Replace "Kallé"
            p24.runs[17].text = ""                 # Clear space
            p24.runs[18].text = ""                 # Clear "Feum"
    
    # Para 25: "08/02/2026" → Ngày dự kiến air, "charminglae" → ID KOC
    if ngay_du_kien_air or id_koc:
        p25 = doc.paragraphs[25]
        if len(p25.runs) > 30:
            if ngay_du_kien_air:
                # Convert timestamp (ms) to date string if needed
                date_str = _format_date_field(ngay_du_kien_air)
                p25.runs[12].text = date_str
            if id_koc and id_koc != "N/A":
                p25.runs[30].text = str(id_koc)
    
    # Para 29: Commission "5% tự nhiên + 5% chạy ads"
    if hoa_hong_tu_nhien or hoa_hong_chay_ads:
        p29 = doc.paragraphs[29]
        if len(p29.runs) > 7:
            if hoa_hong_tu_nhien:
                val = _format_percent(hoa_hong_tu_nhien)
                p29.runs[3].text = f"{val}% "
            if hoa_hong_chay_ads:
                val = _format_percent(hoa_hong_chay_ads)
                p29.runs[7].text = f" {val}% "
    
    # Para 36: "charminglae" in payment section → ID KOC
    if id_koc and id_koc != "N/A":
        p36 = doc.paragraphs[36]
        if len(p36.runs) > 15:
            p36.runs[15].text = str(id_koc)
    
    # === 4. Insert CCCD images ===
    cccd_truoc_path = data.get("cccd_truoc_path", "")
    cccd_sau_path = data.get("cccd_sau_path", "")
    
    if cccd_truoc_path and os.path.exists(cccd_truoc_path):
        fsize = os.path.getsize(cccd_truoc_path)
        if fsize > 100:  # skip if file too small (corrupted)
            _insert_cccd_image(doc, 112, cccd_truoc_path)
        else:
            print(f"⚠️ CCCD front file too small: {fsize} bytes")
    
    if cccd_sau_path and os.path.exists(cccd_sau_path):
        fsize = os.path.getsize(cccd_sau_path)
        if fsize > 100:
            _insert_cccd_image(doc, 117, cccd_sau_path)
        else:
            print(f"⚠️ CCCD back file too small: {fsize} bytes")
    
    # === 3. Fill Table 2 - Payment Details (Phí dịch vụ) ===
    table_pay = doc.tables[2]
    
    thanh_tien = data.get("thanh_tien", "")
    chi_phi = data.get("chi_phi", "")
    so_luong_clip = data.get("so_luong_clip", "")
    thue_tncn = data.get("thue_tncn", "")
    tong_gia_tri_sau_thue = data.get("tong_gia_tri_sau_thue", "")
    
    # Row 1: Chi phí một clip → Số lượng, Đơn giá(Chi phí), Tổng cộng(Thành tiền)
    if so_luong_clip:
        _set_cell_text(table_pay.rows[1].cells[2], str(int(float(str(so_luong_clip)))), bold=True, font_size=13)
    if chi_phi:
        formatted_cp = _format_currency_vn(chi_phi)
        _set_cell_text(table_pay.rows[1].cells[3], formatted_cp, bold=True, font_size=13)
    if thanh_tien:
        formatted_tt = _format_currency_vn(thanh_tien)
        _set_cell_text(table_pay.rows[1].cells[4], formatted_tt, bold=True, font_size=13)
    
    # Row 2: TNCN (10%) → Đơn giá, Tổng cộng
    if thue_tncn:
        formatted_tncn = _format_currency_vn(thue_tncn)
        _set_cell_text(table_pay.rows[2].cells[3], formatted_tncn, bold=True, font_size=13)
        _set_cell_text(table_pay.rows[2].cells[4], formatted_tncn, bold=True, font_size=13)
    
    # Row 3: Tổng giá trị sau thuế → Tổng cộng
    if tong_gia_tri_sau_thue:
        formatted_tgst = _format_currency_vn(tong_gia_tri_sau_thue)
        _set_cell_text(table_pay.rows[3].cells[4], formatted_tgst, bold=True, font_size=13)
    
    # Row 4: Bằng chữ → convert to Vietnamese words
    if tong_gia_tri_sau_thue:
        bang_chu = f"Bằng chữ: {_number_to_vietnamese_words(tong_gia_tri_sau_thue)}"
        _set_cell_text(table_pay.rows[4].cells[0], bang_chu, bold=True)
        # Set italic on the run
        if table_pay.rows[4].cells[0].paragraphs[0].runs:
            table_pay.rows[4].cells[0].paragraphs[0].runs[0].italic = True
    
    # === 3. Chuẩn hóa formatting toàn bộ ===
    _normalize_formatting(doc)
    
    # === 4. Save output ===
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        doc.save(output_path)
        return output_path
    
    # Generate temp file with meaningful name
    id_koc = data.get("id_koc", "unknown")
    safe_name = "".join(c for c in ho_ten if c.isalnum() or c in " _-").strip().replace(" ", "_")
    filename = f"HD_KOC_{id_koc}_{safe_name}.docx"
    
    tmp_dir = tempfile.mkdtemp(prefix="contract_")
    output = os.path.join(tmp_dir, filename)
    doc.save(output)
    
    logger.info(f"✅ Contract generated: {output}")
    return output


def parse_lark_record_to_contract_data(fields: Dict) -> Dict:
    """
    Convert Lark Base record fields to contract_generator data format.
    
    Lark Base field names (Vietnamese with spaces) → internal keys.
    """
    # Handle phone field - Lark may return as object or string
    sdt = fields.get("SDT Bên B", "")
    if isinstance(sdt, dict):
        sdt = sdt.get("value", sdt.get("text", ""))
    
    return {
        "id_koc": fields.get("ID KOC", ""),
        "ho_ten": fields.get("Họ và Tên Bên B", ""),
        "dia_chi": fields.get("Địa chỉ Bên B", ""),
        "mst": fields.get("MST Bên B", ""),
        "sdt": str(sdt),
        "cccd": fields.get("CCCD Bên B", ""),
        "cccd_ngay_cap": fields.get("CCCD Ngày Cấp", ""),
        "cccd_noi_cap": fields.get("CCCD Nơi Cấp", ""),
        "gmail": fields.get("Gmail Bên B", ""),
        "stk": fields.get("STK bên B", ""),
        # Payment fields (Table 2)
        "chi_phi": fields.get("Chi phí", ""),
        "thanh_tien": fields.get("Thành Tiền", fields.get("Thành Tiên", "")),
        "so_luong_clip": fields.get("Số lượng clip", ""),
        "thue_tncn": fields.get("Thuế TNCN", ""),
        "tong_gia_tri_sau_thue": fields.get("Tổng giá trị sau thuế", ""),
        # Paragraph fields (Điều 2, 3)
        "thuong_hieu": fields.get("Thương hiệu", ""),
        "ngay_du_kien_air": fields.get("Ngày dự kiến air", ""),
        "hoa_hong_tu_nhien": fields.get("% Hoa hồng tự nhiên", fields.get("Hoa hồng tự nhiên", "")),
        "hoa_hong_chay_ads": fields.get("% Hoa hồng chạy ads", fields.get("Hoa hồng chạy ads", "")),
        # CCCD image paths (populated by main.py after downloading from Lark)
        "cccd_truoc_path": fields.get("cccd_truoc_path", ""),
        "cccd_sau_path": fields.get("cccd_sau_path", ""),
        # Template selection
        "template": fields.get("Template", "HDKOC"),
    }
