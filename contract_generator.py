"""
Contract Generator - Fill Word template with KOC data from Lark Base
Version 1.0.0

Fills Table 1 (Bên B info) in the contract template:
  Row 0: Tên Bên B
  Row 1: Địa chỉ
  Row 2: MST
  Row 3: SĐT
  Row 4: Gmail
  Row 5: CCCD + ngày cấp + nơi cấp
  Row 6: STK
"""

import os
import copy
import tempfile
from datetime import datetime
from typing import Dict, Optional
from docx import Document
from docx.shared import Pt
import logging

logger = logging.getLogger(__name__)

# Template path - stored in repo
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
TEMPLATE_HDKOC = os.path.join(TEMPLATE_DIR, "Mau_hop_dong_KOC.docx")


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
    template = template_path or TEMPLATE_HDKOC
    
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
    
    # === 3. Save output ===
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
    }
