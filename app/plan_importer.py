from __future__ import annotations

import re
import zipfile
from pathlib import Path
from typing import List
from xml.etree import ElementTree as ET


W_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def extract_text_from_docx(docx_path: Path) -> str:
    if not docx_path.exists():
        raise FileNotFoundError(f"DOCX file not found: {docx_path}")
    if docx_path.suffix.lower() != ".docx":
        raise ValueError("Only .docx files are supported")
    with zipfile.ZipFile(docx_path) as archive:
        try:
            xml_bytes = archive.read("word/document.xml")
        except KeyError as exc:
            raise ValueError("Invalid DOCX: missing word/document.xml") from exc
    root = ET.fromstring(xml_bytes)
    paragraphs: List[str] = []
    for p in root.findall(".//w:p", W_NS):
        text_parts = [node.text or "" for node in p.findall(".//w:t", W_NS)]
        line = "".join(text_parts).strip()
        if line:
            paragraphs.append(line)
    return "\n".join(paragraphs)


def fallback_markdown_from_raw_text(raw_text: str) -> str:
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return "# Imported Sprint\n\n## General\n- [ ] Define tasks\n"
    sprint_title = "Imported Sprint"
    for line in lines[:12]:
        if "sprint" in line.lower() or "plan" in line.lower():
            sprint_title = re.sub(r"\s+", " ", line).strip(" :-")
            break
    sections: dict[str, List[str]] = {"General": []}
    current = "General"
    heading_pattern = re.compile(r"^[A-Za-z][A-Za-z0-9 &/\-]{2,60}:?$")
    for raw_line in lines:
        line = re.sub(r"^\d+[\).\s-]+", "", raw_line).strip()
        line = re.sub(r"^[-*•]\s+", "", line).strip()
        if not line:
            continue
        maybe_heading = heading_pattern.match(line) is not None and len(line.split()) <= 8
        if maybe_heading and any(k in line.lower() for k in ["goal", "objective", "scope", "engineering", "product", "seo", "qa", "data", "design", "tech"]):
            current = line.rstrip(":")
            sections.setdefault(current, [])
            continue
        sections.setdefault(current, []).append(line.rstrip("."))
    out: List[str] = [f"# {sprint_title}", ""]
    for name, items in sections.items():
        uniq = []
        seen = set()
        for item in items:
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            uniq.append(item)
        if not uniq:
            continue
        out.append(f"## {name}")
        out.extend(f"- [ ] {item}" for item in uniq)
        out.append("")
    return "\n".join(out).rstrip() + "\n"


def extract_table_from_docx(docx_path: Path) -> List[dict]:
    """Extract the first table with 'Task' and 'Definition' columns as a list of row dicts."""
    if not docx_path.exists():
        raise FileNotFoundError(f"DOCX file not found: {docx_path}")
    if docx_path.suffix.lower() != ".docx":
        raise ValueError("Only .docx files are supported")
    with zipfile.ZipFile(docx_path) as archive:
        try:
            xml_bytes = archive.read("word/document.xml")
        except KeyError as exc:
            raise ValueError("Invalid DOCX: missing word/document.xml") from exc
    root = ET.fromstring(xml_bytes)
    for tbl in root.findall(".//w:tbl", W_NS):
        rows = tbl.findall(".//w:tr", W_NS)
        if len(rows) < 2:
            continue
        # Parse header row
        header_cells = rows[0].findall(".//w:tc", W_NS)
        headers = []
        for cell in header_cells:
            parts = [node.text or "" for node in cell.findall(".//w:t", W_NS)]
            headers.append("".join(parts).strip())
        lowered_headers = [h.lower() for h in headers]
        if "task" not in lowered_headers or "definition" not in lowered_headers:
            continue
        # Parse data rows
        result: List[dict] = []
        for row in rows[1:]:
            cells = row.findall(".//w:tc", W_NS)
            cell_texts = []
            for cell in cells:
                parts = [node.text or "" for node in cell.findall(".//w:t", W_NS)]
                cell_texts.append("".join(parts).strip())
            # Pad to match header length
            while len(cell_texts) < len(headers):
                cell_texts.append("")
            row_dict = {headers[i]: cell_texts[i] for i in range(len(headers))}
            if row_dict.get("Task", "").strip():
                result.append(row_dict)
        if result:
            return result
    return []


def table_rows_to_markdown(rows: List[dict], sprint_title: str = "Imported Sprint") -> str:
    """Convert extracted table rows into a markdown document with a table."""
    if not rows:
        return f"# {sprint_title}\n\n## General\n- [ ] Define tasks\n"
    # Use the column order from the first row's keys
    columns = list(rows[0].keys())
    # Build markdown table
    lines: List[str] = [f"# {sprint_title}", ""]
    header_line = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    lines.append(header_line)
    lines.append(separator)
    for row in rows:
        cells = [row.get(col, "").replace("|", "\\|") for col in columns]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    return "\n".join(lines)


def extract_sprint_name_from_markdown(markdown_text: str) -> str:
    for line in markdown_text.splitlines():
        s = line.strip()
        if s.startswith("# "):
            return s.lstrip("#").strip()
    return "Current Sprint"
