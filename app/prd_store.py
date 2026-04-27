"""
PRD (Product Requirements Document) store.

Handles PDF text extraction, chunking, persistence, and indexed retrieval.

Storage layout:
  workspace/prds/{prd_id}.pdf     — original PDF preserved for re-extraction
  workspace/prds/{prd_id}.json    — extracted text + chunks + metadata
  workspace/prd_index.json        — [{id, title, filename, uploaded_at, chunk_count, is_active}]
"""

from __future__ import annotations

import io
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .storage import PRD_DIR, PRD_INDEX_FILE, ensure_workspace


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ── PDF extraction ─────────────────────────────────────────────────────────────

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract plain text from PDF bytes using pypdf.
    Raises ValueError if the PDF has no extractable text (scanned / image-only)."""
    try:
        from pypdf import PdfReader  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError("pypdf is not installed. Run: pip install pypdf") from exc

    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            pages.append(text.strip())

    if not pages:
        raise ValueError(
            "No extractable text found in this PDF. "
            "If it is a scanned document, please re-export as a text PDF from the source app."
        )
    return "\n\n".join(pages)


# ── Chunking ───────────────────────────────────────────────────────────────────

def chunk_text(text: str, chunk_size: int = 2000, overlap: int = 200) -> List[str]:
    """Split text into overlapping chunks for embedding and synthesis."""
    text = text.strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]
    chunks: List[str] = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(text):
            break
        start = end - overlap
    return chunks


# ── Index helpers ──────────────────────────────────────────────────────────────

def _load_index() -> List[Dict[str, Any]]:
    ensure_workspace()
    if not PRD_INDEX_FILE.exists():
        return []
    try:
        data = json.loads(PRD_INDEX_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _save_index(index: List[Dict[str, Any]]) -> None:
    ensure_workspace()
    PRD_INDEX_FILE.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")


def _prd_json_path(prd_id: str):
    return PRD_DIR / f"{prd_id}.json"


def _prd_pdf_path(prd_id: str):
    return PRD_DIR / f"{prd_id}.pdf"


# ── CRUD ───────────────────────────────────────────────────────────────────────

def save_prd(title: str, pdf_bytes: bytes, filename: str) -> Dict[str, Any]:
    """Extract text, chunk, persist PDF + JSON, update index. Returns the PRD record."""
    ensure_workspace()

    text = extract_text_from_pdf(pdf_bytes)  # raises ValueError on scanned PDFs
    chunks = chunk_text(text)
    prd_id = uuid.uuid4().hex[:16]
    now = _now_iso()

    # Persist original PDF
    _prd_pdf_path(prd_id).write_bytes(pdf_bytes)

    record: Dict[str, Any] = {
        "id": prd_id,
        "title": title.strip(),
        "filename": filename,
        "uploaded_at": now,
        "chunk_count": len(chunks),
        "char_count": len(text),
        "is_active": True,
        "text": text,
        "chunks": chunks,
    }
    _prd_json_path(prd_id).write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    # Update index
    index = _load_index()
    index.insert(0, {
        "id": prd_id,
        "title": record["title"],
        "filename": filename,
        "uploaded_at": now,
        "chunk_count": len(chunks),
        "is_active": True,
    })
    _save_index(index)

    return record


def load_prd(prd_id: str) -> Optional[Dict[str, Any]]:
    path = _prd_json_path(prd_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def list_prds(active_only: bool = True) -> List[Dict[str, Any]]:
    """Return index entries (no full text / chunks) sorted newest-first."""
    index = _load_index()
    if active_only:
        index = [r for r in index if r.get("is_active", True)]
    return index


def deactivate_prd(prd_id: str) -> bool:
    """Soft-delete: set is_active=False in index and JSON record."""
    index = _load_index()
    found = False
    for entry in index:
        if entry.get("id") == prd_id:
            entry["is_active"] = False
            found = True
    if found:
        _save_index(index)

    record = load_prd(prd_id)
    if record:
        record["is_active"] = False
        _prd_json_path(prd_id).write_text(
            json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    return found


# ── KB / entity helpers ────────────────────────────────────────────────────────

def get_prd_rules(prd_id: str) -> List[Dict[str, Any]]:
    """Return KB facts (overall_kb_events) that were extracted from this PRD."""
    from .storage import OVERALL_KB_EVENTS_FILE  # noqa: PLC0415
    if not OVERALL_KB_EVENTS_FILE.exists():
        return []
    tag = f"prd:{prd_id}"
    rows: List[Dict[str, Any]] = []
    for line in OVERALL_KB_EVENTS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        source = str(row.get("source", ""))
        if tag in source and row.get("is_active", True):
            rows.append({
                "id": row.get("id", ""),
                "text": row.get("text", ""),
                "knowledge_type": row.get("knowledge_type", ""),
                "scope": row.get("scope", ""),
                "confidence": row.get("confidence", 0.0),
            })
    return rows


def get_prd_entity_graph(prd_id: str) -> Dict[str, Any]:
    """Return entities and relations whose source_events contain this PRD's tag."""
    from .kb_graph import load_entities, load_relations  # noqa: PLC0415
    tag = f"prd:{prd_id}"

    all_entities = load_entities(active_only=True)
    prd_entities = [
        e for e in all_entities
        if any(tag in ev for ev in (e.get("source_events") or []))
    ]
    entity_ids = {e["id"] for e in prd_entities}

    all_relations = load_relations(active_only=True)
    prd_relations = [
        r for r in all_relations
        if r.get("source_entity_id") in entity_ids
        or r.get("target_entity_id") in entity_ids
    ]

    # Only edges where BOTH endpoints are PRD-native entities
    prd_relations = [
        r for r in prd_relations
        if r.get("source_entity_id") in entity_ids
        and r.get("target_entity_id") in entity_ids
    ]

    nodes = [
        {
            "id": e["id"],
            "name": e["name"],
            "entity_type": e.get("entity_type", "topic"),
            "description": e.get("description", ""),
        }
        for e in prd_entities
    ]
    edges = [
        {
            "source": r["source_entity_id"],
            "target": r["target_entity_id"],
            "relation_type": r.get("relation_type", "related_to"),
            "label": r.get("label", ""),
            "confidence": r.get("confidence", 0.8),
        }
        for r in prd_relations
    ]

    return {"nodes": nodes, "edges": edges}
