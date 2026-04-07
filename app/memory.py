from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None

from .storage import (
    OVERALL_KB_ARCHIVE_DIR,
    OVERALL_KB_EVENTS_FILE,
    OVERALL_KB_VECTORS_FILE,
    ensure_workspace,
)


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _norm_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _event_id(text: str, timestamp: str, knowledge_type: str, scope: str) -> str:
    payload = f"{timestamp}|{knowledge_type}|{scope}|{_norm_text(text)}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_events_from_file(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    rows.sort(key=lambda x: str(x.get("timestamp", "")))
    return rows


def _write_events_to_file(path: Path, rows: List[Dict[str, Any]]) -> None:
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows if isinstance(row, dict))
    if payload:
        payload += "\n"
    path.write_text(payload, encoding="utf-8")


def _archive_file_for(timestamp: str) -> Path:
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        dt = datetime.utcnow()
    return OVERALL_KB_ARCHIVE_DIR / f"{dt.strftime('%Y-%m')}.jsonl"


def _append_to_archive(event: Dict[str, Any]) -> None:
    archive_file = _archive_file_for(str(event.get("timestamp", "")))
    with archive_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")


def _archive_old_events(rows: List[Dict[str, Any]], retention_days: int = 60) -> List[Dict[str, Any]]:
    keep: List[Dict[str, Any]] = []
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    for row in rows:
        ts = str(row.get("timestamp", ""))
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            keep.append(row)
            continue
        if dt < cutoff:
            archived = dict(row)
            archived["archived"] = True
            _append_to_archive(archived)
        else:
            keep.append(row)
    return keep


def _all_archive_events() -> List[Dict[str, Any]]:
    ensure_workspace()
    rows: List[Dict[str, Any]] = []
    for path in sorted(OVERALL_KB_ARCHIVE_DIR.glob("*.jsonl")):
        rows.extend(_load_events_from_file(path))
    rows.sort(key=lambda x: str(x.get("timestamp", "")))
    return rows


def load_overall_kb_events(include_archived: bool = True) -> List[Dict[str, Any]]:
    ensure_workspace()
    active = _load_events_from_file(OVERALL_KB_EVENTS_FILE)
    if not include_archived:
        return active
    combined = active + _all_archive_events()
    combined.sort(key=lambda x: str(x.get("timestamp", "")))
    return combined


def append_overall_kb_event(
    text: str,
    knowledge_type: str,
    confidence: float,
    scope: str,
    source_message_id: str = "",
    source: str = "chat",
    timestamp: Optional[str] = None,
    correction_window_hours: int = 10,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_workspace()
    ts = timestamp or _now_iso()
    normalized = _norm_text(text)
    if not normalized:
        return {}
    eid = _event_id(text=normalized, timestamp=ts, knowledge_type=knowledge_type, scope=scope)
    rows = _load_events_from_file(OVERALL_KB_EVENTS_FILE)
    existing_ids = {str(r.get("id", "")) for r in rows}
    if eid in existing_ids:
        return {}

    supersedes_event_id = ""
    version = 1
    try:
        now_dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        now_dt = datetime.utcnow()

    # Supersede near-duplicate active facts in correction window.
    for row in rows:
        if not bool(row.get("is_active", True)):
            continue
        same_type = str(row.get("knowledge_type", "")) == knowledge_type
        same_scope = str(row.get("scope", "")) == scope
        if not (same_type and same_scope):
            continue
        if _norm_text(str(row.get("text", ""))) != normalized:
            continue
        old_ts = str(row.get("timestamp", ""))
        try:
            old_dt = datetime.fromisoformat(old_ts.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            continue
        age_h = (now_dt - old_dt).total_seconds() / 3600.0
        if 0 <= age_h <= correction_window_hours:
            row["is_active"] = False
            row["valid_to"] = ts
            row["superseded_by"] = eid
            if not supersedes_event_id:
                supersedes_event_id = str(row.get("id", ""))
            version = max(version, int(row.get("version", 1)) + 1)

    row = {
        "id": eid,
        "timestamp": ts,
        "source": source,
        "event_type": "overall_knowledge",
        "text": text.strip(),
        "knowledge_type": knowledge_type,
        "confidence": max(0.0, min(1.0, float(confidence))),
        "scope": scope,
        "source_message_id": source_message_id,
        "is_active": True,
        "supersedes_event_id": supersedes_event_id,
        "version": version,
        "valid_from": ts,
        "valid_to": None,
        "archived": False,
        "metadata": metadata or {},
    }
    rows.append(row)
    rows = _archive_old_events(rows)
    _write_events_to_file(OVERALL_KB_EVENTS_FILE, rows)
    return row


def _openai_client() -> Optional[Any]:
    if load_dotenv is not None:
        load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key or OpenAI is None:
        return None
    try:
        return OpenAI(api_key=api_key)
    except Exception:
        return None


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _embed_texts(texts: List[str], model: str = "text-embedding-3-small") -> Optional[List[List[float]]]:
    client = _openai_client()
    if client is None or not texts:
        return None
    try:
        resp = client.embeddings.create(model=model, input=texts)
        return [item.embedding for item in resp.data]
    except Exception:
        return None


def _load_vectors() -> Dict[str, Any]:
    return _load_json(OVERALL_KB_VECTORS_FILE, {"model": "text-embedding-3-small", "vectors": {}})


def _save_vectors(payload: Dict[str, Any]) -> None:
    _save_json(OVERALL_KB_VECTORS_FILE, payload)


def rebuild_overall_kb_vectors(model: str = "text-embedding-3-small") -> int:
    events = load_overall_kb_events(include_archived=True)
    texts = [str(e.get("text", "")) for e in events]
    ids = [str(e.get("id", "")) for e in events]
    embeds = _embed_texts(texts, model=model)
    if not embeds:
        return 0
    vectors = {eid: vec for eid, vec in zip(ids, embeds) if eid}
    _save_vectors({"model": model, "vectors": vectors, "rebuilt_at": _now_iso()})
    return len(vectors)


def _ensure_vectors(events: List[Dict[str, Any]], model: str = "text-embedding-3-small") -> Dict[str, List[float]]:
    store = _load_vectors()
    if store.get("model") != model:
        store = {"model": model, "vectors": {}}
    vectors: Dict[str, List[float]] = store.get("vectors", {})
    missing = [e for e in events if str(e.get("id", "")) not in vectors]
    if missing:
        embeds = _embed_texts([str(e.get("text", "")) for e in missing], model=model)
        if embeds:
            for event, vec in zip(missing, embeds):
                eid = str(event.get("id", ""))
                if eid:
                    vectors[eid] = vec
            store["vectors"] = vectors
            _save_vectors(store)
    return vectors


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]{3,}", (text or "").lower())


def _kb_text_matches_query(text: str, query: str) -> bool:
    t_norm = _norm_text(text)
    q_norm = _norm_text(query)
    if not t_norm or not q_norm:
        return False
    if q_norm in t_norm or t_norm in q_norm:
        return True
    t_tokens = set(_tokenize(t_norm))
    q_tokens = set(_tokenize(q_norm))
    overlap = t_tokens.intersection(q_tokens)
    return len(overlap) >= 2


def search_overall_kb_events(query: str, active_only: bool = True, limit: int = 10) -> List[Dict[str, Any]]:
    rows = _load_events_from_file(OVERALL_KB_EVENTS_FILE)
    if active_only:
        rows = [r for r in rows if bool(r.get("is_active", True))]
    matches = [r for r in rows if _kb_text_matches_query(str(r.get("text", "")), query)]
    matches.sort(key=lambda r: str(r.get("timestamp", "")), reverse=True)
    return matches[: max(1, int(limit))]


def deactivate_overall_kb_events(query: str, reason: str = "manual_delete", limit: int = 50) -> Dict[str, Any]:
    rows = _load_events_from_file(OVERALL_KB_EVENTS_FILE)
    now = _now_iso()
    changed_ids: List[str] = []
    changed_rows: List[Dict[str, Any]] = []
    for row in rows:
        if len(changed_ids) >= max(1, int(limit)):
            break
        if not bool(row.get("is_active", True)):
            continue
        if not _kb_text_matches_query(str(row.get("text", "")), query):
            continue
        row["is_active"] = False
        row["valid_to"] = now
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        metadata["deactivated_reason"] = reason
        metadata["deactivated_at"] = now
        row["metadata"] = metadata
        changed_ids.append(str(row.get("id", "")))
        changed_rows.append(row)
    if changed_ids:
        _write_events_to_file(OVERALL_KB_EVENTS_FILE, rows)
    return {
        "query": query,
        "deactivated_count": len(changed_ids),
        "deactivated_ids": changed_ids,
        "deactivated_events": changed_rows,
    }


def reactivate_overall_kb_events_by_reason(reason: str = "chat_delete_command") -> int:
    rows = _load_events_from_file(OVERALL_KB_EVENTS_FILE)
    changed = 0
    for row in rows:
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            continue
        if bool(row.get("is_active", True)):
            continue
        if str(metadata.get("deactivated_reason", "")).strip().lower() != reason.strip().lower():
            continue
        row["is_active"] = True
        row["valid_to"] = None
        metadata.pop("deactivated_reason", None)
        metadata.pop("deactivated_at", None)
        row["metadata"] = metadata
        changed += 1
    if changed:
        _write_events_to_file(OVERALL_KB_EVENTS_FILE, rows)
    return changed


def overwrite_overall_kb_events(
    old_query: str,
    new_text: str,
    source_message_id: str = "",
    source: str = "chat_overwrite_command",
    confidence: float = 0.95,
    limit: int = 50,
) -> Dict[str, Any]:
    old_query = (old_query or "").strip()
    new_text = (new_text or "").strip()
    if not old_query or not new_text:
        return {"overwritten_count": 0, "new_event": {}, "overwritten_events": []}
    rows = _load_events_from_file(OVERALL_KB_EVENTS_FILE)
    now = _now_iso()
    changed_rows: List[Dict[str, Any]] = []
    changed_ids: List[str] = []
    for row in rows:
        if len(changed_ids) >= max(1, int(limit)):
            break
        if not bool(row.get("is_active", True)):
            continue
        if not _kb_text_matches_query(str(row.get("text", "")), old_query):
            continue
        row["is_active"] = False
        row["valid_to"] = now
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        metadata["deactivated_reason"] = "chat_overwrite_command"
        metadata["deactivated_at"] = now
        row["metadata"] = metadata
        changed_rows.append(dict(row))
        changed_ids.append(str(row.get("id", "")))
    if changed_ids:
        _write_events_to_file(OVERALL_KB_EVENTS_FILE, rows)

    base = changed_rows[0] if changed_rows else {}
    knowledge_type = str(base.get("knowledge_type", "")).strip().lower() or "process_rule"
    scope = str(base.get("scope", "")).strip().lower() or "project"
    new_event = append_overall_kb_event(
        text=new_text,
        knowledge_type=knowledge_type,
        scope=scope,
        confidence=confidence,
        source_message_id=source_message_id,
        source=source,
        metadata={"overwrites_query": old_query, "overwrites_event_ids": changed_ids},
    )
    return {
        "overwritten_count": len(changed_ids),
        "overwritten_ids": changed_ids,
        "overwritten_events": changed_rows,
        "new_event": new_event,
    }


def overwrite_overall_kb_events_by_ids(
    target_ids: List[str],
    new_text: str,
    source_message_id: str = "",
    source: str = "chat_overwrite_command",
    confidence: float = 0.95,
) -> Dict[str, Any]:
    ids = [str(i).strip() for i in (target_ids or []) if str(i).strip()]
    new_text = (new_text or "").strip()
    if not ids or not new_text:
        return {"overwritten_count": 0, "new_event": {}, "overwritten_events": []}
    rows = _load_events_from_file(OVERALL_KB_EVENTS_FILE)
    id_set = set(ids)
    now = _now_iso()
    changed_rows: List[Dict[str, Any]] = []
    changed_ids: List[str] = []
    for row in rows:
        rid = str(row.get("id", ""))
        if rid not in id_set:
            continue
        if not bool(row.get("is_active", True)):
            continue
        row["is_active"] = False
        row["valid_to"] = now
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        metadata["deactivated_reason"] = "chat_overwrite_command"
        metadata["deactivated_at"] = now
        row["metadata"] = metadata
        changed_rows.append(dict(row))
        changed_ids.append(rid)
    if changed_ids:
        _write_events_to_file(OVERALL_KB_EVENTS_FILE, rows)

    base = changed_rows[0] if changed_rows else {}
    knowledge_type = str(base.get("knowledge_type", "")).strip().lower() or "process_rule"
    scope = str(base.get("scope", "")).strip().lower() or "project"
    new_event = append_overall_kb_event(
        text=new_text,
        knowledge_type=knowledge_type,
        scope=scope,
        confidence=confidence,
        source_message_id=source_message_id,
        source=source,
        metadata={"overwrites_event_ids": changed_ids},
    )
    return {
        "overwritten_count": len(changed_ids),
        "overwritten_ids": changed_ids,
        "overwritten_events": changed_rows,
        "new_event": new_event,
    }


def overall_kb_lexical_search(query: str, top_k: int = 8, active_only: bool = False) -> List[Tuple[float, Dict[str, Any]]]:
    events = load_overall_kb_events(include_archived=True)
    if active_only:
        events = [e for e in events if bool(e.get("is_active", True))]
    if not events:
        return []
    q_tokens = set(_tokenize(query))
    q_norm = _norm_text(query)
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for event in events:
        text = str(event.get("text", ""))
        t_tokens = set(_tokenize(text))
        overlap = len(q_tokens.intersection(t_tokens))
        contains = 1 if q_norm and q_norm in _norm_text(text) else 0
        score = overlap * 2 + contains
        if score > 0:
            scored.append((float(score), event))
    scored.sort(key=lambda x: (x[0], str(x[1].get("timestamp", ""))), reverse=True)
    return scored[:top_k]


def overall_kb_hybrid_search(query: str, top_k: int = 8, active_only: bool = False) -> List[Tuple[float, Dict[str, Any]]]:
    events = load_overall_kb_events(include_archived=True)
    if active_only:
        events = [e for e in events if bool(e.get("is_active", True))]
    if not events:
        return []

    lex = overall_kb_lexical_search(query, top_k=max(top_k, 12), active_only=active_only)
    lex_scores = {str(ev.get("id", "")): score for score, ev in lex}
    vectors = _ensure_vectors(events)
    q_vecs = _embed_texts([query])
    sem_scores: Dict[str, float] = {}
    if q_vecs:
        qv = q_vecs[0]
        for event in events:
            eid = str(event.get("id", ""))
            vec = vectors.get(eid)
            if vec:
                sem_scores[eid] = max(0.0, _cosine(qv, vec))

    max_lex = max(lex_scores.values()) if lex_scores else 1.0
    merged: List[Tuple[float, Dict[str, Any]]] = []
    for event in events:
        eid = str(event.get("id", ""))
        if not eid:
            continue
        lex_n = (lex_scores.get(eid, 0.0) / max_lex) if max_lex else 0.0
        sem_n = sem_scores.get(eid, 0.0)
        active_bonus = 0.08 if bool(event.get("is_active", True)) else 0.0
        score = 0.6 * lex_n + 0.4 * sem_n + active_bonus
        if score > 0:
            merged.append((score, event))
    merged.sort(key=lambda x: (x[0], str(x[1].get("timestamp", ""))), reverse=True)
    return merged[:top_k]


def render_overall_kb_citations(events: List[Dict[str, Any]]) -> str:
    if not events:
        return ""
    lines = ["Knowledge sources:"]
    for e in events:
        lines.append(f"- [kb:{e.get('id')}] {e.get('timestamp')} ({e.get('knowledge_type')}/{e.get('scope')})")
    return "\n".join(lines)


def overall_kb_debug_payload(limit: int = 30) -> Dict[str, Any]:
    active = _load_events_from_file(OVERALL_KB_EVENTS_FILE)
    archived = _all_archive_events()
    return {
        "active_count": len(active),
        "archived_count": len(archived),
        "active_events": active[-limit:],
        "archived_tail": archived[-limit:],
        "vector_count": len((_load_vectors().get("vectors", {}) or {})),
    }
