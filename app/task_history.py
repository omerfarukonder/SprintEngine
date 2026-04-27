from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .models import SprintState
from .storage import DAILY_LOGS_DIR, TASK_MEMORY_EVENTS_FILE, ensure_workspace


Refiner = Callable[[str], str]


def _norm_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _load_rows() -> List[Dict[str, Any]]:
    ensure_workspace()
    if not TASK_MEMORY_EVENTS_FILE.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in TASK_MEMORY_EVENTS_FILE.read_text(encoding="utf-8").splitlines():
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


def _append_row(row: Dict[str, Any]) -> None:
    ensure_workspace()
    with TASK_MEMORY_EVENTS_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _event_key(task_id: str, text: str, timestamp: str, event_type: str) -> str:
    ts_bucket = str(timestamp or "")[:19]
    return f"{task_id}|{_norm_text(text)}|{ts_bucket}|{event_type}"


def _existing_event_keys(rows: List[Dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in rows:
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        task_id = str(metadata.get("task_id", "")).strip()
        text = str(row.get("text", "")).strip()
        ts = str(row.get("timestamp", "")).strip()
        event_type = str(row.get("event_type", "task_history_event")).strip() or "task_history_event"
        if not task_id or not text or not ts:
            continue
        out.add(_event_key(task_id=task_id, text=text, timestamp=ts, event_type=event_type))
    return out


def _event_id(task_id: str, text: str, timestamp: str, source_message_id: str, event_type: str) -> str:
    payload = f"{timestamp}|{task_id}|{event_type}|{_norm_text(text)}|{source_message_id}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def append_task_history_event(
    *,
    task_id: str,
    task_name: str,
    text: str,
    status: str,
    traffic_light: str,
    source: str = "task_history",
    source_message_id: str = "",
    timestamp: Optional[str] = None,
    event_type: str = "task_history_event",
    metadata: Optional[Dict[str, Any]] = None,
    refiner: Optional[Refiner] = None,
) -> Dict[str, Any]:
    ensure_workspace()
    ts = (timestamp or datetime.utcnow().isoformat()).strip()
    raw_text = str(text or "").strip()
    if not task_id.strip() or not task_name.strip() or not raw_text or not ts:
        return {}

    refined_text = raw_text
    if refiner is not None:
        try:
            candidate = str(refiner(raw_text) or "").strip()
        except Exception:
            candidate = ""
        if candidate:
            refined_text = candidate

    rows = _load_rows()
    event_key = _event_key(task_id=task_id.strip(), text=refined_text, timestamp=ts, event_type=event_type)
    if event_key in _existing_event_keys(rows):
        return {}

    row = {
        "id": _event_id(
            task_id=task_id.strip(),
            text=refined_text,
            timestamp=ts,
            source_message_id=source_message_id.strip(),
            event_type=event_type,
        ),
        "timestamp": ts,
        "source": source.strip() or "task_history",
        "event_type": event_type.strip() or "task_history_event",
        "text": f"{task_name.strip()}: {refined_text}",
        "metadata": {
            "task_id": task_id.strip(),
            "task_name": task_name.strip(),
            "status": status.strip(),
            "traffic_light": traffic_light.strip(),
            "source_message_id": source_message_id.strip(),
            "raw_text": raw_text,
        },
    }
    if metadata:
        for k, v in metadata.items():
            row["metadata"][k] = v
    _append_row(row)
    return row


def list_task_history(task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    task_id = (task_id or "").strip()
    if not task_id:
        return []
    rows = _load_rows()
    matched: List[Dict[str, Any]] = []
    for row in rows:
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            continue
        if str(metadata.get("task_id", "")).strip() == task_id:
            matched.append(row)
    matched.sort(key=lambda x: str(x.get("timestamp", "")), reverse=True)
    return matched[: max(1, int(limit))]


def first_task_history_timestamp(task_id: str) -> str:
    task_id = (task_id or "").strip()
    if not task_id:
        return ""
    rows = _load_rows()
    first_ts = ""
    for row in rows:
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            continue
        if str(metadata.get("task_id", "")).strip() != task_id:
            continue
        ts = str(row.get("timestamp", "")).strip()
        if not ts:
            continue
        if not first_ts or ts < first_ts:
            first_ts = ts
    return first_ts


def resolve_task_id_by_name(state: SprintState, task_name: str) -> str:
    target = (task_name or "").strip().lower()
    if not target:
        return ""
    for task in state.tasks:
        if task.task_name.strip().lower() == target:
            return task.id
    for task in state.tasks:
        n = task.task_name.strip().lower()
        if n and (target in n or n in target):
            return task.id
    rows = _load_rows()
    rows.sort(key=lambda x: str(x.get("timestamp", "")), reverse=True)
    for row in rows:
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            continue
        n = str(metadata.get("task_name", "")).strip().lower()
        tid = str(metadata.get("task_id", "")).strip()
        if tid and n == target:
            return tid
    return ""


def _extract_task_names_from_changes(changes: List[str]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for raw in changes:
        text = str(raw or "").strip()
        if not text:
            continue
        m_name = re.search(r"'([^']+)'", text)
        if not m_name:
            continue
        task_name = m_name.group(1).strip()
        status = ""
        light = ""
        m_status = re.search(r"\bto\s+([a-z_]+)\/([a-z_]+)\b", text, flags=re.IGNORECASE)
        if m_status:
            status = m_status.group(1).strip().lower()
            light = m_status.group(2).strip().lower()
        out.append(
            {
                "task_name": task_name,
                "status": status,
                "traffic_light": light,
                "applied_change": text,
            }
        )
    return out


def _task_scoped_text(user_message: str, task_name: str) -> str:
    message = str(user_message or "").strip()
    if not message:
        return ""
    prefix_pattern = re.compile(rf"^\s*{re.escape(task_name)}\s*:\s*(.+)$", flags=re.IGNORECASE)
    m = prefix_pattern.match(message)
    if m:
        return m.group(1).strip()
    return message


def _build_task_maps(state: SprintState) -> tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    id_by_name: Dict[str, str] = {}
    defaults_by_name: Dict[str, Dict[str, str]] = {}
    for task in state.tasks:
        key = task.task_name.strip().lower()
        if not key:
            continue
        id_by_name[key] = task.id
        defaults_by_name[key] = {
            "status": getattr(task.status, "value", str(task.status)).strip().lower(),
            "traffic_light": getattr(task.traffic_light, "value", str(task.traffic_light)).strip().lower(),
        }
    return id_by_name, defaults_by_name


def _resolve_task_id_from_rows(task_name: str, rows: List[Dict[str, Any]]) -> str:
    target = (task_name or "").strip().lower()
    if not target:
        return ""
    ordered = sorted(rows, key=lambda x: str(x.get("timestamp", "")), reverse=True)
    for row in ordered:
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            continue
        n = str(metadata.get("task_name", "")).strip().lower()
        tid = str(metadata.get("task_id", "")).strip()
        if tid and n == target:
            return tid
    return ""


def _parse_daily_markdown_blocks(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return rows

    current_ts = ""
    current_msg = ""
    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        head = re.match(r"^\-\s+\[[^\]]*\]\s+([^|]+)\|\s+update:\s+(.+)$", line)
        if head:
            current_ts = head.group(1).strip()
            current_msg = head.group(2).strip()
            continue
        detail = re.match(r"^\s{2}\-\s+(.+?):\s+([a-z_]+)\/([a-z_]+)\s*$", line, flags=re.IGNORECASE)
        if detail and current_ts and current_msg:
            rows.append(
                {
                    "timestamp": current_ts,
                    "user_message": current_msg,
                    "task_name": detail.group(1).strip(),
                    "status": detail.group(2).strip().lower(),
                    "traffic_light": detail.group(3).strip().lower(),
                    "applied_change": f"Updated '{detail.group(1).strip()}' to {detail.group(2).strip().lower()}/{detail.group(3).strip().lower()}",
                }
            )
    return rows


def backfill_task_history_from_sources(
    state: SprintState,
    *,
    include_markdown: bool = True,
) -> Dict[str, Any]:
    rows = _load_rows()
    dedupe_keys = _existing_event_keys(rows)
    id_by_name, defaults_by_name = _build_task_maps(state)
    added = 0
    skipped_unmapped = 0
    skipped_duplicate = 0

    def resolve_task_id(task_name: str) -> str:
        key = task_name.strip().lower()
        if key in id_by_name:
            return id_by_name[key]
        return _resolve_task_id_from_rows(task_name=task_name, rows=rows)

    def resolve_defaults(task_name: str) -> Dict[str, str]:
        return defaults_by_name.get(task_name.strip().lower(), {"status": "", "traffic_light": ""})

    def append_backfill_event(
        *,
        task_name: str,
        timestamp: str,
        user_message: str,
        status: str,
        traffic_light: str,
        applied_change: str,
        source: str,
    ) -> None:
        nonlocal added, skipped_unmapped, skipped_duplicate
        task_id = resolve_task_id(task_name)
        if not task_id:
            skipped_unmapped += 1
            return
        text = _task_scoped_text(user_message=user_message, task_name=task_name)
        if not text:
            return
        event_type = "task_history_event"
        event_key = _event_key(task_id=task_id, text=text, timestamp=timestamp, event_type=event_type)
        if event_key in dedupe_keys:
            skipped_duplicate += 1
            return
        fallback = resolve_defaults(task_name)
        row = append_task_history_event(
            task_id=task_id,
            task_name=task_name,
            text=text,
            status=status or fallback.get("status", ""),
            traffic_light=traffic_light or fallback.get("traffic_light", ""),
            source=source,
            timestamp=timestamp,
            event_type=event_type,
            metadata={
                "backfill": True,
                "backfill_source": source,
                "applied_change": applied_change,
                "raw_user_message": user_message,
            },
            refiner=None,
        )
        if row:
            dedupe_keys.add(event_key)
            rows.append(row)
            added += 1

    for log in state.daily_logs:
        ts = str(getattr(log, "timestamp", "")).strip()
        user_message = str(getattr(log, "user_message", "")).strip()
        applied_changes = [str(x).strip() for x in getattr(log, "applied_changes", []) if str(x).strip()]
        if not ts or not user_message or not applied_changes:
            continue
        for item in _extract_task_names_from_changes(applied_changes):
            append_backfill_event(
                task_name=item["task_name"],
                timestamp=ts,
                user_message=user_message,
                status=item["status"],
                traffic_light=item["traffic_light"],
                applied_change=item["applied_change"],
                source="backfill_sprint_state",
            )

    if include_markdown:
        ensure_workspace()
        for path in sorted(DAILY_LOGS_DIR.glob("*.md")):
            for row in _parse_daily_markdown_blocks(path):
                append_backfill_event(
                    task_name=row["task_name"],
                    timestamp=row["timestamp"],
                    user_message=row["user_message"],
                    status=row["status"],
                    traffic_light=row["traffic_light"],
                    applied_change=row["applied_change"],
                    source="backfill_daily_markdown",
                )

    return {
        "added_count": added,
        "skipped_unmapped": skipped_unmapped,
        "skipped_duplicate": skipped_duplicate,
        "total_rows": len(_load_rows()),
        "path": str(TASK_MEMORY_EVENTS_FILE),
    }
