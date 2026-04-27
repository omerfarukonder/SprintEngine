"""
Task digest — rolling per-task history summaries for synthesis.

Each task gets a single digest record: a short LLM-generated narrative of
its full event history. The digest is only rebuilt when new events arrive
(content-addressed by event count), so LLM calls are amortized over time.

Synthesis uses digests instead of raw evts[:3], giving every task a richer
history signal without blowing the context budget.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .storage import TASK_DIGESTS_FILE, TASK_MEMORY_EVENTS_FILE, ensure_workspace


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_jsonl(path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
        except json.JSONDecodeError:
            continue
    return rows


def _save_jsonl(path, rows: List[Dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
        encoding="utf-8",
    )


def _load_task_events(task_id: str) -> List[Dict[str, Any]]:
    rows = _load_jsonl(TASK_MEMORY_EVENTS_FILE)
    matched = []
    for row in rows:
        meta = row.get("metadata", {})
        if isinstance(meta, dict) and str(meta.get("task_id", "")).strip() == task_id:
            matched.append(row)
    matched.sort(key=lambda x: str(x.get("timestamp", "")))
    return matched


def _openai_client():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    try:
        from openai import OpenAI
        return OpenAI(api_key=api_key)
    except Exception:
        return None


def _llm_summarize_events(task_name: str, events: List[Dict[str, Any]]) -> str:
    """Ask the LLM for a 2-4 sentence digest of a task's history."""
    client = _openai_client()
    if client is None:
        return _fallback_digest(events)
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
    lines = []
    for e in events:
        ts = str(e.get("timestamp", ""))[:10]
        text = str(e.get("text", "")).strip()
        if text:
            lines.append(f"[{ts}] {text[:200]}")
    event_block = "\n".join(lines)
    system_prompt = (
        "You summarize a task's update history into a concise digest for a knowledge base.\n"
        "Write 2-4 sentences capturing: what the task is, key decisions or blockers encountered, "
        "and the current direction. Strip dates, percentages, and transient status words. "
        "Focus on durable organizational knowledge: ownership, constraints, dependencies, approaches chosen."
    )
    user_prompt = f"Task: {task_name}\n\nHistory:\n{event_block[:4000]}\n\nDigest:"
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.1,
            max_completion_tokens=200,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        return _fallback_digest(events)


def _fallback_digest(events: List[Dict[str, Any]]) -> str:
    """Plain-text fallback when no LLM is available: most recent 5 events joined."""
    recent = events[-5:]
    lines = [str(e.get("text", "")).strip() for e in recent if e.get("text")]
    return " | ".join(lines)


def load_all_task_digests() -> Dict[str, str]:
    """Return a mapping of task_id → digest_text for all tasks that have digests."""
    ensure_workspace()
    rows = _load_jsonl(TASK_DIGESTS_FILE)
    return {str(r.get("task_id", "")): str(r.get("digest_text", "")) for r in rows if r.get("task_id")}


def load_task_digest(task_id: str) -> Optional[Dict[str, Any]]:
    """Return the full digest record for a single task, or None."""
    ensure_workspace()
    rows = _load_jsonl(TASK_DIGESTS_FILE)
    for r in rows:
        if str(r.get("task_id", "")) == task_id:
            return r
    return None


def update_task_digest(task_id: str, task_name: str) -> Dict[str, Any]:
    """Create or refresh the digest for a task.

    Only calls the LLM when there are > 5 events AND new events since the last
    digest (content-addressed by event count). Returns the digest record.
    """
    ensure_workspace()
    events = _load_task_events(task_id)
    event_count = len(events)

    rows = _load_jsonl(TASK_DIGESTS_FILE)
    existing: Optional[Dict[str, Any]] = None
    for r in rows:
        if str(r.get("task_id", "")) == task_id:
            existing = r
            break

    # No new events since last digest — return cached
    if existing and existing.get("event_count_covered", 0) >= event_count:
        return existing

    # Too few events for LLM summarization — use fallback
    if event_count <= 5:
        digest_text = _fallback_digest(events)
    else:
        digest_text = _llm_summarize_events(task_name, events)

    if not digest_text:
        return existing or {}

    record: Dict[str, Any] = {
        "task_id": task_id,
        "task_name": task_name,
        "digest_text": digest_text,
        "event_count_covered": event_count,
        "updated_at": _now_iso(),
    }

    # Update in-place or append
    updated = False
    for i, r in enumerate(rows):
        if str(r.get("task_id", "")) == task_id:
            rows[i] = record
            updated = True
            break
    if not updated:
        rows.append(record)

    _save_jsonl(TASK_DIGESTS_FILE, rows)
    return record
