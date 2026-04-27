from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import re
from typing import List

from .models import SprintState, TaskStatus, TrafficLight
from .task_history import list_task_history, resolve_task_id_by_name


def list_tasks(state: SprintState) -> str:
    if not state.tasks:
        return "No tasks loaded yet. Import a sprint plan first."
    lines = [f"- {t.task_name}: {t.status.value} ({t.traffic_light.value})" for t in state.tasks]
    return "Sprint tasks:\n" + "\n".join(lines)


def risky_tasks(state: SprintState) -> str:
    risky = [
        t
        for t in state.tasks
        if t.traffic_light in {TrafficLight.red, TrafficLight.yellow} or t.status == TaskStatus.blocked
    ]
    if not risky:
        return "No risky tasks right now."
    lines = [f"- {t.task_name}: {t.status.value}, light={t.traffic_light.value}" for t in risky]
    return "Risky tasks:\n" + "\n".join(lines)


def yesterday_log(state: SprintState) -> str:
    if not state.daily_logs:
        return "No daily logs yet."
    target = date.today() - timedelta(days=1)
    logs = [l for l in state.daily_logs if l.timestamp.date() == target]
    if not logs:
        return "No logs from yesterday."
    return "Yesterday:\n" + "\n".join(f"- {l.timestamp.isoformat()}: {l.user_message}" for l in logs)


def _find_task_by_name(state: SprintState, question: str):
    lowered = question.lower()
    best = None
    best_score = 0
    for task in state.tasks:
        name_words = set(task.task_name.lower().split())
        q_words = set(lowered.split())
        overlap = len(name_words & q_words)
        if overlap > best_score:
            best_score = overlap
            best = task
    return best if best_score >= 2 else None


def _looks_like_history_query(question: str) -> bool:
    lowered = (question or "").lower()
    triggers = [
        "history",
        "timeline",
        "past updates",
        "update history",
        "track record",
        "what happened",
        "changes on",
        "changes for",
    ]
    if any(token in lowered for token in triggers):
        return True
    return lowered.strip().startswith(("hist ", "tl ", "timeline "))


def _extract_history_target(question: str) -> str:
    text = (question or "").strip()
    if not text:
        return ""
    patterns = [
        r"(?i)(?:history|timeline|past updates|update history|track record)\s+(?:of|for|on)\s+(.+)$",
        r"(?i)(?:what happened with|what happened on)\s+(.+)$",
        r"(?i)(?:changes on|changes for)\s+(.+)$",
        r"(?i)^(?:hist|timeline|tl)\s+(.+)$",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return (m.group(1) or "").strip(" ?.")
    return text.strip(" ?.")


def _pretty_status(status: str) -> str:
    val = (status or "").strip().lower()
    if not val:
        return "unknown"
    return val.replace("_", " ")


def _date_only(timestamp: str) -> str:
    ts = (timestamp or "").strip()
    if not ts:
        return ""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return ts[:10]


def _event_status(row: dict) -> str:
    metadata = row.get("metadata", {})
    if isinstance(metadata, dict):
        status = str(metadata.get("status", "")).strip().lower()
        if status:
            return status
    text = str(row.get("text", "")).lower()
    if "on hold" in text:
        return "on_hold"
    if "blocked" in text:
        return "blocked"
    if any(t in text for t in ["done", "completed", "deployed"]):
        return "done"
    if "progress" in text:
        return "in_progress"
    return "unknown"


def _event_snippet(row: dict, task_name: str) -> str:
    text = str(row.get("text", "")).strip()
    if not text:
        return "no detail captured."
    prefix = f"{task_name}:"
    if text.lower().startswith(prefix.lower()):
        text = text[len(prefix) :].strip()
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 180:
        text = text[:177].rstrip() + "..."
    if not text.endswith("."):
        text += "."
    return text[0].lower() + text[1:] if text else "no detail captured."


def _segment_history(events: list, task_name: str) -> list[dict]:
    ordered = sorted(events, key=lambda x: str(x.get("timestamp", "")))
    daily: dict[str, dict] = {}
    for row in ordered:
        ts = str(row.get("timestamp", "")).strip()
        day = _date_only(ts)
        if not day:
            continue
        daily[day] = {
            "day": day,
            "status": _event_status(row),
            "snippet": _event_snippet(row, task_name=task_name),
        }
    day_rows = [daily[d] for d in sorted(daily.keys())]

    segments: list[dict] = []
    current = None
    for row in day_rows:
        day = row["day"]
        status = row["status"]
        snippet = row["snippet"]
        if current is None:
            current = {
                "start": day,
                "end": day,
                "status": status,
                "first_note": snippet,
                "last_note": snippet,
                "count": 1,
            }
            continue
        if current["status"] == status:
            current["end"] = day
            current["last_note"] = snippet
            current["count"] += 1
            continue
        segments.append(current)
        current = {
            "start": day,
            "end": day,
            "status": status,
            "first_note": snippet,
            "last_note": snippet,
            "count": 1,
        }
    if current is not None:
        segments.append(current)
    return segments


def _render_task_history(state: SprintState, question: str, limit: int = 12) -> str:
    target = _extract_history_target(question)
    task_id = resolve_task_id_by_name(state=state, task_name=target)
    if not task_id:
        task = _find_task_by_name(state, question)
        if task is not None:
            task_id = task.id
            target = task.task_name
    if not task_id:
        return "I could not find the task for that history request. Try a clearer task name."
    events = list_task_history(task_id=task_id, limit=max(1, int(limit)))
    if not events:
        return f"No timeline events found yet for '{target}'."
    task_name = target or task_id
    segments = _segment_history(events, task_name=task_name)
    if not segments:
        return f"No timeline events found yet for '{task_name}'."
    if len(segments) > 8:
        segments = segments[-8:]
    lines = []
    for seg in segments:
        status_text = _pretty_status(seg["status"])
        if seg["start"] == seg["end"]:
            lines.append(f"On {seg['start']}, the task was {status_text}; {seg['last_note']}")
        else:
            lines.append(
                f"From {seg['start']} to {seg['end']}, the task stayed {status_text}; latest note: {seg['last_note']}"
            )
    return (
        f"Timeline for **{task_name}**:\n"
        + "\n".join(lines)
    )


def answer_query(state: SprintState, question: str) -> str:
    lowered = question.lower()
    if _looks_like_history_query(question):
        return _render_task_history(state, question, limit=12)
    if "what are" in lowered and "tasks" in lowered:
        return list_tasks(state)
    if "risky" in lowered or "risk" in lowered:
        return risky_tasks(state)
    if "yesterday" in lowered and "log" in lowered:
        return yesterday_log(state)
    task = _find_task_by_name(state, question)
    if task:
        definition = task.definition or "No description available."
        status_line = f"Owner: {task.owner or '—'} | ETA: {task.eta or '—'} | Status: {task.status.value} ({task.traffic_light.value})"
        update = f"\nLatest update: {task.latest_update}" if task.latest_update else ""
        link = f"\nLink: {task.task_link}" if task.task_link else ""
        return f"**{task.task_name}**\n{definition}\n{status_line}{update}{link}"
    return "I can help with tasks, risks, and yesterday logs. Or send daily updates."


def generate_status_table(state: SprintState, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = [
        "| # | Task | Definition | Light | Status | Latest Update |",
        "|---|------|------------|-------|--------|---------------|",
    ]
    for idx, task in enumerate(state.tasks, start=1):
        definition = task.definition or task.task_name
        latest = task.latest_update or "-"
        lines.append(
            f"| {idx} | {task.task_name} | {definition} | "
            f"{task.traffic_light.value} | {task.status.value} | {latest} |"
        )
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return destination
