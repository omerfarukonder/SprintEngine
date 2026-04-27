from __future__ import annotations

from datetime import date, datetime, timedelta
import re
from typing import Optional

from .models import SprintState, TaskStatus
from .task_history import first_task_history_timestamp


MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _to_date(raw: str) -> Optional[date]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _parse_first_eta_date(raw_eta: str, default_year: int) -> Optional[date]:
    text = (raw_eta or "").strip()
    if not text:
        return None

    iso_match = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if iso_match:
        try:
            return date(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3)))
        except ValueError:
            pass

    month_match = re.search(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2})(?:[\s,/-]+(\d{2,4}))?\b",
        text,
        flags=re.IGNORECASE,
    )
    if month_match:
        m = MONTHS.get(month_match.group(1)[:3].lower())
        day_val = int(month_match.group(2))
        y_raw = (month_match.group(3) or "").strip()
        year = default_year
        if y_raw:
            year = int(y_raw)
            if year < 100:
                year += 2000
        try:
            return date(year, m, day_val)
        except ValueError:
            return None

    return None


def _derive_start_date(task) -> Optional[date]:
    explicit = _to_date(str(getattr(task, "start_date", "") or ""))
    if explicit:
        return explicit
    first_ts = first_task_history_timestamp(task.id)
    if first_ts:
        parsed = _to_date(first_ts)
        if parsed:
            return parsed
    return None


def _derive_end_date(task, start_date: date) -> tuple[Optional[date], bool]:
    eta = str(getattr(task, "eta", "") or "").strip()
    if not eta:
        return start_date + timedelta(days=5), True

    iso = _to_date(eta)
    if iso:
        return iso, False

    parsed = _parse_first_eta_date(eta, default_year=start_date.year if start_date else datetime.utcnow().year)
    if parsed:
        # Keep ETA aligned with status table source-of-truth.
        return parsed, True

    return start_date + timedelta(days=5), True


def build_gantt_items(state: SprintState) -> list[dict]:
    out: list[dict] = []
    for task in state.tasks:
        if task.status == TaskStatus.on_hold:
            continue
        start = _derive_start_date(task)
        if start is None:
            continue
        end, uncertain = _derive_end_date(task, start)
        if end is None:
            continue
        if end < start:
            end = start + timedelta(days=1)
            uncertain = True
        out.append(
            {
                "task_id": task.id,
                "task_name": task.task_name,
                "status": task.status.value,
                "traffic_light": task.traffic_light.value,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "eta_raw": task.eta,
                "uncertain_end": bool(uncertain),
            }
        )
    out.sort(key=lambda x: (x["start_date"], x["task_name"].lower()))
    return out


def update_gantt_dates(state: SprintState, task_id: str, start_date_iso: str, end_date_iso: str):
    start = _to_date(start_date_iso)
    end = _to_date(end_date_iso)
    if start is None or end is None:
        raise ValueError("start_date and end_date must be ISO date values (YYYY-MM-DD).")
    if end < start:
        raise ValueError("end_date cannot be before start_date.")
    for task in state.tasks:
        if task.id != task_id:
            continue
        task.start_date = start.isoformat()
        task.eta = end.isoformat()
        task.last_updated_at = datetime.utcnow()
        return task
    raise ValueError(f"Task not found: {task_id}")
