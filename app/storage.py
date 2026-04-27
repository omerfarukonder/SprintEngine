from __future__ import annotations

import json
import shutil
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from .models import SprintState


BASE_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BASE_DIR / "workspace"
DAILY_LOGS_DIR = WORKSPACE_DIR / "daily_logs"
TABLES_DIR = WORKSPACE_DIR / "generated_tables"
REPORTS_DIR = WORKSPACE_DIR / "reports"
BACKUPS_DIR = WORKSPACE_DIR / "backups"
STATE_FILE = WORKSPACE_DIR / "sprint_state.json"
PLAN_FILE = WORKSPACE_DIR / "sprint_plan.md"
OVERALL_KB_EVENTS_FILE = WORKSPACE_DIR / "overall_kb_events.jsonl"
OVERALL_KB_VECTORS_FILE = WORKSPACE_DIR / "overall_kb_vectors.json"
OVERALL_KB_ARCHIVE_DIR = WORKSPACE_DIR / "overall_kb_archive"
KB_ENTITIES_FILE = WORKSPACE_DIR / "kb_entities.jsonl"
KB_RELATIONS_FILE = WORKSPACE_DIR / "kb_relations.jsonl"
TASK_MEMORY_EVENTS_FILE = WORKSPACE_DIR / "task_memory_events.jsonl"
TASK_DIGESTS_FILE = WORKSPACE_DIR / "task_digests.jsonl"
PRD_DIR = WORKSPACE_DIR / "prds"
PRD_INDEX_FILE = WORKSPACE_DIR / "prd_index.json"
LATEST_SPRINT_REPORT_FILE = REPORTS_DIR / "latest_sprint_report.md"
MEETING_SUMMARIES_FILE = WORKSPACE_DIR / "meeting_summaries.json"
INIT_BACKUP_GLOB = "sprint_state.initialize.*.json"


def ensure_workspace() -> None:
    WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    OVERALL_KB_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    PRD_DIR.mkdir(parents=True, exist_ok=True)
    if not TASK_MEMORY_EVENTS_FILE.exists():
        TASK_MEMORY_EVENTS_FILE.write_text("", encoding="utf-8")
    if not TASK_DIGESTS_FILE.exists():
        TASK_DIGESTS_FILE.write_text("", encoding="utf-8")


def load_state() -> SprintState:
    ensure_workspace()
    if not STATE_FILE.exists():
        state = SprintState()
        save_state(state)
        return state
    raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return SprintState.model_validate(raw)


def save_state(state: SprintState) -> None:
    ensure_workspace()
    payload = state.model_dump(mode="json")
    STATE_FILE.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def append_daily_markdown_log(text: str, for_date: Optional[date] = None) -> Path:
    ensure_workspace()
    day = for_date or date.today()
    path = DAILY_LOGS_DIR / f"{day.isoformat()}.md"
    if not path.exists():
        path.write_text(f"# Daily Log {day.isoformat()}\n\n", encoding="utf-8")
    with path.open("a", encoding="utf-8") as fh:
        fh.write(text.rstrip() + "\n\n")
    return path


def load_latest_sprint_report() -> str:
    ensure_workspace()
    if not LATEST_SPRINT_REPORT_FILE.exists():
        return ""
    return LATEST_SPRINT_REPORT_FILE.read_text(encoding="utf-8")


def save_latest_sprint_report(markdown: str) -> Path:
    ensure_workspace()
    payload = (markdown or "").rstrip() + "\n"
    LATEST_SPRINT_REPORT_FILE.write_text(payload, encoding="utf-8")
    return LATEST_SPRINT_REPORT_FILE


def list_initialize_backups() -> list[Path]:
    ensure_workspace()
    paths = sorted(BACKUPS_DIR.glob(INIT_BACKUP_GLOB), key=lambda p: p.name, reverse=True)
    return paths


def create_initialize_backup(max_keep: int = 5) -> Optional[Path]:
    ensure_workspace()
    if not STATE_FILE.exists():
        return None
    stamp = date.today().isoformat().replace("-", "") + "-" + datetime.utcnow().strftime("%H%M%S")
    backup = BACKUPS_DIR / f"sprint_state.initialize.{stamp}.json"
    shutil.copy2(STATE_FILE, backup)
    backups = list_initialize_backups()
    for old in backups[max(1, int(max_keep)) :]:
        old.unlink(missing_ok=True)
    return backup


def restore_latest_initialize_backup(remove_after_restore: bool = True) -> Optional[Path]:
    ensure_workspace()
    backups = list_initialize_backups()
    if not backups:
        return None
    latest = backups[0]
    shutil.copy2(latest, STATE_FILE)
    if remove_after_restore:
        latest.unlink(missing_ok=True)
    return latest


def _load_meeting_summaries_raw() -> dict[str, Any]:
    ensure_workspace()
    if not MEETING_SUMMARIES_FILE.exists():
        return {"meetings": []}
    try:
        raw = json.loads(MEETING_SUMMARIES_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"meetings": []}
    if not isinstance(raw, dict) or not isinstance(raw.get("meetings"), list):
        return {"meetings": []}
    return raw


def list_meeting_summaries() -> list[dict[str, Any]]:
    data = _load_meeting_summaries_raw()
    meetings = data.get("meetings") or []
    out: list[dict[str, Any]] = []
    for item in meetings:
        if not isinstance(item, dict):
            continue
        mid = str(item.get("id", "")).strip()
        name = str(item.get("meeting_name", "")).strip()
        summary = str(item.get("summary", "")).strip()
        created = str(item.get("created_at", "")).strip()
        if not mid or not name:
            continue
        out.append({"id": mid, "meeting_name": name, "summary": summary, "created_at": created})
    out.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return out


def append_meeting_summary(meeting_name: str, summary: str) -> dict[str, Any]:
    ensure_workspace()
    record = {
        "id": uuid.uuid4().hex[:16],
        "meeting_name": meeting_name.strip(),
        "summary": (summary or "").strip(),
        "created_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    data = _load_meeting_summaries_raw()
    meetings = data.setdefault("meetings", [])
    if not isinstance(meetings, list):
        data["meetings"] = []
        meetings = data["meetings"]
    meetings.append(record)
    MEETING_SUMMARIES_FILE.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return record
