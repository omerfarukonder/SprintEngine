from __future__ import annotations

from datetime import datetime
import re
from typing import List

from .models import SprintState, TaskStatus, TrafficLight


def _clean(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _status_badge(task_status: TaskStatus, traffic_light: TrafficLight) -> str:
    del traffic_light
    status_value = getattr(task_status, "value", str(task_status)).lower().strip()
    status_text = _clean(status_value).replace("_", " ").title() or "Unknown"
    if status_value == TaskStatus.blocked.value:
        return f"🔴 {status_text}"
    if status_value == TaskStatus.done.value:
        return f"🟢 {status_text}"
    if status_value == TaskStatus.in_progress.value:
        return f"🟡 {status_text}"
    if status_value == TaskStatus.on_hold.value:
        return f"⚪ {status_text}"
    return f"⚪ {status_text}"


def _task_label(name: str, link: str) -> str:
    safe_name = _clean(name).replace("|", "\\|")
    safe_link = _clean(link)
    if safe_link:
        return f"[{safe_name}]({safe_link})"
    return safe_name


def _build_table_rows(tasks: list, empty_message: str) -> List[str]:
    rows: List[str] = ["| Task | Status | Update |", "| --- | --- | --- |"]
    for task in tasks:
        latest = _clean(task.latest_update) or "No recent update."
        task_cell = _task_label(task.task_name, task.task_link)
        status_cell = _status_badge(task.status, task.traffic_light)
        escaped_status = status_cell.replace("|", "\\|")
        escaped_latest = latest.replace("|", "\\|")
        rows.append(
            f"| {task_cell} | {escaped_status} | {escaped_latest} |"
        )
    if len(rows) == 2:
        rows.append(f"| _{empty_message}_ |  |  |")
    return rows


def _progress_table(state: SprintState) -> List[str]:
    committed = [t for t in state.tasks if t.status != TaskStatus.on_hold]
    committed.sort(key=lambda t: t.last_updated_at, reverse=True)
    return _build_table_rows(
        tasks=committed[:12],
        empty_message="No committed tasks with recent updates.",
    )


def _on_stack_table(state: SprintState) -> List[str]:
    on_hold = [t for t in state.tasks if t.status == TaskStatus.on_hold]
    on_hold.sort(key=lambda t: t.last_updated_at, reverse=True)
    return _build_table_rows(
        tasks=on_hold[:12],
        empty_message="No tasks are currently on hold.",
    )


def _replace_or_append_section(markdown: str, section_title: str, body: str) -> str:
    header = f"## {section_title}"
    pattern = re.compile(
        rf"(^## {re.escape(section_title)}\s*$)([\s\S]*?)(?=^##\s+|\Z)",
        flags=re.MULTILINE,
    )
    if pattern.search(markdown):
        return pattern.sub(lambda m: f"{m.group(1)}\n{body.rstrip()}\n\n", markdown, count=1)
    payload = markdown.rstrip()
    if payload:
        payload += "\n\n"
    return payload + f"{header}\n{body.rstrip()}\n"


def _enforce_report_tables(markdown: str, state: SprintState) -> str:
    out = markdown.strip()
    progress_body = "\n".join(_progress_table(state))
    stack_body = "\n".join(_on_stack_table(state))
    out = _replace_or_append_section(out, "Progress vs Plan", progress_body)
    out = _replace_or_append_section(out, "On Stack", stack_body)
    return out.rstrip() + "\n"


def _recent_updates(state: SprintState, limit: int = 12) -> List[str]:
    rows = [log.user_message for log in state.daily_logs[-max(1, int(limit)) :]]
    return [_clean(r) for r in rows if _clean(r)]


def _fallback_report(state: SprintState, updates: List[str]) -> str:
    total = len(state.tasks)
    done = sum(1 for t in state.tasks if t.status == TaskStatus.done)
    blocked = sum(1 for t in state.tasks if t.status == TaskStatus.blocked)
    at_risk = sum(1 for t in state.tasks if t.traffic_light in {TrafficLight.yellow, TrafficLight.red})

    progress_rows = _progress_table(state)
    stack_rows = _on_stack_table(state)

    blocker_lines: List[str] = []
    for task in state.tasks:
        if task.status != TaskStatus.blocked and not task.blockers:
            continue
        details = "; ".join(_clean(b) for b in task.blockers if _clean(b)) or "Dependency unresolved."
        blocker_lines.append(f"- {task.task_name}: {details}")
    if not blocker_lines:
        blocker_lines = ["- No active blockers reported in latest updates."]

    next_steps: List[str] = []
    for task in sorted(state.tasks, key=lambda t: t.last_updated_at, reverse=True):
        if len(next_steps) >= 5:
            break
        if task.status in {TaskStatus.done, TaskStatus.not_started}:
            continue
        eta = _clean(task.eta)
        suffix = f" by {eta}" if eta else ""
        next_steps.append(f"- {task.task_name}: confirm next checkpoint{suffix}.")
    if not next_steps:
        next_steps = ["- Align owners on next checkpoint dates for all active tasks."]

    evidence = [f"- {u}" for u in updates[:5]] or ["- No recent daily log entries yet."]
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    return (
        f"# Sprint Report - {state.sprint_name}\n\n"
        f"_Generated at: {generated_at}_\n\n"
        "## Summary\n"
        f"- Sprint health: {done}/{total} tasks completed, {blocked} blocked, {at_risk} at risk.\n"
        "- Overall trajectory: execution is active; dependency handling remains the main risk area.\n\n"
        "## Progress vs Plan\n"
        + "\n".join(progress_rows)
        + "\n\n## On Stack\n"
        + "\n".join(stack_rows)
        + "\n\n## Risks and Blockers\n"
        + "\n".join(blocker_lines)
        + "\n\n## Decisions Needed\n"
        "- Confirm owner and ETA for each blocked dependency.\n"
        "- Decide escalation path for items waiting on external teams.\n\n"
        "## Next 7 Days\n"
        + "\n".join(next_steps)
        + "\n\n## Evidence (Latest Updates)\n"
        + "\n".join(evidence)
        + "\n"
    )


def generate_sprint_report_markdown(state: SprintState, llm: object | None = None) -> str:
    updates = _recent_updates(state, limit=12)
    if llm is not None and getattr(llm, "enabled", False):
        try:
            text = llm.generate_sprint_report(state=state, recent_updates=updates)
            if isinstance(text, str) and text.strip():
                return _enforce_report_tables(text, state)
        except Exception:
            pass
    return _enforce_report_tables(_fallback_report(state, updates), state)
