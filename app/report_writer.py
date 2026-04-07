from __future__ import annotations

from datetime import datetime
from typing import List

from .models import SprintState, TaskStatus, TrafficLight


def _clean(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _recent_updates(state: SprintState, limit: int = 12) -> List[str]:
    rows = [log.user_message for log in state.daily_logs[-max(1, int(limit)) :]]
    return [_clean(r) for r in rows if _clean(r)]


def _fallback_report(state: SprintState, updates: List[str]) -> str:
    total = len(state.tasks)
    done = sum(1 for t in state.tasks if t.status == TaskStatus.done)
    blocked = sum(1 for t in state.tasks if t.status == TaskStatus.blocked)
    at_risk = sum(1 for t in state.tasks if t.traffic_light in {TrafficLight.yellow, TrafficLight.red})

    progress_lines: List[str] = []
    for task in sorted(state.tasks, key=lambda t: t.last_updated_at, reverse=True)[:8]:
        latest = _clean(task.latest_update) or "No recent update."
        progress_lines.append(f"- {task.task_name}: {latest}")
    if not progress_lines:
        progress_lines = ["- No task updates captured yet."]

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
        "## Business Impact\n"
        "- Completed items reduce delivery uncertainty and improve release readiness.\n"
        "- Delays are concentrated in blocked/dependency-bound tasks.\n\n"
        "## Progress vs Plan\n"
        + "\n".join(progress_lines)
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
                return text.rstrip() + "\n"
        except Exception:
            pass
    return _fallback_report(state, updates)
