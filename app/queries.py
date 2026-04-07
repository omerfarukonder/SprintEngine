from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import List

from .models import SprintState, TaskStatus, TrafficLight


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


def answer_query(state: SprintState, question: str) -> str:
    lowered = question.lower()
    if "what are" in lowered and "tasks" in lowered:
        return list_tasks(state)
    if "risky" in lowered or "risk" in lowered:
        return risky_tasks(state)
    if "yesterday" in lowered and "log" in lowered:
        return yesterday_log(state)
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
