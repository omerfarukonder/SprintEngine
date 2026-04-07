from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from .models import SprintState, Task, TaskStatus, TrafficLight


TASK_LINE_PATTERNS = [
    re.compile(r"^\s*-\s+\[(?P<mark>[ xX])\]\s+(?P<name>.+?)\s*$"),
    re.compile(r"^\s*-\s+(?P<name>.+?)\s*$"),
]
TABLE_SEPARATOR_RE = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$")


def _extract_link(text: str) -> tuple[str, str]:
    md = re.search(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", text)
    if md:
        label = (md.group(1) or "").strip() or text.strip()
        return label, md.group(2).strip()
    plain = re.search(r"(https?://\S+)", text)
    if plain:
        cleaned = text.replace(plain.group(1), "").strip(" -:()")
        return cleaned or text.strip(), plain.group(1).strip()
    return text.strip(), ""


def _task_id(name: str, section: str, index: int) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", f"{section}-{name}".lower()).strip("-")
    return f"{normalized or 'task'}-{index}"


def _parse_table_task_status(raw_status: str, task_name: str) -> Tuple[TaskStatus, TrafficLight]:
    text = (raw_status or "").strip().lower()
    name = (task_name or "").strip().lower()
    if "blocked" in text or "blocked" in name:
        return TaskStatus.blocked, TrafficLight.red
    if "uat" in text:
        return TaskStatus.in_progress, TrafficLight.yellow
    if any(t in text for t in ["done", "completed", "live"]):
        return TaskStatus.done, TrafficLight.green
    if any(t in text for t in ["in progress", "ongoing", "awaiting", "connecting"]):
        return TaskStatus.in_progress, TrafficLight.yellow
    return TaskStatus.not_started, TrafficLight.green


def _parse_markdown_table_tasks(markdown_text: str) -> List[Task]:
    tasks: List[Task] = []
    lines = markdown_text.splitlines()
    in_table = False
    task_idx = -1
    definition_idx = -1
    links_idx = -1
    status_idx = -1

    for line in lines:
        if "|" not in line:
            if in_table:
                break
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 2:
            continue

        if not in_table:
            lowered = [c.lower() for c in cells]
            if "task" in lowered and "definition" in lowered:
                in_table = True
                task_idx = lowered.index("task")
                definition_idx = lowered.index("definition")
                links_idx = lowered.index("links") if "links" in lowered else -1
                status_idx = (
                    lowered.index("end of sprint status")
                    if "end of sprint status" in lowered
                    else (lowered.index("status") if "status" in lowered else -1)
                )
            continue

        if TABLE_SEPARATOR_RE.match(line):
            continue

        max_idx = max(task_idx, definition_idx, links_idx, status_idx, 0)
        if len(cells) <= max_idx:
            continue

        raw_task = cells[task_idx].strip() if task_idx >= 0 else ""
        raw_definition = cells[definition_idx].strip() if definition_idx >= 0 else ""
        raw_links = cells[links_idx].strip() if links_idx >= 0 else ""
        raw_status = cells[status_idx].strip() if status_idx >= 0 else ""

        if not raw_task or raw_task.lower() == "task":
            continue

        task_name, linked_from_task = _extract_link(raw_task)
        definition = raw_definition if raw_definition and raw_definition not in {"-", "—"} else task_name
        _, link_from_links = _extract_link(raw_links) if raw_links else ("", "")
        task_link = link_from_links or linked_from_task
        status, light = _parse_table_task_status(raw_status, task_name)

        tasks.append(
            Task(
                id=_task_id(task_name, "table", len(tasks) + 1),
                task_name=task_name,
                definition=definition,
                task_link=task_link,
                status=status,
                traffic_light=light,
                last_updated_at=datetime.utcnow(),
            )
        )

    return tasks


def parse_sprint_plan(markdown_text: str) -> List[Task]:
    # Prefer explicit sprint plan tables to preserve exact task/definition text.
    table_tasks = _parse_markdown_table_tasks(markdown_text)
    if table_tasks:
        return table_tasks

    tasks: List[Task] = []
    current_section = "general"

    for line in markdown_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            current_section = stripped.lstrip("#").strip().lower() or "general"
            continue

        matched = None
        for pattern in TASK_LINE_PATTERNS:
            matched = pattern.match(line)
            if matched:
                break
        if not matched:
            continue

        raw_name = matched.group("name").strip()
        task_name, task_link = _extract_link(raw_name)
        status = TaskStatus.done if matched.groupdict().get("mark", " ").lower() == "x" else TaskStatus.not_started
        light = TrafficLight.red if "blocked" in task_name.lower() else TrafficLight.green
        if light == TrafficLight.red:
            status = TaskStatus.blocked

        tasks.append(
            Task(
                id=_task_id(task_name, current_section, len(tasks) + 1),
                task_name=task_name,
                definition=task_name,
                task_link=task_link,
                status=status,
                traffic_light=light,
                last_updated_at=datetime.utcnow(),
            )
        )
    return tasks


def initialize_state_from_plan(plan_file: Path, sprint_name: str = "Current Sprint") -> SprintState:
    raw = plan_file.read_text(encoding="utf-8") if plan_file.exists() else ""
    return SprintState(sprint_name=sprint_name, tasks=parse_sprint_plan(raw))
