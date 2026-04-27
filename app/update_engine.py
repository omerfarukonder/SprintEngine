from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple

from .models import DailyLogEntry, SprintState, Task, TaskStatus, TrafficLight


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "task"


def _new_task_id(state: SprintState, task_name: str) -> str:
    base = _slugify(task_name)
    i = 1
    existing = {t.id for t in state.tasks}
    candidate = f"{base}-{i}"
    while candidate in existing:
        i += 1
        candidate = f"{base}-{i}"
    return candidate


def _extract_owner(message: str) -> str:
    m = re.search(r"owned by\s+(.+?)(?:,| and |$)", message, flags=re.IGNORECASE)
    return m.group(1).strip().strip(".") if m else ""


def _extract_eta(message: str) -> str:
    m = re.search(r"(?:eta|deliver(?:ed|y)? by|due(?: date)?(?: is)?)\s+(.+?)(?:,| and |$)", message, flags=re.IGNORECASE)
    return m.group(1).strip().strip(".") if m else ""


def _next_weekday(base: date, weekday: int) -> date:
    days_ahead = (weekday - base.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return base + timedelta(days=days_ahead)


def _parse_iso_date(value: str) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _extract_start_date(message: str, base_date: date | None = None) -> str:
    lowered = (message or "").lower().strip()
    if not lowered:
        return ""

    today = date.today()
    reference = base_date or today
    # Prefer explicit date references when present.
    iso_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", lowered)
    if iso_match:
        return iso_match.group(1)

    month_match = re.search(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2})(?:[\s,/-]+(\d{2,4}))?\b",
        lowered,
        flags=re.IGNORECASE,
    )
    if month_match:
        month_map = {
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
        month = month_map.get(month_match.group(1)[:3].lower())
        day_num = int(month_match.group(2))
        year_raw = (month_match.group(3) or "").strip()
        year = today.year
        if year_raw:
            year = int(year_raw)
            if year < 100:
                year += 2000
        try:
            return date(year, month, day_num).isoformat()
        except ValueError:
            pass

    has_start_signal = "start" in lowered or "started" in lowered or "kickoff" in lowered or "kicked off" in lowered
    if has_start_signal:
        rel = re.search(r"\b(\d+)\s+(day|days|week|weeks)\s+(later|after)\b", lowered)
        if rel:
            qty = int(rel.group(1))
            unit = rel.group(2)
            delta_days = qty * (7 if "week" in unit else 1)
            return (reference + timedelta(days=delta_days)).isoformat()
        rel = re.search(r"\b(\d+)\s+(day|days|week|weeks)\s+(earlier|before)\b", lowered)
        if rel:
            qty = int(rel.group(1))
            unit = rel.group(2)
            delta_days = qty * (7 if "week" in unit else 1)
            return (reference - timedelta(days=delta_days)).isoformat()

    if "start next week" in lowered or "starting next week" in lowered or "will start next week" in lowered:
        return _next_weekday(today, 0).isoformat()  # next Monday
    if "started last week" in lowered or "start last week" in lowered:
        return (today - timedelta(days=today.weekday() + 7)).isoformat()
    if "start tomorrow" in lowered or "starting tomorrow" in lowered:
        return (today + timedelta(days=1)).isoformat()
    if "started yesterday" in lowered or "start yesterday" in lowered:
        return (today - timedelta(days=1)).isoformat()
    if "start today" in lowered or "starting today" in lowered or "started today" in lowered:
        return today.isoformat()

    # Generic start signal without explicit date: assume now.
    start_verbs = [" project started", " task started", " started ", " kickoff ", " kicked off "]
    if any(token in f" {lowered} " for token in start_verbs):
        return today.isoformat()
    return ""


def _looks_like_eta_only_message(message: str) -> bool:
    lowered = (message or "").strip().lower()
    if not lowered:
        return False
    has_eta_signal = any(
        token in lowered
        for token in ["eta", "due", "deliver by", "delivery by", "due date", "deadline"]
    )
    if not has_eta_signal:
        return False
    # If the message clearly carries execution/progress signals, it's not ETA-only.
    non_eta_signals = [
        "blocked",
        "stuck",
        "in progress",
        "progress",
        "working",
        "started",
        "done",
        "completed",
        "on hold",
        "paused",
        "next steps",
        "impact",
    ]
    return not any(token in lowered for token in non_eta_signals)


def _parse_add_task_message(message: str) -> Dict[str, str] | None:
    if not message.lower().strip().startswith("add "):
        return None
    quoted = re.search(r'add\s+["“](.+?)["”]', message, flags=re.IGNORECASE)
    if quoted:
        task_name = quoted.group(1).strip()
    else:
        plain = re.search(r"add\s+(.+?)(?:\s+as\s+a?\s*new task|\s+as\s+new task|,|$)", message, flags=re.IGNORECASE)
        task_name = plain.group(1).strip().strip(".") if plain else ""
    if not task_name:
        return None
    return {"task_name": task_name, "owner": _extract_owner(message), "eta": _extract_eta(message)}


def parse_group_command(message: str) -> Dict[str, Any] | None:
    """Parse: group: <child1>, <child2>, ... => <parent name>
    Returns {parent, children} or None if the message doesn't match."""
    stripped = message.strip()
    if not re.match(r"^group\s*:", stripped, flags=re.IGNORECASE):
        return None
    rest = re.sub(r"^group\s*:\s*", "", stripped, flags=re.IGNORECASE).strip()
    if "=>" not in rest:
        return None
    parts = rest.split("=>", 1)
    children_raw = parts[0].strip()
    parent_name = parts[1].strip().strip('"').strip("'")
    children = [c.strip().strip('"').strip("'") for c in children_raw.split(",") if c.strip()]
    if not parent_name or not children:
        return None
    return {"parent": parent_name, "children": children}


def parse_define_command(message: str) -> Dict[str, str] | None:
    """Parse: define: <task name> => <definition text>
    Returns {task_name, definition} or None if the message doesn't match."""
    stripped = message.strip()
    # Must start with "define:" (case-insensitive)
    if not re.match(r"^define\s*:", stripped, flags=re.IGNORECASE):
        return None
    # Strip the "define:" prefix
    rest = re.sub(r"^define\s*:\s*", "", stripped, flags=re.IGNORECASE).strip()
    # Split on the "=>" separator
    if "=>" not in rest:
        return None
    parts = rest.split("=>", 1)
    task_name = parts[0].strip().strip('"').strip("'")
    definition = parts[1].strip().strip('"').strip("'")
    if not task_name or not definition:
        return None
    return {"task_name": task_name, "definition": definition}


def apply_define_update(
    state: SprintState, task_name: str, definition: str
) -> Tuple[SprintState, List[Task], List[str]]:
    """Set or update the definition of an existing task. Creates the task if it doesn't exist."""
    task = _resolve_task_by_name(state, task_name)
    if task is None:
        # Create a minimal new task with the definition
        task = Task(
            id=_new_task_id(state, task_name),
            task_name=task_name,
            definition=definition,
            created_from="chat_define",
        )
        state.tasks.append(task)
        state.updated_at = datetime.utcnow()
        return state, [task], [f"Created task '{task_name}' with definition"]
    old_def = task.definition or ""
    task.definition = definition
    task.last_updated_at = datetime.utcnow()
    state.updated_at = datetime.utcnow()
    verb = "Updated" if old_def else "Set"
    return state, [task], [f"{verb} definition for '{task_name}'"]


def _parse_link_task_message(message: str) -> Dict[str, str] | None:
    """Parse: link <task name> to <url>  /  link <url> to <task name>"""
    lowered = message.lower().strip()
    if not lowered.startswith("link "):
        return None
    # Extract URL from anywhere in the message
    url_match = re.search(r"(https?://\S+)", message)
    if not url_match:
        return None
    url = url_match.group(1).strip().rstrip(".,;)")
    # Remove the "link " prefix and the URL, leaving the task name
    remaining = message[5:]  # strip "link "
    remaining = remaining.replace(url_match.group(1), "")
    # Remove connector words
    remaining = re.sub(r"\b(to|for|with)\b", "", remaining, flags=re.IGNORECASE)
    task_name = remaining.strip().strip("\"'"".:, ")
    if not task_name:
        return None
    return {"task_name": task_name, "url": url}


def _parse_remove_task_message(message: str) -> str | None:
    lowered = message.lower().strip()
    if not any(lowered.startswith(p) for p in ["remove ", "delete ", "drop "]):
        return None
    quoted = re.search(r'(?:remove|delete|drop)\s+["“](.+?)["”]', message, flags=re.IGNORECASE)
    if quoted:
        return quoted.group(1).strip()
    plain = re.search(r"(?:remove|delete|drop)\s+(?:task\s+)?(.+?)(?:,|$)", message, flags=re.IGNORECASE)
    return plain.group(1).strip().strip(".") if plain else None


def _resolve_task_by_name(state: SprintState, task_name: str) -> Task | None:
    name = task_name.strip().lower()
    for task in state.tasks:
        if task.task_name.strip().lower() == name:
            return task
    for task in state.tasks:
        if name and (name in task.task_name.lower() or task.task_name.lower() in name):
            return task
    return None


def _match_tasks(state: SprintState, message: str) -> List[Task]:
    lowered = message.lower()
    matched = [task for task in state.tasks if task.task_name.lower() in lowered]
    if matched:
        return matched
    words = {w.strip(".,!?") for w in lowered.split() if len(w) > 3}
    fuzzy: List[Task] = []
    for task in state.tasks:
        task_words = {w.strip(".,!?") for w in task.task_name.lower().split()}
        if words.intersection(task_words):
            fuzzy.append(task)
    return fuzzy[:3]


def _infer_status(message: str) -> Tuple[TaskStatus | None, TrafficLight | None]:
    lowered = message.lower()
    lowered_compact = re.sub(r"\s+", "", lowered)
    resumed_patterns = [
        r"\bnow\s+in\s*[-_ ]?progress\b",
        r"\bin\s*[-_ ]?progress\s+now\b",
        r"\binp\s*rogress\b",
        r"\bunblocked\b",
        r"\bno\s+longer\s+blocked\b",
        r"\bnot\s+blocked\s+anymore\b",
        r"\bresumed\b",
        r"\bback\s+on\s+track\b",
        r"\bstarted\s+again\b",
    ]
    if any(re.search(pat, lowered, flags=re.IGNORECASE) for pat in resumed_patterns):
        return TaskStatus.in_progress, TrafficLight.green
    if "inprogress" in lowered_compact or "inprogres" in lowered_compact:
        return TaskStatus.in_progress, TrafficLight.green
    future_completion = any(
        t in lowered
        for t in [
            "will be done",
            "to be done",
            "will be completed",
            "to be completed",
            "will be finished",
            "to be finished",
            "this sprint",
            "within this sprint",
            "in this sprint",
            "will be started",
            "to be started",
            "will be included",
            "to be included",
        ]
    )
    if any(t in lowered for t in ["on hold", "on-hold", "hold for now", "put on hold", "paused", "pause this"]):
        return TaskStatus.on_hold, TrafficLight.yellow
    if any(t in lowered for t in ["follow up", "follow-up", "needs follow up", "needs follow-up", "followup"]):
        return TaskStatus.follow_up, TrafficLight.yellow
    negated_blocked = any(
        phrase in lowered
        for phrase in ["not blocked", "no longer blocked", "unblocked", "not stuck", "unstuck", "resolved blocker"]
    )
    if not negated_blocked and any(t in lowered for t in ["blocked", "stuck"]):
        return TaskStatus.blocked, TrafficLight.red
    if any(t in lowered for t in ["slowly", "delayed", "delay", "at risk", "risky"]):
        return TaskStatus.in_progress, TrafficLight.yellow
    if future_completion:
        return TaskStatus.in_progress, TrafficLight.green
    if any(t in lowered for t in ["done", "finished", "completed"]):
        return TaskStatus.done, TrafficLight.green
    if any(t in lowered for t in ["working", "progress", "started", "doing"]):
        return TaskStatus.in_progress, TrafficLight.green
    return None, None


def _has_explicit_blocker_signal(message: str) -> bool:
    lowered = (message or "").lower()
    if any(token in lowered for token in ["blocked on ", "blocker:", "blocked", "stuck"]):
        if any(
            phrase in lowered
            for phrase in ["not blocked", "no longer blocked", "unblocked", "not stuck", "unstuck"]
        ):
            return False
        return True
    return False


def _light_from_latest_update(latest_update: str, current_light: TrafficLight) -> TrafficLight:
    text = (latest_update or "").lower()
    if not text:
        return current_light
    if any(token in text for token in ["blocked", "stuck", "cannot", "can't", "unable", "failed", "failure"]):
        return TrafficLight.red
    if any(token in text for token in ["slow", "delayed", "awaiting", "waiting", "at risk", "dependency"]):
        return TrafficLight.yellow
    if any(
        token in text
        for token in [
            "moving forward",
            "on track",
            "in progress",
            "working on",
            "started",
            "will be deployed today",
            "deploy today",
            "deployed today",
            "implemented",
            "completed",
            "done",
        ]
    ):
        return TrafficLight.green
    return current_light


def _enforce_completed_is_green(task: Task) -> None:
    if task.status == TaskStatus.done:
        task.traffic_light = TrafficLight.green
    elif task.status == TaskStatus.on_hold:
        task.traffic_light = TrafficLight.yellow


def apply_recency_light_policy(state: SprintState, stale_hours: int = 24) -> None:
    now = datetime.utcnow()
    for task in state.tasks:
        task.traffic_light = _light_from_latest_update(task.latest_update, task.traffic_light)
        _enforce_completed_is_green(task)
        if task.status == TaskStatus.done:
            continue
        age_hours = (now - task.last_updated_at).total_seconds() / 3600.0
        if age_hours > stale_hours and task.traffic_light != TrafficLight.red:
            task.traffic_light = TrafficLight.yellow


def _extract_blockers(message: str) -> List[str]:
    lowered = message.lower()
    if "blocked on " in lowered:
        i = lowered.index("blocked on ") + len("blocked on ")
        val = message[i:].strip().strip(".")
        return [val] if val else []
    if "blocker:" in lowered:
        i = lowered.index("blocker:") + len("blocker:")
        val = message[i:].strip().strip(".")
        return [val] if val else []
    return []


def _strip_status_noise(text: str) -> str:
    cleaned = (text or "").strip()
    patterns = [
        r"\bnot[_\s-]?started\b",
        r"\bin[_\s-]?progress\b",
        r"\bon[_\s-]?hold\b",
        r"\bfollow[_\s-]?up\b",
        r"\bpaused\b",
        r"\bblocked\b",
        r"\bdone\b",
        r"\bcompleted\b",
        r"\bgreen\b",
        r"\byellow\b",
        r"\bred\b",
        r"\btraffic[_\s-]?light\b",
        r"\bstatus\b",
    ]
    for pat in patterns:
        cleaned = re.sub(pat, "", cleaned, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cleaned).strip(" .,:;-")


def _apply_amazon_update_principles(task_name: str, raw_update: str, status: TaskStatus, blockers: List[str]) -> str:
    text = (raw_update or "").strip()
    if not text:
        if status == TaskStatus.done:
            return "Delivered planned scope and advanced the milestone.\n\nNext Steps: monitor QA and rollout."
        if blockers:
            return "Dependency unresolved and delivery risk remains.\n\nNext Steps: unblock with owner."
        return "Execution progressed on planned scope and moved sprint goals forward.\n\nNext Steps: continue the next checkpoint."

    text = re.sub(re.escape(task_name or ""), "", text, flags=re.IGNORECASE)
    text = _strip_status_noise(text)
    text = re.sub(r"\bimpact\s*:\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bnext\s*:\s*", "Next Steps: ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bnext step[s]?\s*:\s*", "Next Steps: ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" .")

    next_steps = ""
    next_match = re.search(r"Next Steps:\s*(.+)$", text, flags=re.IGNORECASE)
    if next_match:
        next_steps = next_match.group(1).strip().strip(".")
        text = text[: next_match.start()].strip().strip(".")

    if blockers:
        blocker_text = "; ".join([b.strip() for b in blockers if b.strip()]) or text or "Dependency unresolved"
        next_value = next_steps or "unblock dependency and confirm revised checkpoint"
        return f"Blocked by {blocker_text} and timeline is at risk.\n\nNext Steps: {next_value}."
    if status == TaskStatus.done:
        detail = text or "Delivered planned scope and moved scope to completed"
        next_value = next_steps or "validate quality and monitor rollout"
        return f"{detail}.\n\nNext Steps: {next_value}."

    lowered = text.lower()
    if any(token in lowered for token in ["awaiting", "waiting", "pending", "dependency"]):
        detail = text or "Awaiting dependency handoff; progress is paused on dependent work"
        next_value = next_steps or "follow up owner and resume execution"
        return f"{detail}.\n\nNext Steps: {next_value}."

    detail = text or "Execution progressed on planned scope and sprint goals moved forward"
    next_value = next_steps or "continue to the next checkpoint"
    return f"{detail}.\n\nNext Steps: {next_value}."


def apply_daily_update(state: SprintState, message: str) -> Tuple[SprintState, List[Task], List[str]]:
    message_start_date = _extract_start_date(message)
    add_payload = _parse_add_task_message(message)
    if add_payload:
        existing = _resolve_task_by_name(state, add_payload["task_name"])
        if existing is None:
            new_task = Task(
                id=_new_task_id(state, add_payload["task_name"]),
                task_name=add_payload["task_name"],
                definition=add_payload["task_name"],
                owner=add_payload["owner"],
                eta=add_payload["eta"],
                start_date=message_start_date or None,
                latest_update=message,
                next_expected_checkpoint=(date.today() + timedelta(days=1)).isoformat(),
            )
            state.tasks.append(new_task)
            changed = [new_task]
            changes = [f"Added task '{new_task.task_name}'"]
        else:
            if add_payload["owner"]:
                existing.owner = add_payload["owner"]
            if add_payload["eta"]:
                existing.eta = add_payload["eta"]
            if message_start_date:
                existing.start_date = message_start_date
            existing.latest_update = message
            existing.last_updated_at = datetime.utcnow()
            changed = [existing]
            changes = [f"Task '{existing.task_name}' already exists; metadata updated"]
        follow = build_follow_ups(state)
        state.updated_at = datetime.utcnow()
        state.daily_logs.append(DailyLogEntry(user_message=message, applied_changes=changes, follow_up_prompts=follow))
        return state, changed, follow

    remove_name = _parse_remove_task_message(message)
    if remove_name:
        removed = [t for t in state.tasks if remove_name.lower() in t.task_name.lower() or t.task_name.lower() == remove_name.lower()]
        state.tasks = [t for t in state.tasks if t not in removed]
        changes = [f"Removed task '{t.task_name}'" for t in removed] or ["No matching task removed"]
        follow = build_follow_ups(state)
        state.updated_at = datetime.utcnow()
        state.daily_logs.append(DailyLogEntry(user_message=message, applied_changes=changes, follow_up_prompts=follow))
        return state, removed, follow

    link_payload = _parse_link_task_message(message)
    if link_payload:
        task = _resolve_task_by_name(state, link_payload["task_name"])
        if task:
            task.task_link = link_payload["url"]
            task.last_updated_at = datetime.utcnow()
            changes = [f"Linked '{task.task_name}' to {link_payload['url']}"]
            follow = build_follow_ups(state)
            state.updated_at = datetime.utcnow()
            state.daily_logs.append(DailyLogEntry(user_message=message, applied_changes=changes, follow_up_prompts=follow))
            return state, [task], follow

    targets = _match_tasks(state, message)
    if not targets and len(state.tasks) == 1:
        targets = [state.tasks[0]]
    if not targets:
        follow_ups = build_follow_ups(state)
        state.updated_at = datetime.utcnow()
        state.daily_logs.append(
            DailyLogEntry(
                user_message=message,
                applied_changes=["No matching task found; no update applied."],
                follow_up_prompts=follow_ups,
            )
        )
        return state, [], follow_ups
    inferred_status, inferred_light = _infer_status(message)
    blockers = _extract_blockers(message)
    eta_only_message = _looks_like_eta_only_message(message)
    eta_text = _extract_eta(message)
    today = date.today()
    changed_tasks: List[Task] = []
    changes: List[str] = []
    for task in targets:
        base_start = _parse_iso_date(getattr(task, "start_date", "") or "") or today
        start_date_text = _extract_start_date(message, base_date=base_start)
        if eta_only_message and eta_text:
            if task.eta != eta_text:
                task.eta = eta_text
                changed_tasks.append(task)
                changes.append(f"Updated ETA for '{task.task_name}' to {eta_text}")
            if start_date_text and task.start_date != start_date_text:
                task.start_date = start_date_text
                if task not in changed_tasks:
                    changed_tasks.append(task)
                    changes.append(f"Updated start date for '{task.task_name}' to {start_date_text}")
            continue
        if inferred_status:
            task.status = inferred_status
        if inferred_light:
            task.traffic_light = inferred_light
        if blockers and task.status != TaskStatus.on_hold:
            task.blockers = blockers
            task.status = TaskStatus.blocked
            task.traffic_light = TrafficLight.red
            task.do_not_ask_until = date.today() + timedelta(days=1)
        elif inferred_status in {TaskStatus.in_progress, TaskStatus.done, TaskStatus.on_hold, TaskStatus.follow_up}:
            # Once a task is explicitly resumed/completed/parked, previous blockers should not keep it sticky-red.
            task.blockers = []
        elif task.status == TaskStatus.done:
            task.do_not_ask_until = date.today() + timedelta(days=3)
        else:
            task.do_not_ask_until = date.today() + timedelta(days=2)
        task.latest_update = _apply_amazon_update_principles(
            task_name=task.task_name,
            raw_update=message,
            status=task.status,
            blockers=task.blockers,
        )
        task.traffic_light = _light_from_latest_update(task.latest_update, task.traffic_light)
        _enforce_completed_is_green(task)
        if start_date_text:
            task.start_date = start_date_text
        task.last_updated_at = datetime.utcnow()
        task.next_expected_checkpoint = (date.today() + timedelta(days=1)).isoformat()
        changed_tasks.append(task)
        changes.append(f"Updated '{task.task_name}' to {task.status.value}/{task.traffic_light.value}")
    follow_ups = build_follow_ups(state)
    state.updated_at = datetime.utcnow()
    state.daily_logs.append(DailyLogEntry(user_message=message, applied_changes=changes, follow_up_prompts=follow_ups))
    return state, changed_tasks, follow_ups


def apply_structured_updates(state: SprintState, message: str, updates: List[Dict[str, Any]]) -> Tuple[SprintState, List[Task], List[str]]:
    status_map = {s.value: s for s in TaskStatus}
    light_map = {l.value: l for l in TrafficLight}
    changed_tasks: List[Task] = []
    changes: List[str] = []

    msg_status, msg_light = _infer_status(message)
    has_explicit_blocker_signal = _has_explicit_blocker_signal(message)

    for item in updates:
        if not isinstance(item, dict):
            continue
        action = str(item.get("action", "update")).strip().lower()
        task_name = str(item.get("task_name", "")).strip()
        owner = str(item.get("owner", "")).strip()
        eta = str(item.get("eta", "")).strip()
        definition = str(item.get("definition", "")).strip()
        task_link = str(item.get("task_link", "")).strip()

        if action == "add":
            if not task_name:
                continue
            existing = _resolve_task_by_name(state, task_name)
            if existing is None:
                task = Task(
                    id=_new_task_id(state, task_name),
                    task_name=task_name,
                    definition=definition or task_name,
                    owner=owner,
                    eta=eta,
                    latest_update=str(item.get("latest_update", "")).strip() or message,
                    next_expected_checkpoint=str(item.get("next_expected_checkpoint", "")).strip() or (date.today() + timedelta(days=1)).isoformat(),
                    start_date=_extract_start_date(message) or None,
                )
                state.tasks.append(task)
                changed_tasks.append(task)
                changes.append(f"Added task '{task.task_name}'")
            else:
                if owner:
                    existing.owner = owner
                if eta:
                    existing.eta = eta
                existing.latest_update = str(item.get("latest_update", "")).strip() or message
                existing_start = _parse_iso_date(getattr(existing, "start_date", "") or "") or date.today()
                parsed_start = _extract_start_date(message, base_date=existing_start)
                if parsed_start:
                    existing.start_date = parsed_start
                existing.last_updated_at = datetime.utcnow()
                changed_tasks.append(existing)
                changes.append(f"Task '{existing.task_name}' already exists; metadata updated")
            continue

        if action == "remove":
            target = _resolve_task_by_name(state, task_name)
            if target is None:
                continue
            state.tasks = [t for t in state.tasks if t.id != target.id]
            changed_tasks.append(target)
            changes.append(f"Removed task '{target.task_name}'")
            continue

        task = _resolve_task_by_name(state, task_name)
        if task is None:
            continue
        status = status_map.get(str(item.get("status", "")).strip().lower())
        light = light_map.get(str(item.get("traffic_light", "")).strip().lower())
        blockers = item.get("blockers", [])
        latest_update = str(item.get("latest_update", "")).strip()
        next_checkpoint = str(item.get("next_expected_checkpoint", "")).strip()
        has_blockers_signal = isinstance(blockers, list) and len([str(b).strip() for b in blockers if str(b).strip()]) > 0
        has_status_signal = status is not None
        has_light_signal = light is not None
        has_latest_signal = bool(latest_update)
        has_definition_signal = bool(definition)
        has_owner_signal = bool(owner)
        has_next_checkpoint_signal = bool(next_checkpoint)
        do_not_ask_days = item.get("do_not_ask_days")
        has_do_not_ask_signal = isinstance(do_not_ask_days, int) and do_not_ask_days >= 0
        eta_only_update = bool(eta) and not any(
            [
                has_status_signal,
                has_light_signal,
                has_blockers_signal,
                has_latest_signal,
                has_definition_signal,
                has_owner_signal,
                has_next_checkpoint_signal,
                has_do_not_ask_signal,
            ]
        )
        task_start_base = _parse_iso_date(getattr(task, "start_date", "") or "") or date.today()
        msg_start_date = _extract_start_date(message, base_date=task_start_base)
        if eta_only_update:
            if task.eta != eta:
                task.eta = eta
                changed_tasks.append(task)
                changes.append(f"Updated ETA for '{task.task_name}' to {eta}")
            if msg_start_date and task.start_date != msg_start_date:
                task.start_date = msg_start_date
                if task not in changed_tasks:
                    changed_tasks.append(task)
                    changes.append(f"Updated start date for '{task.task_name}' to {msg_start_date}")
            continue
        if (
            msg_status in {TaskStatus.in_progress, TaskStatus.done, TaskStatus.on_hold, TaskStatus.follow_up}
            and status == TaskStatus.blocked
            and not has_explicit_blocker_signal
        ):
            # User's explicit "resume/progress" statement should win over an over-conservative LLM blocked label.
            status = msg_status
            if msg_light is not None:
                light = msg_light

        progress_override_active = (
            msg_status in {TaskStatus.in_progress, TaskStatus.done, TaskStatus.on_hold, TaskStatus.follow_up}
            and not has_explicit_blocker_signal
        )

        if status is not None:
            task.status = status
        if light is not None:
            task.traffic_light = light
        if isinstance(blockers, list):
            parsed_blockers = [str(b).strip() for b in blockers if str(b).strip()]
            task.blockers = parsed_blockers
            if progress_override_active and task.status in {
                TaskStatus.in_progress,
                TaskStatus.done,
                TaskStatus.on_hold,
                TaskStatus.follow_up,
            }:
                # Keep progress status authoritative unless user explicitly says blocked/stuck.
                task.blockers = []
            elif task.blockers and task.status not in (TaskStatus.done, TaskStatus.on_hold):
                task.status = TaskStatus.blocked
                task.traffic_light = TrafficLight.red
        elif status in {TaskStatus.in_progress, TaskStatus.done, TaskStatus.on_hold, TaskStatus.follow_up}:
            task.blockers = []
        elif msg_status is not None:
            task.status = msg_status
            if msg_light is not None:
                task.traffic_light = msg_light
        task.latest_update = _apply_amazon_update_principles(
            task_name=task.task_name,
            raw_update=latest_update or message,
            status=task.status,
            blockers=task.blockers,
        )
        task.traffic_light = _light_from_latest_update(task.latest_update, task.traffic_light)
        _enforce_completed_is_green(task)
        if definition:
            task.definition = definition
        if task_link:
            task.task_link = task_link
        if owner:
            task.owner = owner
        if eta:
            task.eta = eta
        if msg_start_date:
            task.start_date = msg_start_date
        task.next_expected_checkpoint = next_checkpoint or (date.today() + timedelta(days=1)).isoformat()
        if isinstance(do_not_ask_days, int) and do_not_ask_days >= 0:
            task.do_not_ask_until = date.today() + timedelta(days=do_not_ask_days)
        elif task.status == TaskStatus.done:
            task.do_not_ask_until = date.today() + timedelta(days=3)
        elif task.status == TaskStatus.blocked:
            task.do_not_ask_until = date.today() + timedelta(days=1)
        else:
            task.do_not_ask_until = date.today() + timedelta(days=2)
        task.last_updated_at = datetime.utcnow()
        changed_tasks.append(task)
        changes.append(f"Updated '{task.task_name}' to {task.status.value}/{task.traffic_light.value}")

    follow = build_follow_ups(state)
    state.updated_at = datetime.utcnow()
    state.daily_logs.append(DailyLogEntry(user_message=message, applied_changes=changes, follow_up_prompts=follow))
    return state, changed_tasks, follow


def build_follow_ups(state: SprintState) -> List[str]:
    today = date.today()
    prompts: List[str] = []
    for task in state.tasks:
        if task.do_not_ask_until and today < task.do_not_ask_until:
            continue
        stale_days = (datetime.utcnow() - task.last_updated_at).days
        if task.traffic_light == TrafficLight.red:
            prompts.append(f"What do you need to unblock '{task.task_name}'?")
        elif task.status == TaskStatus.blocked and stale_days >= 1:
            prompts.append(f"Any movement on blocker for '{task.task_name}'?")
        elif task.traffic_light == TrafficLight.yellow and stale_days >= 2:
            prompts.append(f"Can you share progress on '{task.task_name}'?")
        elif task.traffic_light == TrafficLight.green and stale_days >= 3:
            prompts.append(f"Quick checkpoint for '{task.task_name}'?")
    return prompts
