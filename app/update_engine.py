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
    if any(t in lowered for t in ["blocked", "stuck"]):
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
    if not targets and state.tasks:
        targets = [state.tasks[0]]
    inferred_status, inferred_light = _infer_status(message)
    blockers = _extract_blockers(message)
    eta_only_message = _looks_like_eta_only_message(message)
    eta_text = _extract_eta(message)
    changed_tasks: List[Task] = []
    changes: List[str] = []
    for task in targets:
        if eta_only_message and eta_text:
            if task.eta != eta_text:
                task.eta = eta_text
                changed_tasks.append(task)
                changes.append(f"Updated ETA for '{task.task_name}' to {eta_text}")
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
        if eta_only_update:
            if task.eta != eta:
                task.eta = eta
                changed_tasks.append(task)
                changes.append(f"Updated ETA for '{task.task_name}' to {eta}")
            continue
        if status is not None:
            task.status = status
        if light is not None:
            task.traffic_light = light
        if isinstance(blockers, list):
            task.blockers = [str(b).strip() for b in blockers if str(b).strip()]
            if task.blockers and task.status not in (TaskStatus.done, TaskStatus.on_hold):
                task.status = TaskStatus.blocked
                task.traffic_light = TrafficLight.red
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
