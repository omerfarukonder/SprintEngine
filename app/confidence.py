from __future__ import annotations

import re


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def compute_query_confidence(
    question: str,
    answer: str,
    *,
    has_known_task_ref: bool = False,
    used_llm: bool = False,
    llm_intent: str = "",
) -> float:
    text = (question or "").strip().lower()
    out = 0.58

    if used_llm:
        out += 0.07
        if llm_intent == "clarify":
            out -= 0.05

    if "?" in text:
        out += 0.04
    if any(token in text for token in ["history", "timeline", "what happened", "show", "list", "summarize"]):
        out += 0.06
    if has_known_task_ref:
        out += 0.12

    ans = (answer or "").lower()
    if ans.startswith("timeline for **"):
        out += 0.12
    if "could not find the task" in ans or "no timeline events found" in ans:
        out -= 0.16
    if ans.startswith("i can help with tasks, risks, and yesterday logs"):
        out -= 0.18

    return _clamp(out, 0.30, 0.95)


def compute_update_confidence(
    question: str,
    *,
    changed_count: int,
    has_known_task_ref: bool = False,
    used_llm: bool = False,
    used_structured: bool = False,
    used_fallback: bool = False,
) -> float:
    out = 0.52
    lowered = (question or "").lower()

    if changed_count > 0:
        out += 0.18
        out += min(0.12, changed_count * 0.04)
    else:
        out -= 0.15

    if has_known_task_ref:
        out += 0.09
    if used_llm:
        out += 0.06
    if used_structured:
        out += 0.05
    if used_fallback:
        out -= 0.03

    if re.search(r"\b(add|create|remove|delete|drop|link)\b", lowered):
        out += 0.04
    if any(t in lowered for t in ["blocked", "in progress", "done", "on hold", "eta", "owner"]):
        out += 0.03

    return _clamp(out, 0.28, 0.97)


def compute_faq_confidence(answer: str) -> float:
    ans = (answer or "").strip().lower()
    out = 0.76
    if any(ans.startswith(prefix) for prefix in ["added faq", "saved answer", "archived q", "updated the question"]):
        out += 0.10
    if ans.startswith("describe what you want"):
        out -= 0.12
    return _clamp(out, 0.50, 0.95)


def compute_kb_command_confidence(*, success: bool) -> float:
    return 0.78 if success else 0.55


def compute_capture_only_confidence() -> float:
    return 0.74
