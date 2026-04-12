from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from .faq_store import (
    FAQ_MD,
    active_items_in_order,
    load_faq_items,
    new_faq_item,
    save_faq_items,
)
from .llm import SprintCopilotLLM
from .models import FaqItem

_copilot = SprintCopilotLLM()


def _refine_faq(text: str, kind: str) -> str:
    return _copilot.refine_faq_text(text, kind)


def _active_by_display_index(items: List[FaqItem], n: int) -> FaqItem | None:
    active = active_items_in_order(items)
    if n < 1 or n > len(active):
        return None
    return active[n - 1]


def _save_answer_for_active_q(items: List[FaqItem], n: int, answer_raw: str) -> str:
    """Persist answer for display index n (Qn). Returns user-facing status message."""
    answer_text = _refine_faq(answer_raw.strip(), "answer")
    target = _active_by_display_index(items, n)
    if target is None:
        return f"There is no active Q{n}. Use the FAQ list on the right to see current numbers."
    for it in items:
        if it.id == target.id:
            it.answer = answer_text
            break
    save_faq_items(items)
    return f"Saved answer for Q{n}."


def _try_numbered_answer_prefix(msg: str, items: List[FaqItem]) -> Optional[str]:
    """
    If the message leads with a FAQ number (Q2, Question 3, #1, …), treat the rest as the answer.
    Works without OpenAI. Runs before the LLM so behavior is predictable.
    """
    s = msg.strip()
    if not s:
        return None
    # Let explicit archive / add-question commands fall through to their handlers
    if re.match(r"(?is)^archive\s+", s) or re.match(r"(?is)^add\s+(?:this\s+)?question\s*:", s):
        return None

    m = re.match(
        r"(?is)^(?:\s*)(?:answer\s+(?:for|to)\s+)?(?:q|question)\s*(\d+)\s*[:.;,\-–]\s*(.+)$",
        s,
    )
    if not m:
        m = re.match(r"(?is)^(?:\s*)#(\d+)\s*[:.;,\-–]\s*(.+)$", s)
    if m:
        n = int(m.group(1))
        body = (m.group(2) or "").strip()
        if not body:
            return None
        return _save_answer_for_active_q(items, n, body)

    # "Q2 we use Postgres" / "Question 2 rollout is Friday" — number then whitespace, then answer (no colon)
    m = re.match(r"(?is)^(?:\s*)(?:q|question)\s*(\d+)\s+(\S.*)$", s)
    if m:
        n = int(m.group(1))
        body = (m.group(2) or "").strip()
        if len(body) < 2:
            return None
        return _save_answer_for_active_q(items, n, body)

    return None


def _active_rows_for_llm(items: List[FaqItem]) -> List[Dict[str, Any]]:
    active = active_items_in_order(items)
    rows: List[Dict[str, Any]] = []
    for i, it in enumerate(active, start=1):
        excerpt = (it.answer or "").strip()
        if len(excerpt) > 280:
            excerpt = excerpt[:277] + "..."
        rows.append({"n": i, "question": (it.question or "").strip(), "answer_excerpt": excerpt})
    return rows


def _execute_llm_faq_intent(intent: Dict[str, Any], items: List[FaqItem], raw_message: str) -> Optional[str]:
    action = str(intent.get("action", "")).strip().lower()
    target_q = intent.get("target_q")
    q_idx: Optional[int] = None
    if target_q is not None:
        try:
            q_idx = int(target_q)
        except (TypeError, ValueError):
            q_idx = None
    qtext = str(intent.get("question_text", "") or "").strip()
    atext = str(intent.get("answer_text", "") or "").strip()
    clarify = str(intent.get("clarify_message", "") or "").strip()

    active = active_items_in_order(items)
    n_active = len(active)

    def resolve_target() -> FaqItem | None:
        if q_idx is None or q_idx < 1 or q_idx > n_active:
            return None
        return _active_by_display_index(items, q_idx)

    if action == "clarify":
        return clarify or "Which FAQ should this apply to (Q1, Q2, …), or say a bit more about what you want to add?"

    if action == "add_question":
        q = qtext or raw_message.strip()
        if not q:
            return None
        q = _refine_faq(q, "question")
        items.append(new_faq_item(q))
        if atext:
            new_item = items[-1]
            new_item.answer = _refine_faq(atext, "answer")
        save_faq_items(items)
        n = len(active_items_in_order(items))
        if atext:
            return f"Added FAQ Q{n} with an answer saved."
        return f"Added FAQ Q{n}: {q}"

    if action == "set_answer":
        if not atext:
            return None
        t = resolve_target()
        if t is None:
            return None
        atext = _refine_faq(atext, "answer")
        for it in items:
            if it.id == t.id:
                it.answer = atext
                break
        save_faq_items(items)
        return f"Saved answer for Q{q_idx}."

    if action == "update_question":
        if not qtext:
            return None
        t = resolve_target()
        if t is None:
            return None
        qtext = _refine_faq(qtext, "question")
        for it in items:
            if it.id == t.id:
                it.question = qtext
                break
        save_faq_items(items)
        return f"Updated the question text for Q{q_idx}."

    if action == "archive":
        t = resolve_target()
        if t is None:
            return None
        for it in items:
            if it.id == t.id:
                it.archived = True
                it.archived_at = datetime.utcnow().isoformat()
                break
        save_faq_items(items)
        return f"Archived Q{q_idx}. It is listed under Archived in {FAQ_MD.name}."

    return None


def _try_natural_faq(msg: str, items: List[FaqItem]) -> Optional[str]:
    if not _copilot.enabled:
        return None
    intent = _copilot.interpret_faq_intent(msg, _active_rows_for_llm(items))
    if not intent:
        return None
    return _execute_llm_faq_intent(intent, items, msg)


def process_faq_message(message: str) -> str:
    """Apply FAQ commands; persist. Returns assistant reply text."""
    msg = message.strip()
    if not msg:
        return (
            "Describe what you want: add a FAQ, answer or revise an item, rephrase a question, or archive one. "
            "Tip: start with a number like Q2: … or Question 2 — … to save an answer to that FAQ. "
            "With OpenAI enabled, plain language works too; phrases like “add this question: …” still work."
        )

    items = load_faq_items()

    numbered = _try_numbered_answer_prefix(msg, items)
    if numbered is not None:
        return numbered

    natural = _try_natural_faq(msg, items)
    if natural is not None:
        return natural

    # Explicit add: "add this question: ..." / "add question: ..."
    m_add = re.match(
        r"(?is)^add\s+(?:this\s+)?question\s*:\s*(.+)$",
        msg,
    )
    if m_add:
        q = m_add.group(1).strip()
        if not q:
            return "Add a question after the colon, e.g. add this question: Where are we on task X?"
        q = _refine_faq(q, "question")
        items.append(new_faq_item(q))
        save_faq_items(items)
        n = len(active_items_in_order(items))
        return f"Added FAQ Q{n}: {q}"

    # Answer: "for the q1, the answer is ..." / "for question 1, the answer is ..."
    m_ans = re.match(
        r"(?is)^for\s+(?:the\s+)?q\s*(\d+)\s*,?\s*(?:the\s+)?answer\s+is\s+(.+)$",
        msg,
    )
    if not m_ans:
        m_ans = re.match(
            r"(?is)^for\s+question\s*(\d+)\s*,?\s*(?:the\s+)?answer\s+is\s+(.+)$",
            msg,
        )
    if m_ans:
        n = int(m_ans.group(1))
        return _save_answer_for_active_q(items, n, (m_ans.group(2) or "").strip())

    # Archive: "archive q1" / "archive question 2" / "archive 1"
    m_arc = re.match(r"(?is)^archive\s+(?:q|question)\s*(\d+)\s*$", msg)
    if not m_arc:
        m_arc = re.match(r"(?is)^archive\s+(\d+)\s*$", msg)
    if m_arc:
        n = int(m_arc.group(1))
        target = _active_by_display_index(items, n)
        if target is None:
            return f"There is no active Q{n} to archive."
        for it in items:
            if it.id == target.id:
                it.archived = True
                it.archived_at = datetime.utcnow().isoformat()
                break
        save_faq_items(items)
        return f"Archived Q{n}. It is listed under Archived in {FAQ_MD.name}."

    # Natural language: whole message becomes a new question
    q = _refine_faq(msg, "question")
    items.append(new_faq_item(q))
    save_faq_items(items)
    n = len(active_items_in_order(items))
    return f"Added FAQ Q{n}: {q}"
