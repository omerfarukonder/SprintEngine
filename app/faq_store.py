from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import List

from .models import FaqItem


BASE_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BASE_DIR / "workspace"
FAQ_JSON = WORKSPACE_DIR / "sprint_faq.json"
FAQ_MD = WORKSPACE_DIR / "sprint_faq.md"


def ensure_faq_workspace() -> None:
    WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)


def load_faq_items() -> List[FaqItem]:
    ensure_faq_workspace()
    if not FAQ_JSON.exists():
        return []
    raw = json.loads(FAQ_JSON.read_text(encoding="utf-8"))
    items = raw.get("items", [])
    if not isinstance(items, list):
        return []
    out: List[FaqItem] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        try:
            out.append(FaqItem.model_validate(row))
        except Exception:
            continue
    return out


def save_faq_items(items: List[FaqItem]) -> None:
    ensure_faq_workspace()
    payload = {"items": [i.model_dump(mode="json") for i in items]}
    FAQ_JSON.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    FAQ_MD.write_text(_render_faq_markdown(items), encoding="utf-8")


def new_faq_item(question: str) -> FaqItem:
    return FaqItem(
        id=str(uuid.uuid4()),
        question=question.strip(),
        answer="",
        archived=False,
        archived_at=None,
        created_at=datetime.utcnow(),
    )


def active_items_in_order(items: List[FaqItem]) -> List[FaqItem]:
    return [x for x in items if not x.archived]


def archived_items_in_order(items: List[FaqItem]) -> List[FaqItem]:
    arch = [x for x in items if x.archived]
    arch.sort(key=lambda x: (x.archived_at or "", x.id))
    return arch


def select_archived_for_context(user_question: str, archived: List[FaqItem]) -> List[FaqItem]:
    """Pick archived FAQ rows that may be relevant to the user's message (for chat, not the sidebar)."""
    if not archived:
        return []
    ql = user_question.lower()
    hints = (
        "archived faq",
        "archived question",
        "old faq",
        "previous faq",
        "faq archive",
    )
    if any(p in ql for p in hints):
        return archived[-8:]
    qw = set(re.findall(r"[a-z0-9]+", ql))
    qw = {w for w in qw if len(w) > 2}
    if not qw:
        return []
    scored: List[tuple[int, FaqItem]] = []
    for it in archived:
        hay = f"{it.question} {it.answer}".lower()
        hay_words = set(re.findall(r"[a-z0-9]+", hay))
        overlap = len(qw & hay_words)
        scored.append((overlap, it))
    scored.sort(key=lambda x: (-x[0], x[1].id))
    best = [x for x in scored if x[0] > 0]
    if best:
        return [x[1] for x in best[:6]]
    return []


def format_archived_faq_block(selected: List[FaqItem]) -> str:
    if not selected:
        return ""
    lines = []
    for it in selected:
        q = (it.question or "").strip() or "(no question)"
        a = (it.answer or "").strip() or "(no answer recorded)"
        lines.append(f"- Q: {q}\n  A: {a}")
    return (
        "Archived FAQs (not listed in the FAQ panel; use when answering if relevant):\n" + "\n".join(lines)
    )


def _render_faq_markdown(items: List[FaqItem]) -> str:
    lines = [
        "# Sprint FAQ",
        "",
        "_This file is regenerated when you add, answer, or archive FAQs in the app (FAQ chat mode)._",
        "",
        "## Active",
        "",
    ]
    active = active_items_in_order(items)
    if not active:
        lines.append("_No active questions._")
        lines.append("")
    for i, it in enumerate(active, start=1):
        lines.append(f"### Q{i}")
        lines.append(f"**Question:** {it.question}")
        ans = (it.answer or "").strip()
        lines.append(f"**Answer:** {ans if ans else '*(no answer yet)*'}")
        lines.append("")

    lines.append("## Archived")
    lines.append("")
    arch = archived_items_in_order(items)
    if not arch:
        lines.append("_No archived questions._")
        lines.append("")
    for it in arch:
        when = (it.archived_at or "")[:10] if it.archived_at else "?"
        lines.append(f"### Archived · {when}")
        lines.append(f"**Question:** {it.question}")
        ans = (it.answer or "").strip()
        lines.append(f"**Answer:** {ans if ans else '*(no answer)*'}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"
