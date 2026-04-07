from __future__ import annotations

from datetime import datetime
from pathlib import Path
import hashlib
import re

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .llm import SprintCopilotLLM
from .memory import (
    append_overall_kb_event,
    overall_kb_hybrid_search,
    overwrite_overall_kb_events,
    overwrite_overall_kb_events_by_ids,
    reactivate_overall_kb_events_by_reason,
    search_overall_kb_events,
    overall_kb_debug_payload,
    rebuild_overall_kb_vectors,
    render_overall_kb_citations,
)
from .models import ChatRequest, ChatResponse, ImportDocxRequest, SprintState
from pydantic import BaseModel
from .parser import initialize_state_from_plan
from .plan_importer import extract_sprint_name_from_markdown, extract_table_from_docx, extract_text_from_docx, fallback_markdown_from_raw_text, table_rows_to_markdown
from .queries import answer_query, generate_status_table
from .report_writer import generate_sprint_report_markdown
from .storage import (
    LATEST_SPRINT_REPORT_FILE,
    PLAN_FILE,
    TABLES_DIR,
    append_daily_markdown_log,
    create_initialize_backup,
    ensure_workspace,
    list_initialize_backups,
    load_latest_sprint_report,
    load_state,
    restore_latest_initialize_backup,
    save_latest_sprint_report,
    save_state,
)
from .update_engine import apply_daily_update, apply_structured_updates, apply_recency_light_policy, build_follow_ups


app = FastAPI(title="Sprint Engine", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")
llm = SprintCopilotLLM()


class SprintReportSaveRequest(BaseModel):
    content: str


class InitializeRequest(BaseModel):
    mode: str = "sync_missing"
    confirm_text: str = ""


def _source_message_id(message: str) -> str:
    payload = f"{datetime.utcnow().isoformat()}|{message}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:20]


def _capture_overall_kb_from_message(state, message: str) -> bool:
    captured_any = False
    if not llm.enabled:
        return captured_any
    extracted = llm.extract_overall_knowledge(state, message)
    if not extracted:
        return captured_any
    source_id = _source_message_id(message)
    for item in extracted.get("items", []):
        confidence = float(item.get("confidence", 0.0))
        if confidence < 0.7:
            continue
        append_overall_kb_event(
            text=str(item.get("text", "")).strip(),
            knowledge_type=str(item.get("knowledge_type", "")).strip().lower(),
            scope=str(item.get("scope", "")).strip().lower(),
            confidence=confidence,
            source_message_id=source_id,
            source="chat_intent",
            metadata={"raw_message": message, "sprint_name": state.sprint_name},
        )
        captured_any = True
    return captured_any


def _message_references_known_task(state, message: str) -> bool:
    lowered = message.lower()
    for task in state.tasks:
        name = task.task_name.strip().lower()
        if name and name in lowered:
            return True
    return False


def _looks_like_query(message: str) -> bool:
    lowered = message.lower().strip()
    if "?" in lowered:
        return True
    contains_query_phrases = any(
        token in lowered
        for token in [
            "status table",
            "all tasks",
            "tasks in the status table",
            "list me",
            "lsit me",
        ]
    )
    if contains_query_phrases:
        return True
    query_markers = (
        "what ",
        "what's ",
        "which ",
        "why ",
        "how ",
        "show ",
        "list ",
        "lsit ",
        "summarize ",
        "tell me ",
        "can you explain",
    )
    return lowered.startswith(query_markers)


def _looks_like_explicit_task_command(message: str) -> bool:
    lowered = message.lower().strip()
    direct_prefixes = (
        "add ",
        "create ",
        "remove ",
        "delete ",
        "drop ",
        "link ",
        "new task",
        "this is a new task",
    )
    if lowered.startswith(direct_prefixes):
        return True
    explicit_markers = (
        "add this task",
        "add task",
        "create task",
        "new task:",
        "take it as a new task",
        "make this a new task",
    )
    return any(marker in lowered for marker in explicit_markers)


def _augment_answer_with_overall_kb(question: str, answer: str) -> str:
    results = overall_kb_hybrid_search(question, top_k=3, active_only=True)
    if not results:
        return answer
    strong = [(score, event) for score, event in results if score >= 0.35]
    if not strong:
        return answer
    snippets = []
    events = []
    for _, event in strong[:2]:
        text = str(event.get("text", "")).strip()
        if not text:
            continue
        snippets.append(f"- {text}")
        events.append(event)
    if not snippets:
        return answer
    citations = render_overall_kb_citations(events)
    return f"{answer}\n\nRelevant organizational knowledge:\n" + "\n".join(snippets) + f"\n\n{citations}"


def _is_overall_kb_overwrite_command(message: str) -> bool:
    lowered = message.lower()
    has_overwrite_word = any(w in lowered for w in ["overwrite", "replace", "update"])
    has_kb_ref = any(
        w in lowered
        for w in [
            "general org kb",
            "general organizational knowledge",
            "general knowledge base",
            "overall kb",
            "organizational knowledge base",
        ]
    )
    return has_overwrite_word and has_kb_ref and "->" in message


def _is_overall_kb_list_command(message: str) -> bool:
    lowered = message.lower()
    has_list_word = any(w in lowered for w in ["list", "show", "find", "search", "what are"])
    has_kb_ref = any(
        w in lowered
        for w in [
            "general org kb",
            "general organizational knowledge",
            "general knowledge base",
            "overall kb",
            "organizational knowledge base",
        ]
    )
    return has_list_word and has_kb_ref


def _extract_kb_query_text(message: str) -> str:
    if "->" in message:
        left = message.split("->", 1)[0].strip()
        if left:
            return left.strip(": ").strip()
    quoted = re.search(r'["“](.+?)["”]', message)
    if quoted:
        return quoted.group(1).strip()
    m = re.search(r"related to\s+(.+)$", message, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().strip(".")
    return message.strip()


def _extract_kb_overwrite_pair(message: str) -> tuple[str, str]:
    if "->" not in message:
        return "", ""
    old_part, right_part = message.split("->", 1)
    old_text = old_part.strip().strip(":").strip()
    right = right_part.strip()
    m = re.search(r"(?:overwrite|replace|update)(?:\s+it)?(?:\s+in\s+the\s+general\s+org\s+kb)?\s+with\s+(.+)$", right, flags=re.IGNORECASE)
    if m:
        return old_text, m.group(1).strip().strip(".")
    m2 = re.search(r"with\s+(.+)$", right, flags=re.IGNORECASE)
    if m2:
        return old_text, m2.group(1).strip().strip(".")
    cleaned = re.sub(
        r"(?i)\b(overwrite|replace|update)\b|\bgeneral org kb\b|\bgeneral organizational knowledge\b|\bgeneral knowledge base\b|\boverall kb\b|\borganizational knowledge base\b",
        "",
        right,
    ).strip(" .:")
    return old_text, cleaned


def _extract_kb_overwrite_ids(message: str) -> list[str]:
    m = re.search(r"\bids?\s*[:=]\s*([a-f0-9,\s]+)", message, flags=re.IGNORECASE)
    if not m:
        return []
    raw = m.group(1)
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if re.fullmatch(r"[a-f0-9]{20}", p)]


def _extract_sprint_name_from_filename(file_path: Path) -> str:
    # Expected pattern: SEO Tech Sprint Plan Q1-726.docx
    # Only quarter and numeric suffix vary.
    m = re.fullmatch(r"SEO Tech Sprint Plan Q([1-9])-([0-9]{3,4})\.docx", file_path.name)
    if not m:
        return ""
    quarter = m.group(1)
    code = m.group(2)
    return f"SEO Tech Sprint Plan Q{quarter}-{code}"


def _merge_missing_tasks(current: SprintState, planned: SprintState) -> tuple[SprintState, int, int]:
    existing_names = {t.task_name.strip().lower() for t in current.tasks}
    existing_by_name = {t.task_name.strip().lower(): t for t in current.tasks}
    added = 0
    synced = 0
    for task in planned.tasks:
        name_key = task.task_name.strip().lower()
        if not name_key:
            continue
        if name_key in existing_by_name:
            # Keep progress history, but force canonical plan name/definition/link.
            existing = existing_by_name[name_key]
            changed = False
            if existing.task_name != task.task_name:
                existing.task_name = task.task_name
                changed = True
            if task.definition and existing.definition != task.definition:
                existing.definition = task.definition
                changed = True
            if task.task_link != existing.task_link:
                existing.task_link = task.task_link
                changed = True
            if changed:
                synced += 1
            continue
        current.tasks.append(task)
        existing_names.add(name_key)
        existing_by_name[name_key] = task
        added += 1
    current.updated_at = datetime.utcnow()
    return current, added, synced


@app.on_event("startup")
def on_startup() -> None:
    ensure_workspace()
    reactivate_overall_kb_events_by_reason("chat_delete_command")
    load_state()


@app.get("/")
def root() -> FileResponse:
    return FileResponse("static/index.html")


@app.post("/api/initialize")
def initialize_from_plan(req: InitializeRequest | None = None) -> dict:
    if not PLAN_FILE.exists():
        raise HTTPException(status_code=404, detail="workspace/sprint_plan.md not found")
    payload = req or InitializeRequest()
    mode = (payload.mode or "sync_missing").strip().lower()
    if mode not in {"sync_missing", "destructive"}:
        mode = "sync_missing"

    current = load_state()
    backup = create_initialize_backup(max_keep=5)

    plan_text = PLAN_FILE.read_text(encoding="utf-8")
    derived_name = extract_sprint_name_from_markdown(plan_text)
    sprint_name = current.sprint_name if current.sprint_name != "Current Sprint" else (derived_name or current.sprint_name)
    planned = initialize_state_from_plan(PLAN_FILE, sprint_name=sprint_name)

    if mode == "destructive":
        if (payload.confirm_text or "").strip() != "RESET":
            raise HTTPException(status_code=400, detail="Destructive initialize requires confirm_text='RESET'.")
        state = planned
        save_state(state)
        return {
            "ok": True,
            "mode": mode,
            "task_count": len(state.tasks),
            "backup_path": str(backup) if backup else "",
            "message": "Destructive initialize completed.",
        }

    merged, added, synced = _merge_missing_tasks(current=current, planned=planned)
    save_state(merged)
    return {
        "ok": True,
        "mode": mode,
        "task_count": len(merged.tasks),
        "added_count": added,
        "synced_count": synced,
        "backup_path": str(backup) if backup else "",
        "message": "Safe initialize completed. Existing updates were preserved and plan task text was synced.",
    }


@app.post("/api/initialize/undo")
def undo_initialize() -> dict:
    restored = restore_latest_initialize_backup(remove_after_restore=True)
    if restored is None:
        raise HTTPException(status_code=404, detail="No initialize backup available to restore.")
    state = load_state()
    return {
        "ok": True,
        "restored_from": str(restored),
        "task_count": len(state.tasks),
        "daily_log_count": len(state.daily_logs),
        "remaining_undo_steps": len(list_initialize_backups()),
    }


@app.get("/api/initialize/status")
def initialize_status() -> dict:
    state = load_state()
    backups = list_initialize_backups()
    task_count = len(state.tasks)
    log_count = len(state.daily_logs)
    warning = task_count > 0 or log_count > 0
    return {
        "task_count": task_count,
        "daily_log_count": log_count,
        "warning": warning,
        "undo_available": len(backups) > 0,
        "undo_steps_available": len(backups),
    }


@app.post("/api/import-docx-plan")
def import_docx_plan(req: ImportDocxRequest) -> dict:
    cleaned_path = req.file_path.strip().strip("\"'").strip()
    file_path = Path(cleaned_path).expanduser()
    derived_sprint_name = _extract_sprint_name_from_filename(file_path)
    if not derived_sprint_name:
        detail = (
            "File name format is invalid. Expected: 'SEO Tech Sprint Plan Q1-726.docx' "
            "(only the numeric part after Q and hyphen may vary)."
        )
        raise HTTPException(status_code=400, detail=detail)
    resolved_sprint_name = derived_sprint_name
    # Try structured table extraction first to preserve exact task names/definitions.
    try:
        table_rows = extract_table_from_docx(file_path)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if table_rows:
        markdown = table_rows_to_markdown(table_rows, sprint_title=resolved_sprint_name)
        source = "docx_table"
    else:
        try:
            raw_text = extract_text_from_docx(file_path)
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        markdown = llm.normalize_sprint_text_to_markdown(raw_text) if llm.enabled else None
        source = "openai" if markdown else "fallback"
        if not markdown:
            markdown = fallback_markdown_from_raw_text(raw_text)
    PLAN_FILE.write_text(markdown, encoding="utf-8")

    task_count = 0
    added = 0
    synced = 0
    if req.auto_initialize:
        sprint_name = resolved_sprint_name or extract_sprint_name_from_markdown(markdown)
        planned = initialize_state_from_plan(PLAN_FILE, sprint_name=sprint_name)
        current = load_state()
        if current.tasks:
            current.sprint_name = sprint_name
            merged, added, synced = _merge_missing_tasks(current=current, planned=planned)
            save_state(merged)
            task_count = len(merged.tasks)
        else:
            save_state(planned)
            task_count = len(planned.tasks)

    return {
        "ok": True,
        "source": source,
        "plan_path": str(PLAN_FILE),
        "auto_initialized": req.auto_initialize,
        "task_count": task_count,
        "added_count": added,
        "synced_count": synced,
        "sprint_name": resolved_sprint_name,
    }


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    state = load_state()
    apply_recency_light_policy(state, stale_hours=24)
    question = req.message.strip()
    if not question:
        raise HTTPException(status_code=400, detail="message cannot be empty")
    mode = (req.mode or "auto").strip().lower()
    if mode not in {"auto", "query", "update"}:
        mode = "auto"

    if mode == "query":
        follow = build_follow_ups(state)
        base = answer_query(state, question)
        base = _augment_answer_with_overall_kb(question, base)
        return ChatResponse(answer=base, changed_tasks=[], follow_ups=follow, confidence=0.6)

    if _is_overall_kb_list_command(question):
        query_text = _extract_kb_query_text(question)
        matches = search_overall_kb_events(query_text, active_only=True, limit=8)
        follow = build_follow_ups(state)
        if not matches:
            return ChatResponse(
                answer=f"No active organizational knowledge found for: '{query_text}'.",
                changed_tasks=[],
                follow_ups=follow,
                confidence=0.6,
            )
        lines = [f"- [{m.get('id', '')}] {m.get('text', '')}" for m in matches]
        return ChatResponse(
            answer=(
                "Found these active organizational knowledge items:\n"
                + "\n".join(lines)
                + "\n\nUse overwrite with selection, e.g. ids: <id1>,<id2>."
            ),
            changed_tasks=[],
            follow_ups=follow,
            confidence=0.6,
        )

    if _is_overall_kb_overwrite_command(question):
        old_text, new_text = _extract_kb_overwrite_pair(question)
        target_ids = _extract_kb_overwrite_ids(question)
        follow = build_follow_ups(state)
        if not old_text or not new_text:
            return ChatResponse(
                answer="Please provide overwrite in this form: '<old statement> -> overwrite in general org kb with <new statement>'.",
                changed_tasks=[],
                follow_ups=follow,
                confidence=0.6,
            )
        if target_ids:
            result = overwrite_overall_kb_events_by_ids(
                target_ids=target_ids,
                new_text=new_text,
                source_message_id=_source_message_id(question),
            )
            if int(result.get("overwritten_count", 0)) == 0:
                return ChatResponse(
                    answer="No active organizational knowledge matched the provided IDs.",
                    changed_tasks=[],
                    follow_ups=follow,
                    confidence=0.6,
                )
        else:
            matches = search_overall_kb_events(old_text, active_only=True, limit=8)
            if not matches:
                return ChatResponse(
                    answer=f"No active organizational knowledge matched for overwrite: '{old_text}'.",
                    changed_tasks=[],
                    follow_ups=follow,
                    confidence=0.6,
                )
            result = overwrite_overall_kb_events(
                old_query=old_text,
                new_text=new_text,
                source_message_id=_source_message_id(question),
            )
        overwritten = result.get("overwritten_events", []) or []
        overwritten_lines = [f"- [{row.get('id', '')}] {row.get('text', '')}" for row in overwritten]
        answer = (
            f"Overwrote {result.get('overwritten_count', 0)} organizational knowledge item(s).\n"
            f"New value:\n- {new_text}\n\nReplaced items:\n" + "\n".join(overwritten_lines)
        )
        return ChatResponse(answer=answer, changed_tasks=[], follow_ups=follow, confidence=0.6)

    kb_captured = _capture_overall_kb_from_message(state, question) if mode != "update" else False
    references_task = _message_references_known_task(state, question)
    explicit_task_command = _looks_like_explicit_task_command(question)
    is_query = (mode == "query") or (
        mode != "update"
        and (
            _looks_like_query(question)
            or any(t in question.lower() for t in ["what are", "which tasks", "risky", "yesterday", "summarize"])
        )
    )
    if kb_captured and not references_task and not is_query and not explicit_task_command:
        follow = build_follow_ups(state)
        return ChatResponse(
            answer="Captured this as general organizational knowledge and stored it in the overall knowledge base.",
            changed_tasks=[],
            follow_ups=follow,
            confidence=0.6,
        )

    llm_result = llm.interpret_message(state, question) if llm.enabled else None

    if llm_result:
        intent = llm_result.get("intent", "")
        assistant_response = llm_result.get("assistant_response", "").strip()
        if intent in {"query", "clarify"}:
            follow = build_follow_ups(state)
            base = assistant_response or answer_query(state, question)
            if intent == "query":
                base = _augment_answer_with_overall_kb(question, base)
            return ChatResponse(answer=base, changed_tasks=[], follow_ups=follow, confidence=0.6)
        updates = llm_result.get("updates", [])
        updates = llm.refine_updates(updates, question) if llm.enabled else updates
        state, changed, follow = apply_structured_updates(state, question, updates)
        if not changed:
            state, changed, follow = apply_daily_update(state, question)
        save_state(state)
        append_daily_markdown_log(
            f"- [{state.sprint_name}] {datetime.utcnow().isoformat()} | update: {question}\n"
            + "\n".join(f"  - {c.task_name}: {c.status.value}/{c.traffic_light.value}" for c in changed)
        )
        return ChatResponse(
            answer=assistant_response or ("Update applied." if changed else "No clear task update found."),
            changed_tasks=changed,
            follow_ups=follow,
            confidence=0.6,
        )

    if is_query:
        follow = build_follow_ups(state)
        base = answer_query(state, question)
        base = _augment_answer_with_overall_kb(question, base)
        return ChatResponse(answer=base, changed_tasks=[], follow_ups=follow, confidence=0.6)

    state, changed, follow = apply_daily_update(state, question)
    save_state(state)
    append_daily_markdown_log(
        f"- [{state.sprint_name}] {datetime.utcnow().isoformat()} | update: {question}\n"
        + "\n".join(f"  - {c.task_name}: {c.status.value}/{c.traffic_light.value}" for c in changed)
    )
    return ChatResponse(
        answer="Update applied (rules fallback)." if changed else "No clear task update found.",
        changed_tasks=changed,
        follow_ups=follow,
        confidence=0.6,
    )


@app.get("/api/tasks")
def tasks() -> dict:
    state = load_state()
    apply_recency_light_policy(state, stale_hours=24)
    return {
        "sprint_name": state.sprint_name,
        "tasks": [task.model_dump(mode="json") for task in state.tasks],
    }


@app.get("/api/followups")
def followups() -> dict:
    state = load_state()
    apply_recency_light_policy(state, stale_hours=24)
    return {"follow_ups": build_follow_ups(state)}


@app.get("/api/plan")
def plan() -> dict:
    if not PLAN_FILE.exists():
        return {"content": "", "exists": False}
    return {"content": PLAN_FILE.read_text(encoding="utf-8"), "exists": True}


@app.post("/api/generate-table")
def generate_table() -> dict:
    state = load_state()
    apply_recency_light_policy(state, stale_hours=24)
    output = TABLES_DIR / f"status-table-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.md"
    generate_status_table(state, output)
    return {"ok": True, "path": str(output)}


@app.get("/api/memory/debug")
def memory_debug() -> dict:
    return {"overall_kb": overall_kb_debug_payload(limit=30)}


@app.post("/api/memory/rebuild-overall-kb-vectors")
def rebuild_overall_kb_vectors_api() -> dict:
    count = rebuild_overall_kb_vectors()
    return {"ok": True, "vector_count": count}


@app.get("/api/report")
def get_sprint_report() -> dict:
    content = load_latest_sprint_report()
    return {
        "exists": bool(content.strip()),
        "content": content,
        "path": str(LATEST_SPRINT_REPORT_FILE),
    }


@app.post("/api/report/generate")
def generate_sprint_report() -> dict:
    state = load_state()
    markdown = generate_sprint_report_markdown(state=state, llm=llm)
    path = save_latest_sprint_report(markdown)
    return {"ok": True, "content": markdown, "path": str(path), "saved": True}


@app.post("/api/report/save")
def save_sprint_report(req: SprintReportSaveRequest) -> dict:
    path = save_latest_sprint_report(req.content)
    return {"ok": True, "path": str(path), "content_length": len((req.content or "").strip())}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8001, reload=True)
