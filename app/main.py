from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional
import hashlib
import re
import tempfile
import threading
import time

import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .confidence import (
    compute_capture_only_confidence,
    compute_faq_confidence,
    compute_kb_command_confidence,
    compute_query_confidence,
    compute_update_confidence,
)
from .gantt import build_gantt_items, update_gantt_dates
from .llm import SprintCopilotLLM
from .meeting_transcription import transcribe_and_summarize
from .memory import (
    append_overall_kb_event,
    build_overall_kb_graph_payload,
    overall_kb_hybrid_search,
    refine_overall_kb_active_events,
    refine_overall_kb_candidate,
    overwrite_overall_kb_events,
    overwrite_overall_kb_events_by_ids,
    reactivate_overall_kb_events_by_reason,
    search_overall_kb_events,
    overall_kb_debug_payload,
    rebuild_overall_kb_vectors,
    render_overall_kb_citations,
)
from .kb_graph import (
    apply_manual_grouping,
    build_entity_graph_payload,
    ingest_extracted,
    load_entities,
    load_relations,
    refine_entity_graph,
    resolve_entity_hierarchy,
)
from .unified_graph import batch_micro_synthesize_prd, build_unified_graph_payload, micro_synthesize, rebuild_unified_vectors, run_cross_source_synthesis
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
    append_meeting_summary,
    create_initialize_backup,
    ensure_workspace,
    list_initialize_backups,
    list_meeting_summaries,
    load_latest_sprint_report,
    load_state,
    restore_latest_initialize_backup,
    save_latest_sprint_report,
    save_state,
)
from .task_history import (
    append_task_history_event,
    backfill_task_history_from_sources,
    list_task_history,
    resolve_task_id_by_name,
)
from .update_engine import apply_daily_update, apply_structured_updates, apply_recency_light_policy, build_follow_ups, parse_define_command, apply_define_update, parse_group_command


app = FastAPI(title="Sprint Engine", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")
llm = SprintCopilotLLM()

MAX_MEETING_AUDIO_BYTES = 25 * 1024 * 1024
MAX_MEETING_NAME_LEN = 200
KB_GRAPH_CACHE_TTL_SECONDS = 15.0
_kb_graph_cache: dict[tuple, dict] = {}
_unified_graph_cache: dict[tuple, dict] = {}


class SprintReportSaveRequest(BaseModel):
    content: str


class GanttUpdateRequest(BaseModel):
    start_date: str
    end_date: str


class InitializeRequest(BaseModel):
    mode: str = "sync_missing"
    confirm_text: str = ""


class GenerateExcerptRequest(BaseModel):
    system_prompt: str
    input_context: str


def _source_message_id(message: str) -> str:
    payload = f"{datetime.utcnow().isoformat()}|{message}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:20]


def _record_task_history_events(changed_tasks: list, message: str, source_message_id: str) -> None:
    if not changed_tasks:
        return
    for task in changed_tasks:
        raw_text = str(getattr(task, "latest_update", "") or "").strip() or message.strip()
        if not raw_text:
            continue
        refiner = None
        if llm.enabled:
            refiner = lambda text, task_name=task.task_name: llm.refine_task_history_text(task_name, text)
        append_task_history_event(
            task_id=str(task.id),
            task_name=str(task.task_name),
            text=raw_text,
            status=str(getattr(task.status, "value", task.status)),
            traffic_light=str(getattr(task.traffic_light, "value", task.traffic_light)),
            source="task_history_ingest",
            source_message_id=source_message_id,
            metadata={
                "ingest_source": "api_chat",
                "user_message": message,
            },
            refiner=refiner,
        )


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
        raw_text = str(item.get("text", "")).strip()
        candidate = refine_overall_kb_candidate(raw_text)
        if not bool(candidate.get("keep", False)):
            continue
        durability = float(candidate.get("durability_score", 0.0))
        if durability < 0.62:
            continue
        canonical_text = str(candidate.get("canonical_text", raw_text)).strip() or raw_text
        append_overall_kb_event(
            text=canonical_text,
            knowledge_type=str(item.get("knowledge_type", "")).strip().lower(),
            scope=str(item.get("scope", "")).strip().lower(),
            confidence=confidence,
            source_message_id=source_id,
            source="chat_intent",
            metadata={
                "raw_message": message,
                "sprint_name": state.sprint_name,
                "refine_reason": str(candidate.get("reason", "")),
                "durability_score": durability,
            },
        )
        captured_any = True
    return captured_any


def _bg_micro_synthesize(trigger: str, payload_text: str) -> None:
    """Fire micro_synthesize in a background daemon thread so it never blocks a response."""
    def _run() -> None:
        try:
            micro_synthesize(trigger, payload_text)
        except Exception:
            pass
    threading.Thread(target=_run, daemon=True).start()


def _capture_entity_graph_from_message(state, message: str) -> bool:
    """Extract entities & relations from a message and upsert into the entity graph."""
    if not llm.enabled:
        return False
    extracted = llm.extract_entity_relations(state, message)
    if not extracted:
        return False
    ents = extracted.get("entities", [])
    rels = extracted.get("relations", [])
    if not ents and not rels:
        return False
    source_id = _source_message_id(message)
    result = ingest_extracted(extracted, source_event=source_id)
    return (result.get("entities_upserted", 0) + result.get("relations_upserted", 0)) > 0


def _sync_overall_kb_from_task_definitions(state, limit: int = 500) -> dict:
    inserted = 0
    scanned = 0
    for task in (state.tasks or [])[: max(1, int(limit))]:
        task_name = str(getattr(task, "task_name", "") or "").strip()
        definition = str(getattr(task, "definition", "") or "").strip()
        if not task_name or not definition:
            continue
        scanned += 1
        text = f"DEFINITION: {task_name} — {definition}"
        event = append_overall_kb_event(
            text=text,
            knowledge_type="process_rule",
            scope="project",
            confidence=0.93,
            source_message_id=_source_message_id(text),
            source="sprint_plan_definition",
            metadata={
                "ingest_source": "sprint_plan_definition",
                "task_id": str(getattr(task, "id", "") or ""),
                "task_name": task_name,
            },
        )
        if event:
            inserted += 1
    return {"scanned": scanned, "inserted": inserted}


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


def _build_archived_faq_prompt_section(question: str) -> str:
    from .faq_store import (
        archived_items_in_order,
        format_archived_faq_block,
        load_faq_items,
        select_archived_for_context,
    )

    archived = archived_items_in_order(load_faq_items())
    picked = select_archived_for_context(question, archived)
    if not picked:
        return ""
    return format_archived_faq_block(picked)


def _augment_answer_with_archived_faq(question: str, answer: str) -> str:
    from .faq_store import (
        archived_items_in_order,
        format_archived_faq_block,
        load_faq_items,
        select_archived_for_context,
    )

    archived = archived_items_in_order(load_faq_items())
    picked = select_archived_for_context(question, archived)
    if not picked:
        return answer
    block = format_archived_faq_block(picked)
    return f"{answer}\n\n{block}"


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


def _extract_force_kb_command(message: str) -> tuple[str, str, str]:
    m = re.search(
        r"(?is)^\s*(?:/force-kb|force\s*(?:write|save)?\s*(?:to\s*)?(?:general knowledge(?: base)?|overall kb|org kb))\s*:\s*(.+)$",
        message.strip(),
    )
    if not m:
        return "", "", ""
    payload = m.group(1).strip()
    ktype = "process_rule"
    scope = "project"
    t = re.search(r"\btype\s*=\s*([a-z_]+)\b", payload, flags=re.IGNORECASE)
    s = re.search(r"\bscope\s*=\s*([a-z_]+)\b", payload, flags=re.IGNORECASE)
    if t:
        ktype = str(t.group(1)).strip().lower()
    if s:
        scope = str(s.group(1)).strip().lower()
    cleaned = re.sub(r"\btype\s*=\s*[a-z_]+\b", "", payload, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bscope\s*=\s*[a-z_]+\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[\[\]\(\)]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -;:,")
    return cleaned, ktype, scope


def _is_overall_kb_refine_command(message: str) -> bool:
    lowered = message.lower()
    has_refine = any(w in lowered for w in ["refine", "clean", "dedupe", "deduplicate", "curate"])
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
    return has_refine and has_kb_ref


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
    state = load_state()
    _sync_overall_kb_from_task_definitions(state, limit=1000)


@app.get("/")
def root() -> FileResponse:
    return FileResponse("static/index.html")


@app.post("/api/initialize")
def initialize_from_plan(req: Optional[InitializeRequest] = None) -> dict:
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
        _sync_overall_kb_from_task_definitions(state, limit=1000)
        return {
            "ok": True,
            "mode": mode,
            "task_count": len(state.tasks),
            "backup_path": str(backup) if backup else "",
            "message": "Destructive initialize completed.",
        }

    merged, added, synced = _merge_missing_tasks(current=current, planned=planned)
    save_state(merged)
    _sync_overall_kb_from_task_definitions(merged, limit=1000)
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
            _sync_overall_kb_from_task_definitions(merged, limit=1000)
            task_count = len(merged.tasks)
        else:
            save_state(planned)
            _sync_overall_kb_from_task_definitions(planned, limit=1000)
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
    if mode not in {"auto", "query", "update", "faq"}:
        mode = "auto"

    if mode == "faq":
        from .faq_commands import process_faq_message

        answer = process_faq_message(question)
        return ChatResponse(
            answer=answer,
            changed_tasks=[],
            follow_ups=[],
            confidence=compute_faq_confidence(answer),
        )

    if mode == "query":
        follow = build_follow_ups(state)
        base = answer_query(state, question)
        base = _augment_answer_with_archived_faq(question, base)
        return ChatResponse(
            answer=base,
            changed_tasks=[],
            follow_ups=follow,
            confidence=compute_query_confidence(
                question=question,
                answer=base,
                has_known_task_ref=_message_references_known_task(state, question),
            ),
        )

    forced_text, forced_type, forced_scope = _extract_force_kb_command(question)
    if forced_text:
        follow = build_follow_ups(state)
        event = append_overall_kb_event(
            text=forced_text,
            knowledge_type=forced_type,
            scope=forced_scope,
            confidence=1.0,
            source_message_id=_source_message_id(question),
            source="chat_force_kb_command",
            metadata={"forced": True, "raw_message": question, "sprint_name": state.sprint_name},
        )
        if not event:
            return ChatResponse(
                answer="Force-write command accepted, but an identical active rule already exists.",
                changed_tasks=[],
                follow_ups=follow,
                confidence=compute_kb_command_confidence(success=True),
            )
        return ChatResponse(
            answer=(
                "Force-saved into overall knowledge base.\n"
                f"- [{event.get('id', '')}] {event.get('text', '')}\n"
                f"(type={event.get('knowledge_type', '')}, scope={event.get('scope', '')})"
            ),
            changed_tasks=[],
            follow_ups=follow,
            confidence=compute_kb_command_confidence(success=True),
        )

    define_cmd = parse_define_command(question)
    if define_cmd:
        state, changed, changes = apply_define_update(
            state,
            task_name=define_cmd["task_name"],
            definition=define_cmd["definition"],
        )
        save_state(state)
        follow = build_follow_ups(state)
        if changed:
            task = changed[0]
            _record_task_history_events(changed, question, source_message_id=_source_message_id(question))
            # Update entity graph with the new definition
            _capture_entity_graph_from_message(state, f"{task.task_name}: {task.definition}")
            # Incremental KB enrichment + digest refresh (Phases 3 & 4)
            _bg_micro_synthesize("define", f"{task.task_name}: {task.definition}")
            def _refresh_digest(t=task):
                try:
                    from .task_digest import update_task_digest  # noqa: PLC0415
                    update_task_digest(t.id, t.task_name)
                except Exception:
                    pass
            threading.Thread(target=_refresh_digest, daemon=True).start()
            return ChatResponse(
                answer=f"{changes[0]}.\nDefinition: \"{task.definition}\"",
                changed_tasks=changed,
                follow_ups=follow,
                confidence=0.97,
            )

    group_cmd = parse_group_command(question)
    if group_cmd:
        follow = build_follow_ups(state)
        result = apply_manual_grouping(
            parent_name=group_cmd["parent"],
            child_names=group_cmd["children"],
        )
        _kb_graph_cache.clear()
        lines = [f"Grouped {len(group_cmd['children'])} entities under '{group_cmd['parent']}'."]
        if result["parents_created"]:
            lines.append(f"Created parent node: '{group_cmd['parent']}'.")
        lines.append(f"Added {result['edges_added']} has_subtopic edge(s).")
        return ChatResponse(
            answer="\n".join(lines),
            changed_tasks=[],
            follow_ups=follow,
            confidence=0.97,
        )

    if _is_overall_kb_refine_command(question):
        follow = build_follow_ups(state)
        result = refine_overall_kb_active_events(limit=1000)
        return ChatResponse(
            answer=(
                "Refined overall knowledge base.\n"
                f"- Changed: {result.get('changed', 0)}\n"
                f"- Deactivated transient: {result.get('deactivated_transient', 0)}\n"
                f"- Rewritten canonical: {result.get('rewritten', 0)}\n"
                f"- Deduplicated: {result.get('deduplicated', 0)}\n"
                f"- Active after refine: {result.get('active_after', 0)}"
            ),
            changed_tasks=[],
            follow_ups=follow,
            confidence=compute_kb_command_confidence(success=True),
        )

    if _is_overall_kb_list_command(question):
        query_text = _extract_kb_query_text(question)
        matches = search_overall_kb_events(query_text, active_only=True, limit=8)
        follow = build_follow_ups(state)
        if not matches:
            return ChatResponse(
                answer=f"No active organizational knowledge found for: '{query_text}'.",
                changed_tasks=[],
                follow_ups=follow,
                confidence=compute_kb_command_confidence(success=False),
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
            confidence=compute_kb_command_confidence(success=True),
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
                confidence=compute_kb_command_confidence(success=False),
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
                    confidence=compute_kb_command_confidence(success=False),
                )
        else:
            matches = search_overall_kb_events(old_text, active_only=True, limit=8)
            if not matches:
                return ChatResponse(
                    answer=f"No active organizational knowledge matched for overwrite: '{old_text}'.",
                    changed_tasks=[],
                    follow_ups=follow,
                    confidence=compute_kb_command_confidence(success=False),
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
        return ChatResponse(
            answer=answer,
            changed_tasks=[],
            follow_ups=follow,
            confidence=compute_kb_command_confidence(success=int(result.get("overwritten_count", 0)) > 0),
        )

    kb_captured = _capture_overall_kb_from_message(state, question) if mode != "update" else False
    _capture_entity_graph_from_message(state, question)
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
            confidence=compute_capture_only_confidence(),
        )

    faq_ctx = _build_archived_faq_prompt_section(question)
    llm_result = (
        llm.interpret_message(state, question, archived_faq_context=faq_ctx) if llm.enabled else None
    )

    if llm_result:
        intent = llm_result.get("intent", "")
        assistant_response = llm_result.get("assistant_response", "").strip()
        if intent in {"query", "clarify"}:
            follow = build_follow_ups(state)
            base = assistant_response or answer_query(state, question)
            # Archived FAQ text is already in the LLM prompt when faq_ctx is non-empty; avoid duplicating it here.
            if not faq_ctx.strip():
                base = _augment_answer_with_archived_faq(question, base)
            return ChatResponse(
                answer=base,
                changed_tasks=[],
                follow_ups=follow,
                confidence=compute_query_confidence(
                    question=question,
                    answer=base,
                    has_known_task_ref=references_task,
                    used_llm=True,
                    llm_intent=intent,
                ),
            )
        updates = llm_result.get("updates", [])
        updates = llm.refine_updates(updates, question) if llm.enabled else updates
        state, changed, follow = apply_structured_updates(state, question, updates)
        used_fallback_update = False
        if not changed:
            state, changed, follow = apply_daily_update(state, question)
            used_fallback_update = True
        _record_task_history_events(changed, question, source_message_id=_source_message_id(question))
        save_state(state)
        # For newly added tasks or definition updates, feed the definition into entity graph
        added_or_defined = [
            t for t in changed
            if getattr(t, "created_from", "") == "chat_define"
            or any(u.get("action") == "add" and u.get("task_name", "").lower() == t.task_name.lower()
                   for u in (updates or []))
        ]
        for t in added_or_defined:
            if t.definition and t.definition != t.task_name:
                _capture_entity_graph_from_message(state, f"{t.task_name}: {t.definition}")
                _bg_micro_synthesize("task_add", f"{t.task_name}: {t.definition}")
        # Incremental synthesis for any substantive update text (Phase 3)
        for t in changed:
            latest = str(getattr(t, "latest_update", "") or "").strip()
            if len(latest) > 80:
                _bg_micro_synthesize("task_update", f"{t.task_name}: {latest}")
        append_daily_markdown_log(
            f"- [{state.sprint_name}] {datetime.utcnow().isoformat()} | update: {question}\n"
            + "\n".join(f"  - {c.task_name}: {c.status.value}/{c.traffic_light.value}" for c in changed)
        )
        return ChatResponse(
            answer=assistant_response or ("Update applied." if changed else "No clear task update found."),
            changed_tasks=changed,
            follow_ups=follow,
            confidence=compute_update_confidence(
                question=question,
                changed_count=len(changed),
                has_known_task_ref=references_task,
                used_llm=True,
                used_structured=True,
                used_fallback=used_fallback_update,
            ),
        )

    if is_query:
        follow = build_follow_ups(state)
        base = answer_query(state, question)
        base = _augment_answer_with_archived_faq(question, base)
        return ChatResponse(
            answer=base,
            changed_tasks=[],
            follow_ups=follow,
            confidence=compute_query_confidence(
                question=question,
                answer=base,
                has_known_task_ref=references_task,
            ),
        )

    state, changed, follow = apply_daily_update(state, question)
    _record_task_history_events(changed, question, source_message_id=_source_message_id(question))
    save_state(state)
    append_daily_markdown_log(
        f"- [{state.sprint_name}] {datetime.utcnow().isoformat()} | update: {question}\n"
        + "\n".join(f"  - {c.task_name}: {c.status.value}/{c.traffic_light.value}" for c in changed)
    )
    return ChatResponse(
        answer="Update applied (rules fallback)." if changed else "No clear task update found.",
        changed_tasks=changed,
        follow_ups=follow,
        confidence=compute_update_confidence(
            question=question,
            changed_count=len(changed),
            has_known_task_ref=references_task,
            used_fallback=True,
        ),
    )


@app.get("/api/tasks")
def tasks() -> dict:
    state = load_state()
    apply_recency_light_policy(state, stale_hours=24)
    return {
        "sprint_name": state.sprint_name,
        "tasks": [task.model_dump(mode="json") for task in state.tasks],
    }


@app.get("/api/gantt/tasks")
def gantt_tasks() -> dict:
    state = load_state()
    apply_recency_light_policy(state, stale_hours=24)
    items = build_gantt_items(state)
    return {"sprint_name": state.sprint_name, "items": items}


@app.post("/api/gantt/tasks/{task_id}")
def gantt_update_task(task_id: str, req: GanttUpdateRequest) -> dict:
    state = load_state()
    try:
        task = update_gantt_dates(state=state, task_id=task_id, start_date_iso=req.start_date, end_date_iso=req.end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    save_state(state)
    append_task_history_event(
        task_id=task.id,
        task_name=task.task_name,
        text=f"Gantt dates updated: start {req.start_date}, end {req.end_date}.",
        status=task.status.value,
        traffic_light=task.traffic_light.value,
        source="gantt_edit",
        metadata={"start_date": req.start_date, "end_date": req.end_date},
    )
    return {"ok": True, "task": task.model_dump(mode='json')}


@app.get("/api/tasks/{task_id}/history")
def task_history(task_id: str, limit: int = Query(default=50, ge=1, le=500)) -> dict:
    events = list_task_history(task_id=task_id, limit=limit)
    return {"task_id": task_id, "count": len(events), "events": events}


@app.get("/api/tasks/history")
def task_history_by_name(task_name: str = Query(..., min_length=1), limit: int = Query(default=50, ge=1, le=500)) -> dict:
    state = load_state()
    task_id = resolve_task_id_by_name(state=state, task_name=task_name)
    if not task_id:
        raise HTTPException(status_code=404, detail=f"Task not found for name: {task_name}")
    events = list_task_history(task_id=task_id, limit=limit)
    return {"task_name": task_name, "task_id": task_id, "count": len(events), "events": events}


@app.post("/api/tasks/history/backfill")
def backfill_task_history(include_markdown: bool = Query(default=True)) -> dict:
    state = load_state()
    result = backfill_task_history_from_sources(state=state, include_markdown=include_markdown)
    return {"ok": True, **result}


@app.get("/api/followups")
def followups() -> dict:
    state = load_state()
    apply_recency_light_policy(state, stale_hours=24)
    return {"follow_ups": build_follow_ups(state)}


@app.get("/api/faq")
def get_faq() -> dict:
    from .faq_store import FAQ_MD, active_items_in_order, load_faq_items

    items = load_faq_items()
    active = active_items_in_order(items)
    return {
        "active": [
            {"n": i + 1, "id": x.id, "question": x.question, "answer": x.answer or ""}
            for i, x in enumerate(active)
        ],
        "markdown_path": str(FAQ_MD),
    }


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


@app.get("/api/memory/overall-kb-graph")
def overall_kb_graph(
    active_only: bool = Query(default=True),
    include_archived: bool = Query(default=False),
    semantic_threshold: float = Query(default=0.8, ge=0.0, le=1.0),
    top_k_semantic: int = Query(default=3, ge=0, le=10),
    max_nodes: int = Query(default=250, ge=50, le=1000),
) -> dict:
    key = (
        bool(active_only),
        bool(include_archived),
        round(float(semantic_threshold), 4),
        int(top_k_semantic),
        int(max_nodes),
    )
    now = time.monotonic()
    cached = _kb_graph_cache.get(key)
    if cached and (now - float(cached.get("ts", 0.0)) <= KB_GRAPH_CACHE_TTL_SECONDS):
        payload = dict(cached.get("payload", {}))
        stats = payload.get("stats", {}) if isinstance(payload.get("stats", {}), dict) else {}
        stats["cache_hit"] = True
        stats["cache_ttl_seconds"] = KB_GRAPH_CACHE_TTL_SECONDS
        payload["stats"] = stats
        return payload

    payload = build_overall_kb_graph_payload(
        active_only=active_only,
        include_archived=include_archived,
        semantic_threshold=semantic_threshold,
        top_k_semantic=top_k_semantic,
        max_nodes=max_nodes,
    )
    _kb_graph_cache[key] = {"ts": now, "payload": payload}
    stats = payload.get("stats", {}) if isinstance(payload.get("stats", {}), dict) else {}
    stats["cache_hit"] = False
    stats["cache_ttl_seconds"] = KB_GRAPH_CACHE_TTL_SECONDS
    payload["stats"] = stats
    return payload


@app.get("/api/memory/unified-graph")
def unified_graph_api(
    active_only: bool = Query(default=True),
    include_archived: bool = Query(default=False),
    semantic_threshold: float = Query(default=0.78, ge=0.0, le=1.0),
    top_k_semantic: int = Query(default=3, ge=0, le=10),
    max_nodes: int = Query(default=300, ge=50, le=1000),
    max_task_events_per_task: int = Query(default=5, ge=0, le=50),
) -> dict:
    key = (
        bool(active_only),
        bool(include_archived),
        round(float(semantic_threshold), 4),
        int(top_k_semantic),
        int(max_nodes),
        int(max_task_events_per_task),
    )
    now = time.monotonic()
    cached = _unified_graph_cache.get(key)
    if cached and (now - float(cached.get("ts", 0.0)) <= KB_GRAPH_CACHE_TTL_SECONDS):
        payload = dict(cached.get("payload", {}))
        stats = payload.get("stats", {}) if isinstance(payload.get("stats", {}), dict) else {}
        stats["cache_hit"] = True
        stats["cache_ttl_seconds"] = KB_GRAPH_CACHE_TTL_SECONDS
        payload["stats"] = stats
        return payload

    payload = build_unified_graph_payload(
        active_only=active_only,
        include_archived=include_archived,
        semantic_threshold=semantic_threshold,
        top_k_semantic=top_k_semantic,
        max_nodes=max_nodes,
        max_task_events_per_task=max_task_events_per_task,
    )
    _unified_graph_cache[key] = {"ts": now, "payload": payload}
    stats = payload.get("stats", {}) if isinstance(payload.get("stats", {}), dict) else {}
    stats["cache_hit"] = False
    stats["cache_ttl_seconds"] = KB_GRAPH_CACHE_TTL_SECONDS
    payload["stats"] = stats
    return payload


@app.post("/api/memory/rebuild-unified-vectors")
def rebuild_unified_vectors_api() -> dict:
    count = rebuild_unified_vectors()
    _unified_graph_cache.clear()
    return {"ok": True, "vector_count": count}


# ── PRD endpoints ─────────────────────────────────────────────────────────────

@app.post("/api/prds")
async def upload_prd(title: str = Form(...), file: UploadFile = File(...)) -> dict:
    from .prd_store import save_prd, get_prd_rules  # noqa: PLC0415
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")
    pdf_bytes = await file.read()
    if len(pdf_bytes) > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="PDF exceeds 50 MB limit.")
    try:
        record = save_prd(title=title, pdf_bytes=pdf_bytes, filename=file.filename)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to process PDF: {exc}") from exc

    prd_id = record["id"]
    chunks = record.get("chunks", [])

    def _bg_prd_synthesis():
        try:
            batch_micro_synthesize_prd(prd_id, chunks, max_chunks=10)
        except Exception:
            pass
    threading.Thread(target=_bg_prd_synthesis, daemon=True).start()

    _unified_graph_cache.clear()
    _kb_graph_cache.clear()
    return {
        "ok": True,
        "id": prd_id,
        "title": record["title"],
        "chunk_count": record["chunk_count"],
        "char_count": record["char_count"],
    }


@app.get("/api/prds")
def list_prds_api() -> dict:
    from .prd_store import list_prds  # noqa: PLC0415
    return {"prds": list_prds(active_only=True)}


@app.get("/api/prds/{prd_id}/rules")
def get_prd_rules_api(prd_id: str) -> dict:
    from .prd_store import get_prd_rules, get_prd_entity_graph  # noqa: PLC0415
    return {
        "rules": get_prd_rules(prd_id),
        "graph": get_prd_entity_graph(prd_id),
    }


@app.delete("/api/prds/{prd_id}")
def delete_prd_api(prd_id: str) -> dict:
    from .prd_store import deactivate_prd  # noqa: PLC0415
    ok = deactivate_prd(prd_id)
    if not ok:
        raise HTTPException(status_code=404, detail="PRD not found.")
    _unified_graph_cache.clear()
    return {"ok": True}


@app.post("/api/memory/synthesize-cross-source")
def synthesize_cross_source_api() -> dict:
    """Run Step 6: mine all active non-KB sources, extract durable organizational facts,
    write accepted candidates into the general KB with source='cross_source_synthesis'."""
    result = run_cross_source_synthesis()
    _unified_graph_cache.clear()
    _kb_graph_cache.clear()
    return result


@app.post("/api/memory/rebuild-overall-kb-vectors")
def rebuild_overall_kb_vectors_api() -> dict:
    count = rebuild_overall_kb_vectors()
    return {"ok": True, "vector_count": count}


@app.post("/api/memory/sync-plan-definitions")
def sync_plan_definitions_into_overall_kb() -> dict:
    state = load_state()
    result = _sync_overall_kb_from_task_definitions(state, limit=5000)
    return {"ok": True, **result}


@app.get("/api/memory/entity-graph")
def entity_graph_api(
    active_only: bool = Query(default=True),
    entity_type: str = Query(default="all"),
    search: str = Query(default=""),
) -> dict:
    return build_entity_graph_payload(
        active_only=active_only,
        entity_type_filter=entity_type,
        search_query=search,
    )


@app.post("/api/memory/rebuild-entity-graph")
def rebuild_entity_graph_api() -> dict:
    """Backfill the entity graph from all existing active KB events."""
    from .memory import load_overall_kb_events
    from .unified_graph import _extract_entities_from_text  # noqa: PLC0415
    events = load_overall_kb_events(include_archived=False)
    active = [e for e in events if e.get("is_active", True)]
    processed = 0
    total_ents = 0
    total_rels = 0
    batch_size = 5
    for i in range(0, len(active), batch_size):
        batch = active[i : i + batch_size]
        combined = "\n".join(str(e.get("text", "")) for e in batch if e.get("text"))
        if not combined.strip():
            continue
        extracted = _extract_entities_from_text(combined, source_label=f"backfill_batch_{i}")
        if extracted and (extracted.get("entities") or extracted.get("relations")):
            result = ingest_extracted(extracted, source_event=f"backfill_batch_{i}")
            total_ents += result.get("entities_upserted", 0)
            total_rels += result.get("relations_upserted", 0)
        processed += len(batch)
    refine = refine_entity_graph(max_entities=50, max_new_relations=40)
    return {
        "ok": True,
        "events_processed": processed,
        "entities_upserted": total_ents,
        "relations_upserted": total_rels,
        "entities_merged": refine.get("entities_merged", 0),
        "relations_refined_added": refine.get("relations_added", 0),
        "relations_refined_fixed": refine.get("relations_fixed", 0),
    }


@app.post("/api/memory/refine-entity-graph")
def refine_entity_graph_api() -> dict:
    """Run an LLM refinement pass over the current entity graph."""
    result = refine_entity_graph(max_entities=60, max_new_relations=50)
    _kb_graph_cache.clear()
    return {"ok": True, **result}


async def _read_upload_with_limit(upload: UploadFile, max_bytes: int) -> bytes:
    data = bytearray()
    while True:
        chunk = await upload.read(1024 * 1024)
        if not chunk:
            break
        if len(data) + len(chunk) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"Audio file is too large (max {max_bytes // (1024 * 1024)} MB).",
            )
        data.extend(chunk)
    return bytes(data)


def _suffix_from_upload(filename: Optional[str], content_type: Optional[str]) -> str:
    if filename:
        p = Path(filename)
        if p.suffix and len(p.suffix) <= 12:
            return p.suffix.lower()
    ct = (content_type or "").lower()
    if "webm" in ct:
        return ".webm"
    if "wav" in ct:
        return ".wav"
    if "mp4" in ct or "m4a" in ct:
        return ".m4a"
    if "mpeg" in ct or "mp3" in ct:
        return ".mp3"
    return ".webm"


@app.post("/api/meetings/process")
async def process_meeting_recording(
    audio: UploadFile = File(...),
    meeting_name: str = Form(...),
) -> dict:
    if not llm.enabled:
        raise HTTPException(status_code=503, detail="OpenAI API is not configured (missing OPENAI_API_KEY).")
    name = (meeting_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="meeting_name is required.")
    if len(name) > MAX_MEETING_NAME_LEN:
        raise HTTPException(status_code=400, detail="meeting_name is too long.")

    raw = await _read_upload_with_limit(audio, MAX_MEETING_AUDIO_BYTES)
    if len(raw) < 32:
        raise HTTPException(status_code=400, detail="Audio recording is empty or too short.")

    client = llm._get_client()
    if client is None:
        raise HTTPException(status_code=503, detail="OpenAI client is not available.")

    suffix = _suffix_from_upload(audio.filename, audio.content_type)
    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(raw)
            tmp_path = Path(tmp.name)
        transcript, summary = transcribe_and_summarize(
            client=client,
            chat_model=llm.model,
            audio_path=tmp_path,
            meeting_name=name,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Transcription or summarization failed: {exc!s}") from exc
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

    saved = append_meeting_summary(name, summary)
    # Incremental KB enrichment from the meeting summary (Phase 3)
    _bg_micro_synthesize("meeting", f"{name}: {summary}")
    return {
        "ok": True,
        "meeting_name": name,
        "transcript": transcript,
        "summary": summary,
        "saved": saved,
    }


@app.get("/api/meetings")
def get_meeting_summaries() -> dict:
    return {"meetings": list_meeting_summaries()}


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


@app.post("/api/memory/resolve-hierarchy")
def resolve_hierarchy_api() -> dict:
    """Detect parent-child relationships by name pattern and upsert has_subtopic edges."""
    result = resolve_entity_hierarchy()
    _kb_graph_cache.clear()
    return {"ok": True, **result}


@app.post("/api/content/generate-excerpt")
def generate_excerpt(req: GenerateExcerptRequest) -> dict:
    """Generate a brand+category excerpt using OpenAI."""
    import os
    try:
        from openai import OpenAI as _OpenAI
    except ImportError:
        raise HTTPException(status_code=500, detail="openai package not installed. Run: pip install openai>=1.0")

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=400, detail="OPENAI_API_KEY is not set on the server.")

    system_prompt = (req.system_prompt or "").strip()
    input_context = (req.input_context or "").strip()
    if not input_context:
        raise HTTPException(status_code=400, detail="input_context is required.")

    try:
        client = _OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": system_prompt or "You are a helpful assistant."},
                {"role": "user", "content": input_context},
            ],
            max_completion_tokens=500,
            temperature=0.7,
            timeout=30,
        )
        excerpt = response.choices[0].message.content or ""
        return {"ok": True, "excerpt": excerpt}
    except Exception as exc:
        return {"ok": False, "excerpt": "", "error": str(exc)}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8001, reload=True)
