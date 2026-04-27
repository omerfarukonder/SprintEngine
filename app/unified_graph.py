"""
Unified Knowledge Graph — builds a cross-source graph from all 5 stores.

Source types
------------
  kb_fact     → overall_kb_events.jsonl
  task        → sprint_state.json (tasks)
  task_event  → task_memory_events.jsonl
  faq         → sprint_faq.json
  meeting     → meeting_summaries.json

New edge types (on top of existing KB-internal ones)
-----------------------------------------------------
  owns        task_event → task   (structural, task_id reference)
  references  kb_fact    → task   (structural, ingest metadata task_id)

Cross-source semantic edges are built with the same cosine formula used
by the existing KB-only graph, but against a separate unified vector store
so existing vectors are never touched.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .storage import (
    OVERALL_KB_EVENTS_FILE,
    TASK_MEMORY_EVENTS_FILE,
    ensure_workspace,
    load_state,
)

BASE_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BASE_DIR / "workspace"
FAQ_JSON = WORKSPACE_DIR / "sprint_faq.json"
MEETING_SUMMARIES_FILE = WORKSPACE_DIR / "meeting_summaries.json"
UNIFIED_VECTORS_FILE = WORKSPACE_DIR / "unified_kb_vectors.json"

EDGE_SEMANTIC_THRESHOLD_DEFAULT = 0.78
EDGE_TOP_K_SEMANTIC_DEFAULT = 3


# ── helpers ────────────────────────────────────────────────────────────────────

def _norm_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _node_id(source_type: str, original_id: str) -> str:
    return f"{source_type}:{original_id}"


def _load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _save_json_file(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]{3,}", (text or "").lower())


def _clean_label_for_taxonomy(label: str) -> str:
    """Strip type prefix (DEFINITION:, CONSTRAINT:, RULE:, …) and trailing description
    so only the concept name remains for similarity comparison."""
    # Remove leading ALL-CAPS prefix followed by ': '
    label = re.sub(r"^[A-Z_]+:\s*", "", label.strip())
    # Take only the part before ' — ' or ': ' (description separator)
    label = re.split(r"\s*[—–]\s*|\s*:\s+", label, maxsplit=1)[0]
    return label.lower().strip()


def _label_similar(a: str, b: str, threshold: float = 0.40) -> bool:
    ca, cb = _clean_label_for_taxonomy(a), _clean_label_for_taxonomy(b)
    if not ca or not cb:
        return False
    return SequenceMatcher(None, ca, cb).ratio() >= threshold


# ── OpenAI embedding ───────────────────────────────────────────────────────────

def _openai_client() -> Optional[Any]:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    try:
        from openai import OpenAI
        return OpenAI(api_key=api_key)
    except Exception:
        return None


def _embed_texts(texts: List[str], model: str = "text-embedding-3-small") -> Optional[List[List[float]]]:
    client = _openai_client()
    if client is None or not texts:
        return None
    try:
        resp = client.embeddings.create(model=model, input=texts)
        return [item.embedding for item in resp.data]
    except Exception:
        return None


# ── Unified vector store ───────────────────────────────────────────────────────

def _load_unified_vectors() -> Dict[str, Any]:
    return _load_json_file(UNIFIED_VECTORS_FILE, {"model": "text-embedding-3-small", "vectors": {}})


def _save_unified_vectors(payload: Dict[str, Any]) -> None:
    _save_json_file(UNIFIED_VECTORS_FILE, payload)


def _ensure_unified_vectors(
    nodes: List[Dict[str, Any]],
    model: str = "text-embedding-3-small",
) -> Dict[str, List[float]]:
    store = _load_unified_vectors()
    if store.get("model") != model:
        store = {"model": model, "vectors": {}}
    vectors: Dict[str, List[float]] = store.get("vectors", {})
    missing = [n for n in nodes if n["id"] not in vectors]
    if missing:
        batch_size = 100
        updated = False
        for i in range(0, len(missing), batch_size):
            batch = missing[i : i + batch_size]
            embeds = _embed_texts([n["text"] for n in batch], model=model)
            if embeds:
                for node, vec in zip(batch, embeds):
                    vectors[node["id"]] = vec
                    updated = True
        if updated:
            store["vectors"] = vectors
            _save_unified_vectors(store)
    return vectors


# ── Step 6: Cross-source synthesis ────────────────────────────────────────────
#
# Phase 1: Source Registry — each source is a formatter function in _SOURCE_FORMATTERS.
#   Adding a new source (e.g. PRDs) = implement _format_xxx and append to the list.
#
# Phase 2: KB Feedback Loop — existing entity graph fed back as synthesis context
#   so the LLM finds new connections rather than re-extracting known facts.
#
# Phase 4: Task Digests — richer per-task history via task_digest.py replaces evts[:3].

def _format_tasks_section(
    task_nodes: List[Dict[str, Any]],
    task_digests: Dict[str, str],
    budget: int = 4000,
) -> str:
    if not task_nodes:
        return ""
    lines = ["=== TASKS ==="]
    for n in task_nodes:
        owner = n.get("owner", "") or "?"
        status = n.get("_status", "") or "?"
        definition = str(n.get("text", ""))[:400]
        digest = task_digests.get(n["original_id"], "")
        latest = n.get("_latest_update", "") or ""
        line = f"[{n['label']}] owner={owner} status={status}: {definition}"
        if digest:
            line += f" | history: {digest[:200]}"
        elif latest:
            line += f" | latest: {latest[:120]}"
        lines.append(line)
    block = "\n".join(lines)
    return block[:budget] + "\n[...truncated]" if len(block) > budget else block


def _format_meetings_section(budget: int = 2000) -> str:
    nodes = _meeting_nodes()
    if not nodes:
        return ""
    lines = ["=== MEETING SUMMARIES ==="]
    for n in nodes:
        summary = n.get("_summary", "") or ""
        lines.append(f"[{n['label']}]: {summary[:500]}")
    block = "\n".join(lines)
    return block[:budget] + "\n[...truncated]" if len(block) > budget else block


def _format_faqs_section(budget: int = 1500) -> str:
    nodes = [n for n in _faq_nodes() if n.get("is_active", True)]
    if not nodes:
        return ""
    lines = ["=== FAQs ==="]
    for n in nodes:
        q = n.get("_question", "") or ""
        a = n.get("_answer", "") or ""
        entry = f"Q: {q}"
        if a:
            entry += f"\nA: {a[:300]}"
        lines.append(entry)
    block = "\n".join(lines)
    return block[:budget] + "\n[...truncated]" if len(block) > budget else block


def _format_task_events_section(
    task_nodes: List[Dict[str, Any]],
    task_digests: Dict[str, str],
    budget: int = 1500,
) -> str:
    task_id_map: Dict[str, str] = {n["original_id"]: n["id"] for n in task_nodes}
    te_nodes = _task_event_nodes(task_id_map)
    if not te_nodes:
        return ""
    by_task: Dict[str, List[Dict[str, Any]]] = {}
    for n in te_nodes:
        by_task.setdefault(n.get("_task_id", ""), []).append(n)
    tid_to_label = {n["original_id"]: n["label"] for n in task_nodes}
    lines = ["=== RECENT TASK UPDATES ==="]
    for tid, evts in by_task.items():
        if tid in task_digests:
            continue  # digest already included in the tasks section
        evts.sort(key=lambda x: str(x.get("timestamp", "")), reverse=True)
        task_label = tid_to_label.get(tid, tid)
        for e in evts[:5]:
            ts = str(e.get("timestamp", ""))[:10]
            lines.append(f"[{task_label}] @ {ts}: {e['text'][:200]}")
    if len(lines) == 1:
        return ""
    block = "\n".join(lines)
    return block[:budget] + "\n[...truncated]" if len(block) > budget else block


def _format_prds_section(budget: int = 3000) -> str:
    try:
        from .prd_store import list_prds, load_prd  # noqa: PLC0415
    except Exception:
        return ""
    prds = list_prds(active_only=True)
    if not prds:
        return ""
    lines = ["=== PRDs ==="]
    chars_used = len(lines[0])
    for entry in prds:
        prd = load_prd(entry["id"])
        if not prd:
            continue
        title = prd.get("title", "")
        for i, chunk in enumerate(prd.get("chunks", [])[:4]):
            line = f"[PRD: {title} | chunk {i + 1}]: {chunk[:500]}"
            if chars_used + len(line) > budget:
                lines.append("[...truncated]")
                return "\n".join(lines)
            lines.append(line)
            chars_used += len(line)
    return "\n".join(lines) if len(lines) > 1 else ""


# Registry: list of callables that accept (task_nodes, task_digests) and return a section string.
# New source types just append a new lambda here — no other changes needed.
_SOURCE_FORMATTERS = [
    lambda tn, td: _format_tasks_section(tn, td),
    lambda tn, td: _format_meetings_section(),
    lambda tn, td: _format_faqs_section(),
    lambda tn, td: _format_task_events_section(tn, td),
    lambda tn, td: _format_prds_section(),
]


def _build_synthesis_context(max_chars: int = 12000) -> Tuple[str, List[str]]:
    """Build combined context from all registered sources + entity graph feedback.
    Returns (context_text, existing_active_kb_texts)."""
    kb_rows = _load_jsonl(OVERALL_KB_EVENTS_FILE)
    existing_kb_texts: List[str] = [
        str(r.get("text", "")).strip()
        for r in kb_rows
        if r.get("is_active", True) and r.get("text")
    ]

    # Phase 4: load per-task digests for richer history
    try:
        from .task_digest import load_all_task_digests  # noqa: PLC0415
        task_digests = load_all_task_digests()
    except Exception:
        task_digests = {}

    task_nodes = _task_nodes()

    # Phase 1: iterate registered source formatters
    sections: List[str] = []
    for fmt in _SOURCE_FORMATTERS:
        section = fmt(task_nodes, task_digests)
        if section:
            sections.append(section)

    # Phase 2: entity graph feedback — existing graph as synthesis context
    try:
        from .kb_graph import summarize_for_synthesis as _ent_summary  # noqa: PLC0415
        ent_block = _ent_summary(max_relations=60)
        if ent_block:
            block = "=== KNOWN ENTITY GRAPH ===\n" + ent_block
            if len(block) > 1500:
                block = block[:1500] + "\n[...truncated]"
            sections.append(block)
    except Exception:
        pass

    full_context = "\n\n".join(sections)
    if len(full_context) > max_chars:
        full_context = full_context[:max_chars] + "\n[...truncated]"
    return full_context, existing_kb_texts


def _synthesize_from_context(context_text: str, existing_kb_texts: List[str]) -> List[Dict[str, Any]]:
    """Call LLM to extract cross-source durable organizational facts."""
    client = _openai_client()
    if client is None or not context_text.strip():
        return []
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
    existing_block = "\n".join(f"- {t[:150]}" for t in existing_kb_texts[:60])
    system_prompt = (
        "You extract durable organizational knowledge by synthesizing evidence across multiple sources.\n"
        "Return strict JSON with key: items.\n"
        "Each item: {text, knowledge_type, scope, confidence}\n"
        "knowledge_type must be one of: constraint, capability, dependency, risk, organizational_limit, process_rule\n"
        "scope must be one of: team, system, process, project\n"
        "confidence: 0.0–1.0\n\n"
        "Rules:\n"
        "- Only extract facts EXPLICITLY stated in the provided context.\n"
        "- Only include durable reusable facts: team ownership, constraints, dependencies, "
        "process rules, cross-team alignment requirements.\n"
        "- Do NOT include task-specific progress, dates, ETAs, or status updates.\n"
        "- CRITICAL DEDUP: Carefully read EVERY item in 'Existing KB' below. "
        "Do NOT output any fact that is semantically equivalent to, a subset of, or a rephrasing of an existing item. "
        "If the existing KB already says '$startingprice$ must use the lowest valid price from page 1', "
        "do NOT output a new fact that says 'the system must exclude pagination beyond page 1' — that is covered.\n"
        "- Use the 'Known Entity Graph' section to find NEW bridging connections between entities "
        "already known — do not re-state ownership or relations already present there.\n"
        "- When useful, rewrite as canonical policy: 'For X, Y must...' or 'If X, then Y.'\n"
        "- Each fact must be SPECIFIC and ACTIONABLE — not vague. "
        "Bad: 'the page must handle the fallback scenario'. "
        "Good: 'If no PLP products are returned, $startingprice$ must display \"N/A\" instead of a zero or empty value.'\n"
        "- Do NOT produce near-duplicates — if two facts cover the same rule from different angles, "
        "combine them into ONE comprehensive fact. Aim for 3–8 high-quality NEW facts only.\n"
        "- If the context adds no new information beyond what is already in the Existing KB, return {\"items\": []}."
    )
    user_prompt = (
        f"Existing KB (do NOT duplicate or rephrase any of these):\n{existing_block}\n\n"
        f"Cross-source context:\n{context_text}\n\n"
        "Output only NEW facts not already covered above. JSON only."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        parsed = json.loads(resp.choices[0].message.content or "{}")
    except Exception:
        return []
    if not isinstance(parsed, dict):
        return []
    items = parsed.get("items", [])
    if not isinstance(items, list):
        return []
    allowed_types = {"constraint", "capability", "dependency", "risk", "organizational_limit", "process_rule"}
    allowed_scopes = {"team", "system", "process", "project"}
    result: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        ktype = str(item.get("knowledge_type", "")).strip().lower()
        scope = str(item.get("scope", "")).strip().lower()
        try:
            conf = float(item.get("confidence", 0.0))
        except (TypeError, ValueError):
            conf = 0.0
        if not text or ktype not in allowed_types or scope not in allowed_scopes:
            continue
        result.append({"text": text, "knowledge_type": ktype, "scope": scope, "confidence": min(1.0, max(0.0, conf))})
    return result


def _extract_entities_from_tasks() -> Dict[str, Any]:
    """Ask the LLM to extract entity/relation graph nodes from all sprint tasks.

    Returns the same {entities, relations} dict that ingest_extracted() expects.
    Each task becomes a 'process' or 'domain' entity; owners become 'person' entities;
    dependency/ownership relations are inferred where mentioned in definitions."""
    client = _openai_client()
    if client is None:
        return {}
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

    task_nodes = _task_nodes()
    if not task_nodes:
        return {}

    lines: List[str] = []
    for n in task_nodes:
        name = n.get("label", "")
        owner = n.get("owner", "") or ""
        definition = str(n.get("text", ""))[:300]
        status = n.get("_status", "") or ""
        line = f"- Task: {name}"
        if owner:
            line += f" | Owner: {owner}"
        if status:
            line += f" | Status: {status}"
        if definition:
            line += f" | Description: {definition}"
        lines.append(line)

    task_block = "\n".join(lines)

    system_prompt = (
        "You are an entity-relationship extractor building a knowledge graph from sprint tasks.\n"
        "Given a list of sprint tasks with owners and descriptions, extract ENTITIES and RELATIONS.\n\n"
        "Return strict JSON with two keys: entities, relations.\n\n"
        "entities: array of objects with keys:\n"
        "  - name: short canonical label\n"
        "  - type: one of: team, domain, topic, system, person, process, constraint\n"
        "  - description: one sentence explaining what this entity is or does\n\n"
        "relations: array of objects with keys:\n"
        "  - source: entity name (must match entities array)\n"
        "  - target: entity name (must match entities array)\n"
        "  - type: one of: has_subtopic, owns, depends_on, has_constraint, related_to, part_of, communicates_with, blocks\n"
        "  - label: short human-readable description\n"
        "  - confidence: 0.0 to 1.0\n\n"
        "Rules:\n"
        "- Each task should become a 'process' entity using its task name as the entity name.\n"
        "- Each owner (person name) should become a 'person' entity.\n"
        "- Infer 'owns' relations between owners and their tasks.\n"
        "- Extract any teams, systems, or domains mentioned in task descriptions.\n"
        "- Extract dependency/blocking relationships if mentioned in descriptions.\n"
        "- MERGE obvious variants into one entity: use short canonical names, strip syntax like $...$.\n"
        "- Do NOT extract generic/common nouns (HTML, URL, page, API, etc.) as entities.\n"
        "- Do NOT extract transient facts: dates, ETAs, percentage progress, status words."
    )
    user_prompt = (
        f"Sprint tasks:\n{task_block}\n\n"
        "Extract entities and relations. Output JSON only."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        parsed = json.loads(resp.choices[0].message.content or "{}")
    except Exception:
        return {}

    if not isinstance(parsed, dict):
        return {}

    VALID_ENTITY_TYPES = {"team", "domain", "topic", "system", "person", "process", "constraint"}
    VALID_RELATION_TYPES = {
        "has_subtopic", "owns", "depends_on", "has_constraint",
        "related_to", "part_of", "communicates_with", "blocks",
    }

    entities: List[Dict[str, Any]] = []
    for item in parsed.get("entities", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        etype = str(item.get("type", "")).strip().lower()
        desc = str(item.get("description", "")).strip()
        if not name:
            continue
        entities.append({
            "name": name,
            "type": etype if etype in VALID_ENTITY_TYPES else "process",
            "description": desc,
            "facts": [],
        })

    entity_names_lower = {e["name"].lower() for e in entities}

    relations: List[Dict[str, Any]] = []
    for item in parsed.get("relations", []):
        if not isinstance(item, dict):
            continue
        source = str(item.get("source", "")).strip()
        target = str(item.get("target", "")).strip()
        rtype = str(item.get("type", "")).strip().lower()
        label = str(item.get("label", "")).strip()
        try:
            conf = float(item.get("confidence", 0.8))
        except (TypeError, ValueError):
            conf = 0.8
        if not source or not target:
            continue
        if source.lower() not in entity_names_lower or target.lower() not in entity_names_lower:
            continue
        relations.append({
            "source": source,
            "target": target,
            "type": rtype if rtype in VALID_RELATION_TYPES else "related_to",
            "label": label,
            "confidence": max(0.0, min(1.0, conf)),
        })

    return {"entities": entities, "relations": relations}


def _strip_entity_name(name: str) -> str:
    """Normalize entity name: strip $..$ wrappers, leading articles, trailing noise."""
    s = name.strip()
    s = re.sub(r"^\$+|\$+$", "", s)
    s = re.sub(r'^(the|a|an)\s+', '', s, flags=re.IGNORECASE)
    s = s.strip()
    return s


def _entity_canonical_key(name: str) -> str:
    """Produce a merge key for entity dedup: lowercase, stripped, collapsed whitespace."""
    s = _strip_entity_name(name).lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _dedup_extracted_entities(
    entities: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Merge entities with similar canonical keys.

    Returns (deduped_entities, rename_map) where rename_map maps original names
    to the canonical survivor name so relations can be remapped.
    """
    NOISE_KEYS = {
        "html", "css", "url", "api", "json", "xml", "page", "content",
        "date", "time", "time data", "desktop", "mobile", "mweb",
        "refresh frequency", "pricing parts", "fallback text",
        "sites date format", "plp url",
    }
    canonical_groups: Dict[str, List[Dict[str, Any]]] = {}
    for ent in entities:
        key = _entity_canonical_key(ent["name"])
        if key in NOISE_KEYS or len(key) < 2:
            continue
        canonical_groups.setdefault(key, []).append(ent)

    _MERGE_THRESHOLD = 0.78
    merged_keys = list(canonical_groups.keys())
    parent_map: Dict[str, str] = {}  # key → canonical parent key
    for key in merged_keys:
        parent_map.setdefault(key, key)

    for i in range(len(merged_keys)):
        for j in range(i + 1, len(merged_keys)):
            ki, kj = merged_keys[i], merged_keys[j]
            ri, rj = parent_map.get(ki, ki), parent_map.get(kj, kj)
            if ri == rj:
                continue
            sim = SequenceMatcher(None, ki, kj).ratio()
            if sim >= _MERGE_THRESHOLD or ki in kj or kj in ki:
                survivor = ri if len(ri) >= len(rj) else rj
                loser = rj if survivor == ri else ri
                for k in list(parent_map.keys()):
                    if parent_map[k] == loser:
                        parent_map[k] = survivor

    final_groups: Dict[str, List[Dict[str, Any]]] = {}
    for key, group in canonical_groups.items():
        parent = parent_map.get(key, key)
        final_groups.setdefault(parent, []).extend(group)

    deduped: List[Dict[str, Any]] = []
    rename_map: Dict[str, str] = {}
    for _key, group in final_groups.items():
        survivor = max(group, key=lambda e: len(e.get("description", "")))
        survivor["name"] = _strip_entity_name(survivor["name"])
        deduped.append(survivor)
        for ent in group:
            if ent["name"] != survivor["name"]:
                rename_map[ent["name"]] = survivor["name"]
            stripped = _strip_entity_name(ent["name"])
            if stripped != survivor["name"]:
                rename_map[stripped] = survivor["name"]

    return deduped, rename_map


def _extract_entities_from_text(text: str, source_label: str = "input") -> Dict[str, Any]:
    """Generic entity/relation extractor for arbitrary text.

    Used by micro_synthesize to process a single payload (task definition,
    meeting summary, etc.) without requiring a full task list.
    Returns the same {entities, relations} dict that ingest_extracted() expects.
    """
    client = _openai_client()
    if client is None or not text.strip():
        return {}
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

    system_prompt = (
        "You are an entity-relationship extractor building an organizational knowledge graph.\n"
        "Given a text snippet, extract named entities and their relationships.\n\n"
        "Return strict JSON: {entities, relations}\n\n"
        "entities: [{name, type, description}]\n"
        "  type: one of: team, domain, topic, system, person, process, constraint\n\n"
        "relations: [{source, target, type, label, confidence}]\n"
        "  type: one of: has_subtopic, owns, depends_on, has_constraint, related_to, part_of, communicates_with, blocks\n\n"
        "Rules:\n"
        "- Only extract entities explicitly named in the text.\n"
        "- Use SHORT CANONICAL names: strip surrounding syntax like $...$ or quotes. "
        "E.g. use 'startingprice' not '$startingprice$', use 'CMS' not 'the CMS system'.\n"
        "- MERGE variants into one entity: if the same concept appears with different phrasing "
        "(e.g. 'SEO content area' and 'SEO content', 'widget areas' and 'widgets', "
        "'Hydra CMS' and 'Hydra'), pick the most specific canonical name and use it consistently.\n"
        "- Do NOT extract generic/common nouns as entities: HTML, CSS, URL, date, time, "
        "desktop, mobile, API, JSON, page, content, etc. are too generic.\n"
        "- Do NOT extract data fields or payload attributes as entities (e.g. 'PLP URL', 'timestamp').\n"
        "- Do NOT extract dates, ETAs, percentage progress, or format specifications.\n"
        "- Prefer fewer, high-quality entities over many low-value ones.\n"
        "- Each entity must be a proper noun, a named system/tool, a named team, or a domain-specific concept.\n"
        "- If the text contains no meaningful named entities, return empty arrays."
    )
    user_prompt = f"Source: {source_label}\n\nText:\n{text[:3000]}\n\nOutput JSON only."

    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        parsed = json.loads(resp.choices[0].message.content or "{}")
    except Exception:
        return {}

    if not isinstance(parsed, dict):
        return {}

    VALID_ENTITY_TYPES = {"team", "domain", "topic", "system", "person", "process", "constraint"}
    VALID_RELATION_TYPES = {
        "has_subtopic", "owns", "depends_on", "has_constraint",
        "related_to", "part_of", "communicates_with", "blocks",
    }

    entities: List[Dict[str, Any]] = []
    for item in parsed.get("entities", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        etype = str(item.get("type", "")).strip().lower()
        desc = str(item.get("description", "")).strip()
        if not name:
            continue
        entities.append({
            "name": name,
            "type": etype if etype in VALID_ENTITY_TYPES else "topic",
            "description": desc,
            "facts": [],
        })

    entities, rename_map = _dedup_extracted_entities(entities)

    entity_names_lower = {e["name"].lower() for e in entities}
    relations: List[Dict[str, Any]] = []
    for item in parsed.get("relations", []):
        if not isinstance(item, dict):
            continue
        source = rename_map.get(str(item.get("source", "")).strip(), str(item.get("source", "")).strip())
        target = rename_map.get(str(item.get("target", "")).strip(), str(item.get("target", "")).strip())
        rtype = str(item.get("type", "")).strip().lower()
        label = str(item.get("label", "")).strip()
        try:
            conf = float(item.get("confidence", 0.8))
        except (TypeError, ValueError):
            conf = 0.8
        if not source or not target:
            continue
        if source.lower() not in entity_names_lower or target.lower() not in entity_names_lower:
            continue
        if source == target:
            continue
        relations.append({
            "source": source,
            "target": target,
            "type": rtype if rtype in VALID_RELATION_TYPES else "related_to",
            "label": label,
            "confidence": max(0.0, min(1.0, conf)),
        })

    return {"entities": entities, "relations": relations}


def micro_synthesize(
    trigger: str,
    payload_text: str,
    extra_existing_kb_texts: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Lightweight single-payload synthesis — runs automatically after meaningful writes.

    Phase 3: Incremental Ingestion.
    Instead of processing all sources, focuses on the changed content only.
    Uses the last 30 KB facts for dedup and the entity graph for context.
    Should be called from a background thread to avoid blocking responses.

    extra_existing_kb_texts: additional fact texts to treat as "already known"
    for dedup (used by batch_micro_synthesize_prd to chain chunk results).
    """
    if not payload_text.strip():
        return {"ok": False, "reason": "empty_payload"}

    ensure_workspace()

    # Recent KB facts for dedup (last 30 active, sorted newest-first)
    kb_rows = _load_jsonl(OVERALL_KB_EVENTS_FILE)
    active_kb = sorted(
        [r for r in kb_rows if r.get("is_active", True) and r.get("text")],
        key=lambda r: str(r.get("timestamp", "")),
        reverse=True,
    )
    existing_kb_texts = [str(r["text"]).strip() for r in active_kb[:30]]
    if extra_existing_kb_texts:
        existing_kb_texts = list(extra_existing_kb_texts) + existing_kb_texts

    # Entity graph feedback
    ent_block = ""
    try:
        from .kb_graph import summarize_for_synthesis as _ent_summary  # noqa: PLC0415
        ent_block = _ent_summary(max_relations=30)
    except Exception:
        pass

    context = f"=== {trigger.upper()} ===\n{payload_text[:3000]}"
    if ent_block:
        context += f"\n\n=== KNOWN ENTITY GRAPH ===\n{ent_block[:1000]}"

    candidates = _synthesize_from_context(context, existing_kb_texts)

    from .memory import append_overall_kb_event, refine_overall_kb_candidate  # noqa: PLC0415
    written = 0
    written_texts: List[str] = []
    for cand in candidates:
        result = refine_overall_kb_candidate(cand["text"])
        if not result.get("keep", False):
            continue
        canonical = str(result.get("canonical_text", cand["text"])).strip() or cand["text"]
        ev = append_overall_kb_event(
            text=canonical,
            knowledge_type=cand["knowledge_type"],
            scope=cand["scope"],
            confidence=cand["confidence"],
            source=f"micro_synthesis:{trigger}",
        )
        if ev:
            written += 1
            written_texts.append(canonical)

    # Entity extraction — only when text is substantial enough to name real entities
    entity_result: Dict[str, Any] = {"entities_upserted": 0, "relations_upserted": 0}
    if len(payload_text.strip()) > 60:
        extracted = _extract_entities_from_text(payload_text, source_label=trigger)
        if extracted.get("entities") or extracted.get("relations"):
            from .kb_graph import ingest_extracted, resolve_entity_hierarchy  # noqa: PLC0415
            entity_result = ingest_extracted(extracted, source_event=f"micro_synthesis:{trigger}")
            resolve_entity_hierarchy()

    return {
        "ok": True,
        "trigger": trigger,
        "written": written,
        "written_texts": written_texts,
        "entities_upserted": entity_result.get("entities_upserted", 0),
        "relations_upserted": entity_result.get("relations_upserted", 0),
    }


def batch_micro_synthesize_prd(prd_id: str, chunks: List[str], max_chunks: int = 10) -> Dict[str, Any]:
    """Process PRD chunks sequentially, accumulating written facts for cross-chunk dedup.

    Unlike firing independent threads per chunk, this ensures later chunks
    see earlier chunks' KB facts so the same rule isn't written multiple times.
    """
    trigger = f"prd:{prd_id}"
    accumulated_texts: List[str] = []
    total_written = 0
    total_entities = 0
    total_relations = 0

    for chunk in chunks[:max_chunks]:
        result = micro_synthesize(
            trigger=trigger,
            payload_text=chunk,
            extra_existing_kb_texts=accumulated_texts,
        )
        chunk_texts = result.get("written_texts", [])
        accumulated_texts.extend(chunk_texts)
        total_written += result.get("written", 0)
        total_entities += result.get("entities_upserted", 0)
        total_relations += result.get("relations_upserted", 0)

    return {
        "ok": True,
        "prd_id": prd_id,
        "chunks_processed": min(len(chunks), max_chunks),
        "written": total_written,
        "entities_upserted": total_entities,
        "relations_upserted": total_relations,
    }


def run_cross_source_synthesis() -> Dict[str, Any]:
    """Extract cross-source organizational facts and write them to the general KB.

    Mines all active non-KB sources (tasks, meetings, FAQs, task events), calls the
    LLM to extract durable organizational facts that are not already in the KB, runs
    each candidate through the standard durability gate, and appends accepted facts
    with source='cross_source_synthesis'."""
    ensure_workspace()
    context, existing_kb_texts = _build_synthesis_context()
    if not context.strip():
        return {"ok": False, "reason": "no_context", "written": 0, "skipped": 0, "candidates": 0}

    candidates = _synthesize_from_context(context, existing_kb_texts)
    if not candidates:
        return {"ok": True, "written": 0, "skipped": 0, "candidates": 0}

    # Import lazily to avoid circular dependency (memory imports storage, not unified_graph)
    from .memory import append_overall_kb_event, refine_overall_kb_candidate  # noqa: PLC0415

    written = 0
    skipped = 0
    for cand in candidates:
        text = cand["text"]
        result = refine_overall_kb_candidate(text)
        if not result.get("keep", False):
            skipped += 1
            continue
        canonical = str(result.get("canonical_text", text)).strip() or text
        ev = append_overall_kb_event(
            text=canonical,
            knowledge_type=cand["knowledge_type"],
            scope=cand["scope"],
            confidence=cand["confidence"],
            source="cross_source_synthesis",
        )
        if ev:
            written += 1
        else:
            skipped += 1

    # Second pass: extract entities/relations from task names + descriptions
    # and upsert them into the entity knowledge graph.
    from .kb_graph import ingest_extracted, refine_entity_graph, resolve_entity_hierarchy  # noqa: PLC0415
    extracted = _extract_entities_from_tasks()
    entity_result = {"entities_upserted": 0, "relations_upserted": 0}
    if extracted.get("entities") or extracted.get("relations"):
        entity_result = ingest_extracted(extracted, source_event="cross_source_synthesis")

    # Third pass: resolve parent-child hierarchy from entity name patterns.
    hierarchy_result = resolve_entity_hierarchy()

    refine_result = refine_entity_graph(max_entities=40, max_new_relations=30)

    return {
        "ok": True,
        "written": written,
        "skipped": skipped,
        "candidates": len(candidates),
        "entities_upserted": entity_result.get("entities_upserted", 0),
        "relations_upserted": entity_result.get("relations_upserted", 0),
        "parents_created": hierarchy_result.get("parents_created", 0),
        "hierarchy_edges": hierarchy_result.get("edges_added", 0),
        "entities_merged": refine_result.get("entities_merged", 0),
        "relations_refined_added": refine_result.get("relations_added", 0),
        "relations_refined_fixed": refine_result.get("relations_fixed", 0),
    }


def rebuild_unified_vectors(model: str = "text-embedding-3-small") -> int:
    """Re-embed all nodes from all sources. Call after major KB changes."""
    ensure_workspace()
    task_nodes = _task_nodes()
    task_id_map: Dict[str, str] = {n["original_id"]: n["id"] for n in task_nodes}
    all_nodes = (
        _kb_fact_nodes(active_only=False)
        + task_nodes
        + _task_event_nodes(task_id_map)
        + _faq_nodes()
        + _meeting_nodes()
    )
    store: Dict[str, Any] = {"model": model, "vectors": {}}
    batch_size = 100
    for i in range(0, len(all_nodes), batch_size):
        batch = all_nodes[i : i + batch_size]
        embeds = _embed_texts([n["text"] for n in batch], model=model)
        if embeds:
            for node, vec in zip(batch, embeds):
                store["vectors"][node["id"]] = vec
    _save_unified_vectors(store)
    return len(store["vectors"])


# ── Source adapters ────────────────────────────────────────────────────────────

def _kb_fact_nodes(active_only: bool = True) -> List[Dict[str, Any]]:
    rows = _load_jsonl(OVERALL_KB_EVENTS_FILE)
    nodes: List[Dict[str, Any]] = []
    for row in rows:
        if active_only and not bool(row.get("is_active", True)):
            continue
        eid = str(row.get("id", "")).strip()
        if not eid:
            continue
        text = str(row.get("text", "")).strip()
        meta = row.get("metadata", {})
        if not isinstance(meta, dict):
            meta = {}
        nodes.append(
            {
                "id": _node_id("kb_fact", eid),
                "original_id": eid,
                "source_type": "kb_fact",
                "label": (text[:88] + "…") if len(text) > 91 else text,
                "text": text,
                "subtype": str(row.get("knowledge_type", "")).strip().lower(),
                "scope": str(row.get("scope", "")).strip().lower(),
                "confidence": float(row.get("confidence", 0.0) or 0.0),
                "is_active": bool(row.get("is_active", True)),
                "timestamp": str(row.get("timestamp", "")),
                "version": int(row.get("version", 1) or 1),
                "source": str(row.get("source", "")),
                "owner": "",
                # internal fields for edge building (stripped from public payload)
                "_supersedes_event_id": str(row.get("supersedes_event_id", "") or ""),
                "_superseded_by": str(row.get("superseded_by", "") or ""),
                "_overwrites_event_ids": meta.get("overwrites_event_ids", [])
                if isinstance(meta.get("overwrites_event_ids"), list)
                else [],
                "_ingest_task_id": str(meta.get("task_id", "") or ""),
            }
        )
    return nodes


def _task_nodes() -> List[Dict[str, Any]]:
    try:
        state = load_state()
    except Exception:
        return []
    nodes: List[Dict[str, Any]] = []
    for task in state.tasks or []:
        tid = str(getattr(task, "id", "") or "").strip()
        if not tid:
            continue
        name = str(getattr(task, "task_name", "") or "").strip()
        definition = str(getattr(task, "definition", "") or "").strip()
        latest = str(getattr(task, "latest_update", "") or "").strip()
        blockers = getattr(task, "blockers", []) or []
        blockers_text = "; ".join(str(b) for b in blockers) if blockers else ""
        owner = str(getattr(task, "owner", "") or "").strip()
        status_val = task.status.value if hasattr(task.status, "value") else str(task.status)
        traffic_val = task.traffic_light.value if hasattr(task.traffic_light, "value") else str(task.traffic_light)
        eta = str(getattr(task, "eta", "") or "").strip()
        ts = str(getattr(task, "last_updated_at", "") or "")
        full_text = " | ".join(p for p in [name, definition, latest] if p)
        nodes.append(
            {
                "id": _node_id("task", tid),
                "original_id": tid,
                "source_type": "task",
                "label": name,
                "text": full_text,
                "subtype": status_val,
                "scope": "project",
                "confidence": 1.0,
                "is_active": status_val not in {"done", "on_hold"},
                "timestamp": ts,
                "version": 1,
                "source": "sprint_state",
                "owner": owner,
                "_status": status_val,
                "_traffic": traffic_val,
                "_eta": eta,
                "_blockers": blockers_text,
                "_latest_update": latest,
            }
        )
    return nodes


def _task_event_nodes(task_id_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """task_id_map: original task_id → unified node id for that task."""
    rows = _load_jsonl(TASK_MEMORY_EVENTS_FILE)
    seen: Dict[str, Set[str]] = {}  # task_id → set of normed texts (dedup per task)
    nodes: List[Dict[str, Any]] = []
    for row in rows:
        meta = row.get("metadata", {})
        if not isinstance(meta, dict):
            meta = {}
        task_id = str(meta.get("task_id", "") or "").strip()
        text = str(row.get("text", "")).strip()
        ts = str(row.get("timestamp", "")).strip()
        if not task_id or not text:
            continue
        normed = _norm_text(text)
        seen.setdefault(task_id, set())
        if normed in seen[task_id]:
            continue
        seen[task_id].add(normed)
        raw_key = f"{task_id}|{normed}|{ts[:19]}"
        eid = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:20]
        nodes.append(
            {
                "id": _node_id("task_event", eid),
                "original_id": eid,
                "source_type": "task_event",
                "label": (text[:88] + "…") if len(text) > 91 else text,
                "text": text,
                "subtype": str(row.get("event_type", "task_history_event")).strip(),
                "scope": "project",
                "confidence": 0.9,
                "is_active": True,
                "timestamp": ts,
                "version": 1,
                "source": "task_memory_events",
                "owner": str(meta.get("owner", "") or "").strip(),
                "_task_id": task_id,
                "_parent_node_id": task_id_map.get(task_id, ""),
            }
        )
    return nodes


def _faq_nodes() -> List[Dict[str, Any]]:
    data = _load_json_file(FAQ_JSON, {"items": []})
    items = data.get("items", []) if isinstance(data, dict) else []
    nodes: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        fid = str(item.get("id", "")).strip()
        question = str(item.get("question", "") or "").strip()
        answer = str(item.get("answer", "") or "").strip()
        status = str(item.get("status", "") or "").strip()
        if not fid or not question:
            continue
        full_text = question + (" | " + answer if answer else "")
        nodes.append(
            {
                "id": _node_id("faq", fid),
                "original_id": fid,
                "source_type": "faq",
                "label": (question[:88] + "…") if len(question) > 91 else question,
                "text": full_text,
                "subtype": status or "active",
                "scope": "project",
                "confidence": 0.85,
                "is_active": status != "archived",
                "timestamp": str(item.get("created_at", "") or ""),
                "version": 1,
                "source": "sprint_faq",
                "owner": "",
                "_question": question,
                "_answer": answer,
            }
        )
    return nodes


def _meeting_nodes() -> List[Dict[str, Any]]:
    data = _load_json_file(MEETING_SUMMARIES_FILE, {"meetings": []})
    meetings = data.get("meetings", []) if isinstance(data, dict) else []
    nodes: List[Dict[str, Any]] = []
    for m in meetings:
        if not isinstance(m, dict):
            continue
        mid = str(m.get("id", "")).strip()
        name = str(m.get("meeting_name", "") or "").strip()
        summary = str(m.get("summary", "") or "").strip()
        created = str(m.get("created_at", "") or "").strip()
        if not mid or not name:
            continue
        full_text = name + (" | " + summary if summary else "")
        nodes.append(
            {
                "id": _node_id("meeting", mid),
                "original_id": mid,
                "source_type": "meeting",
                "label": name,
                "text": full_text,
                "subtype": "meeting_summary",
                "scope": "project",
                "confidence": 0.9,
                "is_active": True,
                "timestamp": created,
                "version": 1,
                "source": "meeting_summaries",
                "owner": "",
                "_meeting_name": name,
                "_summary": summary,
            }
        )
    return nodes


# ── Unified search ─────────────────────────────────────────────────────────────

def unified_hybrid_search(
    query: str,
    top_k: int = 8,
    active_only: bool = True,
    source_types: Optional[List[str]] = None,
) -> List[Tuple[float, Dict[str, Any]]]:
    """Search across all 5 stores with the same hybrid formula as KB-only search."""
    task_nodes = _task_nodes()
    task_id_map = {n["original_id"]: n["id"] for n in task_nodes}
    all_nodes = (
        _kb_fact_nodes(active_only=active_only)
        + task_nodes
        + _task_event_nodes(task_id_map)
        + _faq_nodes()
        + _meeting_nodes()
    )
    if active_only:
        all_nodes = [n for n in all_nodes if bool(n.get("is_active", True))]
    if source_types:
        allowed = set(source_types)
        all_nodes = [n for n in all_nodes if n["source_type"] in allowed]
    if not all_nodes:
        return []

    q_tokens = set(_tokenize(query))
    q_norm = _norm_text(query)
    lex_scores: Dict[str, float] = {}
    for n in all_nodes:
        t_tokens = set(_tokenize(n["text"]))
        overlap = len(q_tokens & t_tokens)
        contains = 1 if q_norm and q_norm in _norm_text(n["text"]) else 0
        score = float(overlap * 2 + contains)
        if score > 0:
            lex_scores[n["id"]] = score

    vectors = _ensure_unified_vectors(all_nodes)
    q_vecs = _embed_texts([query])
    sem_scores: Dict[str, float] = {}
    if q_vecs:
        qv = q_vecs[0]
        for n in all_nodes:
            vec = vectors.get(n["id"])
            if vec:
                sem_scores[n["id"]] = max(0.0, _cosine(qv, vec))

    max_lex = max(lex_scores.values()) if lex_scores else 1.0
    merged: List[Tuple[float, Dict[str, Any]]] = []
    for n in all_nodes:
        nid = n["id"]
        lex_n = (lex_scores.get(nid, 0.0) / max_lex) if max_lex else 0.0
        sem_n = sem_scores.get(nid, 0.0)
        active_bonus = 0.08 if bool(n.get("is_active", True)) else 0.0
        score = 0.6 * lex_n + 0.4 * sem_n + active_bonus
        if score > 0:
            merged.append((score, n))
    merged.sort(key=lambda x: x[0], reverse=True)
    return merged[:top_k]


# ── Graph builder ──────────────────────────────────────────────────────────────

def build_unified_graph_payload(
    active_only: bool = True,
    include_archived: bool = False,
    semantic_threshold: float = EDGE_SEMANTIC_THRESHOLD_DEFAULT,
    top_k_semantic: int = EDGE_TOP_K_SEMANTIC_DEFAULT,
    max_nodes: int = 300,
    max_task_events_per_task: int = 5,
) -> Dict[str, Any]:
    ensure_workspace()

    # load all nodes
    task_nodes = _task_nodes()
    task_id_map: Dict[str, str] = {n["original_id"]: n["id"] for n in task_nodes}
    kb_nodes = _kb_fact_nodes(active_only=active_only)
    faq_nodes = _faq_nodes()
    meeting_nodes = _meeting_nodes()

    # cap task_event nodes to the N most recent per task to keep the graph legible
    raw_te_nodes = _task_event_nodes(task_id_map)
    if max_task_events_per_task > 0:
        by_task: Dict[str, List[Dict[str, Any]]] = {}
        for n in raw_te_nodes:
            tid = n.get("_task_id", "")
            by_task.setdefault(tid, []).append(n)
        te_nodes: List[Dict[str, Any]] = []
        for tid, evts in by_task.items():
            evts.sort(key=lambda x: str(x.get("timestamp", "")), reverse=True)
            te_nodes.extend(evts[:max_task_events_per_task])
    else:
        te_nodes = raw_te_nodes

    all_nodes = kb_nodes + task_nodes + te_nodes + faq_nodes + meeting_nodes

    if active_only:
        all_nodes = [n for n in all_nodes if bool(n.get("is_active", True))]

    # cap to max_nodes (most recent first)
    all_nodes.sort(key=lambda n: str(n.get("timestamp", "")), reverse=True)
    total_before_cap = len(all_nodes)
    if max_nodes > 0 and len(all_nodes) > max_nodes:
        all_nodes = all_nodes[:max_nodes]

    by_id: Dict[str, Dict[str, Any]] = {n["id"]: n for n in all_nodes}
    ids = list(by_id.keys())

    # public node list (strip internal _ fields)
    public_nodes: List[Dict[str, Any]] = []
    for n in all_nodes:
        public_nodes.append(
            {
                "id": n["id"],
                "label": n["label"],
                "text": n["text"],
                "source_type": n["source_type"],
                "subtype": n.get("subtype", ""),
                "scope": n.get("scope", ""),
                "confidence": n.get("confidence", 0.0),
                "is_active": n.get("is_active", True),
                "timestamp": n.get("timestamp", ""),
                "version": n.get("version", 1),
                "source": n.get("source", ""),
                "owner": n.get("owner", ""),
                # source-specific extras for the detail panel
                "status": n.get("_status", ""),
                "traffic_light": n.get("_traffic", ""),
                "eta": n.get("_eta", ""),
                "blockers": n.get("_blockers", ""),
                "latest_update": n.get("_latest_update", ""),
                "question": n.get("_question", ""),
                "answer": n.get("_answer", ""),
                "meeting_name": n.get("_meeting_name", ""),
                "summary": n.get("_summary", ""),
            }
        )

    # edge building
    edges: List[Dict[str, Any]] = []
    edge_keys: Set[Tuple[str, str, str]] = set()
    edge_counts: Dict[str, int] = {
        "version": 0,
        "overwrite": 0,
        "taxonomy": 0,
        "semantic": 0,
        "owns": 0,
        "references": 0,
    }

    def add_edge(src: str, tgt: str, etype: str, weight: float = 1.0, directed: bool = True) -> None:
        s, t = src.strip(), tgt.strip()
        if not s or not t or s == t:
            return
        if s not in by_id or t not in by_id:
            return
        key: Tuple[str, str, str] = (s, t, etype) if directed else (min(s, t), max(s, t), etype)
        if key in edge_keys:
            return
        edge_keys.add(key)
        edges.append({"source": s, "target": t, "type": etype, "weight": float(max(0.0, min(1.0, weight)))})
        edge_counts[etype] = edge_counts.get(etype, 0) + 1

    # KB-internal: version chains
    for n in kb_nodes:
        if n["id"] not in by_id:
            continue
        sup = n.get("_supersedes_event_id", "")
        sup_by = n.get("_superseded_by", "")
        if sup:
            add_edge(_node_id("kb_fact", sup), n["id"], "version", directed=True)
        if sup_by:
            add_edge(n["id"], _node_id("kb_fact", sup_by), "version", directed=True)

    # KB-internal: overwrite chains
    for n in kb_nodes:
        if n["id"] not in by_id:
            continue
        for old_id in n.get("_overwrites_event_ids", []):
            add_edge(_node_id("kb_fact", str(old_id).strip()), n["id"], "overwrite", directed=True)

    # KB-internal: taxonomy (same scope + subtype + similar label → same concept lineage)
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for n in kb_nodes:
        if n["id"] not in by_id:
            continue
        key_g = (n.get("scope", ""), n.get("subtype", ""))
        groups.setdefault(key_g, []).append(n)
    for _, group in groups.items():
        if len(group) < 2:
            continue
        group.sort(key=lambda x: str(x.get("timestamp", "")), reverse=True)
        for i in range(len(group) - 1):
            # Only connect nodes that are about the same concept (label similarity gate)
            if _label_similar(group[i].get("label", ""), group[i + 1].get("label", "")):
                add_edge(group[i]["id"], group[i + 1]["id"], "taxonomy", weight=0.35, directed=False)

    # structural: owns (task_event → task)
    for n in te_nodes:
        if n["id"] not in by_id:
            continue
        parent = n.get("_parent_node_id", "")
        if parent and parent in by_id:
            add_edge(n["id"], parent, "owns", weight=1.0, directed=True)

    # structural: references (kb_fact → task via ingest metadata task_id)
    for n in kb_nodes:
        if n["id"] not in by_id:
            continue
        ingest_tid = n.get("_ingest_task_id", "")
        if ingest_tid:
            task_nid = task_id_map.get(ingest_tid, "")
            if task_nid and task_nid in by_id:
                add_edge(n["id"], task_nid, "references", weight=0.9, directed=True)

    # semantic: cross-source cosine similarity
    top_k = max(0, int(top_k_semantic))
    threshold = max(0.0, min(1.0, float(semantic_threshold)))
    if top_k > 0 and len(ids) > 1:
        vectors_store = _ensure_unified_vectors(all_nodes)
        vectors: Dict[str, List[float]] = {
            nid: vectors_store[nid] for nid in ids if nid in vectors_store
        }
        vec_ids = list(vectors.keys())
        for i, src_id in enumerate(vec_ids):
            src_vec = vectors[src_id]
            scored: List[Tuple[float, str]] = []
            for j in range(i + 1, len(vec_ids)):
                tgt_id = vec_ids[j]
                sim = _cosine(src_vec, vectors[tgt_id])
                if sim >= threshold:
                    scored.append((sim, tgt_id))
            scored.sort(key=lambda x: x[0], reverse=True)
            for sim, tgt_id in scored[:top_k]:
                add_edge(src_id, tgt_id, "semantic", weight=sim, directed=False)

    return {
        "nodes": public_nodes,
        "edges": edges,
        "stats": {
            "active_only": bool(active_only),
            "include_archived": bool(include_archived),
            "semantic_threshold": threshold,
            "top_k_semantic": top_k,
            "node_count": len(public_nodes),
            "edge_count": len(edges),
            "edge_counts": edge_counts,
            "truncated": total_before_cap > len(public_nodes),
            "total_before_cap": total_before_cap,
            "generated_at": datetime.utcnow().isoformat(),
            "source_counts": {
                "kb_fact": sum(1 for n in public_nodes if n["source_type"] == "kb_fact"),
                "task": sum(1 for n in public_nodes if n["source_type"] == "task"),
                "task_event": sum(1 for n in public_nodes if n["source_type"] == "task_event"),
                "faq": sum(1 for n in public_nodes if n["source_type"] == "faq"),
                "meeting": sum(1 for n in public_nodes if n["source_type"] == "meeting"),
            },
        },
    }
