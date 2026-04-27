"""
Entity-Relationship Knowledge Graph store.

Manages a graph of canonical entities (teams, domains, topics, systems …)
connected by named, typed relationships (owns, depends_on, has_subtopic …).
Persisted as two JSONL files in the workspace directory.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .storage import KB_ENTITIES_FILE, KB_RELATIONS_FILE, ensure_workspace

# ── Constants ──────────────────────────────────────────────────────────────────

ENTITY_TYPES: Set[str] = {
    "team", "domain", "topic", "system", "person", "process", "constraint",
}

RELATION_TYPES: Set[str] = {
    "has_subtopic", "owns", "depends_on", "has_constraint",
    "related_to", "part_of", "communicates_with", "blocks",
}

ENTITY_COLORS: Dict[str, str] = {
    "team":       "#60a5fa",
    "domain":     "#c084fc",
    "topic":      "#2dd4bf",
    "system":     "#fb923c",
    "person":     "#f472b6",
    "process":    "#a3e635",
    "constraint": "#fbbf24",
}

RELATION_STYLES: Dict[str, Dict[str, Any]] = {
    "has_subtopic":       {"color": "#c084fc", "width": 1.8, "dash": ""},
    "owns":               {"color": "#60a5fa", "width": 2.0, "dash": ""},
    "depends_on":         {"color": "#fb923c", "width": 1.6, "dash": "6 3"},
    "has_constraint":     {"color": "#fbbf24", "width": 1.4, "dash": "4 3"},
    "related_to":         {"color": "#6b7280", "width": 1.0, "dash": "3 3"},
    "part_of":            {"color": "#2dd4bf", "width": 1.6, "dash": ""},
    "communicates_with":  {"color": "#60a5fa", "width": 1.2, "dash": "3 3"},
    "blocks":             {"color": "#ef4444", "width": 2.0, "dash": ""},
}

_ALIAS_SIMILARITY_THRESHOLD = 0.80

# ── Helpers ────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize(name: str) -> str:
    """Lowercase, strip accents, strip $..$ wrappers, collapse whitespace, remove non-alnum except spaces."""
    s = unicodedata.normalize("NFKD", str(name or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"^\$+|\$+$", "", s)
    s = re.sub(r'^(?:the|a|an)\s+', '', s, flags=re.IGNORECASE)
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _canonical_key(name: str) -> str:
    return _normalize(name).replace(" ", "_")


def _entity_id(canonical: str) -> str:
    h = hashlib.sha256(canonical.encode()).hexdigest()[:12]
    return f"ent_{h}"


def _relation_id(src_id: str, tgt_id: str, rtype: str) -> str:
    h = hashlib.sha256(f"{src_id}|{tgt_id}|{rtype}".encode()).hexdigest()[:12]
    return f"rel_{h}"


def _similarity(a: str, b: str) -> float:
    na, nb = _normalize(a), _normalize(b)
    seq_ratio = SequenceMatcher(None, na, nb).ratio()
    if na and nb and (na in nb or nb in na):
        shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
        containment = len(shorter) / len(longer)
        seq_ratio = max(seq_ratio, containment)
    return seq_ratio


def _openai_client() -> Optional[Any]:
    try:
        from dotenv import load_dotenv  # noqa: PLC0415
        load_dotenv()
    except Exception:
        pass
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    try:
        from openai import OpenAI  # noqa: PLC0415
        return OpenAI(api_key=api_key)
    except Exception:
        return None


# ── JSONL I/O ──────────────────────────────────────────────────────────────────

def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _save_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


# ── Entity CRUD ────────────────────────────────────────────────────────────────

def load_entities(active_only: bool = False) -> List[Dict[str, Any]]:
    ensure_workspace()
    rows = _load_jsonl(KB_ENTITIES_FILE)
    if active_only:
        rows = [r for r in rows if r.get("is_active", True)]
    return rows


def save_entities(entities: List[Dict[str, Any]]) -> None:
    ensure_workspace()
    _save_jsonl(KB_ENTITIES_FILE, entities)


def find_entity(
    name: str,
    entities: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """Find an existing entity by canonical key or alias similarity."""
    if entities is None:
        entities = load_entities()
    canon = _canonical_key(name)
    norm = _normalize(name)

    for ent in entities:
        if ent.get("canonical") == canon:
            return ent
        for alias in ent.get("aliases", []):
            if _normalize(alias) == norm:
                return ent

    best, best_score = None, 0.0
    for ent in entities:
        score = _similarity(name, ent.get("name", ""))
        if score > best_score:
            best_score = score
            best = ent
        for alias in ent.get("aliases", []):
            score = _similarity(name, alias)
            if score > best_score:
                best_score = score
                best = ent
    if best and best_score >= _ALIAS_SIMILARITY_THRESHOLD:
        return best
    return None


def upsert_entity(
    name: str,
    entity_type: str = "topic",
    description: str = "",
    facts: Optional[List[str]] = None,
    source_event: str = "",
) -> Dict[str, Any]:
    """Create a new entity or merge into an existing one.  Returns the entity dict."""
    ensure_workspace()
    entities = load_entities()
    existing = find_entity(name, entities)
    now = _now_iso()

    if entity_type not in ENTITY_TYPES:
        entity_type = "topic"

    if existing is not None:
        norm = _normalize(name)
        existing_aliases = [_normalize(a) for a in existing.get("aliases", [])]
        if norm not in existing_aliases and norm != _normalize(existing.get("name", "")):
            existing.setdefault("aliases", []).append(name.strip())

        if description and len(description) > len(existing.get("description", "")):
            existing["description"] = description.strip()

        existing_facts: List[str] = existing.get("facts", [])
        for fact in (facts or []):
            fact_s = fact.strip()
            if fact_s and fact_s not in existing_facts:
                existing_facts.append(fact_s)
        existing["facts"] = existing_facts

        if source_event:
            src_list: List[str] = existing.get("source_events", [])
            if source_event not in src_list:
                src_list.append(source_event)
            existing["source_events"] = src_list

        existing["last_updated"] = now
        save_entities(entities)
        return existing

    canon = _canonical_key(name)
    ent: Dict[str, Any] = {
        "id": _entity_id(canon),
        "name": name.strip(),
        "canonical": canon,
        "aliases": [name.strip()],
        "entity_type": entity_type,
        "description": (description or "").strip(),
        "facts": [f.strip() for f in (facts or []) if f.strip()],
        "first_seen": now,
        "last_updated": now,
        "source_events": [source_event] if source_event else [],
        "is_active": True,
        "metadata": {},
    }
    entities.append(ent)
    save_entities(entities)
    return ent


def _infer_parent_name(name: str) -> Optional[str]:
    """Strip common numbering/phase suffixes to derive a parent entity name.

    Matches patterns like:
      "X - Project 1"  →  "X"
      "X - Phase 2"    →  "X"
      "X - Part 3"     →  "X"
      "X v2"           →  "X"
      "X #3"           →  "X"
      "X - 4"          →  "X"
    Returns None if no pattern matches or the remainder is too short.
    """
    SUFFIX_PATTERNS = [
        # "- <keyword> <number>" e.g. "- Project 1", "- Phase 2", "- Initiative 3"
        r"\s*[-–]\s*(?:project|part|phase|initiative|workstream|stream|track|item|wave|sprint|iteration|release|milestone|module|step)\s*\d+\s*$",
        # "- <number>" e.g. "- 3"
        r"\s*[-–]\s*\d+\s*$",
        # "v<number>" e.g. "v2", "v1.0"
        r"\s+v\d+(?:\.\d+)*\s*$",
        # "#<number>" e.g. "#3"
        r"\s*#\d+\s*$",
        # "(4-digit year)" e.g. "(2024)"
        r"\s*\(\d{4}\)\s*$",
        # "(<number>)" e.g. "(2)"
        r"\s*\(\d+\)\s*$",
    ]
    for pattern in SUFFIX_PATTERNS:
        m = re.search(pattern, name, flags=re.IGNORECASE)
        if m:
            parent = name[: m.start()].strip()
            if len(parent) >= 4:
                return parent
    return None


def _find_entity_exact(name: str, entities: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find entity whose own name matches exactly — no alias or fuzzy fallback.
    Used by hierarchy resolution: an entity whose *name* is the parent name, not one that
    merely lists the parent name as an alias (which would be a child, not the parent)."""
    canon = _canonical_key(name)
    for ent in entities:
        if ent.get("canonical") == canon:
            return ent
    return None


def _upsert_entity_exact(
    name: str,
    entity_type: str = "topic",
    description: str = "",
    source_event: str = "",
) -> Dict[str, Any]:
    """Create or update an entity matched only by its own canonical key.
    Unlike upsert_entity, this never merges into an entity that only has the name as an alias.
    Used by hierarchy resolution to create parent nodes without absorbing child entities."""
    ensure_workspace()
    entities = load_entities()
    now = _now_iso()
    if entity_type not in ENTITY_TYPES:
        entity_type = "topic"

    existing = _find_entity_exact(name, entities)
    if existing is not None:
        if description and len(description) > len(existing.get("description", "")):
            existing["description"] = description.strip()
        if source_event:
            se = existing.get("source_events", [])
            if source_event not in se:
                se.append(source_event)
            existing["source_events"] = se
        existing["last_updated"] = now
        save_entities(entities)
        return existing

    canon = _canonical_key(name)
    ent: Dict[str, Any] = {
        "id": _entity_id(canon),
        "name": name.strip(),
        "canonical": canon,
        "aliases": [name.strip()],
        "entity_type": entity_type,
        "description": (description or "").strip(),
        "facts": [],
        "first_seen": now,
        "last_updated": now,
        "source_events": [source_event] if source_event else [],
        "is_active": True,
        "metadata": {},
    }
    entities.append(ent)
    save_entities(entities)
    return ent


def _upsert_hierarchy_edge(parent_id: str, child_id: str, parent_name: str) -> bool:
    """Add a has_subtopic edge between parent and child entity by ID. Returns True if added."""
    if parent_id == child_id:
        return False
    rtype = "has_subtopic"
    relations = load_relations()
    now = _now_iso()
    for rel in relations:
        if (rel.get("source_entity_id") == parent_id
                and rel.get("target_entity_id") == child_id
                and rel.get("relation_type") == rtype):
            rel["confidence"] = max(rel.get("confidence", 0), 0.92)
            rel["last_updated"] = now
            rel["is_active"] = True
            se = rel.get("source_events", [])
            if "hierarchy_resolution" not in se:
                se.append("hierarchy_resolution")
            rel["source_events"] = se
            save_relations(relations)
            return True
    rel_id = _relation_id(parent_id, child_id, rtype)
    relations.append({
        "id": rel_id,
        "source_entity_id": parent_id,
        "target_entity_id": child_id,
        "relation_type": rtype,
        "label": f"sub-item of {parent_name}",
        "confidence": 0.92,
        "first_seen": now,
        "last_updated": now,
        "source_events": ["hierarchy_resolution"],
        "is_active": True,
        "metadata": {},
    })
    save_relations(relations)
    return True


def resolve_entity_hierarchy() -> Dict[str, int]:
    """Detect parent-child relationships and upsert has_subtopic edges.

    Two strategies:
      1. Suffix patterns: "X - Phase 2" → parent "X"
      2. Prefix containment: if entity "X" exists and "X foo bar" also exists,
         link X → X foo bar (X must be ≥4 chars and a word-boundary prefix).

    Returns {"parents_created": N, "edges_added": M}.
    """
    entities = load_entities(active_only=True)
    parents_created = 0
    edges_added = 0

    # Pass 1: suffix-based hierarchy (original logic)
    for ent in list(entities):
        parent_name = _infer_parent_name(ent["name"])
        if not parent_name:
            continue
        etype = ent.get("entity_type", "domain")

        current_entities = load_entities()
        existing_parent = _find_entity_exact(parent_name, current_entities)
        if existing_parent is None:
            existing_parent = _upsert_entity_exact(
                name=parent_name,
                entity_type=etype,
                description=f"Parent initiative encompassing sub-items of '{parent_name}'.",
                source_event="hierarchy_resolution",
            )
            parents_created += 1

        if _upsert_hierarchy_edge(existing_parent["id"], ent["id"], parent_name):
            edges_added += 1

    # Pass 2: prefix containment — connect "X" → "X something" when both exist
    entities = load_entities(active_only=True)
    norm_to_ent: Dict[str, Dict[str, Any]] = {}
    for ent in entities:
        norm_to_ent[_normalize(ent.get("name", ""))] = ent

    sorted_names = sorted(norm_to_ent.keys(), key=len)
    for i, shorter in enumerate(sorted_names):
        if len(shorter) < 4:
            continue
        prefix_with_space = shorter + " "
        for j in range(i + 1, len(sorted_names)):
            longer = sorted_names[j]
            if not longer.startswith(prefix_with_space):
                continue
            suffix = longer[len(prefix_with_space):]
            if len(suffix) < 2:
                continue
            parent_ent = norm_to_ent[shorter]
            child_ent = norm_to_ent[longer]
            if parent_ent["id"] == child_ent["id"]:
                continue
            if _upsert_hierarchy_edge(parent_ent["id"], child_ent["id"], parent_ent.get("name", "")):
                edges_added += 1

    return {"parents_created": parents_created, "edges_added": edges_added}


def apply_manual_grouping(parent_name: str, child_names: List[str], entity_type: str = "domain") -> Dict[str, int]:
    """Manually group a list of entities under a named parent with has_subtopic edges.

    Creates the parent if it doesn't exist. Used by the 'group:' chat command.
    Returns {"parents_created": N, "edges_added": M}.
    """
    parents_created = 0
    edges_added = 0

    existing_parent = find_entity(parent_name)
    if existing_parent is None:
        upsert_entity(
            name=parent_name,
            entity_type=entity_type,
            description=f"Initiative grouping for: {', '.join(child_names[:5])}.",
            source_event="manual_group_command",
        )
        parents_created += 1

    for child_name in child_names:
        child = find_entity(child_name)
        child_type = child.get("entity_type", entity_type) if child else entity_type
        rel = upsert_relation(
            source_name=parent_name,
            target_name=child_name,
            relation_type="has_subtopic",
            label=f"sub-item of {parent_name}",
            confidence=1.0,
            source_event="manual_group_command",
            source_entity_type=entity_type,
            target_entity_type=child_type,
        )
        if rel:
            edges_added += 1

    return {"parents_created": parents_created, "edges_added": edges_added}


def deactivate_entity(name: str) -> bool:
    entities = load_entities()
    ent = find_entity(name, entities)
    if not ent:
        return False
    ent["is_active"] = False
    ent["last_updated"] = _now_iso()
    save_entities(entities)
    return True


# ── Relation CRUD ──────────────────────────────────────────────────────────────

def load_relations(active_only: bool = False) -> List[Dict[str, Any]]:
    ensure_workspace()
    rows = _load_jsonl(KB_RELATIONS_FILE)
    if active_only:
        rows = [r for r in rows if r.get("is_active", True)]
    return rows


def save_relations(relations: List[Dict[str, Any]]) -> None:
    ensure_workspace()
    _save_jsonl(KB_RELATIONS_FILE, relations)


def upsert_relation(
    source_name: str,
    target_name: str,
    relation_type: str = "related_to",
    label: str = "",
    confidence: float = 0.8,
    source_event: str = "",
    source_entity_type: str = "topic",
    target_entity_type: str = "topic",
) -> Dict[str, Any]:
    """Create or update a relation between two entities (upserting entities if needed)."""
    ensure_workspace()

    if relation_type not in RELATION_TYPES:
        relation_type = "related_to"

    src_ent = find_entity(source_name)
    if src_ent is None:
        src_ent = upsert_entity(source_name, entity_type=source_entity_type, source_event=source_event)
    src_id = src_ent["id"]

    tgt_ent = find_entity(target_name)
    if tgt_ent is None:
        tgt_ent = upsert_entity(target_name, entity_type=target_entity_type, source_event=source_event)
    tgt_id = tgt_ent["id"]

    if src_id == tgt_id:
        return {}

    relations = load_relations()
    now = _now_iso()

    for rel in relations:
        if (rel.get("source_entity_id") == src_id
                and rel.get("target_entity_id") == tgt_id
                and rel.get("relation_type") == relation_type):
            rel["confidence"] = max(rel.get("confidence", 0), confidence)
            if label and len(label) > len(rel.get("label", "")):
                rel["label"] = label.strip()
            if source_event:
                se = rel.get("source_events", [])
                if source_event not in se:
                    se.append(source_event)
                rel["source_events"] = se
            rel["last_updated"] = now
            rel["is_active"] = True
            save_relations(relations)
            return rel

    rel_id = _relation_id(src_id, tgt_id, relation_type)
    rel: Dict[str, Any] = {
        "id": rel_id,
        "source_entity_id": src_id,
        "target_entity_id": tgt_id,
        "relation_type": relation_type,
        "label": (label or "").strip(),
        "confidence": max(0.0, min(1.0, float(confidence))),
        "first_seen": now,
        "last_updated": now,
        "source_events": [source_event] if source_event else [],
        "is_active": True,
        "metadata": {},
    }
    relations.append(rel)
    save_relations(relations)
    return rel


# ── LLM graph refinement ───────────────────────────────────────────────────────

def _merge_entities(keep_id: str, remove_id: str, reason: str = "") -> bool:
    """Merge remove_id into keep_id and remap all relations."""
    if not keep_id or not remove_id or keep_id == remove_id:
        return False
    entities = load_entities()
    keep = next((e for e in entities if e.get("id") == keep_id), None)
    remove = next((e for e in entities if e.get("id") == remove_id), None)
    if not keep or not remove:
        return False

    # Merge aliases / facts / source events
    keep_aliases = keep.get("aliases", []) or []
    for alias in remove.get("aliases", []) or []:
        if alias not in keep_aliases:
            keep_aliases.append(alias)
    if remove.get("name") and remove["name"] not in keep_aliases:
        keep_aliases.append(remove["name"])
    keep["aliases"] = keep_aliases

    keep_facts = keep.get("facts", []) or []
    for fact in remove.get("facts", []) or []:
        if fact and fact not in keep_facts:
            keep_facts.append(fact)
    keep["facts"] = keep_facts

    keep_sources = keep.get("source_events", []) or []
    for src in remove.get("source_events", []) or []:
        if src and src not in keep_sources:
            keep_sources.append(src)
    if reason and "llm_refinement_merge" not in keep_sources:
        keep_sources.append("llm_refinement_merge")
    keep["source_events"] = keep_sources

    if len(remove.get("description", "")) > len(keep.get("description", "")):
        keep["description"] = remove.get("description", "")
    keep["last_updated"] = _now_iso()

    remove["is_active"] = False
    remove["last_updated"] = _now_iso()
    remove_sources = remove.get("source_events", []) or []
    if "llm_refinement_merged" not in remove_sources:
        remove_sources.append("llm_refinement_merged")
    remove["source_events"] = remove_sources

    # Remap relations pointing to the removed entity
    relations = load_relations()
    for rel in relations:
        changed = False
        if rel.get("source_entity_id") == remove_id:
            rel["source_entity_id"] = keep_id
            changed = True
        if rel.get("target_entity_id") == remove_id:
            rel["target_entity_id"] = keep_id
            changed = True
        if rel.get("source_entity_id") == rel.get("target_entity_id"):
            rel["is_active"] = False
            changed = True
        if changed:
            rel["last_updated"] = _now_iso()
            se = rel.get("source_events", []) or []
            if "llm_refinement_merge" not in se:
                se.append("llm_refinement_merge")
            rel["source_events"] = se

    # Deduplicate relations after remap (same src/tgt/type)
    seen: Set[Tuple[str, str, str]] = set()
    deduped: List[Dict[str, Any]] = []
    for rel in relations:
        key = (
            str(rel.get("source_entity_id", "")),
            str(rel.get("target_entity_id", "")),
            str(rel.get("relation_type", "")),
        )
        if not key[0] or not key[1] or key[0] == key[1]:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rel)

    save_entities(entities)
    save_relations(deduped)
    return True


def _llm_refinement_suggestions(
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    client = _openai_client()
    if client is None:
        return {}

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
    entity_lines = []
    for e in entities:
        entity_lines.append(
            {
                "name": e.get("name", ""),
                "type": e.get("entity_type", "topic"),
                "description": e.get("description", ""),
                "aliases": e.get("aliases", [])[:6],
            }
        )

    rel_lines = []
    id_to_name = {e.get("id"): e.get("name", "") for e in entities}
    for r in relations:
        rel_lines.append(
            {
                "source": id_to_name.get(r.get("source_entity_id"), ""),
                "target": id_to_name.get(r.get("target_entity_id"), ""),
                "type": r.get("relation_type", "related_to"),
                "label": r.get("label", ""),
            }
        )

    system_prompt = (
        "You refine an entity graph. Return strict JSON with keys: merge, new_relations, fix_relations.\n"
        "merge: [{keep, remove, reason}] where keep/remove are entity names.\n"
        "new_relations: [{source, target, type, label, confidence}] where type is one of "
        "has_subtopic, owns, depends_on, has_constraint, related_to, part_of, communicates_with, blocks.\n"
        "fix_relations: [{source, target, old_type, new_type, reason}].\n\n"
        "Rules:\n"
        "- Merge only true duplicates/synonyms (e.g., Hydra vs Hydra CMS).\n"
        "- For task/issue names that include a domain keyword (e.g., 'PDP Canonical Fix'), connect to domain with related_to.\n"
        "- Use depends_on ONLY for explicit blocking/precondition language (blocked by, requires, waiting for, needs before).\n"
        "- Do NOT hallucinate entities not in the provided list.\n"
        "- Keep output conservative and high precision."
    )
    user_prompt = (
        f"Entities:\n{json.dumps(entity_lines, ensure_ascii=False)}\n\n"
        f"Existing relations:\n{json.dumps(rel_lines, ensure_ascii=False)}\n\n"
        "Output JSON only."
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
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def refine_entity_graph(
    max_entities: int = 40,
    max_new_relations: int = 40,
) -> Dict[str, int]:
    """LLM final-cleaning pass for merges, relation additions, and relation type fixes."""
    entities = load_entities(active_only=True)
    relations = load_relations(active_only=True)
    if not entities:
        return {"entities_merged": 0, "relations_added": 0, "relations_fixed": 0, "batches": 0}

    # Prioritize disconnected/sparse nodes first.
    degree: Dict[str, int] = {e["id"]: 0 for e in entities}
    for r in relations:
        src = r.get("source_entity_id")
        tgt = r.get("target_entity_id")
        if src in degree:
            degree[src] += 1
        if tgt in degree:
            degree[tgt] += 1
    prioritized = sorted(entities, key=lambda e: degree.get(e["id"], 0))
    sparse = [e for e in prioritized if degree.get(e["id"], 0) <= 2]
    selected = (sparse or prioritized)[: max(1, max_entities)]
    selected_ids = {e["id"] for e in selected}
    selected_relations = [
        r for r in relations
        if r.get("source_entity_id") in selected_ids and r.get("target_entity_id") in selected_ids
    ]

    parsed = _llm_refinement_suggestions(selected, selected_relations)
    if not parsed:
        return {"entities_merged": 0, "relations_added": 0, "relations_fixed": 0, "batches": 0}

    name_to_entity: Dict[str, Dict[str, Any]] = {}
    for e in load_entities(active_only=True):
        name_to_entity[_normalize(e.get("name", ""))] = e
        for alias in e.get("aliases", []) or []:
            name_to_entity[_normalize(alias)] = e

    merged = 0
    added = 0
    fixed = 0

    for m in parsed.get("merge", []) if isinstance(parsed.get("merge", []), list) else []:
        if not isinstance(m, dict):
            continue
        keep_name = _normalize(str(m.get("keep", "")))
        remove_name = _normalize(str(m.get("remove", "")))
        if not keep_name or not remove_name or keep_name == remove_name:
            continue
        keep_ent = name_to_entity.get(keep_name)
        rem_ent = name_to_entity.get(remove_name)
        if not keep_ent or not rem_ent:
            continue
        if _merge_entities(keep_ent["id"], rem_ent["id"], reason=str(m.get("reason", ""))):
            merged += 1

    for nr in (parsed.get("new_relations", []) if isinstance(parsed.get("new_relations", []), list) else [])[:max_new_relations]:
        if not isinstance(nr, dict):
            continue
        src_name = str(nr.get("source", "")).strip()
        tgt_name = str(nr.get("target", "")).strip()
        rtype = str(nr.get("type", "related_to")).strip().lower()
        label = str(nr.get("label", "")).strip()
        try:
            conf = float(nr.get("confidence", 0.82))
        except (TypeError, ValueError):
            conf = 0.82
        if not src_name or not tgt_name:
            continue
        rel = upsert_relation(
            source_name=src_name,
            target_name=tgt_name,
            relation_type=rtype if rtype in RELATION_TYPES else "related_to",
            label=label,
            confidence=conf,
            source_event="llm_graph_refinement",
        )
        if rel:
            added += 1

    for fr in parsed.get("fix_relations", []) if isinstance(parsed.get("fix_relations", []), list) else []:
        if not isinstance(fr, dict):
            continue
        src_name = str(fr.get("source", "")).strip()
        tgt_name = str(fr.get("target", "")).strip()
        old_type = str(fr.get("old_type", "")).strip().lower()
        new_type = str(fr.get("new_type", "")).strip().lower()
        if not src_name or not tgt_name or not new_type or new_type not in RELATION_TYPES:
            continue
        src = find_entity(src_name)
        tgt = find_entity(tgt_name)
        if not src or not tgt:
            continue
        rels = load_relations()
        changed = False
        for rel in rels:
            if (rel.get("source_entity_id") == src.get("id")
                    and rel.get("target_entity_id") == tgt.get("id")
                    and (not old_type or rel.get("relation_type") == old_type)):
                rel["relation_type"] = new_type
                rel["last_updated"] = _now_iso()
                rel["is_active"] = True
                se = rel.get("source_events", []) or []
                if "llm_graph_refinement_fix" not in se:
                    se.append("llm_graph_refinement_fix")
                rel["source_events"] = se
                changed = True
                fixed += 1
        if changed:
            save_relations(rels)

    return {
        "entities_merged": merged,
        "relations_added": added,
        "relations_fixed": fixed,
        "batches": 1,
    }


# ── Synthesis helpers ─────────────────────────────────────────────────────────

def summarize_for_synthesis(max_relations: int = 60) -> str:
    """Return a compact text block of the entity graph for use as synthesis context.
    Sorted by confidence so the most reliable edges appear first."""
    entities = load_entities(active_only=True)
    relations = load_relations(active_only=True)
    if not entities or not relations:
        return ""
    id_to_name: Dict[str, str] = {e["id"]: e["name"] for e in entities}
    relations.sort(key=lambda r: float(r.get("confidence", 0)), reverse=True)
    lines: List[str] = []
    for rel in relations[:max_relations]:
        src = id_to_name.get(rel.get("source_entity_id", ""), "")
        tgt = id_to_name.get(rel.get("target_entity_id", ""), "")
        rtype = rel.get("relation_type", "related_to")
        if src and tgt:
            lines.append(f"{src} --[{rtype}]--> {tgt}")
    return "\n".join(lines)


# ── Graph payload builder ─────────────────────────────────────────────────────

def build_entity_graph_payload(
    active_only: bool = True,
    entity_type_filter: str = "all",
    search_query: str = "",
) -> Dict[str, Any]:
    """Build the visualization payload for the entity-relationship graph."""
    entities = load_entities(active_only=active_only)
    relations = load_relations(active_only=active_only)

    if entity_type_filter and entity_type_filter != "all":
        entities = [e for e in entities if e.get("entity_type") == entity_type_filter]

    if search_query:
        q = search_query.lower().strip()
        entities = [e for e in entities if (
            q in e.get("name", "").lower()
            or q in e.get("description", "").lower()
            or any(q in f.lower() for f in e.get("facts", []))
            or any(q in a.lower() for a in e.get("aliases", []))
        )]

    ent_ids = {e["id"] for e in entities}
    relations = [r for r in relations
                 if r.get("source_entity_id") in ent_ids
                 and r.get("target_entity_id") in ent_ids]

    ent_by_id = {e["id"]: e for e in entities}

    nodes = []
    for ent in entities:
        deg = sum(1 for r in relations
                  if r.get("source_entity_id") == ent["id"]
                  or r.get("target_entity_id") == ent["id"])
        nodes.append({
            "id": ent["id"],
            "name": ent.get("name", ""),
            "entity_type": ent.get("entity_type", "topic"),
            "description": ent.get("description", ""),
            "facts": ent.get("facts", []),
            "aliases": ent.get("aliases", []),
            "degree": deg,
            "first_seen": ent.get("first_seen", ""),
            "last_updated": ent.get("last_updated", ""),
            "is_active": ent.get("is_active", True),
        })

    edges = []
    for rel in relations:
        src = ent_by_id.get(rel.get("source_entity_id", ""))
        tgt = ent_by_id.get(rel.get("target_entity_id", ""))
        edges.append({
            "id": rel["id"],
            "source": rel["source_entity_id"],
            "target": rel["target_entity_id"],
            "relation_type": rel.get("relation_type", "related_to"),
            "label": rel.get("label", ""),
            "confidence": rel.get("confidence", 0.8),
            "source_name": src.get("name", "") if src else "",
            "target_name": tgt.get("name", "") if tgt else "",
        })

    type_counts: Dict[str, int] = {}
    for e in entities:
        t = e.get("entity_type", "unknown")
        type_counts[t] = type_counts.get(t, 0) + 1

    rel_type_counts: Dict[str, int] = {}
    for r in relations:
        t = r.get("relation_type", "unknown")
        rel_type_counts[t] = rel_type_counts.get(t, 0) + 1

    return {
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "total_entities": len(entities),
            "total_relations": len(relations),
            "entity_type_counts": type_counts,
            "relation_type_counts": rel_type_counts,
        },
    }


# ── Bulk ingest from LLM extraction output ────────────────────────────────────

def ingest_extracted(
    extracted: Dict[str, Any],
    source_event: str = "",
) -> Dict[str, int]:
    """Process the output of llm.extract_entity_relations and upsert everything."""
    entities_in = extracted.get("entities", [])
    relations_in = extracted.get("relations", [])
    ent_count = 0
    rel_count = 0

    ent_type_map: Dict[str, str] = {}
    for ent_data in entities_in:
        if not isinstance(ent_data, dict):
            continue
        name = str(ent_data.get("name", "")).strip()
        if not name:
            continue
        etype = str(ent_data.get("type", "topic")).strip().lower()
        desc = str(ent_data.get("description", "")).strip()
        facts = ent_data.get("facts", [])
        if not isinstance(facts, list):
            facts = []
        upsert_entity(
            name=name,
            entity_type=etype if etype in ENTITY_TYPES else "topic",
            description=desc,
            facts=[str(f) for f in facts if f],
            source_event=source_event,
        )
        ent_type_map[name.lower()] = etype
        ent_count += 1

    for rel_data in relations_in:
        if not isinstance(rel_data, dict):
            continue
        src = str(rel_data.get("source", "")).strip()
        tgt = str(rel_data.get("target", "")).strip()
        rtype = str(rel_data.get("type", "related_to")).strip().lower()
        label = str(rel_data.get("label", "")).strip()
        try:
            conf = float(rel_data.get("confidence", 0.8))
        except (TypeError, ValueError):
            conf = 0.8
        if not src or not tgt:
            continue
        upsert_relation(
            source_name=src,
            target_name=tgt,
            relation_type=rtype if rtype in RELATION_TYPES else "related_to",
            label=label,
            confidence=conf,
            source_event=source_event,
            source_entity_type=ent_type_map.get(src.lower(), "topic"),
            target_entity_type=ent_type_map.get(tgt.lower(), "topic"),
        )
        rel_count += 1

    return {"entities_upserted": ent_count, "relations_upserted": rel_count}
