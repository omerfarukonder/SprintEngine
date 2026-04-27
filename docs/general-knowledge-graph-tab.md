# General Knowledge Graph Tab: Developer Guide

This document explains how the **General Knowledge Graph** works end-to-end, where data comes from, how nodes/edges are built, and where to edit behavior safely.

Use this as the primary reference when changing graph logic, knowledge filtering, or UI behavior.

---

## 1) What this feature is

The graph tab visualizes a curated organizational knowledge base:

- durable rules and constraints extracted from chat
- forced manual knowledge entries
- sprint task definitions (ingested as durable definition facts)
- relationships between facts (versioning, overwrite chains, taxonomy links, semantic links)

The goal is to show reusable business/engineering knowledge, not transient sprint status updates.

---

## 2) Main files and responsibilities

### Backend

- `app/memory.py`
  - storage read/write for overall KB events
  - embedding and hybrid search logic
  - graph payload builder (`build_overall_kb_graph_payload`)
  - knowledge refinement utilities:
    - `refine_overall_kb_candidate`
    - `refine_overall_kb_active_events`
  - dedupe behavior in `append_overall_kb_event`

- `app/main.py`
  - chat command handling for KB list/overwrite/refine/force-write
  - automatic ingestion of task definitions into KB (`_sync_overall_kb_from_task_definitions`)
  - graph API endpoint: `GET /api/memory/overall-kb-graph`
  - plan-definition sync endpoint: `POST /api/memory/sync-plan-definitions`
  - short TTL graph cache for response speed

- `app/llm.py`
  - LLM extraction prompt for durable knowledge (`extract_overall_knowledge`)
  - extraction prompt is intentionally strict against transient timeline/status content

- `app/storage.py`
  - file paths:
    - `workspace/overall_kb_events.jsonl`
    - `workspace/overall_kb_vectors.json`
    - `workspace/overall_kb_archive/`

### Frontend (FastAPI static UI)

- `static/index.html`
  - tab button + graph panel layout
  - filters, graph area, detail panel

- `static/app.js`
  - graph data fetch
  - filtering
  - force-like layout + SVG render
  - pan/zoom/node drag
  - node selection detail rendering
  - tab lifecycle behavior
  - polling guard (auto-refresh disabled while knowledge tab is active)

### Streamlit alternative UI

- `streamlit_app.py`
  - keeps existing views intact
  - adds `Knowledge Graph (Alt)` as separate view
  - uses embedded HTML/JS via `streamlit.components.v1` to call graph API and render an alternative graph UI

---

## 3) Data model in the graph

## Nodes

Each node is based on one KB event and includes:

- `id`
- `text`
- `label`
- `knowledge_type`
- `scope`
- `confidence`
- `is_active`
- `timestamp`
- `version`
- `source`

## Edges

Edge types used by the graph payload builder:

- `version`
  - from `supersedes_event_id` / `superseded_by`
- `overwrite`
  - from `metadata.overwrites_event_ids`
- `taxonomy`
  - weak link for same (`scope`, `knowledge_type`) chains
- `semantic`
  - vector cosine similarity links (threshold + top-k capped)

---

## 4) How knowledge enters the system

### A) Automatic chat extraction

Flow:

1. Chat message enters `/api/chat`
2. LLM extracts candidate items
3. each item is gated by confidence + durability filter
4. only accepted durable items are canonicalized and saved

Key controls:

- confidence threshold in `app/main.py` (`_capture_overall_kb_from_message`)
- durability threshold and transient detection in `app/memory.py` (`refine_overall_kb_candidate`)

### B) Forced write command (manual override)

Supported command forms in chat:

- `/force-kb: <text>`
- `force write general knowledge: <text>`

Optional inline params:

- `type=<constraint|capability|dependency|risk|organizational_limit|process_rule>`
- `scope=<team|system|process|project>`

Forced writes bypass normal transient gating but still run append-level duplicate checks.

### C) Sprint plan definition ingestion

Task definitions are synced into KB as durable definition nodes:

- text format: `DEFINITION: <task_name> — <definition>`
- source: `sprint_plan_definition`

This happens:

- on startup
- after initialize
- after DOCX import + initialize
- manually via `POST /api/memory/sync-plan-definitions`

---

## 5) Refinement behavior (important)

Refinement is used to keep only high-value reusable knowledge:

- deactivates transient status/timeline entries
- rewrites accepted entries into canonical policy style
- deactivates active exact duplicates

Chat trigger phrases (handled in `/api/chat`) can run refine on demand, for example:

- "refine overall kb"
- "clean general knowledge base"

The refiner updates event metadata with:

- refine reason
- refined timestamp
- durability score

---

## 6) Graph API and performance

### Graph endpoint

`GET /api/memory/overall-kb-graph`

Query parameters:

- `active_only` (bool)
- `include_archived` (bool)
- `semantic_threshold` (float 0..1)
- `top_k_semantic` (int)
- `max_nodes` (int)

### Server-side TTL cache

Implemented in `app/main.py`:

- in-memory cache key includes endpoint parameters
- default TTL: 15 seconds
- response stats include:
  - `cache_hit`
  - `cache_ttl_seconds`

### Client-side refresh behavior

In `static/app.js`:

- periodic polling runs every 3s for normal dashboard data
- polling is skipped when knowledge tab is active
- graph refresh is user-driven or tab-entry-driven

---

## 7) How to modify behavior safely

### Change what is considered "valuable"

Edit in `app/memory.py`:

- `TRANSIENT_PATTERNS`
- `DURABLE_SIGNALS`
- threshold logic in `refine_overall_kb_candidate`

Recommended approach:

1. adjust patterns minimally
2. run refine command against current data
3. inspect graph output and refine stats

### Change extraction strictness

Edit `extract_overall_knowledge` system prompt in `app/llm.py`.

Use prompt changes for semantic behavior, regex changes for deterministic guardrails.

### Change graph connectivity

Edit `build_overall_kb_graph_payload` in `app/memory.py`.

Safe knobs:

- semantic threshold
- top-k semantic links
- taxonomy link creation strategy
- edge weights

### Change UI interaction or visuals

Edit:

- structure/style: `static/index.html`
- behavior/rendering: `static/app.js`
- streamlit alternative: `streamlit_app.py`

Keep FastAPI static UI and Streamlit alternative independent unless intentionally aligning both.

---

## 8) Command and endpoint quick reference

### Chat commands

- List KB items:
  - "show overall kb ..."
- Overwrite KB item(s):
  - `<old> -> overwrite in general org kb with <new>`
  - optional `ids: <id1>,<id2>`
- Refine KB:
  - "refine overall kb"
- Force-write KB:
  - `/force-kb: ...`
  - `force write general knowledge: ...`

### API endpoints

- `GET /api/memory/debug`
- `GET /api/memory/overall-kb-graph`
- `POST /api/memory/rebuild-overall-kb-vectors`
- `POST /api/memory/sync-plan-definitions`

---

## 9) Typical developer workflows

### Rebuild graph after major KB changes

1. run KB refinement
2. sync plan definitions if needed
3. rebuild vectors endpoint (optional but recommended after large changes)
4. refresh graph tab

### Add a new knowledge class

1. extend allowed types (backend validation and extraction prompt)
2. update node coloring in frontend renderer
3. test list/refine/force-write paths

### Troubleshoot "empty graph"

Check in order:

1. `active_only` filter is too strict
2. refiner deactivated most entries
3. no vectors for semantic links (graph still should show non-semantic edges)
4. endpoint error/caching state

---

## 10) Design constraints and principles

- Keep raw history in events, but show curated active knowledge by default.
- Prefer deterministic filtering for noise control.
- Keep force-write path available for human authority override.
- Preserve provenance (`source`, `metadata`, IDs) to avoid losing auditability.
- Optimize graph readability over maximum edge density.

