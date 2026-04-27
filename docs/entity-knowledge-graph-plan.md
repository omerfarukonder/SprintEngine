# Entity-Relationship Knowledge Graph — Implementation Plan

## 1. Problem

The current Overall KB stores knowledge as **flat text events** — each node in the
graph is a sentence, and edges are inferred from vector similarity, taxonomy grouping,
or version chains.  This produces a noisy cloud of loosely related dots with no
structural meaning.

What we actually need is a **second brain**: an entity-relationship graph where nodes
are real-world concepts (teams, domains, systems, topics) and edges are named
relationships (owns, depends_on, subtopic_of).

### Example

From the sentence:

> "For PLP inlinking, we need to align on the search results to stay as-is,
>  communicating with Mordor team."

**Current system** stores one KB event node:
```
[●] "For PLP inlinking, we need to align on the search results…"
```

**New system** extracts:
```
PLP ──has_subtopic──▶ Inlinking ──depends_on──▶ Search Results ◀──owns── Mordor Team
                                                       │
                                                has_constraint
                                                       │
                                               "Must stay as-is"
```

---

## 2. Architecture

```
Raw inputs (chat messages, sprint updates, meetings)
        │
        ▼
 LLM Entity-Relation Extractor
   extract_entity_relations(state, message)
        │
        ▼
 Entity Resolver (fuzzy name + alias matching)
        │
        ├──▶ upsert_entity()  ──▶ kb_entities.jsonl
        └──▶ upsert_relation() ──▶ kb_relations.jsonl
                │
                ▼
        GET /api/memory/entity-graph
                │
                ▼
        Frontend visualization (FastAPI + Streamlit)
```

The old flat KB events system continues to run in parallel.  No breaking changes.

---

## 3. Data Model

### 3.1 Entity (`kb_entities.jsonl`)

```json
{
  "id":           "ent_<hash>",
  "name":         "Mordor Team",
  "canonical":    "mordor_team",
  "aliases":      ["mordor", "mordor team", "team mordor"],
  "entity_type":  "team",
  "description":  "Owns search results and search-related alignment",
  "facts":        ["Responsible for search result stability"],
  "first_seen":   "2026-04-10T12:00:00Z",
  "last_updated": "2026-04-14T09:30:00Z",
  "source_events":["msg_abc123"],
  "is_active":    true,
  "metadata":     {}
}
```

**Entity types** (extensible):

| Type | Description | Color |
|------|-------------|-------|
| `team` | A group of people | `#60a5fa` (blue) |
| `domain` | A work area / business field | `#c084fc` (purple) |
| `topic` | A sub-area or technique | `#2dd4bf` (teal) |
| `system` | A product, platform, or component | `#fb923c` (orange) |
| `person` | An individual | `#f472b6` (pink) |
| `process` | A workflow or procedure | `#a3e635` (lime) |
| `constraint` | A rule, limit, or policy | `#fbbf24` (amber) |

### 3.2 Relation (`kb_relations.jsonl`)

```json
{
  "id":               "rel_<hash>",
  "source_entity_id": "ent_plp",
  "target_entity_id": "ent_inlinking",
  "relation_type":    "has_subtopic",
  "label":            "Inlinking is a subtopic under PLP",
  "confidence":       0.92,
  "first_seen":       "2026-04-14T09:30:00Z",
  "last_updated":     "2026-04-14T09:30:00Z",
  "source_events":    ["msg_abc123"],
  "is_active":        true,
  "metadata":         {}
}
```

**Relation types** (extensible):

| Type | Meaning | Edge style |
|------|---------|------------|
| `has_subtopic` | A is a subtopic / sub-area of B | solid purple |
| `owns` | A is responsible for / controls B | solid blue |
| `depends_on` | A requires or is affected by B | dashed orange |
| `has_constraint` | A has rule/limit B | dashed amber |
| `related_to` | General association | dotted gray |
| `part_of` | A is a component of B | solid teal |
| `communicates_with` | A coordinates with B | dotted blue |
| `blocks` | A blocks or impedes B | solid red |

---

## 4. Implementation Steps

### Step 1 — Storage paths (`app/storage.py`)

Add two new file path constants:

```python
KB_ENTITIES_FILE = WORKSPACE_DIR / "kb_entities.jsonl"
KB_RELATIONS_FILE = WORKSPACE_DIR / "kb_relations.jsonl"
```

### Step 2 — Entity-relation store (`app/kb_graph.py` — new file)

Core functions:

| Function | Purpose |
|----------|---------|
| `load_entities()` → `list[dict]` | Read all entities from JSONL |
| `save_entities(entities)` | Write full entity list |
| `load_relations()` → `list[dict]` | Read all relations from JSONL |
| `save_relations(relations)` | Write full relation list |
| `normalize_name(name)` → `str` | Lowercase, strip, collapse whitespace |
| `find_entity(name)` → `dict or None` | Match by canonical name or alias |
| `upsert_entity(name, type, description, facts, source_event)` → `dict` | Create or merge |
| `upsert_relation(source, target, type, label, confidence, source_event)` → `dict` | Create or update |
| `build_entity_graph_payload()` → `dict` | Build `{entities, relations, stats}` for the API |

**Entity resolution logic** in `upsert_entity`:

1. `normalize_name(name)` → canonical form
2. Search existing entities where `canonical == normalized` OR `normalized in aliases`
3. If found: merge new aliases, append facts (dedup), update description if longer,
   append source_events, bump `last_updated`
4. If not found: create new entity with `id = "ent_" + hash(canonical)`

**Relation resolution** in `upsert_relation`:

1. Resolve source and target by entity name (calls `find_entity` or `upsert_entity`)
2. Check if relation with same (source_id, target_id, relation_type) exists
3. If found: update confidence (keep max), merge source_events, bump `last_updated`
4. If not found: create new relation

### Step 3 — LLM extraction (`app/llm.py`)

New method `extract_entity_relations(state, message)`.

**System prompt** (key parts):

```
Extract entities and relationships from the user message.
Return strict JSON with two keys: entities, relations.

entities: array of {name, type, description}
  - type must be one of: team, domain, topic, system, person, process, constraint
  - name should be a short canonical label (e.g. "Mordor Team", not "the mordor team that handles search")
  - description: one sentence explaining what this entity is

relations: array of {source, target, type, label, confidence}
  - source and target are entity names (must match an entity in the entities array)
  - type must be one of: has_subtopic, owns, depends_on, has_constraint, related_to, part_of, communicates_with, blocks
  - label: short human-readable description of the relationship
  - confidence: 0.0 to 1.0

Rules:
  - Only extract entities explicitly mentioned or clearly implied
  - Do NOT extract transient status (dates, ETAs, progress percentages)
  - DO extract: teams, domains, topics, systems, rules, ownership, dependencies
  - Merge obvious variants (e.g. "Mordor" and "Mordor team" should be one entity)
```

### Step 4 — Ingestion pipeline (`app/main.py`)

**In `_capture_overall_kb_from_message`** (or a new sibling function):

```python
def _capture_entity_graph_from_message(state, message: str) -> bool:
    extracted = llm.extract_entity_relations(state, message)
    if not extracted:
        return False
    for ent in extracted.get("entities", []):
        upsert_entity(name=ent["name"], entity_type=ent["type"],
                       description=ent.get("description", ""),
                       source_event=message_id)
    for rel in extracted.get("relations", []):
        upsert_relation(source_name=rel["source"], target_name=rel["target"],
                         relation_type=rel["type"], label=rel.get("label", ""),
                         confidence=rel.get("confidence", 0.8),
                         source_event=message_id)
    return True
```

Called alongside (not replacing) the existing flat KB capture.

**Chat commands**:
- `force-entity <json>` — manually add an entity
- `force-relation <json>` — manually add a relation
- `rebuild entity graph` — backfill from all existing KB events

### Step 5 — API endpoint (`app/main.py`)

```python
@app.get("/api/memory/entity-graph")
def entity_graph_api():
    return build_entity_graph_payload()

@app.post("/api/memory/rebuild-entity-graph")
def rebuild_entity_graph_api():
    # Feed all active KB events through the extractor
    ...
```

### Step 6 — Frontend: FastAPI (`static/index.html` + `static/app.js`)

Update the General Knowledge Graph tab:

- **Nodes**: Circles sized by connection count, colored by entity type, labeled with
  entity name (not truncated sentences)
- **Edges**: Styled by relation type (see table above), with small label text showing
  the relation type on hover
- **Detail panel**: Entity name, type badge, description, facts list, connected
  entities with relation labels
- **Filters**: By entity type, search by name
- **Pan/zoom**: Same as current (event delegation, `kbFindNodeGroup`)

### Step 7 — Frontend: Streamlit (`streamlit_app.py`)

Mirror the FastAPI frontend in the embedded HTML component, using the same event
delegation pattern and entity-colored nodes.

### Step 8 — Backfill migration

`POST /api/memory/rebuild-entity-graph`:

1. Load all active KB events from `overall_kb_events.jsonl`
2. Batch them into groups of ~5 messages
3. Feed each batch through `extract_entity_relations`
4. Upsert all extracted entities and relations
5. Return stats: `{entities_created, relations_created, events_processed}`

---

## 5. What Stays the Same

| Component | Change? |
|-----------|---------|
| Task table | No |
| Sprint state | No |
| Update engine | No |
| Report writer | No |
| Chat commands (existing) | No |
| FAQ system | No |
| Meeting transcription | No |
| Old flat KB events | Kept running in parallel |
| Unified graph endpoint | Kept, can optionally merge entity graph later |

---

## 6. Files Changed

| File | Change type |
|------|------------|
| `app/storage.py` | Add 2 constants |
| `app/kb_graph.py` | **New file** — entity store + resolver + graph builder |
| `app/llm.py` | Add `extract_entity_relations()` |
| `app/main.py` | Add ingestion hook + API endpoints + chat commands |
| `static/index.html` | Update KB tab layout/styles for entity view |
| `static/app.js` | New entity graph rendering logic |
| `streamlit_app.py` | Update knowledge alt view |
| `docs/entity-knowledge-graph-plan.md` | This file |

---

## 7. Visual Result

After implementation, the knowledge graph will look like:

```
   ┌──────┐  has_subtopic  ┌───────────┐  depends_on  ┌────────────────┐
   │  PLP │───────────────▶│ Inlinking │─────────────▶│ Search Results │
   └──────┘                └───────────┘              └───────┬────────┘
     domain                    topic                     system│
                                                               │ owns
                                                    ┌──────────┴───────┐
                                                    │   Mordor Team    │
                                                    └──────────────────┘
                                                          team

   ┌─────────────────────┐
   │ Core Web Vitals     │──depends_on──▶ [Page Speed] ◀──owns── [Platform Team]
   └─────────────────────┘
          topic
```

Each node is a **concept you can talk about**, not a sentence someone typed.
Each edge tells you **how** two concepts are related, not just "78% similar".
