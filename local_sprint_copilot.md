# Local Sprint Copilot – P0 & P1 Instructions

## Overview
This document describes the design and implementation plan for a local AI-powered sprint copilot.

---

# P0 – Local Sprint Copilot (Core Version)

## Goal
A local tool that:
- Reads a sprint plan (Markdown)
- Accepts daily updates via chat
- Updates task statuses and summaries
- Answers questions about the sprint
- Generates tables and summaries
- Asks intelligent follow-ups

---

## Architecture

User → Local UI → FastAPI Backend → LLM → Local Files (MD + JSON)

---

## Folder Structure

/workspace
  sprint_plan.md
  sprint_state.json
  daily_logs/
  generated_tables/

---

## Core Features

### 1. Sprint Plan Parsing
- Load tasks from markdown
- Convert to structured JSON

### 2. Daily Updates
Input:
“Finished PRD, blocked on data”

Output:
- Update task status
- Update traffic light
- Append daily log
- Set next checkpoint

---

### 3. Task Model

{
  "task_name": "",
  "status": "",
  "traffic_light": "",
  "latest_update": "",
  "blockers": [],
  "next_expected_checkpoint": "",
  "do_not_ask_until": ""
}

---

### 4. Follow-up Logic

Rules:
- Do not ask before do_not_ask_until
- Ask if:
  - Red task
  - Blocked task (1+ day)
  - Yellow stale (2+ days)
  - Green stale (3+ days)

---

### 5. Queries

Supported:
- What are this sprint’s tasks?
- Which tasks are risky?
- What did I log yesterday?

---

### 6. Table Generation

Example:

| Task | Status | Light | Blocker |
|------|--------|------|--------|

---

# P1 – Slack Integration

## Goal
Integrate Slack as a data source.

---

## Capabilities

- Read selected channels
- Read threads
- Capture tasks from mentions
- Store Slack content locally

---

## Slack Permissions

- channels:history
- groups:history
- im:history
- app_mentions

---

## Architecture Addition

Slack → Sync Worker → Local Storage → Copilot

---

## Sync Modes

1. Mention-based
2. Selected channels
3. Manual sync

---

## Use Cases

- “Summarize SEO channel this week”
- “Create tasks from thread”

---

## Notes

- Do NOT ingest entire Slack
- Use incremental sync
- Store references to threads

---

# MVP Scope

## P0
- Chat UI
- Markdown updates
- Task tracking
- Smart follow-ups

## P1
- Slack ingestion (limited)
- Thread-based task creation

---

# Final Principle

Conversation → Structure → Execution → Follow-up
