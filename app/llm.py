from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None

from .models import SprintState


class SprintCopilotLLM:
    def __init__(self) -> None:
        if load_dotenv is not None:
            load_dotenv()
        self.api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.model = os.getenv("OPENAI_MODEL", "gpt-5.4-mini").strip()
        self._client = None

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and OpenAI is not None)

    def _get_client(self) -> Optional[OpenAI]:
        if not self.enabled:
            return None
        if self._client is None:
            self._client = OpenAI(api_key=self.api_key)
        return self._client

    @staticmethod
    def _state_context(state: SprintState) -> str:
        tasks = []
        for task in state.tasks:
            tasks.append(
                {
                    "task_name": task.task_name,
                    "definition": task.definition,
                    "task_link": task.task_link,
                    "owner": task.owner,
                    "eta": task.eta,
                    "status": task.status.value,
                    "traffic_light": task.traffic_light.value,
                    "blockers": task.blockers,
                    "latest_update": task.latest_update,
                    "next_expected_checkpoint": task.next_expected_checkpoint,
                }
            )
        recent_logs = [{"timestamp": log.timestamp.isoformat(), "user_message": log.user_message} for log in state.daily_logs[-8:]]
        return json.dumps({"sprint_name": state.sprint_name, "tasks": tasks, "recent_logs": recent_logs}, indent=2)

    def interpret_message(
        self, state: SprintState, message: str, archived_faq_context: str = ""
    ) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if client is None:
            return None
        system_prompt = (
            "You are a sprint copilot that must output strict JSON only.\n"
            "Decide if user message is a query or update.\n"
            "For queries, provide concise assistant_response and no updates.\n"
            "For updates, provide updates using existing task names from context.\n"
            "You can also add/remove tasks based on user intent.\n"
            "If uncertain, return intent='clarify' with assistant_response asking a precise follow-up.\n"
            "Allowed status values: not_started, in_progress, on_hold, follow_up, blocked, done.\n"
            "Use on_hold when the user says 'on hold', 'paused', or deprioritized. Use blocked only when an external blocker prevents progress.\n"
            "Allowed traffic_light values: green, yellow, red.\n"
            "For updates, latest_update must follow concise business writing.\n"
            "Put progress summary first. If impact detail exists in user input, merge it naturally into summary text.\n"
            "Do not use an explicit 'Impact:' label.\n"
            "Add next actions in a separate paragraph as: Next Steps: <next step>.\n"
            "Do not include task name, status words, or traffic color words in latest_update.\n"
            "Use concrete facts and keep it under 240 characters when possible.\n"
            "Return JSON with keys: intent, assistant_response, updates.\n"
            "updates is a list with keys: action, task_name, definition, task_link, owner, eta, status, traffic_light, "
            "latest_update, blockers, next_expected_checkpoint, do_not_ask_days.\n"
            "definition is a 1-3 sentence description of what the task is and why it exists. "
            "Set it when adding a new task or when the user explicitly describes what a task is about.\n"
            "task_link is a URL to associate with the task. Only set it when the user provides a URL.\n"
            "action must be one of: add, update, remove.\n"
            "If an 'Archived FAQs' section appears in the user prompt, those Q/A pairs are not shown in the FAQ sidebar; "
            "use them in assistant_response only when they help answer the user's message."
        )
        extra = ""
        if archived_faq_context.strip():
            extra = f"\n\n{archived_faq_context.strip()}\n"
        user_prompt = (
            f"State:\n{self._state_context(state)}{extra}\nUser message:\n{message}\n\nOutput JSON only."
        )
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            )
            content = completion.choices[0].message.content or "{}"
        except Exception:
            # Never crash chat flow on provider errors; caller will use deterministic fallback.
            return None
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            return None
        intent = str(parsed.get("intent", "")).strip().lower()
        if intent not in {"query", "update", "clarify"}:
            return None
        if not isinstance(parsed.get("updates", []), list):
            parsed["updates"] = []
        if not isinstance(parsed.get("assistant_response", ""), str):
            parsed["assistant_response"] = ""
        return parsed

    def refine_updates(self, updates: List[Dict[str, Any]], raw_message: str) -> List[Dict[str, Any]]:
        client = self._get_client()
        if client is None or not updates:
            return updates
        payload: List[Dict[str, Any]] = []
        for item in updates:
            if not isinstance(item, dict):
                continue
            payload.append(
                {
                    "action": str(item.get("action", "update")).strip().lower(),
                    "task_name": str(item.get("task_name", "")).strip(),
                    "status": str(item.get("status", "")).strip().lower(),
                    "traffic_light": str(item.get("traffic_light", "")).strip().lower(),
                    "latest_update": str(item.get("latest_update", "")).strip(),
                }
            )
        if not payload:
            return updates
        system_prompt = (
            "You refine sprint task latest updates.\n"
            "Return strict JSON only with key: updates.\n"
            "Keep action/task_name/status/traffic_light unchanged.\n"
            "Rewrite latest_update into concise business English.\n"
            "Remove task name and remove status/color wording (e.g., in progress, on hold, follow up, blocked, done, green/yellow/red).\n"
            "Format as: summary sentence(s), then a new paragraph: Next Steps: <next step>.\n"
            "If impact detail exists, blend it into summary sentence(s) naturally and never use 'Impact:' label.\n"
            "Keep only concrete details and avoid generic fluff.\n"
            "If no explanatory detail exists, return latest_update as empty string."
        )
        user_prompt = (
            f"Raw user message:\n{raw_message}\n\n"
            f"Updates to refine:\n{json.dumps(payload, ensure_ascii=True)}\n\n"
            "Output JSON only."
        )
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            )
            parsed = json.loads(completion.choices[0].message.content or "{}")
        except Exception:
            return updates
        refined = parsed.get("updates", []) if isinstance(parsed, dict) else []
        if not isinstance(refined, list):
            return updates
        by_key: Dict[str, Dict[str, Any]] = {}
        for item in refined:
            if not isinstance(item, dict):
                continue
            key = f"{str(item.get('action', '')).strip().lower()}|{str(item.get('task_name', '')).strip().lower()}"
            by_key[key] = item
        out: List[Dict[str, Any]] = []
        for original in updates:
            if not isinstance(original, dict):
                continue
            key = f"{str(original.get('action', '')).strip().lower()}|{str(original.get('task_name', '')).strip().lower()}"
            picked = by_key.get(key)
            if picked and isinstance(picked.get("latest_update", ""), str):
                original["latest_update"] = str(picked.get("latest_update", "")).strip()
            out.append(original)
        return out or updates

    def refine_task_history_text(self, task_name: str, raw_text: str) -> str:
        """Softly polish task history text while preserving factual content."""
        text = (raw_text or "").strip()
        if not text or not self.enabled:
            return text
        client = self._get_client()
        if client is None:
            return text
        system_prompt = (
            "You refine task timeline notes for readability.\n"
            "Do grammar and clarity improvements only.\n"
            "Do NOT add or remove facts, dates, owners, status signals, blockers, URLs, or commitments.\n"
            "Do NOT infer missing context.\n"
            "Keep meaning exactly the same and keep it concise.\n"
            "Return strict JSON with one key: refined."
        )
        user_prompt = (
            f"Task: {task_name}\n"
            f"Original text:\n{text}\n\n"
            "Return JSON only."
        )
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.0,
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            )
            parsed = json.loads(completion.choices[0].message.content or "{}")
        except Exception:
            return text
        if not isinstance(parsed, dict):
            return text
        refined = str(parsed.get("refined", "")).strip()
        if not refined:
            return text
        return refined

    def generate_sprint_report(self, state: SprintState, recent_updates: List[str]) -> Optional[str]:
        client = self._get_client()
        if client is None:
            return None
        system_prompt = (
            "Write a weekly sprint report in clear business prose.\n"
            "Follow this structure exactly:\n"
            "## Summary\n## Progress vs Plan\n## Risks and Blockers\n## Decisions Needed\n## Next 7 Days\n## On Stack\n"
            "The 'Progress vs Plan' section MUST be a markdown table with exactly these columns:\n"
            "| Task | Status | Update |\n"
            "| --- | --- | --- |\n"
            "Include only tasks that are NOT on_hold in 'Progress vs Plan'.\n"
            "The 'On Stack' section MUST be another markdown table with the same columns and include only on_hold tasks.\n"
            "For task names, use markdown links when task_link exists.\n"
            "For status, prefix with color emoji: 🟢 or 🟡 or 🔴.\n"
            "Keep the Update cell concise (one sentence, no line breaks).\n"
            "All other sections remain prose or bullet lists as appropriate.\n"
            "Use concise, concrete language and evidence from input updates.\n"
            "Avoid vague claims. Include owners/tasks when present.\n"
            "Output markdown only."
        )
        updates_block = "\n".join(f"- {u}" for u in recent_updates[:12]) if recent_updates else "- No recent updates."
        user_prompt = (
            f"State:\n{self._state_context(state)}\n\n"
            f"Recent updates:\n{updates_block}\n\n"
            "Generate the sprint report now."
        )
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            )
            text = completion.choices[0].message.content
        except Exception:
            return None
        if not isinstance(text, str):
            return None
        text = text.strip()
        if not text:
            return None
        return text + ("" if text.endswith("\n") else "\n")

    def normalize_sprint_text_to_markdown(self, raw_text: str) -> Optional[str]:
        client = self._get_client()
        if client is None:
            return None
        system_prompt = (
            "You convert arbitrary sprint planning text into clean markdown.\n"
            "Output markdown only, no JSON and no commentary.\n"
            "Target structure:\n# <Sprint Name>\n## <Section Name>\n- [ ] <Task>\n"
            "Preserve concrete tasks/milestones, remove duplicates, keep wording concise."
        )
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Convert this sprint source text:\n\n{raw_text[:120000]}"},
                ],
            )
            text = completion.choices[0].message.content
        except Exception:
            # Import should continue with local fallback when OpenAI fails.
            return None
        if not isinstance(text, str):
            return None
        text = text.strip()
        if not text or "# " not in text:
            return None
        return text + ("" if text.endswith("\n") else "\n")

    def refine_faq_text(self, raw_text: str, kind: str) -> str:
        """Polish grammar and wording; keep meaning. kind is 'question' or 'answer'. Returns original if LLM off or on error."""
        raw_text = (raw_text or "").strip()
        if not raw_text:
            return raw_text
        if not self.enabled:
            return raw_text
        client = self._get_client()
        if client is None:
            return raw_text
        label = "question" if (kind or "").strip().lower() == "question" else "answer"
        system_prompt = (
            "You refine FAQ text for grammar, spelling, and clear professional English only.\n"
            "Do not change facts, meaning, intent, or scope.\n"
            "Do not add new information, names, dates, or claims.\n"
            "Do not remove substantive content—only fix unclear or broken phrasing.\n"
            "Keep questions as questions and answers as answers.\n"
            "Return strict JSON with a single key: refined (string)."
        )
        user_prompt = f"This is an FAQ {label}:\n\n{raw_text}\n\nReturn JSON only."
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            parsed = json.loads(completion.choices[0].message.content or "{}")
        except Exception:
            return raw_text
        if not isinstance(parsed, dict):
            return raw_text
        refined = str(parsed.get("refined", "")).strip()
        if not refined:
            return raw_text
        return refined

    def interpret_faq_intent(self, message: str, active_faq_rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Classify FAQ-mode user text into a structured action. Requires API key."""
        message = (message or "").strip()
        if not message:
            return None
        if not self.enabled:
            return None
        client = self._get_client()
        if client is None:
            return None
        system_prompt = (
            "You interpret user messages in Sprint FAQ editing mode.\n"
            "The user is managing a numbered list of FAQ items (Q1, Q2, …) shown in the app.\n"
            "Infer intent from natural language—do not require rigid command phrases.\n"
            "Return strict JSON only with these keys:\n"
            "- action: one of add_question, set_answer, update_question, archive, clarify.\n"
            "- target_q: integer 1-based index into the ACTIVE list below, or null if not applicable or unknown.\n"
            "- question_text: string. For add_question, the new question wording. For update_question, the replacement question text. Otherwise \"\".\n"
            "- answer_text: string. For set_answer (or add_question if they gave an answer in the same message), the answer body. Otherwise \"\".\n"
            "- clarify_message: string. Non-empty only when action is clarify—one short sentence asking what to do.\n"
            "Rules:\n"
            "- add_question: user adds a new FAQ, or their whole message is a new question to track. If they include both Q and A, fill question_text and answer_text.\n"
            "- set_answer: user supplies or revises an answer for an existing item. Resolve target_q from phrases like 'for Q2', 'second question', 'the one about X', or by topic match to the list below.\n"
            "- update_question: user rephrases or corrects the question text for an existing item (not the answer).\n"
            "- archive: user wants to retire/hide an item (archive, remove from active list, done with this FAQ).\n"
            "- clarify: only if you truly cannot choose a single interpretation; ask briefly.\n"
            "- If the message clearly matches one FAQ topic for answering, prefer set_answer with that target_q.\n"
            "- If no active FAQs exist, add_question is the only sensible add path; do not use target_q.\n"
            "- If the user uses a prefix like 'add this question:' or 'add question:', put only the text after the colon in question_text.\n"
            "- Do not use add_question for greetings, thanks, or off-topic chatter; use clarify if the message is not FAQ-related.\n"
            "Output JSON only."
        )
        ctx = json.dumps(active_faq_rows, ensure_ascii=True, indent=2)
        user_prompt = f"Active FAQs (n is the display number Qn):\n{ctx}\n\nUser message:\n{message}\n"
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            parsed = json.loads(completion.choices[0].message.content or "{}")
        except Exception:
            return None
        if not isinstance(parsed, dict):
            return None
        action = str(parsed.get("action", "")).strip().lower()
        if action not in {"add_question", "set_answer", "update_question", "archive", "clarify"}:
            return None
        tq = parsed.get("target_q")
        target_q: Optional[int] = None
        if tq is not None:
            try:
                target_q = int(tq)
            except (TypeError, ValueError):
                target_q = None
        out: Dict[str, Any] = {
            "action": action,
            "target_q": target_q,
            "question_text": str(parsed.get("question_text", "") or "").strip(),
            "answer_text": str(parsed.get("answer_text", "") or "").strip(),
            "clarify_message": str(parsed.get("clarify_message", "") or "").strip(),
        }
        return out

    def extract_overall_knowledge(self, state: SprintState, message: str) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if client is None:
            return None
        system_prompt = (
            "Extract durable organizational knowledge from the user message.\n"
            "Return strict JSON with key: items.\n"
            "Each item must include: text, knowledge_type, scope, confidence.\n"
            "knowledge_type must be one of: constraint, capability, dependency, risk, organizational_limit, process_rule.\n"
            "scope must be one of: team, system, process, project.\n"
            "Use only facts explicitly present in message; do not invent.\n"
            "ONLY keep durable, reusable organizational rules, constraints, ownership mappings, or cross-team alignment policies.\n"
            "Do NOT include transient timeline/status updates such as waiting, in progress, ETA, or date-specific statements.\n"
            "When useful, rewrite to a canonical policy sentence (for example: 'If X, then Y' or 'For domain D, align team T')."
        )
        user_prompt = (
            f"State:\n{self._state_context(state)}\n\n"
            f"User message:\n{message}\n\n"
            "Output JSON only."
        )
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            parsed = json.loads(completion.choices[0].message.content or "{}")
        except Exception:
            return None
        if not isinstance(parsed, dict):
            return None
        items = parsed.get("items", [])
        if not isinstance(items, list):
            items = []
        normalized = []
        allowed_types = {"constraint", "capability", "dependency", "risk", "organizational_limit", "process_rule"}
        allowed_scopes = {"team", "system", "process", "project"}
        for item in items:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text", "")).strip()
            ktype = str(item.get("knowledge_type", "")).strip().lower()
            scope = str(item.get("scope", "")).strip().lower()
            try:
                confidence = float(item.get("confidence", 0.0))
            except (TypeError, ValueError):
                confidence = 0.0
            if not text or ktype not in allowed_types or scope not in allowed_scopes:
                continue
            normalized.append(
                {
                    "text": text,
                    "knowledge_type": ktype,
                    "scope": scope,
                    "confidence": max(0.0, min(1.0, confidence)),
                }
            )
        return {"items": normalized}

    # ── Entity-relation extraction for the knowledge graph ─────────────────
    def extract_entity_relations(
        self, state: "SprintState", message: str
    ) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if client is None:
            return None
        system_prompt = (
            "You are an entity-relationship extractor that builds a knowledge graph.\n"
            "Given a user message about their work, extract ENTITIES and RELATIONS.\n\n"
            "Return strict JSON with two keys: entities, relations.\n\n"
            "entities: array of objects with keys:\n"
            "  - name: short canonical label (e.g. 'Mordor Team', 'PLP', 'Core Web Vitals')\n"
            "  - type: one of: team, domain, topic, system, person, process, constraint\n"
            "  - description: one sentence explaining what this entity is or does\n\n"
            "relations: array of objects with keys:\n"
            "  - source: entity name (must match one in entities array)\n"
            "  - target: entity name (must match one in entities array)\n"
            "  - type: one of: has_subtopic, owns, depends_on, has_constraint, related_to, part_of, communicates_with, blocks\n"
            "  - label: short human-readable description of the relationship\n"
            "  - confidence: 0.0 to 1.0\n\n"
            "Rules:\n"
            "- Extract entities explicitly mentioned or clearly implied.\n"
            "- Use SHORT CANONICAL names: strip syntax like $...$ or quotes. "
            "Merge obvious variants into one entity (e.g. 'Mordor' and 'Mordor team' = 'Mordor Team').\n"
            "- DO extract: teams, work domains, topics/techniques, systems/platforms, people, processes, rules/constraints.\n"
            "- DO extract ownership, dependencies, subtopic hierarchies, constraints, cross-team alignment.\n"
            "- Do NOT extract generic/common nouns as entities (HTML, URL, page, API, date, time, desktop, mobile).\n"
            "- Do NOT extract transient status (dates, ETAs, progress percentages, 'waiting for review').\n"
            "- If no meaningful entities or relations can be extracted, return empty arrays.\n"
            "- Prefer specific relation types over generic 'related_to' when a clearer type applies.\n"
            "- Prefer fewer, high-quality entities over many low-value ones."
        )
        user_prompt = (
            f"Sprint context:\n{self._state_context(state)}\n\n"
            f"User message:\n{message}\n\n"
            "Extract entities and relations. Output JSON only."
        )
        try:
            completion = client.chat.completions.create(
                model=self.model,
                temperature=0.1,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            parsed = json.loads(completion.choices[0].message.content or "{}")
        except Exception:
            return None
        if not isinstance(parsed, dict):
            return None

        VALID_ENTITY_TYPES = {"team", "domain", "topic", "system", "person", "process", "constraint"}
        VALID_RELATION_TYPES = {
            "has_subtopic", "owns", "depends_on", "has_constraint",
            "related_to", "part_of", "communicates_with", "blocks",
        }

        entities = []
        for item in parsed.get("entities", []):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            etype = str(item.get("type", "")).strip().lower()
            desc = str(item.get("description", "")).strip()
            if not name:
                continue
            if etype not in VALID_ENTITY_TYPES:
                etype = "topic"
            entities.append({"name": name, "type": etype, "description": desc})

        entity_names_lower = {e["name"].lower() for e in entities}

        relations = []
        for item in parsed.get("relations", []):
            if not isinstance(item, dict):
                continue
            source = str(item.get("source", "")).strip()
            target = str(item.get("target", "")).strip()
            rtype = str(item.get("type", "")).strip().lower()
            label = str(item.get("label", "")).strip()
            try:
                confidence = float(item.get("confidence", 0.8))
            except (TypeError, ValueError):
                confidence = 0.8
            if not source or not target:
                continue
            if source.lower() not in entity_names_lower or target.lower() not in entity_names_lower:
                continue
            if rtype not in VALID_RELATION_TYPES:
                rtype = "related_to"
            relations.append({
                "source": source,
                "target": target,
                "type": rtype,
                "label": label,
                "confidence": max(0.0, min(1.0, confidence)),
            })

        return {"entities": entities, "relations": relations}
