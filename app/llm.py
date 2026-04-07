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

    def interpret_message(self, state: SprintState, message: str) -> Optional[Dict[str, Any]]:
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
            "updates is a list with keys: action, task_name, task_link, owner, eta, status, traffic_light, "
            "latest_update, blockers, next_expected_checkpoint, do_not_ask_days.\n"
            "task_link is a URL to associate with the task. Only set it when the user provides a URL.\n"
            "action must be one of: add, update, remove."
        )
        user_prompt = f"State:\n{self._state_context(state)}\n\nUser message:\n{message}\n\nOutput JSON only."
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

    def generate_sprint_report(self, state: SprintState, recent_updates: List[str]) -> Optional[str]:
        client = self._get_client()
        if client is None:
            return None
        system_prompt = (
            "Write a weekly sprint report in clear business prose.\n"
            "Follow this structure exactly:\n"
            "## Summary\n## Business Impact\n## Progress vs Plan\n## Risks and Blockers\n## Decisions Needed\n## Next 7 Days\n"
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
            "Skip transient status-only task updates."
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
