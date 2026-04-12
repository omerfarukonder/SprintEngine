"""
Streamlit frontend for Sprint Engine — IDE-style layout.
Requires the FastAPI backend running on http://127.0.0.1:8001.
Start with:  streamlit run streamlit_app.py
"""

from __future__ import annotations

import html as html_mod
import re as _re
from datetime import datetime

import requests
import streamlit as st

API_BASE = "http://127.0.0.1:8001"

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Sprint Engine", page_icon="🚀", layout="wide", initial_sidebar_state="collapsed")

# ── Session state defaults ───────────────────────────────────────────────────
for key, default in [
    ("chat_history", []), ("chat_mode", "auto"), ("active_view", "tasks"),
    ("report_content", ""), ("report_loaded", False), ("table_sort", "status"),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── API helpers ──────────────────────────────────────────────────────────────
def api_get(path: str):
    try:
        r = requests.get(f"{API_BASE}{path}", timeout=15)
        return r.json() if r.ok else None
    except requests.ConnectionError:
        return None


def api_post(path: str, body: dict | None = None):
    try:
        r = requests.post(f"{API_BASE}{path}", json=body or {}, timeout=60)
        return r.json(), r.ok
    except requests.ConnectionError:
        return {"detail": "Backend not reachable. Is the FastAPI server running?"}, False


# ── Constants ────────────────────────────────────────────────────────────────
LIGHT_EMOJI = {"green": "🟢", "yellow": "🟡", "red": "🔴"}
STATUS_EMOJI = {
    "done": "✅", "in_progress": "🚧", "on_hold": "⏸️",
    "follow_up": "🔁", "blocked": "⛔", "not_started": "⚪",
}
STATUS_RANK = {"blocked": 0, "in_progress": 1, "on_hold": 2, "follow_up": 3, "not_started": 4, "done": 5}
STATUS_COLORS = {
    "blocked": "#ff6b6b", "in_progress": "#ffd43b", "on_hold": "#a9a9a9",
    "follow_up": "#74c0fc", "not_started": "#868e96", "done": "#51cf66",
}

def esc(v: str) -> str:
    return html_mod.escape(str(v or ""))

def pretty_status(status: str) -> str:
    text = (status or "").replace("_", " ").strip().title()
    return "Completed" if text == "Done" else text

def pretty_eta(eta: str) -> str:
    return (eta or "").strip() or "-"

def add_chat(role: str, text: str):
    st.session_state.chat_history.append({"role": role, "text": text})


# ── Global CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Base ── */
.stApp { background: #111111; }
/* Use full viewport width — Streamlit caps .block-container by default, leaving empty space on the right */
section.main > div,
[data-testid="stMainBlockContainer"] {
  max-width: none !important;
}
div.block-container {
  max-width: none !important;
  padding-left: max(1rem, env(safe-area-inset-left)) !important;
  padding-right: max(1rem, env(safe-area-inset-right)) !important;
}
[data-testid="stSidebar"] { background: #0a0a0a; border-right: 1px solid #2a2a2a; }
[data-testid="stSidebar"] [data-testid="stMarkdown"] { color: #d4d4d4; }
header[data-testid="stHeader"] { background: #111111; }

/* ── Activity bar ── */
.act-section {
    font-size:10px; text-transform:uppercase; letter-spacing:1.2px;
    color:#666666; padding:16px 14px 6px; font-weight:600;
}

/* ── Health bar ── */
.health-bar { display:flex; height:6px; border-radius:3px; overflow:hidden; margin:8px 0 4px; }
.health-seg { height:100%; transition:width .3s; }

/* ── Status pills ── */
.st-pill {
    display:inline-flex; align-items:center; gap:5px;
    padding:3px 10px; border-radius:12px; font-size:11.5px; font-weight:600;
    white-space:nowrap;
}

/* ── Task table ── */
.task-table { width:100%; border-collapse:collapse; font-size:12.5px; font-family:'Segoe UI',sans-serif; }
.task-table th {
    background:#0a0a0a; color:#999999; padding:10px 8px; text-align:left;
    font-size:10.5px; text-transform:uppercase; letter-spacing:.8px; font-weight:600;
    border-bottom:2px solid #2a2a2a; position:sticky; top:0; z-index:1;
}
.task-table td {
    padding:10px 8px; border-bottom:1px solid #2a2a2a; color:#d4d4d4;
    vertical-align:top; word-wrap:break-word; overflow-wrap:break-word;
}
.task-table tr:hover td { background:#1a1a1a; }
.task-table td:nth-child(1) { width:32px; color:#666666; font-size:11px; }
.task-table td:nth-child(4) { width:40px; text-align:center; }
.task-table td:nth-child(5) { width:130px; }
.task-table td:nth-child(6) { width:64px; }
.task-table .meta { font-size:10px; color:#666666; margin-top:2px; }
.task-table .next-steps { margin-top:5px; font-size:11.5px; color:#999999; }
.task-table .next-steps b { color:#e09956; }
.task-table a { color:#e09956; text-decoration:none; }
.task-table a:hover { text-decoration:underline; }
.task-table .def-text { color:#999999; font-size:12px; }

/* ── Fixed chat panel — target column that contains the .chat-panel-root marker ── */
[data-testid="stColumn"]:has(.chat-panel-root) {
    position: sticky;
    top: 0;
    align-self: flex-start;
    height: 100vh;
    overflow-y: auto;
    background: #0a0a0a;
    border-left: 1px solid #2a2a2a;
    padding: 12px 8px 8px !important;
    z-index: 50;
}
.chat-panel-root { /* marker element, no visual */ }

.chat-panel-title {
    font-size:11px; text-transform:uppercase; letter-spacing:1px;
    color:#666666; font-weight:600; padding-bottom:6px; margin-bottom:6px;
    border-bottom:1px solid #2a2a2a;
    display:flex; align-items:center; gap:8px;
}
.chat-panel-title .badge {
    background:#222222; color:#999999; padding:1px 8px;
    border-radius:10px; font-size:10px;
}
.chat-msg { padding:5px 0; font-size:12.5px; line-height:1.45; }
.chat-role { font-weight:700; margin-right:6px; }
.chat-role.user { color:#e09956; }
.chat-role.copilot { color:#d4a843; }
.chat-role.system { color:#666666; }
.chat-text { color:#d4d4d4; white-space:pre-wrap; word-break:break-word; }

/* ── Follow-ups in chat panel ── */
.fu-title {
    font-size:10px; text-transform:uppercase; letter-spacing:1px;
    color:#666666; font-weight:600; padding:8px 0 4px;
    border-top:1px solid #2a2a2a; margin-top:6px;
}
.fu-item {
    font-size:11.5px; color:#999999; padding:3px 0;
    border-bottom:1px solid #111111;
}

/* ── Panel styling ── */
.panel-title {
    font-size:11px; text-transform:uppercase; letter-spacing:1px;
    color:#666666; font-weight:600; margin-bottom:10px;
    display:flex; align-items:center; gap:8px;
}
.panel-title .badge {
    background:#222222; color:#999999; padding:1px 8px;
    border-radius:10px; font-size:10px;
}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# DATA FETCH
# ══════════════════════════════════════════════════════════════════════════════
tasks_data = api_get("/api/tasks")
if tasks_data is None:
    st.error("Cannot reach backend. Make sure FastAPI is running on port 8001.")
    st.code("source .venv/bin/activate && python -m app.main", language="bash")
    st.stop()

sprint_name = tasks_data.get("sprint_name", "-")
tasks = tasks_data.get("tasks", [])
followups_data = api_get("/api/followups")
followups = followups_data.get("follow_ups", []) if followups_data else []


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — Activity Bar
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:12px 8px 4px; display:flex; align-items:center; gap:10px;">
        <span style="font-size:22px;">🚀</span>
        <div>
            <div style="font-size:15px; font-weight:700; color:#d4d4d4;">Sprint Engine</div>
            <div style="font-size:10px; color:#666666;">local-first sprint workspace</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="act-section">Navigation</div>', unsafe_allow_html=True)

    for key, label in {"tasks": "📋  Tasks", "report": "📝  Report", "plan": "📄  Plan"}.items():
        if st.button(label, key=f"nav_{key}", use_container_width=True,
                     type="primary" if st.session_state.active_view == key else "secondary"):
            st.session_state.active_view = key
            st.rerun()

    st.markdown('<div class="act-section">Actions</div>', unsafe_allow_html=True)

    with st.expander("🔄 Initialize", expanded=False):
        init_mode = st.selectbox(
            "Mode", ["sync_missing", "destructive"],
            format_func=lambda m: "Sync Missing" if m == "sync_missing" else "Destructive",
            label_visibility="collapsed",
        )
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Run", key="init_run", use_container_width=True):
                confirm = "RESET" if init_mode == "destructive" else ""
                data, ok = api_post("/api/initialize", {"mode": init_mode, "confirm_text": confirm})
                if ok:
                    msg = f"Initialized. Tasks: {data.get('task_count', '?')}"
                    st.toast(msg, icon="✅"); add_chat("system", msg); st.rerun()
                else:
                    st.toast(data.get("detail", "Failed"), icon="❌")
        with c2:
            if st.button("Undo", key="init_undo", use_container_width=True):
                data, ok = api_post("/api/initialize/undo")
                if ok:
                    msg = f"Undo restored. Tasks: {data.get('task_count', '?')}"
                    st.toast(msg, icon="↩️"); add_chat("system", msg); st.rerun()
                else:
                    st.toast(data.get("detail", "Failed"), icon="❌")

    with st.expander("📥 Import DOCX", expanded=False):
        docx_path = st.text_input("Path", placeholder="/Users/.../Sprint Plan.docx", label_visibility="collapsed")
        if st.button("Import", key="import_run", use_container_width=True):
            if docx_path.strip():
                add_chat("system", f"Importing: {docx_path}")
                data, ok = api_post("/api/import-docx-plan", {"file_path": docx_path, "auto_initialize": True})
                if ok:
                    merge = ""
                    if data.get("added_count", 0) or data.get("synced_count", 0):
                        merge = f" +{data['added_count']} synced:{data['synced_count']}"
                    msg = f"Imported ({data.get('source', '?')}). Tasks: {data.get('task_count', '?')}{merge}"
                    st.toast(msg, icon="✅"); add_chat("copilot", msg); st.rerun()
                else:
                    st.toast(data.get("detail", "Failed"), icon="❌")
            else:
                st.toast("Enter a file path.", icon="⚠️")

    with st.expander("📊 Export", expanded=False):
        if st.button("Generate Table File", key="gen_table", use_container_width=True):
            data, ok = api_post("/api/generate-table")
            st.toast(f"Saved: {data.get('path', '')}" if ok else "Failed", icon="📄" if ok else "❌")


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH BAR (top strip)
# ══════════════════════════════════════════════════════════════════════════════
def render_health_bar(tasks_list):
    total = len(tasks_list)
    if not total:
        return
    counts = {}
    for t in tasks_list:
        s = t.get("status", "not_started")
        counts[s] = counts.get(s, 0) + 1
    done = counts.get("done", 0)
    in_prog = counts.get("in_progress", 0)
    blocked = counts.get("blocked", 0)
    on_hold = counts.get("on_hold", 0)
    other = total - done - in_prog - blocked - on_hold
    segments = ""
    for count, color in [
        (done, "#51cf66"), (in_prog, "#ffd43b"), (blocked, "#ff6b6b"),
        (on_hold, "#a9a9a9"), (other, "#333333"),
    ]:
        if count > 0:
            segments += f'<div class="health-seg" style="width:{count/total*100}%;background:{color};"></div>'
    st.markdown(f"""
    <div style="display:flex; align-items:center; gap:12px; padding:2px 0 6px;">
        <span style="font-size:13px; font-weight:600; color:#d4d4d4;">{esc(sprint_name)}</span>
        <div class="health-bar" style="flex:1;">{segments}</div>
        <div style="display:flex; gap:10px; font-size:11px; color:#999999;">
            <span>✅ {done}</span><span>🚧 {in_prog}</span><span>⛔ {blocked}</span>
            <span>⏸️ {on_hold}</span><span>⚪ {other}</span>
            <span style="color:#666666;">({total} total)</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

render_health_bar(tasks)


# ══════════════════════════════════════════════════════════════════════════════
# TWO-COLUMN LAYOUT: Main Content (left) + Chat Panel (right)
# ══════════════════════════════════════════════════════════════════════════════
main_col, chat_col = st.columns([3, 1], gap="medium")

# ──────────────────────────────────────────────────────────────────────────────
# LEFT: Main content area
# ──────────────────────────────────────────────────────────────────────────────
with main_col:

    # ── VIEW: TASKS ──────────────────────────────────────────────────────────
    if st.session_state.active_view == "tasks":
        sort_col, _ = st.columns([1, 4])
        with sort_col:
            st.session_state.table_sort = st.selectbox(
                "Sort", ["status", "plan", "updated"],
                format_func=lambda m: {"status": "Status Priority", "plan": "Plan Order", "updated": "Latest Updated"}[m],
                index=["status", "plan", "updated"].index(st.session_state.table_sort),
                label_visibility="collapsed",
            )

        sorted_tasks = list(tasks)
        sm = st.session_state.table_sort
        if sm == "status":
            sorted_tasks.sort(key=lambda t: (STATUS_RANK.get(t.get("status", ""), 9), t.get("task_name", "")))
        elif sm == "updated":
            sorted_tasks.sort(key=lambda t: t.get("last_updated_at", ""), reverse=True)

        if not sorted_tasks:
            st.info("No tasks yet. Import a DOCX or initialize from sprint_plan.md.")
        else:
            rows_html = ""
            for i, task in enumerate(sorted_tasks, 1):
                light = LIGHT_EMOJI.get(task.get("traffic_light", ""), "-")
                status_raw = task.get("status", "not_started")
                s_color = STATUS_COLORS.get(status_raw, "#868e96")
                s_emoji = STATUS_EMOJI.get(status_raw, "")
                s_text = pretty_status(status_raw)

                try:
                    dt = datetime.fromisoformat(task.get("last_updated_at", "").replace("Z", "+00:00"))
                    updated_str = dt.strftime("%b %d")
                except Exception:
                    updated_str = "-"

                task_name = esc(task.get("task_name", "-"))
                task_link = (task.get("task_link", "") or "").strip()
                task_cell = f'{task_name} <a href="{esc(task_link)}" target="_blank" title="{esc(task_link)}">🔗</a>' if task_link else task_name

                definition = esc(task.get("definition", task.get("task_name", "-")))
                eta = esc(pretty_eta(task.get("eta", "")))

                lu_raw = (task.get("latest_update", "") or "").strip()
                if lu_raw:
                    ns_match = _re.search(r"(?:\n\s*\n|\s+)?(?:\*\*)?Next Steps?:\s*(.+)$", lu_raw, _re.IGNORECASE | _re.DOTALL)
                    if ns_match:
                        summary = esc(lu_raw[:ns_match.start()].strip().replace("**", ""))
                        next_steps = esc(ns_match.group(1).strip().replace("**", ""))
                        lu_cell = f"{summary}<div class='next-steps'><b>Next Steps:</b> {next_steps}</div>"
                    else:
                        lu_cell = esc(lu_raw)
                else:
                    lu_cell = "<span style='color:#333333;'>-</span>"

                pill = f'<span class="st-pill" style="background:{s_color}22;color:{s_color};border:1px solid {s_color}44;">{s_emoji} {s_text}</span>'
                status_cell = f"{pill}<div class='meta'>Updated: {esc(updated_str)}</div>"
                rows_html += f"<tr><td>{i}</td><td>{task_cell}</td><td class='def-text'>{definition}</td><td>{light}</td><td>{status_cell}</td><td>{eta}</td><td>{lu_cell}</td></tr>"

            st.markdown(f"""
            <div style="overflow-x:auto;">
            <table class="task-table"><thead><tr>
            <th>#</th><th>Task</th><th>Definition</th><th></th><th>Status</th><th>ETA</th><th>Latest Update</th>
            </tr></thead><tbody>{rows_html}</tbody></table>
            </div>
            """, unsafe_allow_html=True)

    # ── VIEW: REPORT ─────────────────────────────────────────────────────────
    elif st.session_state.active_view == "report":
        if not st.session_state.report_loaded:
            rdata = api_get("/api/report")
            st.session_state.report_content = rdata.get("content", "") if rdata else ""
            st.session_state.report_loaded = True

        c1, c2, c3 = st.columns([1, 1, 4])
        with c1:
            if st.button("Generate Report", use_container_width=True):
                with st.spinner("Generating..."):
                    data, ok = api_post("/api/report/generate")
                    if ok:
                        st.session_state.report_content = data.get("content", "")
                        st.toast("Report generated", icon="✅"); st.rerun()
                    else:
                        st.toast(data.get("detail", "Failed"), icon="❌")
        with c2:
            if st.button("Save Report", use_container_width=True):
                data, ok = api_post("/api/report/save", {"content": st.session_state.report_content})
                st.toast("Saved" if ok else "Failed", icon="💾" if ok else "❌")

        view_mode = st.radio("Mode", ["Edit", "Preview"], horizontal=True, label_visibility="collapsed")
        if view_mode == "Edit":
            st.session_state.report_content = st.text_area(
                "Report", value=st.session_state.report_content, height=550, label_visibility="collapsed",
            )
        else:
            st.markdown(st.session_state.report_content or "*No report content yet.*")

    # ── VIEW: PLAN ───────────────────────────────────────────────────────────
    elif st.session_state.active_view == "plan":
        plan_data = api_get("/api/plan")
        if plan_data and plan_data.get("exists"):
            st.code(plan_data.get("content", ""), language="markdown")
        else:
            st.info("No sprint_plan.md found yet. Import a DOCX to create one.")


# ──────────────────────────────────────────────────────────────────────────────
# RIGHT: Chat panel (full height alongside main content)
# ──────────────────────────────────────────────────────────────────────────────
with chat_col:
    # Marker element for CSS :has() selector to make this column sticky
    st.markdown('<div class="chat-panel-root"></div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="chat-panel-title">Terminal <span class="badge">Chat</span></div>',
        unsafe_allow_html=True,
    )

    # Chat history — fills available height, input stays at bottom
    chat_container = st.container(height=440)
    with chat_container:
        for entry in st.session_state.chat_history:
            role = entry["role"]
            text = entry["text"]
            role_class = {"you": "user", "copilot": "copilot"}.get(role, "system")
            role_label = {"you": "You:", "copilot": "Copilot:"}.get(role, "System:")
            st.markdown(
                f'<div class="chat-msg"><span class="chat-role {role_class}">{role_label}</span>'
                f'<span class="chat-text">{esc(text)}</span></div>',
                unsafe_allow_html=True,
            )

    # Mode selector
    chat_mode = st.selectbox(
        "Mode", ["auto", "query", "update"],
        format_func=lambda m: {"auto": "Auto", "query": "Query", "update": "Update"}[m],
        label_visibility="collapsed",
    )

    # Chat input — compact 2-3 line text area + send button
    user_input = st.text_area(
        "Message",
        placeholder="Type daily update or ask a question...",
        height=68,
        label_visibility="collapsed",
        key="chat_input_area",
    )
    if st.button("Send", key="send_btn", use_container_width=True, type="primary"):
        text = (user_input or "").strip()
        if text:
            add_chat("you", text)
            data, ok = api_post("/api/chat", {"message": text, "mode": chat_mode})
            if ok:
                add_chat("copilot", data.get("answer", "No response."))
                for t in data.get("changed_tasks", []):
                    light = LIGHT_EMOJI.get(t.get("traffic_light", ""), "")
                    add_chat("system", f"- {t['task_name']}: {pretty_status(t.get('status', ''))} {light}")
            else:
                add_chat("copilot", data.get("detail", "Something went wrong."))
            st.rerun()

    # Follow-ups section inside chat panel
    if followups:
        st.markdown(
            f'<div class="fu-title">Follow-ups <span style="background:#2a2a2a;color:#999999;padding:1px 6px;border-radius:8px;font-size:9px;">{len(followups)}</span></div>',
            unsafe_allow_html=True,
        )
        for fu in followups:
            st.markdown(f'<div class="fu-item">{esc(fu)}</div>', unsafe_allow_html=True)
