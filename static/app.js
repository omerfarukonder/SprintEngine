const log = document.getElementById("log");
const message = document.getElementById("message");
const sendBtn = document.getElementById("sendBtn");
const chatModeBtn = document.getElementById("chatModeBtn");
const chatModeMenu = document.getElementById("chatModeMenu");
const chatModeLabel = document.getElementById("chatModeLabel");
const initBtn = document.getElementById("initBtn");
const initModeSelect = document.getElementById("initModeSelect");
const undoInitBtn = document.getElementById("undoInitBtn");
const initWarningBadge = document.getElementById("initWarningBadge");
const tableBtn = document.getElementById("tableBtn");
const importToggleBtn = document.getElementById("importToggleBtn");
const importControls = document.getElementById("importControls");
const docxPath = document.getElementById("docxPath");
const importDocxBtn = document.getElementById("importDocxBtn");
const chatStatus = document.getElementById("chatStatus");
const tableSort = document.getElementById("tableSort");
const followupsEl = document.getElementById("followups");
const planContent = document.getElementById("planContent");
const statusTableBody = document.getElementById("statusTableBody");
const statusMeta = document.getElementById("statusMeta");
const statusSprintName = document.getElementById("statusSprintName");
const dashboardTabBtn = document.getElementById("dashboardTabBtn");
const reportTabBtn = document.getElementById("reportTabBtn");
const dashboardView = document.getElementById("dashboardView");
const reportView = document.getElementById("reportView");
const generateReportBtn = document.getElementById("generateReportBtn");
const saveReportBtn = document.getElementById("saveReportBtn");
const reportEditor = document.getElementById("reportEditor");
const reportPreview = document.getElementById("reportPreview");
const reportEditViewBtn = document.getElementById("reportEditViewBtn");
const reportPreviewViewBtn = document.getElementById("reportPreviewViewBtn");
const reportStatus = document.getElementById("reportStatus");
const reportMeta = document.getElementById("reportMeta");

let activeTypingToken = 0;
const CHAT_MODES = ["auto", "query", "update"];
const CHAT_MODE_NAMES = { auto: "Auto", query: "Explain-only", update: "Update-only" };
let currentChatMode = "auto";
let currentView = "dashboard";
let reportViewMode = "edit";

function formatCopilotLabel(confidence) {
  const numeric = Number(confidence);
  const bounded = Number.isFinite(numeric) ? Math.max(0, Math.min(1, numeric)) : 0.6;
  return `Copilot (${Math.round(bounded * 100)}%):`;
}

function prettyStatus(status) {
  const text = String(status || "").replaceAll("_", " ").trim();
  if (!text) return "-";
  const pretty = text.split(/\s+/).map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
  return pretty === "Done" ? "Completed" : pretty;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function prettyEta(eta) {
  const raw = String(eta || "").trim();
  if (!raw) return "-";
  const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) {
    const dt = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`);
    if (!Number.isNaN(dt.getTime())) {
      return dt.toLocaleDateString(undefined, { month: "short", day: "numeric" });
    }
  }
  return raw;
}

function renderTaskCell(task) {
  const name = escapeHtml(task.task_name || "-");
  const link = String(task.task_link || "").trim();
  if (!link) return name;
  const safeLink = escapeHtml(link);
  return `<span class="task-cell">${name}<a class="task-link-icon" href="${safeLink}" target="_blank" rel="noopener noreferrer" title="Open task link">🔗</a></span>`;
}

function renderLatestUpdateCell(text) {
  const raw = String(text || "").trim();
  if (!raw) return "-";
  const normalized = raw.replace(/\r\n/g, "\n");
  const nextMatch = normalized.match(/^(.*?)(?:\n\s*\n|\s+)?(?:\*\*)?Next Steps?:\s*(.+)$/is);
  if (!nextMatch) {
    return `<div>${escapeHtml(normalized)}</div>`;
  }
  const summary = (nextMatch[1] || "").trim().replaceAll("**", "");
  const nextSteps = (nextMatch[2] || "").trim().replaceAll("**", "");
  if (!summary) {
    return `<div class="next-steps-line"><strong>Next Steps:</strong> ${escapeHtml(nextSteps)}</div>`;
  }
  return `<div>${escapeHtml(summary)}</div><div class="next-steps-line"><strong>Next Steps:</strong> ${escapeHtml(nextSteps)}</div>`;
}

function appendLine(role, text) {
  const line = document.createElement("div");
  line.className = "chat-line";
  const roleSpan = document.createElement("span");
  roleSpan.className = `chat-role ${role}`;
  if (role === "you") roleSpan.textContent = "You:";
  else if (role === "copilot") roleSpan.textContent = "Copilot:";
  else roleSpan.textContent = "System:";
  const textSpan = document.createElement("span");
  textSpan.className = "chat-text";
  textSpan.textContent = text;
  line.appendChild(roleSpan);
  line.appendChild(textSpan);
  log.appendChild(line);
  log.scrollTop = log.scrollHeight;
}

function nearBottom() {
  return log.scrollHeight - log.scrollTop - log.clientHeight < 28;
}

async function appendAnimated(prefix, text) {
  const token = ++activeTypingToken;
  const shouldStickToBottom = nearBottom();
  const line = document.createElement("div");
  line.className = "chat-line";
  const roleSpan = document.createElement("span");
  roleSpan.className = "chat-role copilot";
  roleSpan.textContent = prefix;
  const textSpan = document.createElement("span");
  textSpan.className = "chat-text";
  line.appendChild(roleSpan);
  line.appendChild(textSpan);
  log.appendChild(line);

  const speed = Math.max(6, Math.min(18, Math.floor(1400 / Math.max(text.length, 40))));
  for (const ch of text) {
    if (token !== activeTypingToken) return;
    textSpan.textContent += ch;
    if (shouldStickToBottom) log.scrollTop = log.scrollHeight;
    await new Promise((resolve) => setTimeout(resolve, speed));
  }
  if (shouldStickToBottom) log.scrollTop = log.scrollHeight;
}

function setChatStatus(text, loading = false) {
  chatStatus.textContent = text;
  chatStatus.classList.toggle("loading", loading);
  chatStatus.classList.toggle("status-dots", loading);
}

function setReportStatus(text, loading = false) {
  reportStatus.textContent = text;
  reportStatus.classList.toggle("loading", loading);
  reportStatus.classList.toggle("status-dots", loading);
}

function setChatControlsDisabled(disabled) {
  sendBtn.disabled = disabled;
  message.disabled = disabled;
  chatModeBtn.disabled = disabled;
}

function setReportControlsDisabled(disabled) {
  generateReportBtn.disabled = disabled;
  saveReportBtn.disabled = disabled;
  reportEditor.disabled = disabled;
}

function markdownToHtml(markdown) {
  const lines = String(markdown || "").split("\n");
  const html = [];
  let inList = false;
  let inCode = false;

  const inline = (text) => {
    let t = escapeHtml(text);
    t = t.replace(/`([^`]+)`/g, "<code>$1</code>");
    t = t.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    t = t.replace(/\*([^*]+)\*/g, "<em>$1</em>");
    t = t.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
    return t;
  };

  for (const raw of lines) {
    const line = raw ?? "";
    const trimmed = line.trim();
    if (trimmed.startsWith("```")) {
      if (!inCode) {
        if (inList) {
          html.push("</ul>");
          inList = false;
        }
        html.push("<pre><code>");
        inCode = true;
      } else {
        html.push("</code></pre>");
        inCode = false;
      }
      continue;
    }
    if (inCode) {
      html.push(`${escapeHtml(line)}\n`);
      continue;
    }
    if (!trimmed) {
      if (inList) {
        html.push("</ul>");
        inList = false;
      }
      continue;
    }
    if (trimmed.startsWith("- ")) {
      if (!inList) {
        html.push("<ul>");
        inList = true;
      }
      html.push(`<li>${inline(trimmed.slice(2))}</li>`);
      continue;
    }
    if (inList) {
      html.push("</ul>");
      inList = false;
    }
    if (trimmed.startsWith("### ")) {
      html.push(`<h3>${inline(trimmed.slice(4))}</h3>`);
      continue;
    }
    if (trimmed.startsWith("## ")) {
      html.push(`<h2>${inline(trimmed.slice(3))}</h2>`);
      continue;
    }
    if (trimmed.startsWith("# ")) {
      html.push(`<h1>${inline(trimmed.slice(2))}</h1>`);
      continue;
    }
    html.push(`<p>${inline(trimmed)}</p>`);
  }

  if (inList) html.push("</ul>");
  if (inCode) html.push("</code></pre>");
  return html.join("\n");
}

function renderReportPreview() {
  reportPreview.innerHTML = markdownToHtml(reportEditor.value || "");
}

function setReportViewMode(mode) {
  reportViewMode = mode === "preview" ? "preview" : "edit";
  const isPreview = reportViewMode === "preview";
  reportEditor.classList.toggle("hidden", isPreview);
  reportPreview.classList.toggle("hidden", !isPreview);
  reportEditViewBtn.classList.toggle("active", !isPreview);
  reportPreviewViewBtn.classList.toggle("active", isPreview);
  localStorage.setItem("copilot:report-view-mode", reportViewMode);
  if (isPreview) renderReportPreview();
}

function setFollowups(items) {
  followupsEl.innerHTML = "";
  if (!items.length) {
    const li = document.createElement("li");
    li.textContent = "No pending follow-ups.";
    followupsEl.appendChild(li);
    return;
  }
  for (const item of items) {
    const li = document.createElement("li");
    li.textContent = item;
    followupsEl.appendChild(li);
  }
}

function renderTasks(tasks) {
  const mode = tableSort?.value || "status";
  const sorted = [...tasks];
  if (mode === "status") {
    const rank = { blocked: 0, in_progress: 1, on_hold: 2, follow_up: 3, not_started: 4, done: 5 };
    sorted.sort((a, b) => {
      const ra = rank[a.status] ?? 9;
      const rb = rank[b.status] ?? 9;
      if (ra !== rb) return ra - rb;
      return String(a.task_name || "").localeCompare(String(b.task_name || ""));
    });
  } else if (mode === "updated") {
    sorted.sort((a, b) => (Date.parse(b.last_updated_at || 0) || 0) - (Date.parse(a.last_updated_at || 0) || 0));
  } else {
    sorted.sort((a, b) => {
      const oa = Number(a.order_index || 0);
      const ob = Number(b.order_index || 0);
      if (oa !== ob) return oa - ob;
      return String(a.task_name || "").localeCompare(String(b.task_name || ""));
    });
  }

  const lightEmoji = { green: "🟢", yellow: "🟡", red: "🔴" };
  const statusEmoji = { done: "✅", in_progress: "🚧", on_hold: "⏸️", follow_up: "🔁", blocked: "⛔", not_started: "⚪" };
  statusTableBody.innerHTML = "";
  if (!sorted.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = "<td colspan='7'>No tasks yet. Click initialize first.</td>";
    statusTableBody.appendChild(tr);
    return;
  }

  for (const [index, task] of sorted.entries()) {
    const tr = document.createElement("tr");
    const definition = task.definition || task.task_name || "-";
    const latestUpdateDate = task.last_updated_at
      ? new Date(task.last_updated_at).toLocaleDateString(undefined, { month: "short", day: "numeric" })
      : "-";
    const eta = prettyEta(task.eta);
    const latestUpdate = task.latest_update || "-";
    const light = `${lightEmoji[task.traffic_light] || ""}`.trim() || "-";
    const status = `${statusEmoji[task.status] || ""} ${prettyStatus(task.status)}`.trim();
    const statusCell = `${escapeHtml(status)}<div class="status-meta-line">Updated: ${escapeHtml(latestUpdateDate)}</div>`;
    tr.innerHTML = `
      <td>${index + 1}</td>
      <td>${renderTaskCell(task)}</td>
      <td>${escapeHtml(definition)}</td>
      <td>${light}</td>
      <td>${statusCell}</td>
      <td>${escapeHtml(eta)}</td>
      <td>${renderLatestUpdateCell(latestUpdate)}</td>
    `;
    statusTableBody.appendChild(tr);
  }
}

async function refreshFollowups() {
  const res = await fetch("/api/followups");
  const data = await res.json();
  setFollowups(data.follow_ups || []);
}

async function refreshTasks() {
  const res = await fetch("/api/tasks");
  const data = await res.json();
  renderTasks(data.tasks || []);
  statusSprintName.textContent = `Sprint: ${data.sprint_name || "-"}`;
  statusMeta.textContent = `Last refreshed: ${new Date().toLocaleTimeString()}`;
}

async function refreshPlan() {
  const res = await fetch("/api/plan");
  const data = await res.json();
  if (!data.exists) {
    planContent.textContent = "No sprint_plan.md found yet.";
    return;
  }
  planContent.textContent = data.content || "";
}

async function refreshAll() {
  await Promise.all([refreshFollowups(), refreshTasks(), refreshPlan(), refreshInitializeGuard()]);
}

async function refreshInitializeGuard() {
  try {
    const res = await fetch("/api/initialize/status");
    const data = await res.json();
    if (!res.ok) return;
    const hasWarning = Boolean(data.warning);
    initWarningBadge.classList.toggle("hidden", !hasWarning);
    undoInitBtn.disabled = !Boolean(data.undo_available);
    const steps = Number(data.undo_steps_available || 0);
    undoInitBtn.textContent = steps > 0 ? `Undo Initialize (${steps})` : "Undo Initialize";
  } catch (error) {
    undoInitBtn.disabled = true;
  }
}

function setActiveView(view) {
  currentView = view === "report" ? "report" : "dashboard";
  const dashboardActive = currentView === "dashboard";
  dashboardView.classList.toggle("active", dashboardActive);
  reportView.classList.toggle("active", !dashboardActive);
  dashboardTabBtn.classList.toggle("active", dashboardActive);
  reportTabBtn.classList.toggle("active", !dashboardActive);
  localStorage.setItem("copilot:active-view", currentView);
}

async function loadReport() {
  setReportStatus("Loading report", true);
  try {
    const res = await fetch("/api/report");
    const data = await res.json();
    if (!res.ok) {
      setReportStatus("Error", false);
      reportMeta.textContent = data.detail || "Failed to load report.";
      return;
    }
    reportEditor.value = data.content || "";
    renderReportPreview();
    reportMeta.textContent = data.exists
      ? `Loaded from: ${data.path}`
      : "No report exists yet. Click Generate Report.";
    setReportStatus("Report ready", false);
  } catch (error) {
    setReportStatus("Error", false);
    reportMeta.textContent = "Failed to load report due to network error.";
  }
}

function setChatMode(mode) {
  const resolved = CHAT_MODES.includes(mode) ? mode : "auto";
  currentChatMode = resolved;
  chatModeLabel.textContent = `Mode: ${CHAT_MODE_NAMES[resolved]}`;
  localStorage.setItem("copilot:chat-mode", resolved);
}

function closeChatModeMenu() {
  chatModeMenu.classList.remove("open");
}

sendBtn.addEventListener("click", async () => {
  const text = message.value.trim();
  if (!text) return;
  appendLine("you", text);
  message.value = "";
  setChatControlsDisabled(true);
  setChatStatus("Thinking", true);
  try {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: text, mode: currentChatMode }),
    });
    const data = await res.json();
    if (!res.ok) {
      appendLine("copilot", data.detail || "Something went wrong.");
      setChatStatus("Error", false);
      return;
    }
    await appendAnimated(formatCopilotLabel(data.confidence), data.answer || "No response.");
    if (Array.isArray(data.changed_tasks) && data.changed_tasks.length) {
      const lightEmoji = { green: "🟢", yellow: "🟡", red: "🔴" };
      for (const task of data.changed_tasks) {
        appendLine("system", `- ${task.task_name}: ${prettyStatus(task.status)} ${lightEmoji[task.traffic_light] || ""}`.trim());
      }
    }
    setFollowups(data.follow_ups || []);
    await refreshTasks();
    setChatStatus("Ready", false);
  } catch (error) {
    appendLine("copilot", "Network error. Please try again.");
    setChatStatus("Error", false);
  } finally {
    setChatControlsDisabled(false);
  }
});

message.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendBtn.click();
  }
});

initBtn.addEventListener("click", async () => {
  const mode = initModeSelect.value === "destructive" ? "destructive" : "sync_missing";
  let confirmText = "";
  if (mode === "destructive") {
    const entered = window.prompt(
      "Destructive initialize will replace current sprint state.\nType RESET to continue."
    );
    if (entered === null) return;
    confirmText = String(entered || "").trim();
    if (confirmText !== "RESET") {
      appendLine("system", "Destructive initialize cancelled.");
      return;
    }
  }
  initBtn.disabled = true;
  initModeSelect.disabled = true;
  undoInitBtn.disabled = true;
  setChatStatus("Initializing", true);
  try {
    const res = await fetch("/api/initialize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode, confirm_text: confirmText }),
    });
    const data = await res.json();
    if (!res.ok) {
      appendLine("copilot", `Initialize failed: ${data.detail || "unknown error"}`);
      setChatStatus("Error", false);
      return;
    }
    const modeLabel = data.mode === "destructive" ? "Destructive" : "Safe";
    const added = typeof data.added_count === "number" ? ` • Added: ${data.added_count}` : "";
    appendLine("system", `${modeLabel} initialize completed. Tasks: ${data.task_count}${added}`);
    await refreshAll();
    setChatStatus("Ready", false);
  } catch (error) {
    appendLine("copilot", "Initialize failed due to network error.");
    setChatStatus("Error", false);
  } finally {
    initBtn.disabled = false;
    initModeSelect.disabled = false;
    refreshInitializeGuard();
  }
});

undoInitBtn.addEventListener("click", async () => {
  undoInitBtn.disabled = true;
  setChatStatus("Restoring initialize backup", true);
  try {
    const res = await fetch("/api/initialize/undo", { method: "POST" });
    const data = await res.json();
    if (!res.ok) {
      appendLine("copilot", `Undo failed: ${data.detail || "unknown error"}`);
      setChatStatus("Error", false);
      return;
    }
    appendLine("system", `Initialize undo restored. Tasks: ${data.task_count}`);
    await refreshAll();
    setChatStatus("Ready", false);
  } catch (error) {
    appendLine("copilot", "Undo failed due to network error.");
    setChatStatus("Error", false);
  } finally {
    refreshInitializeGuard();
  }
});

tableBtn.addEventListener("click", async () => {
  tableBtn.disabled = true;
  setChatStatus("Generating table", true);
  try {
    const res = await fetch("/api/generate-table", { method: "POST" });
    const data = await res.json();
    if (!res.ok) {
      appendLine("copilot", `Table generation failed: ${data.detail || "unknown error"}`);
      setChatStatus("Error", false);
      return;
    }
    appendLine("system", `Table generated at: ${data.path}`);
    await refreshTasks();
    setChatStatus("Ready", false);
  } catch (error) {
    appendLine("copilot", "Table generation failed due to network error.");
    setChatStatus("Error", false);
  } finally {
    tableBtn.disabled = false;
  }
});

importDocxBtn.addEventListener("click", async () => {
  const path = docxPath.value.trim();
  if (!path) {
    appendLine("copilot", "Please provide a DOCX file path first.");
    return;
  }
  appendLine("system", `Importing DOCX: ${path}`);
  importDocxBtn.disabled = true;
  docxPath.disabled = true;
  setChatStatus("Importing DOCX", true);
  try {
    const res = await fetch("/api/import-docx-plan", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ file_path: path, auto_initialize: true }),
    });
    const data = await res.json();
    if (!res.ok) {
      appendLine("copilot", `Import failed: ${data.detail || "unknown error"}`);
      setChatStatus("Error", false);
      return;
    }
    const mergeInfo = (data.added_count > 0 || data.synced_count > 0) ? ` • Added: ${data.added_count} • Synced: ${data.synced_count}` : "";
    appendLine("copilot", `Imported with ${data.source}. Sprint: ${data.sprint_name || "-"} • Tasks loaded: ${data.task_count}${mergeInfo}`);
    await refreshAll();
    setChatStatus("Ready", false);
  } catch (error) {
    appendLine("copilot", "Import failed due to network error.");
    setChatStatus("Error", false);
  } finally {
    importDocxBtn.disabled = false;
    docxPath.disabled = false;
  }
});

importToggleBtn.addEventListener("click", () => {
  const isHidden = importControls.classList.toggle("hidden");
  importToggleBtn.textContent = isHidden ? "Show Import" : "Hide Import";
});

dashboardTabBtn.addEventListener("click", () => {
  setActiveView("dashboard");
});

reportTabBtn.addEventListener("click", async () => {
  setActiveView("report");
  await loadReport();
});

reportEditViewBtn.addEventListener("click", () => {
  setReportViewMode("edit");
});

reportPreviewViewBtn.addEventListener("click", () => {
  setReportViewMode("preview");
});

reportEditor.addEventListener("input", () => {
  if (reportViewMode === "preview") renderReportPreview();
});

generateReportBtn.addEventListener("click", async () => {
  setReportControlsDisabled(true);
  setReportStatus("Generating report", true);
  try {
    const res = await fetch("/api/report/generate", { method: "POST" });
    const data = await res.json();
    if (!res.ok) {
      setReportStatus("Error", false);
      reportMeta.textContent = data.detail || "Report generation failed.";
      return;
    }
    reportEditor.value = data.content || "";
    renderReportPreview();
    reportMeta.textContent = `Generated and saved: ${data.path}`;
    setReportStatus("Report generated", false);
  } catch (error) {
    setReportStatus("Error", false);
    reportMeta.textContent = "Report generation failed due to network error.";
  } finally {
    setReportControlsDisabled(false);
  }
});

saveReportBtn.addEventListener("click", async () => {
  setReportControlsDisabled(true);
  setReportStatus("Saving report", true);
  try {
    const res = await fetch("/api/report/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content: reportEditor.value || "" }),
    });
    const data = await res.json();
    if (!res.ok) {
      setReportStatus("Error", false);
      reportMeta.textContent = data.detail || "Failed to save report.";
      return;
    }
    reportMeta.textContent = `Saved to: ${data.path} • Content length: ${data.content_length}`;
    setReportStatus("Report saved", false);
  } catch (error) {
    setReportStatus("Error", false);
    reportMeta.textContent = "Failed to save report due to network error.";
  } finally {
    setReportControlsDisabled(false);
  }
});

const savedSort = localStorage.getItem("copilot:table-sort");
if (savedSort && ["status", "plan", "updated"].includes(savedSort)) tableSort.value = savedSort;
tableSort.addEventListener("change", async () => {
  localStorage.setItem("copilot:table-sort", tableSort.value);
  await refreshTasks();
});

const savedMode = localStorage.getItem("copilot:chat-mode");
setChatMode(savedMode || "auto");
const savedView = localStorage.getItem("copilot:active-view");
setActiveView(savedView || "dashboard");
const savedReportMode = localStorage.getItem("copilot:report-view-mode");
setReportViewMode(savedReportMode || "edit");
chatModeBtn.addEventListener("click", (event) => {
  event.stopPropagation();
  chatModeMenu.classList.toggle("open");
});
chatModeMenu.querySelectorAll(".mode-option").forEach((el) => {
  el.addEventListener("click", () => {
    setChatMode(el.dataset.mode || "auto");
    closeChatModeMenu();
  });
});
document.addEventListener("click", (event) => {
  if (!chatModeMenu.classList.contains("open")) return;
  if (chatModeMenu.contains(event.target) || chatModeBtn.contains(event.target)) return;
  closeChatModeMenu();
});

setChatStatus("Ready", false);
setReportStatus("Report ready", false);
refreshAll();
if (currentView === "report") {
  loadReport();
}
setInterval(refreshTasks, 3000);
