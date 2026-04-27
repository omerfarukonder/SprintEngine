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
const faqListEl = document.getElementById("faqList");
const faqPathHint = document.getElementById("faqPathHint");
const planContent = document.getElementById("planContent");
const statusTableBody = document.getElementById("statusTableBody");
const statusMeta = document.getElementById("statusMeta");
const statusSprintName = document.getElementById("statusSprintName");
const activityDashboardBtn = document.getElementById("activityDashboardBtn");
const activityGanttBtn = document.getElementById("activityGanttBtn");
const activityReportBtn = document.getElementById("activityReportBtn");
const activityMeetingsBtn = document.getElementById("activityMeetingsBtn");
const activityKnowledgeBtn = document.getElementById("activityKnowledgeBtn");
const activityContentBtn = document.getElementById("activityContentBtn");
const activityPrdBtn = document.getElementById("activityPrdBtn");
const dashboardView = document.getElementById("dashboardView");
const ganttView = document.getElementById("ganttView");
const reportView = document.getElementById("reportView");
const meetingsView = document.getElementById("meetingsView");
const knowledgeView = document.getElementById("knowledgeView");
const contentView = document.getElementById("contentView");
const prdView = document.getElementById("prdView");
const ganttBoard = document.getElementById("ganttBoard");
const ganttMeta = document.getElementById("ganttMeta");
const meetingNameInput = document.getElementById("meetingNameInput");
const meetingCaptureMode = document.getElementById("meetingCaptureMode");
const meetingRecordBtn = document.getElementById("meetingRecordBtn");
const meetingStopBtn = document.getElementById("meetingStopBtn");
const meetingsStatus = document.getElementById("meetingsStatus");
const meetingTranscript = document.getElementById("meetingTranscript");
const meetingSummary = document.getElementById("meetingSummary");
const meetingsList = document.getElementById("meetingsList");
const meetingWaveCanvas = document.getElementById("meetingWaveCanvas");
const meetingWaveLevel = document.getElementById("meetingWaveLevel");
const generateReportBtn = document.getElementById("generateReportBtn");
const saveReportBtn = document.getElementById("saveReportBtn");
const reportEditor = document.getElementById("reportEditor");
const reportPreview = document.getElementById("reportPreview");
const reportEditViewBtn = document.getElementById("reportEditViewBtn");
const reportPreviewViewBtn = document.getElementById("reportPreviewViewBtn");
const reportStatus = document.getElementById("reportStatus");
const reportMeta = document.getElementById("reportMeta");
const appRoot = document.getElementById("appRoot");
const activityToggleBtn = document.getElementById("activityToggleBtn");
const appBody = document.getElementById("appBody");
const egRefreshBtn = document.getElementById("egRefreshBtn");
const egRebuildBtn = document.getElementById("egRebuildBtn");
const egSynthesizeBtn = document.getElementById("egSynthesizeBtn");
const egRefineBtn = document.getElementById("egRefineBtn");
const egTypeFilter = document.getElementById("egTypeFilter");
const egSearchInput = document.getElementById("egSearchInput");
const egStats = document.getElementById("egStats");
const egGraphSvg = document.getElementById("egGraphSvg");
const egGraphEmpty = document.getElementById("egGraphEmpty");
const egDetailMeta = document.getElementById("egDetailMeta");
const egDetailName = document.getElementById("egDetailName");
const egDetailType = document.getElementById("egDetailType");
const egDetailTypeBadge = document.getElementById("egDetailTypeBadge");
const egDetailDesc = document.getElementById("egDetailDesc");
const egDetailFacts = document.getElementById("egDetailFacts");
const egDetailRelations = document.getElementById("egDetailRelations");

let activeTypingToken = 0;
const CHAT_MODES = ["auto", "query", "update", "faq"];
const CHAT_MODE_NAMES = {
  auto: "Auto",
  query: "Explain-only",
  update: "Update-only",
  faq: "FAQ",
};
let currentChatMode = "auto";
let currentView = "dashboard";
let reportViewMode = "edit";
let activeGanttDrag = null;
let egRawData = { nodes: [], edges: [], stats: {} };
let egSelectedId = "";
const egNodePositions = new Map();
let egPan = { x: 0, y: 0 };
let egScale = 1;
let egPanDrag = null;

function formatCopilotLabel(confidence) {
  const numeric = Number(confidence);
  const bounded = Number.isFinite(numeric) ? Math.max(0, Math.min(1, numeric)) : 0.6;
  return `Engine (${Math.round(bounded * 100)}%):`;
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
  else if (role === "copilot") roleSpan.textContent = "Engine:";
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
  const parseTableCells = (line) => {
    const raw = String(line || "").trim().replace(/^\|/, "").replace(/\|$/, "");
    const cells = [];
    let current = "";
    let escaped = false;
    for (const ch of raw) {
      if (escaped) {
        current += ch;
        escaped = false;
        continue;
      }
      if (ch === "\\") {
        escaped = true;
        continue;
      }
      if (ch === "|") {
        cells.push(current.trim());
        current = "";
        continue;
      }
      current += ch;
    }
    cells.push(current.trim());
    return cells.map((cell) => cell.replace(/\\\|/g, "|"));
  };
  const isTableDivider = (line) => {
    const cells = parseTableCells(line);
    if (!cells.length) return false;
    return cells.every((cell) => /^:?-{3,}:?$/.test(cell));
  };
  const statusClass = (value) => {
    const text = String(value || "").toLowerCase();
    if (text.includes("blocked") || text.includes("🔴")) return "report-status--red";
    if (text.includes("done") || text.includes("completed") || text.includes("🟢")) return "report-status--green";
    if (text.includes("in progress") || text.includes("🟡")) return "report-status--yellow";
    if (text.includes("on hold") || text.includes("not started") || text.includes("⚪")) return "report-status--gray";
    return "report-status--neutral";
  };

  for (let i = 0; i < lines.length; i += 1) {
    const raw = lines[i];
    const line = raw ?? "";
    const trimmed = line.trim();
    const nextTrimmed = String(lines[i + 1] || "").trim();
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
    if (trimmed.startsWith("|") && nextTrimmed.startsWith("|") && isTableDivider(nextTrimmed)) {
      if (inList) {
        html.push("</ul>");
        inList = false;
      }
      const headerCells = parseTableCells(trimmed);
      const normalizedHeaders = headerCells.map((h) => String(h || "").trim().toLowerCase());
      i += 1; // skip divider line
      const rowLines = [];
      while (i + 1 < lines.length) {
        const candidate = String(lines[i + 1] || "").trim();
        if (!candidate.startsWith("|")) break;
        rowLines.push(candidate);
        i += 1;
      }
      const thead = `<thead><tr>${headerCells.map((cell) => `<th>${inline(cell)}</th>`).join("")}</tr></thead>`;
      const bodyRows = rowLines
        .map((row) => {
          const cells = parseTableCells(row);
          const tds = cells.map((cell, idx) => {
            const header = normalizedHeaders[idx] || "";
            if (header === "status") {
              const klass = statusClass(cell);
              return `<td><span class="report-status ${klass}">${inline(cell)}</span></td>`;
            }
            return `<td>${inline(cell)}</td>`;
          });
          return `<tr>${tds.join("")}</tr>`;
        })
        .join("");
      html.push(`<table class="report-markdown-table">${thead}<tbody>${bodyRows}</tbody></table>`);
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

function renderFaqPanel(data) {
  if (!faqListEl) return;
  faqListEl.innerHTML = "";
  if (faqPathHint && data && data.markdown_path) {
    faqPathHint.textContent = `Stored in ${data.markdown_path}`;
  }
  const active = (data && data.active) || [];
  if (!active.length) {
    const p = document.createElement("p");
    p.className = "muted";
    p.style.margin = "0";
    p.textContent = "No FAQs yet. Switch to FAQ mode, then add questions or use add this question: …";
    faqListEl.appendChild(p);
    return;
  }
  for (const row of active) {
    const div = document.createElement("div");
    div.className = "faq-block";
    const n = row.n != null ? row.n : "?";
    const q = escapeHtml(row.question || "");
    const a = String(row.answer || "").trim();
    const aHtml = a
      ? `<div class="faq-a"><strong>A:</strong> ${escapeHtml(a)}</div>`
      : `<div class="faq-a muted"><strong>A:</strong> (no answer yet — use: for q${n}, the answer is …)</div>`;
    div.innerHTML = `<div><span class="faq-n">Q${n}</span><span class="faq-q"><strong>Q:</strong> ${q}</span></div>${aHtml}`;
    faqListEl.appendChild(div);
  }
}

// ── Entity graph helpers ──────────────────────────────────────────────────────

const EG_COLORS = {
  team: "#60a5fa", domain: "#c084fc", topic: "#2dd4bf", system: "#fb923c",
  person: "#f472b6", process: "#a3e635", constraint: "#fbbf24",
};
const EG_REL_STYLES = {
  has_subtopic:      { color: "#c084fc", width: 1.8, dash: "" },
  owns:              { color: "#60a5fa", width: 2.0, dash: "" },
  depends_on:        { color: "#fb923c", width: 1.6, dash: "6 3" },
  has_constraint:    { color: "#fbbf24", width: 1.4, dash: "4 3" },
  related_to:        { color: "#6b7280", width: 1.0, dash: "3 3" },
  part_of:           { color: "#2dd4bf", width: 1.6, dash: "" },
  communicates_with: { color: "#60a5fa", width: 1.2, dash: "3 3" },
  blocks:            { color: "#ef4444", width: 2.0, dash: "" },
};

function egHash(text) {
  let h = 0;
  for (const ch of String(text || "")) { h = (h << 5) - h + ch.charCodeAt(0); h |= 0; }
  return Math.abs(h);
}

function egEnsureLayout(nodes, edges) {
  const W = 1000, H = 560;
  const map = new Map();
  for (const n of nodes) {
    let p = egNodePositions.get(n.id);
    if (!p) {
      const s = egHash(n.id + (n.entity_type || ""));
      p = { x: 80 + (s % 840), y: 60 + ((s * 7) % 440) };
      egNodePositions.set(n.id, p);
    }
    map.set(n.id, p);
  }
  if (!nodes.length) return;
  for (let iter = 0; iter < 120; iter++) {
    const f = new Map(nodes.map((n) => [n.id, { x: 0, y: 0 }]));
    for (let i = 0; i < nodes.length; i++) {
      const pa = map.get(nodes[i].id);
      for (let j = i + 1; j < nodes.length; j++) {
        const pb = map.get(nodes[j].id);
        const dx = pa.x - pb.x, dy = pa.y - pb.y;
        const d2 = Math.max(40, dx * dx + dy * dy);
        const rep = 5000 / d2;
        const fx = (dx / Math.sqrt(d2)) * rep, fy = (dy / Math.sqrt(d2)) * rep;
        f.get(nodes[i].id).x += fx; f.get(nodes[i].id).y += fy;
        f.get(nodes[j].id).x -= fx; f.get(nodes[j].id).y -= fy;
      }
    }
    for (const e of edges) {
      const pa = map.get(e.source), pb = map.get(e.target);
      if (!pa || !pb) continue;
      const dx = pb.x - pa.x, dy = pb.y - pa.y;
      const d = Math.max(1, Math.sqrt(dx * dx + dy * dy));
      const spring = (d - 100) * 0.025;
      const fx = (dx / d) * spring, fy = (dy / d) * spring;
      f.get(e.source).x += fx; f.get(e.source).y += fy;
      f.get(e.target).x -= fx; f.get(e.target).y -= fy;
    }
    for (const n of nodes) {
      const p = map.get(n.id), ff = f.get(n.id);
      const gx = (W / 2 - p.x) * 0.002, gy = (H / 2 - p.y) * 0.002;
      p.x = Math.max(30, Math.min(W - 30, p.x + (ff.x + gx) * 0.25));
      p.y = Math.max(30, Math.min(H - 30, p.y + (ff.y + gy) * 0.25));
    }
  }
}

function egRenderDetail() {
  if (!egDetailMeta) return;
  const nodes = egRawData.nodes || [];
  const edges = egRawData.edges || [];
  const node = nodes.find((n) => n.id === egSelectedId);
  if (!node) {
    egDetailMeta.textContent = "Click a node to inspect.";
    if (egDetailName) egDetailName.style.display = "none";
    if (egDetailType) egDetailType.style.display = "none";
    if (egDetailDesc) egDetailDesc.textContent = "";
    if (egDetailFacts) egDetailFacts.innerHTML = "";
    if (egDetailRelations) egDetailRelations.innerHTML = "";
    return;
  }
  const etype = String(node.entity_type || "topic");
  const color = EG_COLORS[etype] || "#d1d5db";
  egDetailMeta.textContent = `Last updated: ${String(node.last_updated || "").slice(0, 10)}`;
  if (egDetailName) { egDetailName.textContent = node.name || "—"; egDetailName.style.display = "block"; }
  if (egDetailType && egDetailTypeBadge) {
    egDetailTypeBadge.textContent = etype;
    egDetailTypeBadge.style.background = color + "22";
    egDetailTypeBadge.style.color = color;
    egDetailTypeBadge.style.border = `1px solid ${color}44`;
    egDetailType.style.display = "block";
  }
  if (egDetailDesc) egDetailDesc.textContent = node.description || "—";
  if (egDetailFacts) {
    const facts = Array.isArray(node.facts) ? node.facts : [];
    egDetailFacts.innerHTML = facts.length
      ? `<div style="font-size:10px;color:#6b7280;text-transform:uppercase;margin-bottom:4px;">Facts</div>` +
        facts.map((f) => `<div style="font-size:12px;color:#d1d5db;padding:2px 0;border-bottom:1px solid #1e1e1e;">• ${escapeHtml(f)}</div>`).join("")
      : "";
  }
  if (egDetailRelations) {
    const rels = edges.filter((e) => e.source === node.id || e.target === node.id).slice(0, 15);
    if (rels.length) {
      const items = rels.map((e) => {
        const isSource = e.source === node.id;
        const otherId = isSource ? e.target : e.source;
        const other = nodes.find((n) => n.id === otherId);
        const otherName = other ? other.name : otherId;
        const rtype = e.relation_type || "related_to";
        const rcolor = (EG_REL_STYLES[rtype] || {}).color || "#6b7280";
        const arrow = isSource ? "→" : "←";
        const label = e.label || rtype.replace(/_/g, " ");
        return `<div><span style="color:${rcolor};font-size:10px;">${escapeHtml(label)}</span> ${arrow} <span style="color:#d1d5db;">${escapeHtml(otherName)}</span></div>`;
      });
      egDetailRelations.innerHTML =
        `<div style="font-size:10px;color:#6b7280;text-transform:uppercase;margin-bottom:4px;">Relations (${rels.length})</div>` +
        items.join("");
    } else {
      egDetailRelations.innerHTML = `<span style="color:#4b5563;font-size:11px;">No relations yet.</span>`;
    }
  }
}

function renderEntityGraph() {
  if (!egGraphSvg) return;
  const nodes = egRawData.nodes || [];
  const edges = egRawData.edges || [];

  const typeFilter = egTypeFilter?.value || "all";
  const search = String(egSearchInput?.value || "").trim().toLowerCase();
  const filtered = nodes.filter((n) => {
    if (typeFilter !== "all" && n.entity_type !== typeFilter) return false;
    if (search && !String(n.name || "").toLowerCase().includes(search) &&
        !String(n.description || "").toLowerCase().includes(search)) return false;
    return true;
  });
  const idSet = new Set(filtered.map((n) => n.id));
  const filteredEdges = edges.filter((e) => idSet.has(e.source) && idSet.has(e.target));

  const stats = egRawData.stats || {};
  if (egStats) {
    egStats.textContent = `${filtered.length} entities • ${filteredEdges.length} relations` +
      (stats.total_entities ? ` (${stats.total_entities} total)` : "");
  }
  const showEmpty = filtered.length === 0;
  egGraphEmpty?.classList.toggle("hidden", !showEmpty);
  if (showEmpty) { egGraphSvg.innerHTML = ""; egRenderDetail(); return; }

  egEnsureLayout(filtered, filteredEdges);

  const selNeighbors = new Set();
  if (egSelectedId) {
    selNeighbors.add(egSelectedId);
    filteredEdges.forEach((e) => {
      if (e.source === egSelectedId) selNeighbors.add(e.target);
      if (e.target === egSelectedId) selNeighbors.add(e.source);
    });
  }
  const hasSel = egSelectedId !== "";

  const edgeEls = filteredEdges.map((e) => {
    const a = egNodePositions.get(e.source), b = egNodePositions.get(e.target);
    if (!a || !b) return "";
    const st = EG_REL_STYLES[e.relation_type] || EG_REL_STYLES.related_to;
    const isConn = e.source === egSelectedId || e.target === egSelectedId;
    const op = hasSel ? (isConn ? 0.9 : 0.06) : 0.65;
    const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
    const label = (e.relation_type || "").replace(/_/g, " ");
    const labelOp = hasSel ? (isConn ? 0.85 : 0.0) : 0.4;
    return `<line x1="${a.x}" y1="${a.y}" x2="${b.x}" y2="${b.y}" stroke="${st.color}" stroke-width="${st.width}" stroke-dasharray="${st.dash}" opacity="${op}" />` +
      (labelOp > 0 ? `<text x="${mx}" y="${my - 3}" text-anchor="middle" font-size="6" fill="${st.color}" opacity="${labelOp}" pointer-events="none">${escapeHtml(label)}</text>` : "");
  }).join("");

  const nodeEls = filtered.map((n) => {
    const p = egNodePositions.get(n.id);
    if (!p) return "";
    const deg = n.degree || 0;
    const r = 8 + Math.min(10, deg * 2);
    const color = EG_COLORS[n.entity_type] || "#d1d5db";
    const dimmed = hasSel && !selNeighbors.has(n.id);
    const sel = n.id === egSelectedId;
    const op = dimmed ? 0.1 : 0.9;
    const labelOp = dimmed ? 0.05 : (sel ? 1.0 : 0.8);
    const stroke = sel ? "#ffffff" : "#0f0f10";
    const sw = sel ? 2.5 : 1.2;
    const name = String(n.name || "");
    const shortName = name.length > 20 ? name.slice(0, 19) + "…" : name;
    const title = escapeHtml(`${n.entity_type || "?"}\n${n.name}\n${n.description || ""}`);
    return `<g class="kb-node" data-id="${escapeHtml(n.id)}">` +
      `<circle cx="${p.x}" cy="${p.y}" r="${r}" fill="${color}" opacity="${op}" stroke="${stroke}" stroke-width="${sw}" />` +
      `<text x="${p.x}" y="${p.y + 3}" text-anchor="middle" font-size="8" font-weight="700" fill="#ffffff" stroke="#000" stroke-width="2.5" paint-order="stroke" opacity="${labelOp}" pointer-events="none" style="user-select:none">${escapeHtml(shortName)}</text>` +
      `<text x="${p.x}" y="${p.y + r + 10}" text-anchor="middle" font-size="6.5" fill="#e2e2e2" stroke="#111" stroke-width="2" paint-order="stroke" opacity="${labelOp * 0.85}" pointer-events="none" style="user-select:none">${escapeHtml(n.entity_type || "")}</text>` +
      `<title>${title}</title></g>`;
  }).join("");

  egGraphSvg.innerHTML = `<g transform="translate(${egPan.x} ${egPan.y}) scale(${egScale})">${edgeEls}${nodeEls}</g>`;
  egRenderDetail();
}

async function refreshEntityGraph() {
  if (!egGraphSvg) return;
  if (egStats) egStats.textContent = "Loading entity graph...";
  try {
    const qs = new URLSearchParams({ active_only: "true" });
    const res = await fetch(`/api/memory/entity-graph?${qs.toString()}`);
    const data = await res.json();
    if (!res.ok) {
      egRawData = { nodes: [], edges: [], stats: {} };
      if (egStats) egStats.textContent = data.detail || "Failed to load.";
      renderEntityGraph();
      return;
    }
    egRawData = {
      nodes: Array.isArray(data.nodes) ? data.nodes : [],
      edges: Array.isArray(data.edges) ? data.edges : [],
      stats: data.stats || {},
    };
    renderEntityGraph();
  } catch {
    egRawData = { nodes: [], edges: [], stats: {} };
    if (egStats) egStats.textContent = "Network error.";
    renderEntityGraph();
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

async function refreshFaq() {
  try {
    const res = await fetch("/api/faq");
    const data = await res.json();
    if (!res.ok) return;
    renderFaqPanel(data);
  } catch {
    if (faqListEl) {
      faqListEl.innerHTML = "";
      const p = document.createElement("p");
      p.className = "muted";
      p.textContent = "Could not load FAQ.";
      faqListEl.appendChild(p);
    }
  }
}

async function refreshTasks() {
  const res = await fetch("/api/tasks");
  const data = await res.json();
  renderTasks(data.tasks || []);
  statusSprintName.textContent = `Sprint: ${data.sprint_name || "-"}`;
  statusMeta.textContent = `Last refreshed: ${new Date().toLocaleTimeString()}`;
  if (currentView === "gantt") {
    await refreshGantt();
  }
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

function parseIsoDate(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const dt = new Date(`${raw}T00:00:00`);
  if (Number.isNaN(dt.getTime())) return null;
  return dt;
}

function dateToIso(d) {
  return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 10);
}

function dayIndexToIso(timelineStartIso, dayIndex) {
  const start = parseIsoDate(timelineStartIso);
  const dt = new Date(start.getTime() + Math.max(0, dayIndex) * 86400000);
  return dateToIso(dt);
}

function daysBetweenInclusive(a, b) {
  const ms = parseIsoDate(a).getTime() - parseIsoDate(b).getTime();
  return Math.round(ms / 86400000) + 1;
}

function ganttBarClass(status, uncertain) {
  const norm = String(status || "").trim().toLowerCase();
  const safe = norm.replaceAll("_", "_");
  return `gantt-bar status-${safe}${uncertain ? " uncertain" : ""}`;
}

async function updateGanttTaskDates(taskId, startDate, endDate) {
  const res = await fetch(`/api/gantt/tasks/${encodeURIComponent(taskId)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ start_date: startDate, end_date: endDate }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || "Failed to update task dates");
  return data;
}

function bindGanttBarDrag(barEl, task, timelineStartIso, totalDays, trackEl) {
  const startDt = parseIsoDate(task.start_date);
  const endDt = parseIsoDate(task.end_date);
  if (!startDt || !endDt) return;
  const originalStart = Math.round((startDt.getTime() - parseIsoDate(timelineStartIso).getTime()) / 86400000);
  const originalEnd = Math.round((endDt.getTime() - parseIsoDate(timelineStartIso).getTime()) / 86400000);
  const handles = barEl.querySelectorAll(".gantt-handle");
  const beginDrag = (mode, event) => {
    event.preventDefault();
    const rect = trackEl.getBoundingClientRect();
    const pxPerDay = rect.width / Math.max(1, totalDays);
    activeGanttDrag = {
      mode,
      startX: event.clientX,
      pxPerDay,
      originalStart,
      originalEnd,
      totalDays,
      barEl,
      taskId: task.task_id,
      timelineStartIso,
    };
    document.body.style.userSelect = "none";
  };
  handles.forEach((h) => {
    h.addEventListener("mousedown", (event) => beginDrag(h.classList.contains("start") ? "start" : "end", event));
  });
  barEl.addEventListener("mousedown", (event) => {
    if (event.target.classList.contains("gantt-handle")) return;
    beginDrag("move", event);
  });
}

function renderGantt(items, sprintName) {
  if (!ganttBoard) return;
  if (!Array.isArray(items) || !items.length) {
    ganttBoard.innerHTML = `<div class="gantt-empty">No gantt rows to show (on-hold tasks are excluded).</div>`;
    if (ganttMeta) ganttMeta.textContent = `Sprint: ${sprintName || "-"} • 0 rows`;
    return;
  }
  const parsed = items
    .map((t) => ({ ...t, _start: parseIsoDate(t.start_date), _end: parseIsoDate(t.end_date) }))
    .filter((t) => t._start && t._end);
  if (!parsed.length) {
    ganttBoard.innerHTML = `<div class="gantt-empty">No valid dated tasks to render.</div>`;
    return;
  }
  const minStart = new Date(Math.min(...parsed.map((t) => t._start.getTime())));
  const maxEnd = new Date(Math.max(...parsed.map((t) => t._end.getTime())));
  const timelineStartIso = dateToIso(minStart);
  const totalDays = Math.max(2, daysBetweenInclusive(dateToIso(maxEnd), timelineStartIso));

  const ticks = [];
  for (let i = 0; i <= totalDays; i += 7) {
    const d = new Date(minStart.getTime() + i * 86400000);
    const left = (i / totalDays) * 100;
    ticks.push(`<span class="gantt-tick" style="left:${left}%">${d.toLocaleDateString(undefined, { month: "short", day: "numeric" })}</span>`);
  }
  const rows = parsed.map((task) => {
    const startOffset = Math.max(0, Math.round((task._start.getTime() - minStart.getTime()) / 86400000));
    const lengthDays = Math.max(1, Math.round((task._end.getTime() - task._start.getTime()) / 86400000) + 1);
    const leftPct = (startOffset / totalDays) * 100;
    const widthPct = (lengthDays / totalDays) * 100;
    return `
      <div class="gantt-row" data-task-id="${escapeHtml(task.task_id)}">
        <div class="gantt-task-title">
          <span>${escapeHtml(task.task_name)}</span>
          <span class="gantt-task-meta">${escapeHtml(prettyStatus(task.status))} • ${escapeHtml(task.start_date)} → ${escapeHtml(task.end_date)}</span>
        </div>
        <div class="gantt-track" data-task-id="${escapeHtml(task.task_id)}">
          <div
            class="${ganttBarClass(task.status, Boolean(task.uncertain_end))}"
            data-task-id="${escapeHtml(task.task_id)}"
            data-start="${escapeHtml(task.start_date)}"
            data-end="${escapeHtml(task.end_date)}"
            style="left:${leftPct}%; width:${Math.max(2, widthPct)}%;"
            title="${escapeHtml(task.task_name)}"
          >
            <span class="gantt-handle start" title="Adjust start"></span>
            <span class="gantt-handle end" title="Adjust end"></span>
          </div>
        </div>
      </div>
    `;
  });
  ganttBoard.innerHTML = `
    <div class="gantt-grid">
      <div class="gantt-header">
        <div class="gantt-header-label">Task</div>
        <div class="gantt-ticks">${ticks.join("")}</div>
      </div>
      ${rows.join("")}
    </div>
  `;
  if (ganttMeta) {
    ganttMeta.textContent = `Sprint: ${sprintName || "-"} • ${parsed.length} tasks • day-level`;
  }
  parsed.forEach((task) => {
    const barEl = ganttBoard.querySelector(`.gantt-bar[data-task-id="${CSS.escape(task.task_id)}"]`);
    const trackEl = ganttBoard.querySelector(`.gantt-track[data-task-id="${CSS.escape(task.task_id)}"]`);
    if (barEl && trackEl) {
      bindGanttBarDrag(barEl, task, timelineStartIso, totalDays, trackEl);
    }
  });
}

async function refreshGantt() {
  if (!ganttBoard) return;
  try {
    const res = await fetch("/api/gantt/tasks");
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "Failed to load gantt");
    renderGantt(data.items || [], data.sprint_name || "-");
  } catch (error) {
    ganttBoard.innerHTML = `<div class="gantt-empty">${escapeHtml(error?.message || "Failed to load gantt view.")}</div>`;
  }
}

async function refreshAll() {
  await Promise.all([refreshFaq(), refreshTasks(), refreshPlan(), refreshInitializeGuard()]);
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
  const resolved =
    view === "report" ? "report"
    : view === "meetings" ? "meetings"
    : view === "gantt" ? "gantt"
    : view === "knowledge" ? "knowledge"
    : view === "content" ? "content"
    : view === "prd" ? "prd"
    : "dashboard";
  currentView = resolved;
  const dashboardActive = resolved === "dashboard";
  const ganttActive = resolved === "gantt";
  const reportActive = resolved === "report";
  const meetingsActive = resolved === "meetings";
  const knowledgeActive = resolved === "knowledge";
  const contentActive = resolved === "content";
  const prdActive = resolved === "prd";
  dashboardView.classList.toggle("active", dashboardActive);
  if (ganttView) ganttView.classList.toggle("active", ganttActive);
  reportView.classList.toggle("active", reportActive);
  if (meetingsView) meetingsView.classList.toggle("active", meetingsActive);
  if (knowledgeView) knowledgeView.classList.toggle("active", knowledgeActive);
  if (contentView) contentView.classList.toggle("active", contentActive);
  if (prdView) prdView.classList.toggle("active", prdActive);
  if (appBody) {
    appBody.classList.toggle("app-body--dashboard", dashboardActive);
  }
  if (activityDashboardBtn) {
    activityDashboardBtn.classList.toggle("is-active", dashboardActive);
    activityDashboardBtn.setAttribute("aria-current", dashboardActive ? "page" : "false");
  }
  if (activityGanttBtn) {
    activityGanttBtn.classList.toggle("is-active", ganttActive);
    activityGanttBtn.setAttribute("aria-current", ganttActive ? "page" : "false");
  }
  if (activityReportBtn) {
    activityReportBtn.classList.toggle("is-active", reportActive);
    activityReportBtn.setAttribute("aria-current", reportActive ? "page" : "false");
  }
  if (activityMeetingsBtn) {
    activityMeetingsBtn.classList.toggle("is-active", meetingsActive);
    activityMeetingsBtn.setAttribute("aria-current", meetingsActive ? "page" : "false");
  }
  if (activityKnowledgeBtn) {
    activityKnowledgeBtn.classList.toggle("is-active", knowledgeActive);
    activityKnowledgeBtn.setAttribute("aria-current", knowledgeActive ? "page" : "false");
  }
  if (activityContentBtn) {
    activityContentBtn.classList.toggle("is-active", contentActive);
    activityContentBtn.setAttribute("aria-current", contentActive ? "page" : "false");
  }
  if (activityPrdBtn) {
    activityPrdBtn.classList.toggle("is-active", prdActive);
    activityPrdBtn.setAttribute("aria-current", prdActive ? "page" : "false");
  }
  if (prdActive) loadPrds();
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
        appendLine("system", `Task update: ${task.task_name} — ${prettyStatus(task.status)} ${lightEmoji[task.traffic_light] || ""}`.trim());
      }
    }
    await refreshFaq();
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

if (activityDashboardBtn) {
  activityDashboardBtn.addEventListener("click", () => {
    setActiveView("dashboard");
  });
}
if (activityGanttBtn) {
  activityGanttBtn.addEventListener("click", async () => {
    setActiveView("gantt");
    await refreshGantt();
  });
}
if (activityReportBtn) {
  activityReportBtn.addEventListener("click", async () => {
    setActiveView("report");
    await loadReport();
  });
}
if (activityMeetingsBtn) {
  activityMeetingsBtn.addEventListener("click", async () => {
    setActiveView("meetings");
    await refreshMeetingsList();
  });
}
if (activityKnowledgeBtn) {
  activityKnowledgeBtn.addEventListener("click", async () => {
    setActiveView("knowledge");
    await refreshEntityGraph();
  });
}
if (activityContentBtn) {
  activityContentBtn.addEventListener("click", () => {
    setActiveView("content");
  });
}

// ── Content Generator ──────────────────────────────────────────────────────────
(function () {
  const cgGenerateBtn = document.getElementById("cgGenerateBtn");
  const cgClearBtn = document.getElementById("cgClearBtn");
  const cgCopyBtn = document.getElementById("cgCopyBtn");
  const cgSystemPrompt = document.getElementById("cgSystemPrompt");
  const cgInputContext = document.getElementById("cgInputContext");
  const cgOutput = document.getElementById("cgOutput");
  const cgStatus = document.getElementById("cgStatus");

  if (!cgGenerateBtn) return;

  function setCgStatus(text, loading = false) {
    cgStatus.textContent = text;
    cgStatus.style.display = text ? "block" : "none";
    cgStatus.classList.toggle("loading", loading);
    cgStatus.classList.toggle("status-dots", loading);
  }

  cgGenerateBtn.addEventListener("click", async () => {
    const systemPrompt = (cgSystemPrompt.value || "").trim();
    const inputContext = (cgInputContext.value || "").trim();
    if (!inputContext) {
      setCgStatus("Please enter input context before generating.");
      return;
    }
    cgGenerateBtn.disabled = true;
    cgGenerateBtn.textContent = "Generating\u2026";
    cgOutput.value = "";
    cgCopyBtn.disabled = true;
    setCgStatus("Generating excerpt\u2026", true);
    try {
      const res = await fetch("/api/content/generate-excerpt", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ system_prompt: systemPrompt, input_context: inputContext }),
      });
      const data = await res.json();
      if (!res.ok || data.ok === false) {
        setCgStatus("Error: " + (data.detail || data.error || "Unknown error"));
      } else {
        cgOutput.value = data.excerpt || "";
        cgCopyBtn.disabled = !data.excerpt;
        setCgStatus("");
      }
    } catch (err) {
      setCgStatus("Network error: " + (err.message || String(err)));
    } finally {
      cgGenerateBtn.disabled = false;
      cgGenerateBtn.textContent = "Generate";
    }
  });

  cgClearBtn.addEventListener("click", () => {
    cgInputContext.value = "";
    cgOutput.value = "";
    cgCopyBtn.disabled = true;
    setCgStatus("");
  });

  cgCopyBtn.addEventListener("click", () => {
    if (!cgOutput.value) return;
    navigator.clipboard.writeText(cgOutput.value).then(() => {
      const prev = cgCopyBtn.textContent;
      cgCopyBtn.textContent = "Copied!";
      setTimeout(() => { cgCopyBtn.textContent = prev; }, 1500);
    });
  });
})();

function setMeetingsStatus(text, loading = false) {
  if (!meetingsStatus) return;
  meetingsStatus.textContent = text;
  meetingsStatus.classList.toggle("loading", loading);
  meetingsStatus.classList.toggle("status-dots", loading);
}

function pickAudioMimeType() {
  const types = ["audio/webm;codecs=opus", "audio/webm", "audio/mp4"];
  for (const t of types) {
    if (MediaRecorder.isTypeSupported(t)) return t;
  }
  return "";
}

function stopTracks(stream) {
  if (!stream) return;
  stream.getTracks().forEach((track) => track.stop());
}

async function createRecordingStream(captureMode) {
  const mic = await navigator.mediaDevices.getUserMedia({ audio: true });
  if (captureMode !== "mic_system") {
    return { stream: mic, sourceStreams: [mic], modeLabel: "Microphone" };
  }
  if (!navigator.mediaDevices.getDisplayMedia) {
    stopTracks(mic);
    throw new Error("This browser does not support computer audio capture.");
  }
  const display = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
  const displayAudioTracks = display.getAudioTracks();
  if (!displayAudioTracks.length) {
    stopTracks(display);
    stopTracks(mic);
    throw new Error("No computer audio track was shared. Enable 'Share audio' in the prompt.");
  }
  const AudioCtx = window.AudioContext || window.webkitAudioContext;
  if (!AudioCtx) {
    stopTracks(display);
    stopTracks(mic);
    throw new Error("Audio mixing is not supported in this browser.");
  }

  meetingMixAudioCtx = new AudioCtx();
  const destination = meetingMixAudioCtx.createMediaStreamDestination();
  const micSource = meetingMixAudioCtx.createMediaStreamSource(mic);
  micSource.connect(destination);
  const displayAudioStream = new MediaStream(displayAudioTracks);
  const displaySource = meetingMixAudioCtx.createMediaStreamSource(displayAudioStream);
  displaySource.connect(destination);

  const mixedTracks = destination.stream.getAudioTracks();
  if (!mixedTracks.length) {
    stopTracks(display);
    stopTracks(mic);
    await meetingMixAudioCtx.close().catch(() => {});
    meetingMixAudioCtx = null;
    throw new Error("Unable to combine microphone and computer audio.");
  }
  const mixedStream = new MediaStream(mixedTracks);
  return { stream: mixedStream, sourceStreams: [mic, display], modeLabel: "Mic + computer audio" };
}

let meetingMediaStream = null;
let meetingSourceStreams = [];
let meetingMixAudioCtx = null;
let meetingAudioCtx = null;
let meetingAnalyser = null;
let meetingWaveArray = null;
let meetingWaveAnim = 0;

function setWaveBadge(text) {
  if (!meetingWaveLevel) return;
  meetingWaveLevel.textContent = text;
}

function clearWaveformCanvas() {
  if (!meetingWaveCanvas) return;
  const ctx = meetingWaveCanvas.getContext("2d");
  if (!ctx) return;
  const dpr = window.devicePixelRatio || 1;
  const width = Math.max(200, Math.floor(meetingWaveCanvas.clientWidth * dpr));
  const height = Math.max(80, Math.floor(meetingWaveCanvas.clientHeight * dpr));
  if (meetingWaveCanvas.width !== width || meetingWaveCanvas.height !== height) {
    meetingWaveCanvas.width = width;
    meetingWaveCanvas.height = height;
  }
  ctx.fillStyle = "#17151b";
  ctx.fillRect(0, 0, width, height);
  ctx.strokeStyle = "#2f2b36";
  ctx.lineWidth = Math.max(1, dpr);
  ctx.beginPath();
  ctx.moveTo(0, height / 2);
  ctx.lineTo(width, height / 2);
  ctx.stroke();
}

function stopWaveform() {
  if (meetingWaveAnim) {
    cancelAnimationFrame(meetingWaveAnim);
    meetingWaveAnim = 0;
  }
  if (meetingAudioCtx) {
    meetingAudioCtx.close().catch(() => {});
  }
  meetingAudioCtx = null;
  meetingAnalyser = null;
  meetingWaveArray = null;
  setWaveBadge("Idle");
  clearWaveformCanvas();
}

function startWaveform(stream) {
  stopWaveform();
  if (!meetingWaveCanvas) return;
  const AudioCtx = window.AudioContext || window.webkitAudioContext;
  if (!AudioCtx) {
    setWaveBadge("Not supported");
    return;
  }
  meetingAudioCtx = new AudioCtx();
  const source = meetingAudioCtx.createMediaStreamSource(stream);
  meetingAnalyser = meetingAudioCtx.createAnalyser();
  meetingAnalyser.fftSize = 2048;
  source.connect(meetingAnalyser);
  meetingWaveArray = new Uint8Array(meetingAnalyser.fftSize);
  const ctx = meetingWaveCanvas.getContext("2d");
  if (!ctx) return;
  setWaveBadge("Listening");

  const draw = () => {
    if (!meetingAnalyser || !meetingWaveArray) return;
    const dpr = window.devicePixelRatio || 1;
    const width = Math.max(200, Math.floor(meetingWaveCanvas.clientWidth * dpr));
    const height = Math.max(80, Math.floor(meetingWaveCanvas.clientHeight * dpr));
    if (meetingWaveCanvas.width !== width || meetingWaveCanvas.height !== height) {
      meetingWaveCanvas.width = width;
      meetingWaveCanvas.height = height;
    }
    meetingAnalyser.getByteTimeDomainData(meetingWaveArray);
    ctx.fillStyle = "#17151b";
    ctx.fillRect(0, 0, width, height);
    ctx.strokeStyle = "#df8f70";
    ctx.lineWidth = Math.max(1.2, 1.8 * dpr);
    ctx.beginPath();
    const step = width / meetingWaveArray.length;
    let x = 0;
    let avgDelta = 0;
    for (let i = 0; i < meetingWaveArray.length; i += 1) {
      const sample = meetingWaveArray[i];
      avgDelta += Math.abs(sample - 128);
      const y = (sample / 255) * height;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
      x += step;
    }
    ctx.stroke();
    const level = avgDelta / meetingWaveArray.length;
    if (level >= 30) setWaveBadge("Loud");
    else if (level >= 15) setWaveBadge("Active");
    else setWaveBadge("Listening");
    meetingWaveAnim = requestAnimationFrame(draw);
  };
  meetingWaveAnim = requestAnimationFrame(draw);
}

function cleanupMeetingStream() {
  stopWaveform();
  for (const source of meetingSourceStreams) {
    stopTracks(source);
  }
  meetingSourceStreams = [];
  if (meetingMixAudioCtx) {
    meetingMixAudioCtx.close().catch(() => {});
    meetingMixAudioCtx = null;
  }
  if (meetingMediaStream) {
    meetingMediaStream.getTracks().forEach((t) => t.stop());
    meetingMediaStream = null;
  }
}

let mediaRecorder = null;
let recordedChunks = [];
let recordingMimeType = "";

async function refreshMeetingsList() {
  if (!meetingsList) return;
  try {
    const res = await fetch("/api/meetings");
    const data = await res.json();
    if (!res.ok) {
      meetingsList.innerHTML = `<p class="meetings-empty">Could not load saved summaries.</p>`;
      return;
    }
    const items = Array.isArray(data.meetings) ? data.meetings : [];
    if (items.length === 0) {
      meetingsList.innerHTML = `<p class="meetings-empty">No saved meetings yet.</p>`;
      return;
    }
    meetingsList.innerHTML = items
      .map((m) => {
        const title = escapeHtml(m.meeting_name || "Untitled");
        const when = escapeHtml(m.created_at || "");
        const body = escapeHtml(m.summary || "");
        return `<div class="meetings-saved-item"><h4>${title}</h4><div class="meetings-saved-meta">${when}</div><div class="meetings-saved-body">${body}</div></div>`;
      })
      .join("");
  } catch (error) {
    meetingsList.innerHTML = `<p class="meetings-empty">Network error loading summaries.</p>`;
  }
}

if (meetingRecordBtn && meetingStopBtn && meetingNameInput) {
  meetingRecordBtn.addEventListener("click", async () => {
    const name = (meetingNameInput.value || "").trim();
    if (!name) {
      setMeetingsStatus("Enter a meeting name first", false);
      return;
    }
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      setMeetingsStatus("Microphone is not supported in this browser", false);
      return;
    }
    recordedChunks = [];
    try {
      const captureMode = meetingCaptureMode ? meetingCaptureMode.value : "mic_only";
      if (captureMode === "mic_system") {
        setMeetingsStatus("Choose screen/tab and enable Share audio…", true);
      }
      const { stream, sourceStreams, modeLabel } = await createRecordingStream(captureMode);
      meetingMediaStream = stream;
      meetingSourceStreams = sourceStreams;
      const display = sourceStreams.find((s) => s.getVideoTracks().length > 0);
      if (display) {
        const videoTrack = display.getVideoTracks()[0];
        if (videoTrack) {
          videoTrack.addEventListener(
            "ended",
            () => {
              if (mediaRecorder && mediaRecorder.state !== "inactive") {
                setMeetingsStatus("System audio share ended. Finishing recording…", true);
                mediaRecorder.stop();
              }
            },
            { once: true }
          );
        }
      }
      startWaveform(stream);
      const mime = pickAudioMimeType();
      const opts = mime ? { mimeType: mime } : undefined;
      mediaRecorder = new MediaRecorder(stream, opts);
      recordingMimeType = mediaRecorder.mimeType || mime || "audio/webm";
      mediaRecorder.ondataavailable = (e) => {
        if (e.data && e.data.size > 0) recordedChunks.push(e.data);
      };
      mediaRecorder.addEventListener("error", () => {
        setMeetingsStatus("Recording error", false);
      });
      mediaRecorder.start(250);
      meetingRecordBtn.disabled = true;
      meetingStopBtn.disabled = false;
      meetingNameInput.disabled = true;
      if (meetingCaptureMode) meetingCaptureMode.disabled = true;
      setMeetingsStatus(`Recording (${modeLabel})…`, true);
    } catch (error) {
      const detail = error instanceof Error ? error.message : "";
      setMeetingsStatus(detail || "Microphone/system audio permission denied or unavailable", false);
      cleanupMeetingStream();
    }
  });

  meetingStopBtn.addEventListener("click", () => {
    if (!mediaRecorder || mediaRecorder.state === "inactive") {
      cleanupMeetingStream();
      meetingRecordBtn.disabled = false;
      meetingStopBtn.disabled = true;
      meetingNameInput.disabled = false;
      if (meetingCaptureMode) meetingCaptureMode.disabled = false;
      return;
    }
    const name = (meetingNameInput.value || "").trim();
    const rec = mediaRecorder;
    rec.addEventListener(
      "stop",
      async () => {
        cleanupMeetingStream();
        const blobType = recordingMimeType || rec.mimeType || "audio/webm";
        const blob = new Blob(recordedChunks, { type: blobType });
        recordedChunks = [];
        mediaRecorder = null;
        meetingRecordBtn.disabled = false;
        meetingNameInput.disabled = false;
        meetingStopBtn.disabled = true;
        if (meetingCaptureMode) meetingCaptureMode.disabled = false;

        if (!name) {
          setMeetingsStatus("Meeting name was empty", false);
          return;
        }
        if (blob.size < 64) {
          setMeetingsStatus("Recording was too short", false);
          return;
        }

        const ext = blobType.includes("webm")
          ? "webm"
          : blobType.includes("mp4")
            ? "m4a"
            : "webm";
        const fd = new FormData();
        fd.append("meeting_name", name);
        fd.append("audio", blob, `recording.${ext}`);

        setMeetingsStatus("Uploading and transcribing…", true);
        if (meetingTranscript) meetingTranscript.textContent = "…";
        if (meetingSummary) meetingSummary.textContent = "…";

        try {
          const res = await fetch("/api/meetings/process", { method: "POST", body: fd });
          const data = await res.json();
          if (!res.ok) {
            if (meetingTranscript) meetingTranscript.textContent = "—";
            if (meetingSummary) meetingSummary.textContent = "—";
            const detail = data.detail;
            const msg =
              typeof detail === "string"
                ? detail
                : Array.isArray(detail) && detail[0]?.msg
                  ? detail[0].msg
                  : "Processing failed";
            setMeetingsStatus(msg, false);
            return;
          }
          if (meetingTranscript) meetingTranscript.textContent = data.transcript || "";
          if (meetingSummary) meetingSummary.textContent = data.summary || "";
          setMeetingsStatus("Done", false);
          await refreshMeetingsList();
        } catch (error) {
          if (meetingTranscript) meetingTranscript.textContent = "—";
          if (meetingSummary) meetingSummary.textContent = "—";
          setMeetingsStatus("Network error", false);
        }
      },
      { once: true }
    );
    rec.stop();
  });
}

function egFindNodeGroup(target) {
  let el = target;
  while (el && el !== egGraphSvg) {
    if (el.classList && el.classList.contains("kb-node")) return el;
    el = el.parentNode;
  }
  return null;
}

if (egGraphSvg) {
  egGraphSvg.addEventListener("click", (evt) => {
    const ng = egFindNodeGroup(evt.target);
    if (!ng) { egSelectedId = ""; renderEntityGraph(); return; }
    const id = ng.getAttribute("data-id") || "";
    if (!id) return;
    egSelectedId = (egSelectedId === id) ? "" : id;
    renderEntityGraph();
  });
  egGraphSvg.addEventListener("mousedown", (evt) => {
    if (egFindNodeGroup(evt.target)) return;
    egPanDrag = { startX: evt.clientX, startY: evt.clientY, panX: egPan.x, panY: egPan.y };
  });
  egGraphSvg.addEventListener("wheel", (evt) => {
    evt.preventDefault();
    egScale = Math.max(0.4, Math.min(3.0, egScale * (evt.deltaY > 0 ? 0.92 : 1.08)));
    renderEntityGraph();
  }, { passive: false });
}

if (egRefreshBtn) {
  egRefreshBtn.addEventListener("click", async () => { await refreshEntityGraph(); });
}
if (egRebuildBtn) {
  egRebuildBtn.addEventListener("click", async () => {
    egRebuildBtn.disabled = true;
    egRebuildBtn.textContent = "Rebuilding...";
    try {
      const res = await fetch("/api/memory/rebuild-entity-graph", { method: "POST" });
      const data = await res.json();
      egRebuildBtn.textContent = `Done (${data.entities_upserted || 0} ent, ${data.relations_upserted || 0} rel)`;
      await refreshEntityGraph();
    } catch {
      egRebuildBtn.textContent = "Error";
    }
    setTimeout(() => { egRebuildBtn.textContent = "Rebuild"; egRebuildBtn.disabled = false; }, 3000);
  });
}
if (egSynthesizeBtn) {
  egSynthesizeBtn.addEventListener("click", async () => {
    egSynthesizeBtn.disabled = true;
    egSynthesizeBtn.textContent = "Synthesizing…";
    try {
      const res = await fetch("/api/memory/synthesize-cross-source", { method: "POST" });
      const data = await res.json();
      if (data.ok === false) {
        egSynthesizeBtn.textContent = "Error";
      } else {
        egSynthesizeBtn.textContent = `Done (${data.written || 0} kb, ${data.entities_upserted || 0} ent, ${data.relations_upserted || 0} rel, ${data.parents_created || 0} groups)`;
        await refreshEntityGraph();
      }
    } catch {
      egSynthesizeBtn.textContent = "Error";
    }
    setTimeout(() => { egSynthesizeBtn.textContent = "Synthesize"; egSynthesizeBtn.disabled = false; }, 4000);
  });
}
if (egRefineBtn) {
  egRefineBtn.addEventListener("click", async () => {
    egRefineBtn.disabled = true;
    egRefineBtn.textContent = "Refining…";
    try {
      const res = await fetch("/api/memory/refine-entity-graph", { method: "POST" });
      const data = await res.json();
      if (data.ok === false) {
        egRefineBtn.textContent = "Error";
      } else {
        egRefineBtn.textContent = `Done (${data.entities_merged || 0} merged, ${data.relations_added || 0} added, ${data.relations_fixed || 0} fixed)`;
        await refreshEntityGraph();
      }
    } catch {
      egRefineBtn.textContent = "Error";
    }
    setTimeout(() => { egRefineBtn.textContent = "Refine"; egRefineBtn.disabled = false; }, 4000);
  });
}
if (egTypeFilter) {
  egTypeFilter.addEventListener("change", () => { renderEntityGraph(); });
}
if (egSearchInput) {
  let egTimer = 0;
  egSearchInput.addEventListener("input", () => {
    window.clearTimeout(egTimer);
    egTimer = window.setTimeout(() => { renderEntityGraph(); }, 140);
  });
}

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

document.addEventListener("mousemove", (event) => {
  if (egPanDrag) {
    const dx = event.clientX - egPanDrag.startX;
    const dy = event.clientY - egPanDrag.startY;
    egPan.x = egPanDrag.panX + dx;
    egPan.y = egPanDrag.panY + dy;
    renderEntityGraph();
    return;
  }
  if (!activeGanttDrag) return;
  const drag = activeGanttDrag;
  const deltaDays = Math.round((event.clientX - drag.startX) / Math.max(1, drag.pxPerDay));
  let startIdx = drag.originalStart;
  let endIdx = drag.originalEnd;
  if (drag.mode === "start") {
    startIdx = Math.max(0, Math.min(drag.originalEnd, drag.originalStart + deltaDays));
  } else if (drag.mode === "end") {
    endIdx = Math.min(drag.totalDays - 1, Math.max(drag.originalStart, drag.originalEnd + deltaDays));
  } else {
    const width = drag.originalEnd - drag.originalStart;
    startIdx = Math.max(0, Math.min(drag.totalDays - 1 - width, drag.originalStart + deltaDays));
    endIdx = startIdx + width;
  }
  drag.currentStart = startIdx;
  drag.currentEnd = endIdx;
  const leftPct = (startIdx / drag.totalDays) * 100;
  const widthPct = ((endIdx - startIdx + 1) / drag.totalDays) * 100;
  drag.barEl.style.left = `${leftPct}%`;
  drag.barEl.style.width = `${Math.max(2, widthPct)}%`;
});

document.addEventListener("mouseup", async () => {
  if (egPanDrag) {
    egPanDrag = null;
    return;
  }
  if (!activeGanttDrag) return;
  const drag = activeGanttDrag;
  activeGanttDrag = null;
  document.body.style.userSelect = "";
  if (drag.currentStart == null || drag.currentEnd == null) return;
  const newStart = dayIndexToIso(drag.timelineStartIso, drag.currentStart);
  const newEnd = dayIndexToIso(drag.timelineStartIso, drag.currentEnd);
  try {
    await updateGanttTaskDates(drag.taskId, newStart, newEnd);
    if (ganttMeta) ganttMeta.textContent = `Saved ${drag.taskId}: ${newStart} → ${newEnd}`;
    await refreshTasks();
    if (currentView === "gantt") await refreshGantt();
  } catch (error) {
    if (ganttMeta) ganttMeta.textContent = error?.message || "Failed to save gantt dates.";
    if (currentView === "gantt") await refreshGantt();
  }
});

function setSidePanelOpen(open) {
  if (!appRoot || !activityToggleBtn) return;
  appRoot.classList.toggle("side-open", open);
  activityToggleBtn.classList.toggle("tools-active", open);
  activityToggleBtn.setAttribute("aria-expanded", open ? "true" : "false");
  localStorage.setItem("copilot:side-panel-open", open ? "1" : "0");
}

function toggleSidePanel() {
  if (!appRoot) return;
  setSidePanelOpen(!appRoot.classList.contains("side-open"));
}

if (activityToggleBtn && appRoot) {
  activityToggleBtn.addEventListener("click", () => toggleSidePanel());
  document.addEventListener("keydown", (e) => {
    if ((e.ctrlKey || e.metaKey) && (e.key === "b" || e.key === "B")) {
      e.preventDefault();
      toggleSidePanel();
    }
  });
  if (localStorage.getItem("copilot:side-panel-open") === "1") {
    setSidePanelOpen(true);
  }
}

setChatStatus("Ready", false);
setReportStatus("Report ready", false);
clearWaveformCanvas();
refreshAll();
if (currentView === "report") {
  loadReport();
} else if (currentView === "gantt") {
  refreshGantt();
} else if (currentView === "knowledge") {
  refreshEntityGraph();
} else if (currentView === "meetings") {
  refreshMeetingsList();
}
setInterval(() => {
  if (currentView === "knowledge") return;
  refreshTasks();
}, 3000);

// ── PRD view ──────────────────────────────────────────────────────────────────
if (activityPrdBtn) {
  activityPrdBtn.addEventListener("click", () => setActiveView("prd"));
}

const prdUploadBtn = document.getElementById("prdUploadBtn");
const prdTitleInput = document.getElementById("prdTitleInput");
const prdFileInput = document.getElementById("prdFileInput");
const prdUploadStatus = document.getElementById("prdUploadStatus");
const prdListEl = document.getElementById("prdList");
const prdEmptyEl = document.getElementById("prdEmpty");
const prdRefreshBtn = document.getElementById("prdRefreshBtn");

const PRD_ENTITY_COLORS = {
  team:"#60a5fa", domain:"#c084fc", topic:"#2dd4bf",
  system:"#fb923c", person:"#f472b6", process:"#a3e635", constraint:"#fbbf24"
};

function setPrdStatus(msg, loading = false) {
  if (!prdUploadStatus) return;
  prdUploadStatus.classList.remove("hidden");
  prdUploadStatus.classList.toggle("loading", loading);
  prdUploadStatus.innerHTML = loading ? `<span class="status-dots"></span> ${msg}` : msg;
}

async function loadPrds() {
  try {
    const res = await fetch("/api/prds");
    const data = await res.json();
    renderPrdList(data.prds || []);
  } catch { renderPrdList([]); }
}

function renderPrdList(prds) {
  if (!prdListEl) return;
  if (!prds.length) {
    prdListEl.innerHTML = "";
    if (prdEmptyEl) prdEmptyEl.style.display = "block";
    return;
  }
  if (prdEmptyEl) prdEmptyEl.style.display = "none";
  prdListEl.innerHTML = prds.map(p => `
    <div class="prd-card" data-prd-id="${p.id}">
      <div class="prd-card-head" onclick="togglePrdCard('${p.id}')">
        <div>
          <div class="prd-card-title">${escHtml(p.title)}</div>
          <div class="prd-card-meta">${escHtml(p.filename)} · ${p.chunk_count} chunks · ${p.uploaded_at ? p.uploaded_at.slice(0,10) : ''}</div>
        </div>
        <button class="secondary" style="font-size:11px;padding:3px 10px;flex-shrink:0;" onclick="event.stopPropagation();deletePrd('${p.id}')">Delete</button>
      </div>
      <div class="prd-card-body" id="prd-body-${p.id}">
        <div id="prd-inner-${p.id}"><div class="prd-empty-graph">Loading…</div></div>
      </div>
    </div>
  `).join("");
}

function escHtml(s) {
  return String(s || "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

async function togglePrdCard(prdId) {
  const body = document.getElementById(`prd-body-${prdId}`);
  if (!body) return;
  const isOpen = body.classList.contains("open");
  body.classList.toggle("open", !isOpen);
  if (!isOpen) await loadPrdDetails(prdId);
}

async function loadPrdDetails(prdId) {
  const inner = document.getElementById(`prd-inner-${prdId}`);
  if (!inner) return;
  inner.innerHTML = `<div class="prd-empty-graph">Loading knowledge…</div>`;
  try {
    const res = await fetch(`/api/prds/${prdId}/rules`);
    const data = await res.json();
    renderPrdDetails(prdId, data.rules || [], data.graph || {});
  } catch {
    inner.innerHTML = `<div class="prd-empty-graph">Failed to load.</div>`;
  }
}

function renderPrdDetails(prdId, rules, graph) {
  const inner = document.getElementById(`prd-inner-${prdId}`);
  if (!inner) return;
  const nodes = graph.nodes || [];
  const edges = graph.edges || [];
  if (!nodes.length) {
    inner.innerHTML = `<div class="prd-empty-graph">No entities extracted yet. Synthesis runs in background — check back in a moment.</div>`;
    return;
  }
  inner.innerHTML = `
    <div style="position:relative;overflow:hidden;background:#19171d;border-radius:0 0 12px 12px;">
      <svg class="prd-graph-svg" id="prd-svg-${prdId}" style="cursor:grab;">
        <g id="prd-zoom-${prdId}"></g>
      </svg>
      <div id="prd-tip-${prdId}" class="prd-node-tip" style="display:none;"></div>
    </div>`;
  requestAnimationFrame(() => renderPrdMiniGraph(prdId, nodes, edges));
}

function renderPrdMiniGraph(prdId, nodes, edges) {
  const svgEl = document.getElementById(`prd-svg-${prdId}`);
  const zoomG = document.getElementById(`prd-zoom-${prdId}`);
  const tip   = document.getElementById(`prd-tip-${prdId}`);
  if (!svgEl || !zoomG) return;

  const W = svgEl.clientWidth || 900, H = 460, CX = W / 2, CY = H / 2;
  svgEl.setAttribute("viewBox", `0 0 ${W} ${H}`);

  const pos = nodes.map((_, i) => {
    const a = (i / nodes.length) * 2 * Math.PI, r = Math.min(W, H) * 0.32;
    return { x: CX + r * Math.cos(a), y: CY + r * Math.sin(a), vx: 0, vy: 0 };
  });
  const byId = {};
  nodes.forEach((n, i) => { byId[n.id] = i; });

  for (let t = 0; t < 160; t++) {
    for (let i = 0; i < pos.length; i++) {
      for (let j = i + 1; j < pos.length; j++) {
        const dx = pos[i].x - pos[j].x, dy = pos[i].y - pos[j].y;
        const d = Math.max(1, Math.sqrt(dx*dx + dy*dy)), f = 1800 / (d*d);
        pos[i].vx += (dx/d)*f; pos[i].vy += (dy/d)*f;
        pos[j].vx -= (dx/d)*f; pos[j].vy -= (dy/d)*f;
      }
    }
    edges.forEach(e => {
      const si = byId[e.source], ti = byId[e.target];
      if (si == null || ti == null) return;
      const dx = pos[ti].x - pos[si].x, dy = pos[ti].y - pos[si].y;
      const d = Math.max(1, Math.sqrt(dx*dx + dy*dy)), f = (d - 110) * 0.035;
      pos[si].vx += (dx/d)*f; pos[si].vy += (dy/d)*f;
      pos[ti].vx -= (dx/d)*f; pos[ti].vy -= (dy/d)*f;
    });
    pos.forEach(p => { p.vx += (CX - p.x) * 0.01; p.vy += (CY - p.y) * 0.01; });
    pos.forEach(p => {
      p.x = Math.max(60, Math.min(W-60, p.x + p.vx*0.5));
      p.y = Math.max(30, Math.min(H-30, p.y + p.vy*0.5));
      p.vx *= 0.72; p.vy *= 0.72;
    });
  }

  const ns = "http://www.w3.org/2000/svg";
  zoomG.innerHTML = "";

  edges.forEach(e => {
    const si = byId[e.source], ti = byId[e.target];
    if (si == null || ti == null) return;
    const line = document.createElementNS(ns, "line");
    line.setAttribute("x1", pos[si].x.toFixed(1)); line.setAttribute("y1", pos[si].y.toFixed(1));
    line.setAttribute("x2", pos[ti].x.toFixed(1)); line.setAttribute("y2", pos[ti].y.toFixed(1));
    line.setAttribute("stroke", "#4a4356"); line.setAttribute("stroke-width", "1.4"); line.setAttribute("opacity", "0.7");
    zoomG.appendChild(line);
    const lbl = document.createElementNS(ns, "text");
    lbl.setAttribute("x", ((pos[si].x + pos[ti].x)/2).toFixed(1));
    lbl.setAttribute("y", ((pos[si].y + pos[ti].y)/2).toFixed(1));
    lbl.setAttribute("text-anchor", "middle"); lbl.setAttribute("font-size", "8");
    lbl.setAttribute("fill", "#6b5f8a"); lbl.setAttribute("pointer-events", "none");
    lbl.textContent = e.relation_type.replace(/_/g, " ");
    zoomG.appendChild(lbl);
  });

  nodes.forEach((n, i) => {
    const color = PRD_ENTITY_COLORS[n.entity_type] || "#9ca3af";
    const g = document.createElementNS(ns, "g");
    g.style.cursor = "pointer";
    const circle = document.createElementNS(ns, "circle");
    circle.setAttribute("cx", pos[i].x.toFixed(1)); circle.setAttribute("cy", pos[i].y.toFixed(1));
    circle.setAttribute("r", "9"); circle.setAttribute("fill", color);
    circle.setAttribute("stroke", "#19171d"); circle.setAttribute("stroke-width", "1.5");
    const txt = document.createElementNS(ns, "text");
    const lbl = (n.name||"").length > 20 ? n.name.slice(0,19)+"…" : n.name;
    txt.setAttribute("x", pos[i].x.toFixed(1)); txt.setAttribute("y", (pos[i].y + 20).toFixed(1));
    txt.setAttribute("text-anchor", "middle"); txt.setAttribute("font-size", "10");
    txt.setAttribute("fill", "#e5e1ec"); txt.setAttribute("stroke", "#19171d");
    txt.setAttribute("stroke-width", "2.5"); txt.setAttribute("paint-order", "stroke");
    txt.setAttribute("pointer-events", "none"); txt.textContent = lbl;
    g.addEventListener("mouseenter", () => { circle.setAttribute("r","11"); circle.setAttribute("stroke", color); });
    g.addEventListener("mouseleave", () => { circle.setAttribute("r","9"); circle.setAttribute("stroke","#19171d"); });
    g.addEventListener("click", ev => {
      ev.stopPropagation();
      const rels = edges.filter(e => e.source===n.id || e.target===n.id).map(e => {
        const oid = e.source===n.id ? e.target : e.source;
        const oname = nodes.find(x=>x.id===oid)?.name || oid;
        return `<div class="prd-tip-rel">${e.source===n.id?"→":"←"} <span class="prd-tip-reltype">${escHtml(e.relation_type.replace(/_/g," "))}</span> ${escHtml(oname)}</div>`;
      }).join("");
      tip.innerHTML = `
        <button class="prd-tip-close" onclick="this.parentElement.style.display='none'">✕</button>
        <div class="prd-tip-type">${escHtml(n.entity_type)}</div>
        <div class="prd-tip-name">${escHtml(n.name)}</div>
        ${n.description ? `<div class="prd-tip-desc">${escHtml(n.description)}</div>` : ""}
        ${rels ? `<div class="prd-tip-rels-label">Relations</div>${rels}` : ""}`;
      tip.style.display = "block";
    });
    g.appendChild(circle); g.appendChild(txt);
    zoomG.appendChild(g);
  });

  svgEl.addEventListener("click", () => { if (tip) tip.style.display = "none"; });

  let tx = 0, ty = 0, sc = 1;
  const applyT = () => zoomG.setAttribute("transform", `translate(${tx},${ty}) scale(${sc})`);
  svgEl.addEventListener("wheel", e => {
    e.preventDefault();
    const f = e.deltaY < 0 ? 1.12 : 0.89;
    const rect = svgEl.getBoundingClientRect();
    const mx = e.clientX - rect.left, my = e.clientY - rect.top;
    tx = mx - (mx - tx)*f; ty = my - (my - ty)*f; sc *= f;
    applyT();
  }, { passive: false });
  let drag = false, dx0, dy0, tx0, ty0;
  svgEl.addEventListener("mousedown", e => { drag=true; dx0=e.clientX; dy0=e.clientY; tx0=tx; ty0=ty; svgEl.style.cursor="grabbing"; });
  window.addEventListener("mousemove", e => { if(!drag)return; tx=tx0+(e.clientX-dx0); ty=ty0+(e.clientY-dy0); applyT(); });
  window.addEventListener("mouseup", () => { drag=false; svgEl.style.cursor="grab"; });
}

async function deletePrd(prdId) {
  if (!confirm("Remove this PRD?")) return;
  try {
    const res = await fetch(`/api/prds/${prdId}`, { method: "DELETE" });
    if (res.ok) await loadPrds();
  } catch { /* ignore */ }
}

if (prdUploadBtn) {
  prdUploadBtn.addEventListener("click", async () => {
    const title = (prdTitleInput?.value || "").trim();
    const file = prdFileInput?.files?.[0];
    if (!title) { setPrdStatus("Please enter a title."); return; }
    if (!file) { setPrdStatus("Please select a PDF file."); return; }
    prdUploadBtn.disabled = true;
    setPrdStatus("Uploading and extracting text…", true);
    const fd = new FormData();
    fd.append("title", title);
    fd.append("file", file);
    try {
      const res = await fetch("/api/prds", { method: "POST", body: fd });
      const data = await res.json();
      if (!res.ok) {
        setPrdStatus(`Error: ${data.detail || "Upload failed."}`);
      } else {
        prdTitleInput.value = "";
        prdFileInput.value = "";
        setPrdStatus(`Uploaded "${escHtml(data.title)}" — ${data.chunk_count} chunks. Knowledge enrichment running in background.`);
        await loadPrds();
      }
    } catch (err) {
      setPrdStatus(`Error: ${err.message}`);
    } finally {
      prdUploadBtn.disabled = false;
    }
  });
}

if (prdRefreshBtn) {
  prdRefreshBtn.addEventListener("click", () => loadPrds());
}

// Render Lucide icons
if (typeof lucide !== "undefined") lucide.createIcons();
