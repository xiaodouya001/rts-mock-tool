
const API = '';
const knownConversationIds = new Set();
const SCENARIOS_WITH_N = new Set(['N-01', 'N-02', 'N-03', 'E-09']);
const LOAD_BENCHMARK_PRESETS = {
  'benchmark-300': { concurrency: 300, messages: 100, interval: 70, rampup: 25000 },
  'benchmark-400': { concurrency: 400, messages: 100, interval: 78, rampup: 30000 },
  'benchmark-500': { concurrency: 500, messages: 100, interval: 85, rampup: 37500 },
};
const SCENARIO_LABELS = {
  'N-01': 'N-01 Normal In-Session Processing',
  'N-02': 'N-02 Idempotent Replay Hit',
  'N-03': 'N-03 Session Complete',
  'E-01': 'E-01 Missing Query conversationId',
  'E-04': 'E-04 Invalid JSON',
  'E-05': 'E-05 Invalid Enum Value',
  'E-06': 'E-06 Missing Required Field',
  'E-07': 'E-07 Field Type Mismatch',
  'E-08': 'E-08 Invalid Timestamp Format',
  'E-09': 'E-09 Sequence Out of Order',
  'E-14': 'E-14 Query/Body conversationId Mismatch',
  'E-15': 'E-15 Business-Rule Validation Failed',
};
let kafkaExpandAll = false;
const _kafkaAllMessages = [];
let _kafkaPage = 1;
let _kafkaPageSize = 50;
let _kafkaCidFilter = '';
let _kafkaInventoryKey = '';
let _kafkaInventoryLoading = false;
let _kafkaInventoryError = '';
let _kafkaInventoryPromise = null;
let _kafkaConsumerConnected = false;
let _kafkaActiveConversationId = '';
const latencyHistory = [];
let _loadUiRunning = false;
let liveChatCsvText = '';
let liveChatCsvFilename = '';
let liveChatSnapshot = null;
/** When true, Start stays disabled even if snapshot.state is briefly wrong (SSE / race). Cleared on idle|completed|failed. */
let _liveChatStartLatch = false;
/** Prevents overlapping startLiveChat() runs (e.g. double-click while awaiting preview or fetch). */
let _liveChatStartInFlight = false;
let liveChatPreviewShowAll = false;
const liveChatSeenMessageIds = new Set();
const liveChatSeenNoteIds = new Set();

/** Do not persist load-test results anymore; clear local leftovers from older UI versions. */
(function clearLegacyLoadPersist() {
  try {
    localStorage.removeItem('mockClient.lastLoadStats');
    localStorage.removeItem('mockClient.lastLoadHeatstrip');
  } catch (e) {}
})();

function escapeHtml(value) {
  const div = document.createElement('div');
  div.textContent = value == null ? '' : String(value);
  return div.innerHTML;
}

function applyLoadBenchmarkPreset(name) {
  const preset = LOAD_BENCHMARK_PRESETS[name];
  if (!preset) return;
  const concurrency = document.getElementById('load-concurrency');
  const messages = document.getElementById('load-messages');
  const interval = document.getElementById('load-interval');
  const rampup = document.getElementById('load-rampup');
  if (concurrency) concurrency.value = String(preset.concurrency);
  if (messages) messages.value = String(preset.messages);
  if (interval) interval.value = String(preset.interval);
  if (rampup) rampup.value = String(preset.rampup);
}

function syncLoadBenchmarkPresetSelection() {
  const sel = document.getElementById('load-benchmark-preset');
  const concurrency = document.getElementById('load-concurrency');
  const messages = document.getElementById('load-messages');
  const interval = document.getElementById('load-interval');
  const rampup = document.getElementById('load-rampup');
  if (!sel || !concurrency || !messages || !interval || !rampup) return;

  const current = {
    concurrency: Number(concurrency.value),
    messages: Number(messages.value),
    interval: Number(interval.value),
    rampup: Number(rampup.value),
  };

  const matched = Object.entries(LOAD_BENCHMARK_PRESETS).find(([, preset]) =>
    preset.concurrency === current.concurrency &&
    preset.messages === current.messages &&
    preset.interval === current.interval &&
    preset.rampup === current.rampup
  );
  sel.value = matched ? matched[0] : 'custom';
}

function setTheme(themeName) {
  document.documentElement.setAttribute('data-theme', themeName);
  try { localStorage.setItem('mockClient.theme', themeName); } catch (e) {}
}
(function initTheme() {
  let saved = 'dracula';
  try { saved = localStorage.getItem('mockClient.theme') || 'dracula'; } catch (e) {}
  if (saved === 'light') {
    saved = 'dracula';
    try { localStorage.setItem('mockClient.theme', 'dracula'); } catch (e) {}
  }
  setTheme(saved);
  const sel = document.getElementById('theme-select');
  if (sel) sel.value = saved;
})();

// ---------------------------------------------------------------------------
// Tab switching
// ---------------------------------------------------------------------------
function switchTab(name) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  const target = document.getElementById('tab-' + name);
  if (target) target.classList.add('active');
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === name);
  });
  try { localStorage.setItem('mockClient.activeTab', name); } catch (e) {}
}
(function restoreTab() {
  try {
    const saved = localStorage.getItem('mockClient.activeTab');
    const hasPanel = saved && document.getElementById('tab-' + saved);
    switchTab(hasPanel ? saved : 'livechat');
  } catch (e) {}
})();

function switchScenarioControlsTab(name) {
  const endpointPane = document.getElementById('scenario-controls-pane-endpoint');
  const testsPane = document.getElementById('scenario-controls-pane-tests');
  const endpointTab = document.getElementById('scenario-controls-tab-endpoint');
  const testsTab = document.getElementById('scenario-controls-tab-tests');
  const isEndpoint = name === 'endpoint';

  if (endpointPane) endpointPane.hidden = !isEndpoint;
  if (testsPane) testsPane.hidden = isEndpoint;
  if (endpointTab) {
    endpointTab.classList.toggle('is-active', isEndpoint);
    endpointTab.setAttribute('aria-selected', isEndpoint ? 'true' : 'false');
  }
  if (testsTab) {
    testsTab.classList.toggle('is-active', !isEndpoint);
    testsTab.setAttribute('aria-selected', !isEndpoint ? 'true' : 'false');
  }
  try { localStorage.setItem('mockClient.scenarioControlsTab', isEndpoint ? 'endpoint' : 'tests'); } catch (e) {}
}

(function restoreScenarioControlsTab() {
  try {
    const saved = localStorage.getItem('mockClient.scenarioControlsTab');
    if (saved === 'endpoint' || saved === 'tests') {
      switchScenarioControlsTab(saved);
    }
  } catch (e) {}
})();

function switchLoadControlsTab(name) {
  const endpointPane = document.getElementById('load-controls-pane-endpoint');
  const parametersPane = document.getElementById('load-controls-pane-parameters');
  const endpointTab = document.getElementById('load-controls-tab-endpoint');
  const parametersTab = document.getElementById('load-controls-tab-parameters');
  const isEndpoint = name === 'endpoint';

  if (endpointPane) endpointPane.hidden = !isEndpoint;
  if (parametersPane) parametersPane.hidden = isEndpoint;
  if (endpointTab) {
    endpointTab.classList.toggle('is-active', isEndpoint);
    endpointTab.setAttribute('aria-selected', isEndpoint ? 'true' : 'false');
  }
  if (parametersTab) {
    parametersTab.classList.toggle('is-active', !isEndpoint);
    parametersTab.setAttribute('aria-selected', !isEndpoint ? 'true' : 'false');
  }
  try { localStorage.setItem('mockClient.loadControlsTab', isEndpoint ? 'endpoint' : 'parameters'); } catch (e) {}
}

(function restoreLoadControlsTab() {
  try {
    const saved = localStorage.getItem('mockClient.loadControlsTab');
    if (saved === 'endpoint' || saved === 'parameters') {
      switchLoadControlsTab(saved);
    }
  } catch (e) {}
})();

(function initKafkaCidUi() {
  _syncKafkaCidPlaceholder();
  updateKafkaConsumerControls();
})();

(function initKafkaCidCombobox() {
  document.addEventListener('click', (event) => {
    const box = document.getElementById('kafka-cid-combobox');
    if (!box || box.contains(event.target)) return;
    closeKafkaCidMenu();
  });
})();

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------
function connectSSE() {
  const es = new EventSource(API + '/api/events');
  const statusEl = document.getElementById('sse-status');

  es.onopen = () => { statusEl.textContent = 'SSE: Connected'; statusEl.style.color = 'var(--green)'; };
  es.onerror = () => { statusEl.textContent = 'SSE: Disconnected'; statusEl.style.color = 'var(--red)'; };

  es.addEventListener('scenario_step', e => {
    const d = JSON.parse(e.data);
    appendStep(d.scenario, d.step);
  });

  es.addEventListener('scenario_start', e => {
    const d = JSON.parse(e.data);
    createScenarioCard(d.scenario);
  });

  es.addEventListener('scenario_done', e => {
    const d = JSON.parse(e.data);
    finalizeScenarioCard(d.scenario, d.passed, d.steps);
  });

  es.addEventListener('kafka_message', e => {
    const d = JSON.parse(e.data);
    addKafkaMessage(d);
  });

  es.addEventListener('conversation_registered', e => {
    const d = JSON.parse(e.data);
    if (d.scenario && d.conversation_id) setScenarioConversationId(d.scenario, d.conversation_id);
  });

  es.addEventListener('stats', e => {
    const d = JSON.parse(e.data);
    updateStats(d);
  });

  es.addEventListener('load_start', e => {
    const d = JSON.parse(e.data);
    setLoadUiRunning(d);
  });

  es.addEventListener('load_done', e => {
    const d = JSON.parse(e.data);
    updateStats(d);
    setLoadUiDone(d);
  });

  es.addEventListener('kafka_status', e => {
    const d = JSON.parse(e.data);
    _kafkaConsumerConnected = Boolean(d.connected);
    _kafkaActiveConversationId = d.connected ? String(d.conversation_id || '') : '';
    const el = document.getElementById('kafka-status');
    if (d.connected) {
      const selected = d.conversation_id ? ` · ${d.conversation_id}` : '';
      el.textContent = '● Connected ' + (d.topic || '') + selected;
      el.style.color = 'var(--green)';
    } else {
      syncKafkaIdleStatus();
    }
    updateKafkaConsumerControls();
  });

  es.addEventListener('kafka_error', e => {
    const d = JSON.parse(e.data);
    _kafkaConsumerConnected = false;
    _kafkaActiveConversationId = '';
    const el = document.getElementById('kafka-status');
    el.textContent = '✗ ' + d.error;
    el.style.color = 'var(--red)';
    updateKafkaConsumerControls();
  });

  es.addEventListener('live_status', e => {
    const d = JSON.parse(e.data);
    if (Array.isArray(d.status_notes)) {
      d.status_notes.forEach(note => appendLiveChatNote(note));
    }
    const prev = liveChatSnapshot || {};
    const merged = { ...prev, ...d };
    const dState = d.state;
    const dStateMissing =
      dState === undefined ||
      dState === null ||
      (typeof dState === 'string' && dState.trim() === '');
    const prevNorm = _liveChatNormState(prev.state);
    if (dStateMissing && (prevNorm === 'running' || prevNorm === 'stopping')) {
      merged.state = prev.state;
    }
    const mergedNorm = _liveChatNormState(merged.state);
    if (mergedNorm) {
      merged.state = mergedNorm;
    }
    liveChatSnapshot = merged;
    setLiveChatStatusBanner(
      _liveChatPrimaryMessage(liveChatSnapshot),
      mergedNorm ?? 'idle',
    );
    updateLiveChatControls(liveChatSnapshot);
  });

  es.addEventListener('live_message', e => {
    const d = JSON.parse(e.data);
    if (!liveChatSnapshot) liveChatSnapshot = {};
    if (!Array.isArray(liveChatSnapshot.history)) liveChatSnapshot.history = [];
    liveChatSnapshot.history.push(d);
    appendLiveChatMessage(d);
    updateLiveChatControls(liveChatSnapshot);
  });

  es.addEventListener('live_error', e => {
    const d = JSON.parse(e.data);
    setLiveChatStatusBanner(d.message || 'Mock Live-Chat failed.', 'failed');
    const errNorm = _liveChatNormState(d.state);
    liveChatSnapshot = {
      ...(liveChatSnapshot || {}),
      state: errNorm || 'failed',
      error: d.message ?? liveChatSnapshot?.error,
    };
    updateLiveChatControls(liveChatSnapshot);
  });
}
connectSSE();
initLiveChatDropzone();
(function initLiveChatStartButton() {
  const el = document.getElementById('btn-livechat-start');
  if (!el) return;
  el.addEventListener('click', (event) => {
    if (el.disabled || el.getAttribute('data-livechat-busy') === '1') {
      event.preventDefault();
      event.stopPropagation();
      return;
    }
    void startLiveChat();
  });
})();
restoreLiveChatSidebarTab();
loadLiveChatStatus();
updateKafkaExpandButtonLabel();
renderHeatstrip();
['load-concurrency', 'load-messages', 'load-interval', 'load-rampup'].forEach((id) => {
  const el = document.getElementById(id);
  if (el) el.addEventListener('input', syncLoadBenchmarkPresetSelection);
});
syncLoadBenchmarkPresetSelection();

(function initScenarioMainResizer() {
  const LS = 'mockClient.scenarioSplitPct';
  const main = document.getElementById('scenario-main');
  const resizer = document.getElementById('scenario-main-resizer');
  if (!main || !resizer) return;

  function setSplitPct(pct) {
    if (!Number.isFinite(pct)) return;
    main.style.setProperty('--scenario-split', pct + '%');
    try { localStorage.setItem(LS, String(Math.round(pct * 10) / 10)); } catch (e) {}
  }

  function applyStored(p) {
    const n = Math.max(10, Math.min(85, Number(p)));
    if (!Number.isFinite(n)) return;
    setSplitPct(n);
  }

  try {
    const s = localStorage.getItem(LS);
    if (s != null && s !== '') applyStored(parseFloat(s, 10));
  } catch (e) {}

  let dragging = false;
  resizer.addEventListener('pointerdown', (e) => {
    if (e.button !== 0) return;
    dragging = true;
    try { resizer.setPointerCapture(e.pointerId); } catch (err) {}
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  });
  resizer.addEventListener('pointermove', (e) => {
    if (!dragging) return;
    const rect = main.getBoundingClientRect();
    const resH = resizer.offsetHeight || 7;
    const avail = rect.height - resH;
    if (avail < 100) return;
    const centerY = e.clientY - rect.top;
    let topPx = centerY - resH / 2;
    const minTop = 80;
    const minBot = 140;
    topPx = Math.max(minTop, Math.min(topPx, avail - minBot));
    const pct = (topPx / avail) * 100;
    setSplitPct(pct);
  });
  function endDrag(e) {
    if (!dragging) return;
    dragging = false;
    try { resizer.releasePointerCapture(e.pointerId); } catch (err) {}
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }
  resizer.addEventListener('pointerup', endDrag);
  resizer.addEventListener('pointercancel', endDrag);
  resizer.addEventListener('dblclick', () => setSplitPct(33));
})();

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------
const scenarioCards = {};
/** conversation_registered may arrive before the card is created. */
const pendingScenarioCids = {};

function escapeScenarioTitle(name) {
  const d = document.createElement('div');
  d.textContent = SCENARIO_LABELS[name] || name;
  return d.innerHTML;
}

function clearScenarioResults() {
  const container = document.getElementById('scenario-results');
  if (!container) return;
  container.innerHTML = '<p class="scenario-empty-hint">Use the buttons in "Scenario Controls" on the left to run a scenario...</p>';
  Object.keys(scenarioCards).forEach((k) => {
    delete scenarioCards[k];
  });
  Object.keys(pendingScenarioCids).forEach((k) => {
    delete pendingScenarioCids[k];
  });
}

function createScenarioCard(name) {
  const container = document.getElementById('scenario-results');
  if (container.querySelector('p')) container.innerHTML = '';

  let card = scenarioCards[name];
  if (card) {
    card.className = 'scenario-card running';
    card.querySelector('.badge').className = 'badge running';
    card.querySelector('.badge').textContent = 'Running';
    card.querySelector('.step-list').innerHTML = '';
    const cidEl = card.querySelector('.scenario-cid-inline');
    if (cidEl) {
      cidEl.textContent = '';
      cidEl.style.display = 'none';
      cidEl.removeAttribute('title');
      cidEl.onclick = null;
    }
    const pendReuse = pendingScenarioCids[name];
    if (pendReuse) {
      delete pendingScenarioCids[name];
      setScenarioConversationId(name, pendReuse);
    }
    return;
  }

  const safeName = escapeScenarioTitle(name);
  card = document.createElement('div');
  card.className = 'scenario-card running';
  card.innerHTML = `
    <div class="title">
      <span class="scenario-name">${safeName}</span>
      <span class="scenario-cid-inline" style="display:none"></span>
      <span class="badge running">Running</span>
    </div>
    <div class="step-list"></div>
  `;
  container.prepend(card);
  scenarioCards[name] = card;
  const pendNew = pendingScenarioCids[name];
  if (pendNew) {
    delete pendingScenarioCids[name];
    setScenarioConversationId(name, pendNew);
  }
}

function setScenarioConversationId(scenarioName, cid) {
  if (!scenarioName || !cid) return;
  const card = scenarioCards[scenarioName];
  if (!card) {
    pendingScenarioCids[scenarioName] = cid;
    return;
  }
  let el = card.querySelector('.scenario-cid-inline');
  if (!el) {
    const title = card.querySelector('.title');
    const badge = title && title.querySelector('.badge');
    el = document.createElement('span');
    el.className = 'scenario-cid-inline';
    if (badge) title.insertBefore(el, badge);
    else title.appendChild(el);
  }
  el.textContent = cid;
  el.title = cid + '\nClick to copy';
  el.style.display = 'inline-block';
  el.onclick = (ev) => {
    ev.stopPropagation();
    navigator.clipboard.writeText(cid).then(() => {
      const prev = el.textContent;
      el.textContent = 'Copied';
      setTimeout(() => { el.textContent = prev; }, 900);
    }).catch(() => {});
  };
}

function appendStep(scenario, step) {
  const card = scenarioCards[scenario];
  if (!card) return;
  const list = card.querySelector('.step-list');
  const div = document.createElement('div');
  div.className = 'step ' + (step.error ? 'error' : 'ok');
  let text = step.action;
  if (step.seq !== undefined) text += ` seq=${step.seq}`;
  if (step.resp_type) text += ` → ${step.resp_type}`;
  if (step.close_code !== undefined) text += ` close=${step.close_code}`;
  if (step.error_code) text += ` [${step.error_code}]`;
  if (step.error) text += ` ✗ ${step.error}`;
  else text += ' ✓';
  div.textContent = text;
  list.appendChild(div);
  list.scrollTop = list.scrollHeight;
}

function finalizeScenarioCard(name, passed, steps) {
  const card = scenarioCards[name];
  if (!card) return;
  card.className = 'scenario-card ' + (passed ? 'pass' : 'fail');
  const badge = card.querySelector('.badge');
  badge.className = 'badge ' + (passed ? 'pass' : 'fail');
  badge.textContent = passed ? 'PASS' : 'FAIL';
}

async function runScenario(name) {
  const wsUrl = document.getElementById('ws-url').value;
  createScenarioCard(name);
  let url = `${API}/api/scenario/run?name=${encodeURIComponent(name)}&ws_url=${encodeURIComponent(wsUrl)}`;
  if (SCENARIOS_WITH_N.has(name)) {
    url += `&n_messages=${encodeURIComponent(document.getElementById('scenario-messages').value)}`;
  }
  const resp = await fetch(url, { method: 'POST' });
  const data = await resp.json();
  finalizeScenarioCard(data.scenario, data.passed, data.steps);
}

async function runAllScenarios() {
  const names = [
    'N-01',
    'N-02',
    'N-03',
    'E-01',
    'E-04',
    'E-05',
    'E-06',
    'E-07',
    'E-08',
    'E-09',
    'E-14',
    'E-15',
  ];
  for (const name of names) {
    await runScenario(name);
  }
}

// ---------------------------------------------------------------------------
// Load test
// ---------------------------------------------------------------------------
let _loadElapsedTimer = null;
let _loadRunStartedAt = null;
let _loadStaticLine = '';
let _loadStopRequested = false;

function _clearLoadElapsedTimer() {
  if (_loadElapsedTimer) {
    clearInterval(_loadElapsedTimer);
    _loadElapsedTimer = null;
  }
  _loadRunStartedAt = null;
}

function resetStatsDisplay() {
  updateStats({
    sent: 0,
    ack: 0,
    error: 0,
    active_connections: 0,
    tps: 0,
    p50_ms: 0,
    p95_ms: 0,
    p99_ms: 0,
    server_p50_ms: 0,
    server_p95_ms: 0,
    server_p99_ms: 0,
    recent_errors: [],
  });
}

function setLoadUiRunning(meta) {
  latencyHistory.length = 0;
  _loadUiRunning = true;
  renderHeatstrip();
  resetStatsDisplay();
  switchTab('loadtest');
  const el = document.getElementById('load-status');
  if (!el) return;
  const c = Number(meta.concurrency);
  const m = Number(meta.messages_per_conv);
  const n = meta.total_sessions != null ? Number(meta.total_sessions) : (Number.isFinite(c) ? c : 0);
  _loadStopRequested = false;
  _loadStaticLine = `Load test running: ${n} conversations are live in this run (about ${c} connections), ${m} messages per conversation. This banner switches to Completed when the run ends.`;
  el.className = 'cp-load-status cp-load-status--running';
  el.textContent = _loadStaticLine + ' Elapsed 0.0s';
  _clearLoadElapsedTimer();
  _loadRunStartedAt = Date.now();
  _loadElapsedTimer = setInterval(() => {
    if (!_loadRunStartedAt || !el) return;
    const sec = ((Date.now() - _loadRunStartedAt) / 1000).toFixed(1);
    const tail = _loadStopRequested ? ' (stop requested, draining)' : '';
    el.textContent = _loadStaticLine + ` Elapsed ${sec}s` + tail;
  }, 400);
  const start = document.getElementById('btn-load-start');
  if (start) start.disabled = true;
}

function setLoadUiDone(d) {
  _clearLoadElapsedTimer();
  _loadStopRequested = false;
  _loadUiRunning = false;
  const el = document.getElementById('load-status');
  if (!el) return;
  el.className = 'cp-load-status cp-load-status--done';
  const elapsed = d.elapsed_sec != null ? d.elapsed_sec : '?';
  const cancelled = d.load_cancelled ? ' Stop was requested in this run; sessions that had not started were not queued.' : '';
  el.textContent = '✓ Completed: server-side measured duration ' + elapsed + 's.' + cancelled + ' Adjust parameters and start the next run when ready.';
  const start = document.getElementById('btn-load-start');
  if (start) start.disabled = false;
}

async function startLoad() {
  const wsUrl = document.getElementById('load-ws-url').value;
  const c = document.getElementById('load-concurrency').value;
  const m = document.getElementById('load-messages').value;
  const interval = document.getElementById('load-interval').value;
  const rampup = document.getElementById('load-rampup').value;
  const resp = await fetch(`${API}/api/load/start?ws_url=${encodeURIComponent(wsUrl)}&concurrency=${c}&messages_per_conv=${m}&interval_ms=${interval}&ramp_up_ms=${rampup}`, { method: 'POST' });
  const data = await resp.json().catch(() => ({}));
  if (data.error) {
    alert(data.error);
    return;
  }
  if (data.status === 'started') {
    setLoadUiRunning({
      concurrency: data.concurrency,
      total_sessions: data.total_sessions,
      messages_per_conv: Number(m),
    });
  }
}

async function stopLoad() {
  _loadStopRequested = true;
  const btn = document.getElementById('btn-load-stop');
  if (btn) { btn.disabled = true; btn.textContent = 'Stopping...'; }
  try {
    await fetch(`${API}/api/load/stop`, { method: 'POST' });
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Stop Load Test'; }
  }
}

// ---------------------------------------------------------------------------
// Mock Live-Chat
// ---------------------------------------------------------------------------
function makeLiveChatConversationId() {
  return 'livechat-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
}

function switchLiveChatSidebarTab(name) {
  const active = name === 'target' ? 'target' : 'csv';
  document.querySelectorAll('[data-livechat-side-tab]').forEach((button) => {
    const isActive = button.dataset.livechatSideTab === active;
    button.classList.toggle('is-active', isActive);
    button.setAttribute('aria-selected', isActive ? 'true' : 'false');
  });
  const csvPane = document.getElementById('livechat-sidebar-pane-csv');
  const targetPane = document.getElementById('livechat-sidebar-pane-target');
  if (csvPane) csvPane.hidden = active !== 'csv';
  if (targetPane) targetPane.hidden = active !== 'target';
  try {
    localStorage.setItem('mockClient.liveChatSidebarTab', active);
  } catch (e) {}
}

function restoreLiveChatSidebarTab() {
  try {
    const saved = localStorage.getItem('mockClient.liveChatSidebarTab') || 'csv';
    switchLiveChatSidebarTab(saved);
    return;
  } catch (e) {}
  if (
    !document.getElementById('livechat-sidebar-pane-csv')?.hidden &&
    !document.getElementById('livechat-sidebar-pane-target')?.hidden
  ) {
    switchLiveChatSidebarTab('csv');
  }
}

function triggerLiveChatFileInput() {
  const input = document.getElementById('livechat-file-input');
  if (input) input.click();
}

function _liveChatStatusBannerClass(state) {
  if (state === 'running' || state === 'stopping') return 'livechat-status livechat-status--running';
  if (state === 'completed') return 'livechat-status livechat-status--completed';
  if (state === 'failed') return 'livechat-status livechat-status--failed';
  return 'livechat-status livechat-status--idle';
}

function setLiveChatStatusBanner(message, state) {
  const el = document.getElementById('livechat-status-banner');
  if (!el) return;
  el.className = _liveChatStatusBannerClass(state);
  el.textContent = message;
}

function setLiveChatConversationPill(conversationId) {
  const cluster = document.getElementById('livechat-conversation-cluster');
  const idEl = document.getElementById('livechat-conversation-id-value');
  if (!cluster || !idEl) return;
  const value = String(conversationId || '').trim();
  cluster.dataset.conversationId = value;
  if (!value) {
    cluster.hidden = true;
    idEl.textContent = '';
    return;
  }
  cluster.hidden = false;
  idEl.textContent = value;
}

function copyLiveChatConversationId() {
  const cluster = document.getElementById('livechat-conversation-cluster');
  const id = (cluster && cluster.dataset.conversationId ? String(cluster.dataset.conversationId) : '').trim();
  if (!id) return;
  const done = () => {
    const btn = document.getElementById('btn-livechat-copy-cid');
    if (btn) {
      const prev = btn.textContent;
      btn.textContent = 'Copied';
      setTimeout(() => {
        btn.textContent = prev;
      }, 1600);
    }
  };
  const fail = () => {
    const btn = document.getElementById('btn-livechat-copy-cid');
    if (btn) {
      const prev = btn.textContent;
      btn.textContent = 'Failed';
      setTimeout(() => {
        btn.textContent = prev;
      }, 1600);
    }
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(id).then(done).catch(fail);
    return;
  }
  try {
    const ta = document.createElement('textarea');
    ta.value = id;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    done();
  } catch (e) {
    fail();
  }
}

function clearLiveChatCsvErrorState() {
  const zone = document.getElementById('livechat-dropzone');
  const summary = document.getElementById('livechat-preview-summary');
  const fileName = document.getElementById('livechat-file-name');
  if (zone) zone.classList.remove('is-invalid');
  if (summary) summary.classList.remove('is-invalid');
  if (fileName) fileName.classList.remove('is-invalid');
}

function setLiveChatCsvErrorState(message) {
  const zone = document.getElementById('livechat-dropzone');
  const summary = document.getElementById('livechat-preview-summary');
  const fileName = document.getElementById('livechat-file-name');
  if (zone) {
    zone.classList.add('is-invalid');
    zone.focus();
  }
  if (summary) {
    summary.classList.remove('is-ready');
    summary.classList.add('is-invalid');
    summary.dataset.ready = '0';
    summary.textContent = message;
  }
  if (fileName) fileName.classList.add('is-invalid');
}

function scrollLiveChatToBottom() {
  const shell = document.querySelector('.livechat-thread-shell');
  if (shell) shell.scrollTop = shell.scrollHeight;
}

function resetLiveChatThread() {
  const thread = document.getElementById('livechat-thread');
  const empty = document.getElementById('livechat-thread-empty');
  if (!thread || !empty) return;
  thread.innerHTML = '';
  empty.style.display = '';
  liveChatSeenMessageIds.clear();
  liveChatSeenNoteIds.clear();
}

function _liveChatHasRenderableContent(snapshot) {
  if (!snapshot) return false;
  return (snapshot.history && snapshot.history.length > 0) || (snapshot.status_notes && snapshot.status_notes.length > 0);
}

/** Lowercase canonical state, or null if missing/unknown (do not treat null as idle for latch rules). */
function _liveChatNormState(raw) {
  if (raw == null) return null;
  const s = String(raw).trim().toLowerCase();
  if (s === '') return 'idle';
  if (s === 'running' || s === 'stopping' || s === 'completed' || s === 'failed' || s === 'idle') {
    return s;
  }
  return null;
}

function _liveChatPrimaryMessage(snapshot) {
  if (!snapshot) {
    return 'Idle';
  }
  if (snapshot.error) return snapshot.error;
  const notes = snapshot.status_notes || [];
  const st = _liveChatNormState(snapshot.state) ?? 'idle';
  if (st === 'running') return 'Running';
  if (st === 'stopping') return 'Stopping';
  if (st === 'completed') return 'Completed';
  if (st === 'failed') return notes.length > 0 ? notes[notes.length - 1].message : 'Failed';
  return notes.length > 0 ? notes[notes.length - 1].message : 'Idle';
}

function updateLiveChatControls(snapshot) {
  if (snapshot !== undefined && snapshot !== null && typeof snapshot === 'object') {
    const patch = Object.fromEntries(Object.entries(snapshot).filter(([, v]) => v !== undefined));
    liveChatSnapshot = { ...(liveChatSnapshot || {}), ...patch };
  }
  const current = liveChatSnapshot || { state: 'idle' };
  const norm = _liveChatNormState(current.state);
  if (norm === 'idle' || norm === 'completed' || norm === 'failed') {
    _liveChatStartLatch = false;
  }
  const sessionBusy =
    _liveChatStartLatch || norm === 'running' || norm === 'stopping';
  const startBtn = document.getElementById('btn-livechat-start');
  const stopBtn = document.getElementById('btn-livechat-stop');
  const clearBtn = document.getElementById('btn-livechat-clear');
  if (startBtn) {
    startBtn.disabled = sessionBusy;
    if (sessionBusy) {
      startBtn.setAttribute('aria-disabled', 'true');
      startBtn.setAttribute('data-livechat-busy', '1');
    } else {
      startBtn.removeAttribute('aria-disabled');
      startBtn.removeAttribute('data-livechat-busy');
    }
    startBtn.title = sessionBusy
      ? 'Wait until this run finishes (Completed) before starting again.'
      : '';
  }
  if (stopBtn) stopBtn.disabled = !sessionBusy;
  if (clearBtn) clearBtn.disabled = sessionBusy;
}

function renderLiveChatPreview(preview) {
  const summary = document.getElementById('livechat-preview-summary');
  const wrap = document.getElementById('livechat-preview-table-wrap');
  const rowsEl = document.getElementById('livechat-preview-rows');
  const showAllBtn = document.getElementById('btn-livechat-preview-all');
  if (!summary || !wrap || !rowsEl || !showAllBtn) return;
  clearLiveChatCsvErrorState();

  if (!preview) {
    summary.classList.remove('is-ready');
    summary.dataset.ready = '0';
    summary.textContent = 'Upload a CSV to inspect recognized columns and preview the first few utterances before starting.';
    wrap.hidden = true;
    showAllBtn.hidden = true;
    rowsEl.innerHTML = '';
    return;
  }

  const columns = preview.recognized_columns || {};
  const rowCount = Number(preview.row_count || 0);
  const shownCount = Number(preview.preview_row_count || (preview.sample_rows || []).length || 0);
  const isFull = !!preview.preview_is_full;
  const csvName = escapeHtml(preview.csv_filename || liveChatCsvFilename || 'CSV');
  const speakerColumn = escapeHtml(columns.speaker || '-');
  const transcriptColumn = escapeHtml(columns.transcript || '-');
  const delayColumn = escapeHtml(columns.delay_ms || 'auto pacing');
  liveChatPreviewShowAll = isFull && rowCount > 12;
  summary.classList.add('is-ready');
  summary.dataset.ready = '1';
  summary.innerHTML = `
    <div class="livechat-preview-meta">
      <div class="livechat-preview-line">
        <span class="livechat-preview-value">${preview.row_count}</span>
        <span>utterances detected from</span>
        <span class="livechat-preview-chip">${csvName}</span>
      </div>
      <div class="livechat-preview-line livechat-preview-line--mapping">
        <span>speaker</span><span class="livechat-preview-arrow">→</span><span class="livechat-preview-chip">${speakerColumn}</span>
      </div>
      <div class="livechat-preview-line livechat-preview-line--mapping">
        <span>transcript</span><span class="livechat-preview-arrow">→</span><span class="livechat-preview-chip">${transcriptColumn}</span>
      </div>
      <div class="livechat-preview-line livechat-preview-line--mapping">
        <span>delay_ms</span><span class="livechat-preview-arrow">→</span><span class="livechat-preview-chip">${delayColumn}</span>
      </div>
      <div class="livechat-preview-line">
        <span>Previewing</span>
        <span class="livechat-preview-value">${isFull ? `all ${rowCount}` : `${shownCount} of ${rowCount}`}</span>
        <span>rows.</span>
      </div>
    </div>
  `;
  if (rowCount > 12) {
    showAllBtn.hidden = false;
    showAllBtn.textContent = isFull ? 'Show First 12' : 'Show All';
    showAllBtn.dataset.mode = isFull ? 'collapse' : 'expand';
  } else {
    showAllBtn.hidden = true;
    showAllBtn.textContent = 'Show All';
    showAllBtn.dataset.mode = 'expand';
  }

  rowsEl.innerHTML = '';
  (preview.sample_rows || []).forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.line_number ?? '-'}</td>
      <td>${row.speaker ?? '-'}</td>
      <td>${escapeHtml(row.transcript || '')}</td>
      <td>${row.delay_ms == null ? 'auto' : row.delay_ms}</td>
    `;
    rowsEl.appendChild(tr);
  });
  wrap.hidden = !(preview.sample_rows && preview.sample_rows.length);
  wrap.scrollTop = 0;
}

function formatLiveChatTime(isoTs) {
  if (!isoTs) return 'Unknown time';
  const date = new Date(isoTs);
  if (Number.isNaN(date.getTime())) return String(isoTs);
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function renderLiveChatNoteMarkup(note) {
  return `
    <span class="livechat-note-dot" aria-hidden="true"></span>
    <span class="livechat-note-text">${escapeHtml(note.message || '')}</span>
    <span class="livechat-note-time">${escapeHtml(formatLiveChatTime(note.at || ''))}</span>
  `;
}

function appendLiveChatNote(note) {
  if (!note || !note.note_id) return;
  const thread = document.getElementById('livechat-thread');
  const empty = document.getElementById('livechat-thread-empty');
  if (!thread || !empty) return;
  const existing = thread.querySelector(`[data-note-id="${note.note_id}"]`);
  if (existing) {
    existing.className = 'livechat-note livechat-note--' + (note.level || 'info');
    existing.innerHTML = renderLiveChatNoteMarkup(note);
    return;
  }

  liveChatSeenNoteIds.add(note.note_id);
  empty.style.display = 'none';
  const div = document.createElement('div');
  div.className = 'livechat-note livechat-note--' + (note.level || 'info');
  div.dataset.noteId = note.note_id;
  div.innerHTML = renderLiveChatNoteMarkup(note);
  thread.appendChild(div);
  scrollLiveChatToBottom();
}

function appendLiveChatMessage(message) {
  if (!message || !message.message_id || liveChatSeenMessageIds.has(message.message_id)) return;
  liveChatSeenMessageIds.add(message.message_id);

  if (message.speaker === 'System' || message.event_type === 'SESSION_COMPLETE') {
    appendLiveChatNote({
      note_id: 'system-' + message.message_id,
      level: 'success',
      message: 'Session completed.',
      at: message.created_at,
    });
    return;
  }

  const thread = document.getElementById('livechat-thread');
  const empty = document.getElementById('livechat-thread-empty');
  if (!thread || !empty) return;
  empty.style.display = 'none';

  const role = String(message.speaker || '').toLowerCase() === 'agent' ? 'agent' : 'customer';
  const avatarText = role === 'agent' ? 'AG' : 'CU';
  const sequenceChip = `<span class="livechat-kafka-chip" title="payload.sequenceNumber">seq ${message.sequence_number ?? '-'}</span>`;
  const offsetChip = `<span class="livechat-kafka-chip" title="Kafka offset">offset ${message.kafka_offset ?? '-'}</span>`;
  const card = document.createElement('div');
  card.className = 'livechat-row ' + role;
  card.dataset.messageId = message.message_id;
  card.innerHTML = `
    <div class="livechat-avatar">${avatarText}</div>
    <div class="livechat-bubble">
      <div class="livechat-meta">
        <span class="livechat-speaker">${message.speaker || '-'}</span>
        <span>${formatLiveChatTime(message.created_at)}</span>
        ${sequenceChip}
        ${offsetChip}
      </div>
      <div class="livechat-payload">${escapeHtml(message.transcript || '')}</div>
    </div>
  `;
  thread.appendChild(card);
  scrollLiveChatToBottom();
}

function renderLiveChatTimeline(snapshot) {
  resetLiveChatThread();
  const thread = document.getElementById('livechat-thread');
  const empty = document.getElementById('livechat-thread-empty');
  if (!thread || !empty) return;
  const timeline = [];
  (snapshot?.status_notes || []).forEach(note => {
    timeline.push({ kind: 'note', at: note.at || '', payload: note });
  });
  (snapshot?.history || []).forEach(message => {
    timeline.push({ kind: 'message', at: message.created_at || '', payload: message });
  });
  timeline.sort((a, b) => String(a.at).localeCompare(String(b.at)));
  timeline.forEach(item => {
    if (item.kind === 'note') appendLiveChatNote(item.payload);
    else appendLiveChatMessage(item.payload);
  });
  empty.style.display = timeline.length ? 'none' : '';
}

function applyLiveChatSnapshot(snapshot, options = {}) {
  liveChatSnapshot = snapshot || null;
  if (liveChatSnapshot && snapshot && snapshot.state != null) {
    const n = _liveChatNormState(snapshot.state);
    if (n) {
      liveChatSnapshot = { ...liveChatSnapshot, state: n };
    }
  }
  if (!snapshot) {
    renderLiveChatPreview(null);
    resetLiveChatThread();
    setLiveChatConversationPill('');
    setLiveChatStatusBanner(_liveChatPrimaryMessage(null), 'idle');
    updateLiveChatControls(null);
    return;
  }

  const {
    conversation_id,
    ws_url,
    kafka_bootstrap,
    kafka_topic,
    chars_per_second,
    pace_jitter_pct,
    preview,
    state,
  } = snapshot;

  const wsInput = document.getElementById('livechat-ws-url');
  const bootstrapInput = document.getElementById('livechat-kafka-bootstrap');
  const topicInput = document.getElementById('livechat-kafka-topic');
  const cpsInput = document.getElementById('livechat-chars-per-second');
  const jitterInput = document.getElementById('livechat-jitter-pct');
  if (wsInput && ws_url) wsInput.value = ws_url;
  if (bootstrapInput && kafka_bootstrap) bootstrapInput.value = kafka_bootstrap;
  if (topicInput && kafka_topic) topicInput.value = kafka_topic;
  if (cpsInput && chars_per_second != null) cpsInput.value = String(chars_per_second);
  if (jitterInput && pace_jitter_pct != null) jitterInput.value = String(Math.round(Number(pace_jitter_pct) * 100));

  if (preview) {
    renderLiveChatPreview(preview);
  } else if (!options.keepPreview) {
    renderLiveChatPreview(null);
  }
  if (options.renderTimeline !== false) {
    renderLiveChatTimeline(snapshot);
  }
  setLiveChatConversationPill(conversation_id);
  const bannerState = _liveChatNormState(liveChatSnapshot?.state ?? state) ?? 'idle';
  setLiveChatStatusBanner(_liveChatPrimaryMessage(liveChatSnapshot), bannerState);
  updateLiveChatControls();
}

async function previewLiveChatCsv(showAll = false) {
  if (!liveChatCsvText) {
    liveChatPreviewShowAll = false;
    renderLiveChatPreview(null);
    updateLiveChatControls(liveChatSnapshot);
    return;
  }
  try {
    const resp = await fetch(`${API}/api/live/preview`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        csv_text: liveChatCsvText,
        csv_filename: liveChatCsvFilename || 'conversation.csv',
        show_all: showAll,
      }),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      liveChatPreviewShowAll = false;
      renderLiveChatPreview(null);
      setLiveChatCsvErrorState(data.detail || 'CSV validation failed. Upload a valid CSV before starting.');
      setLiveChatStatusBanner(data.detail || 'Preview failed', 'failed');
      updateLiveChatControls({ state: 'idle' });
      return;
    }
    liveChatPreviewShowAll = !!showAll;
    renderLiveChatPreview(data);
    setLiveChatStatusBanner(`Ready · ${data.row_count} rows`, 'idle');
    updateLiveChatControls({ state: 'idle', preview: data });
  } catch (e) {
    liveChatPreviewShowAll = false;
    renderLiveChatPreview(null);
    setLiveChatStatusBanner('Preview request failed', 'failed');
    updateLiveChatControls({ state: 'idle' });
  }
}

function showAllLiveChatPreview() {
  previewLiveChatCsv(!liveChatPreviewShowAll);
}

async function handleLiveChatFileSelection(fileList) {
  const file = fileList && fileList[0];
  if (!file) return;
  clearLiveChatCsvErrorState();
  liveChatCsvFilename = file.name;
  liveChatCsvText = await file.text();
  liveChatPreviewShowAll = false;
  const label = document.getElementById('livechat-file-name');
  if (label) {
    const kb = Math.max(1, Math.round(file.size / 1024));
    label.textContent = `${file.name} (${kb} KB)`;
  }
  await previewLiveChatCsv();
}

async function startLiveChat() {
  switchTab('livechat');
  if (_liveChatStartInFlight) return;
  const startBtnGuard = document.getElementById('btn-livechat-start');
  if (
    startBtnGuard &&
    (startBtnGuard.disabled || startBtnGuard.getAttribute('data-livechat-busy') === '1')
  ) {
    return;
  }
  _liveChatStartInFlight = true;
  try {
    if (!liveChatCsvText) {
      setLiveChatCsvErrorState('CSV script required. Drop a CSV here or click Choose CSV before starting Mock Live-Chat.');
      setLiveChatStatusBanner('CSV required', 'failed');
      return;
    }
    const previewReady = document.getElementById('livechat-preview-summary')?.dataset.ready === '1';
    if (!previewReady) {
      await previewLiveChatCsv(liveChatPreviewShowAll);
      if (document.getElementById('livechat-preview-summary')?.dataset.ready !== '1') {
        setLiveChatStatusBanner('Upload a valid CSV before starting', 'failed');
        return;
      }
    }
    const charsPerSecond = Number(document.getElementById('livechat-chars-per-second').value);
    const jitterPct = Number(document.getElementById('livechat-jitter-pct').value);
    if (!Number.isFinite(charsPerSecond) || charsPerSecond <= 0) {
      const cpsInput = document.getElementById('livechat-chars-per-second');
      if (cpsInput) cpsInput.focus();
      setLiveChatStatusBanner('Chars per Second must be greater than 0', 'failed');
      return;
    }
    if (!Number.isFinite(jitterPct) || jitterPct < 0 || jitterPct > 100) {
      const jitterInput = document.getElementById('livechat-jitter-pct');
      if (jitterInput) jitterInput.focus();
      setLiveChatStatusBanner('Typing Jitter (%) must be between 0 and 100', 'failed');
      return;
    }
    const existing = _liveChatNormState(liveChatSnapshot?.state);
    if (existing === 'running' || existing === 'stopping') {
      return;
    }
    const conversationId = makeLiveChatConversationId();
    const payload = {
      csv_text: liveChatCsvText,
      csv_filename: liveChatCsvFilename || 'conversation.csv',
      ws_url: document.getElementById('livechat-ws-url').value,
      kafka_bootstrap: document.getElementById('livechat-kafka-bootstrap').value,
      kafka_topic: document.getElementById('livechat-kafka-topic').value,
      conversation_id: conversationId,
      chars_per_second: charsPerSecond,
      pace_jitter_pct: jitterPct / 100,
    };
    _liveChatStartLatch = true;
    updateLiveChatControls({
      state: 'running',
      preview: liveChatSnapshot?.preview,
    });
    try {
      const resp = await fetch(`${API}/api/live/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        setLiveChatStatusBanner(data.detail || 'Start failed', 'failed');
        updateLiveChatControls({
          state: 'idle',
          preview: liveChatSnapshot?.preview,
        });
        return;
      }
      applyLiveChatSnapshot(data, { renderTimeline: true, keepPreview: true });
    } catch (e) {
      setLiveChatStatusBanner('Start request failed', 'failed');
      updateLiveChatControls({
        state: 'idle',
        preview: liveChatSnapshot?.preview,
      });
    }
  } finally {
    _liveChatStartInFlight = false;
  }
}

async function stopLiveChat() {
  try {
    const resp = await fetch(`${API}/api/live/stop`, { method: 'POST' });
    const data = await resp.json().catch(() => ({}));
    if (resp.ok) {
      applyLiveChatSnapshot(data, { renderTimeline: true, keepPreview: true });
    } else {
      setLiveChatStatusBanner(data.detail || 'Stop failed', 'failed');
    }
  } catch (e) {
    setLiveChatStatusBanner('Stop request failed', 'failed');
  }
}

async function clearLiveChat() {
  try {
    const resp = await fetch(`${API}/api/live/clear`, { method: 'POST' });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      setLiveChatStatusBanner(data.detail || 'Clear failed', 'failed');
      return;
    }
    liveChatCsvText = '';
    liveChatCsvFilename = '';
    liveChatPreviewShowAll = false;
    const label = document.getElementById('livechat-file-name');
    if (label) label.textContent = 'No CSV selected';
    const fileInput = document.getElementById('livechat-file-input');
    if (fileInput) fileInput.value = '';
    renderLiveChatPreview(null);
    applyLiveChatSnapshot(data, { renderTimeline: true });
  } catch (e) {
    setLiveChatStatusBanner('Clear request failed', 'failed');
  }
}

async function loadLiveChatStatus() {
  try {
    const resp = await fetch(`${API}/api/live/status`);
    const data = await resp.json().catch(() => ({}));
    if (resp.ok) applyLiveChatSnapshot(data, { renderTimeline: true, keepPreview: true });
  } catch (e) {}
}

function initLiveChatDropzone() {
  const zone = document.getElementById('livechat-dropzone');
  const input = document.getElementById('livechat-file-input');
  if (!zone || !input) return;
  zone.addEventListener('click', () => triggerLiveChatFileInput());
  zone.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      triggerLiveChatFileInput();
    }
  });
  input.addEventListener('change', () => handleLiveChatFileSelection(input.files));
  ['dragenter', 'dragover'].forEach(name => {
    zone.addEventListener(name, (event) => {
      event.preventDefault();
      zone.classList.add('is-dragover');
    });
  });
  ['dragleave', 'dragend', 'drop'].forEach(name => {
    zone.addEventListener(name, (event) => {
      event.preventDefault();
      zone.classList.remove('is-dragover');
    });
  });
  zone.addEventListener('drop', (event) => {
    const files = event.dataTransfer && event.dataTransfer.files;
    if (files && files.length) {
      handleLiveChatFileSelection(files);
    }
  });
}

// ---------------------------------------------------------------------------
// Kafka
// ---------------------------------------------------------------------------
function _getKafkaTargetKey() {
  const bootstrap = document.getElementById('kafka-bootstrap')?.value?.trim() || '';
  const topic = document.getElementById('kafka-topic')?.value?.trim() || '';
  return `${bootstrap}::${topic}`;
}

function _getKafkaCidFilter() {
  return _kafkaCidFilter;
}

function _getSelectedKafkaConversationId() {
  const value = _getKafkaCidFilter();
  return value && knownConversationIds.has(value) ? value : '';
}

function _setKafkaStatus(text, color = 'var(--text2)') {
  const el = document.getElementById('kafka-status');
  if (!el) return;
  el.textContent = text;
  el.style.color = color;
}

function _syncKafkaCidPlaceholder() {
  const input = document.getElementById('kafka-cid-select');
  if (!input) return;
  if (_kafkaInventoryLoading) {
    input.placeholder = 'Discovering conversationIds...';
    return;
  }
  if (_getSelectedKafkaConversationId()) {
    input.placeholder = 'Selected conversationId';
    return;
  }
  if (_kafkaInventoryKey && _kafkaInventoryKey === _getKafkaTargetKey()) {
    input.placeholder = knownConversationIds.size
      ? `Select conversationId (${knownConversationIds.size} found)`
      : 'No conversationIds found';
    return;
  }
  input.placeholder = 'Open to discover conversationIds';
}

function _renderKafkaCidMenu() {
  const menu = document.getElementById('kafka-cid-menu');
  if (!menu) return;
  menu.innerHTML = '';
  const currentFilter = _getKafkaCidFilter();
  const normalized = currentFilter.toLowerCase();

  if (_kafkaInventoryLoading) {
    const empty = document.createElement('div');
    empty.className = 'kafka-cid-empty';
    empty.textContent = 'Scanning Kafka for conversationIds...';
    menu.appendChild(empty);
    return;
  }

  if (_kafkaInventoryError) {
    const empty = document.createElement('div');
    empty.className = 'kafka-cid-empty';
    empty.textContent = _kafkaInventoryError;
    menu.appendChild(empty);
    return;
  }

  const summary = document.createElement('div');
  summary.className = 'kafka-cid-summary';
  summary.textContent = `${knownConversationIds.size} conversationIds found`;
  menu.appendChild(summary);

  const ids = [...knownConversationIds]
    .sort()
    .filter(id => !normalized || id.toLowerCase().includes(normalized));

  if (!ids.length) {
    const empty = document.createElement('div');
    empty.className = 'kafka-cid-empty';
    empty.textContent = currentFilter ? 'No matching conversationId values.' : 'No conversationIds found in this topic.';
    menu.appendChild(empty);
    return;
  }

  ids.forEach(id => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'kafka-cid-option' + (id === currentFilter ? ' is-active' : '');
    btn.innerHTML = `<span class="kafka-cid-option-label">${escapeHtml(id)}</span>`;
    btn.onclick = () => {
      _setKafkaCidFilter(id);
      closeKafkaCidMenu();
    };
    menu.appendChild(btn);
  });
}

function _setKafkaCidFilter(value) {
  const input = document.getElementById('kafka-cid-select');
  _kafkaCidFilter = String(value || '').trim();
  if (!input) return;
  input.value = _kafkaCidFilter;
  _kafkaPage = 1;
  renderKafkaPage();
  _syncKafkaCidPlaceholder();
  _renderKafkaCidMenu();
  updateKafkaConsumerControls();
}

function syncKafkaIdleStatus() {
  if (_kafkaConsumerConnected) return;
  if (_kafkaInventoryLoading) {
    _setKafkaStatus('○ Discovering conversationIds...', 'var(--yellow)');
    return;
  }
  if (_kafkaInventoryError) {
    _setKafkaStatus('✗ ' + _kafkaInventoryError, 'var(--red)');
    return;
  }
  const selected = _getSelectedKafkaConversationId();
  if (selected) {
    _setKafkaStatus('○ Ready to consume ' + selected, 'var(--text2)');
    return;
  }
  if (_kafkaInventoryKey && _kafkaInventoryKey === _getKafkaTargetKey()) {
    _setKafkaStatus(`○ ${knownConversationIds.size} conversationIds ready`, 'var(--text2)');
    return;
  }
  _setKafkaStatus('○ Open the conversationId list to discover Kafka IDs', 'var(--text2)');
}

function updateKafkaConsumerControls() {
  const startBtn = document.getElementById('btn-kafka-start');
  const stopBtn = document.getElementById('btn-kafka-stop');
  const input = document.getElementById('kafka-cid-select');
  const toggle = document.getElementById('kafka-cid-toggle');
  const bootstrapInput = document.getElementById('kafka-bootstrap');
  const topicInput = document.getElementById('kafka-topic');
  const selected = _getSelectedKafkaConversationId();
  const locked = _kafkaConsumerConnected;
  const isSameSelection = Boolean(selected) && selected === _kafkaActiveConversationId;

  if (bootstrapInput) bootstrapInput.disabled = locked;
  if (topicInput) topicInput.disabled = locked;
  if (input) input.disabled = false;
  if (toggle) toggle.disabled = false;

  if (startBtn) {
    if (locked && isSameSelection) {
      startBtn.textContent = 'Consuming';
      startBtn.disabled = true;
      startBtn.title = `Already consuming ${selected}`;
    } else if (locked) {
      startBtn.textContent = 'Consume Selected';
      startBtn.disabled = _kafkaInventoryLoading || !selected;
      startBtn.title = selected ? `Switch Kafka consumer to ${selected}` : 'Pick one conversationId first';
    } else {
      startBtn.textContent = 'Consume Selected';
      startBtn.disabled = _kafkaInventoryLoading || !selected;
      startBtn.title = selected ? `Start consuming Kafka for ${selected}` : 'Pick one conversationId first';
    }
  }
  if (stopBtn) {
    stopBtn.disabled = !locked;
  }

  if (!locked) {
    syncKafkaIdleStatus();
  }
}

function invalidateKafkaConversationInventory() {
  if (_kafkaConsumerConnected) return;
  _kafkaInventoryKey = '';
  _kafkaInventoryError = '';
  _kafkaInventoryLoading = false;
  _kafkaInventoryPromise = null;
  _kafkaCidFilter = '';
  knownConversationIds.clear();
  const input = document.getElementById('kafka-cid-select');
  if (input) input.value = '';
  closeKafkaCidMenu();
  _syncKafkaCidPlaceholder();
  _renderKafkaCidMenu();
  updateKafkaConsumerControls();
}

async function ensureKafkaConversationInventory(force = false) {
  const bootstrap = document.getElementById('kafka-bootstrap')?.value?.trim() || '';
  const topic = document.getElementById('kafka-topic')?.value?.trim() || '';
  const key = _getKafkaTargetKey();

  if (!bootstrap || !topic) {
    _kafkaInventoryError = 'Fill in Bootstrap and Topic first.';
    _renderKafkaCidMenu();
    updateKafkaConsumerControls();
    return false;
  }

  if (!force && _kafkaInventoryKey === key) {
    _syncKafkaCidPlaceholder();
    _renderKafkaCidMenu();
    updateKafkaConsumerControls();
    return true;
  }

  if (_kafkaInventoryPromise) {
    return _kafkaInventoryPromise;
  }

  _kafkaInventoryLoading = true;
  _kafkaInventoryError = '';
  _syncKafkaCidPlaceholder();
  _renderKafkaCidMenu();
  updateKafkaConsumerControls();

  _kafkaInventoryPromise = (async () => {
    try {
      const resp = await fetch(
        `${API}/api/kafka/conversations?bootstrap=${encodeURIComponent(bootstrap)}&topic=${encodeURIComponent(topic)}`,
      );
      const data = await resp.json();
      if (!resp.ok || data.status === 'error') {
        throw new Error(data.error || 'Failed to discover conversationIds');
      }
      knownConversationIds.clear();
      (data.conversations || []).forEach(item => {
        if (item?.conversation_id) knownConversationIds.add(String(item.conversation_id));
      });
      _kafkaInventoryKey = key;
    } catch (e) {
      knownConversationIds.clear();
      _kafkaInventoryKey = '';
      _kafkaInventoryError = e instanceof Error ? e.message : String(e);
      return false;
    } finally {
      _kafkaInventoryLoading = false;
      _kafkaInventoryPromise = null;
      _syncKafkaCidPlaceholder();
      _renderKafkaCidMenu();
      updateKafkaConsumerControls();
    }
    return true;
  })();

  return _kafkaInventoryPromise;
}

async function openKafkaCidMenu() {
  const box = document.getElementById('kafka-cid-combobox');
  const menu = document.getElementById('kafka-cid-menu');
  const toggle = document.getElementById('kafka-cid-toggle');
  if (!box || !menu || !toggle) return;
  box.classList.add('is-open');
  menu.hidden = false;
  toggle.setAttribute('aria-expanded', 'true');
  await ensureKafkaConversationInventory();
  _renderKafkaCidMenu();
}

function closeKafkaCidMenu() {
  const box = document.getElementById('kafka-cid-combobox');
  const menu = document.getElementById('kafka-cid-menu');
  const toggle = document.getElementById('kafka-cid-toggle');
  if (box) box.classList.remove('is-open');
  if (menu) menu.hidden = true;
  if (toggle) toggle.setAttribute('aria-expanded', 'false');
}

function toggleKafkaCidMenu() {
  const menu = document.getElementById('kafka-cid-menu');
  const input = document.getElementById('kafka-cid-select');
  if (!menu) return;
  if (menu.hidden) {
    openKafkaCidMenu();
    if (input) input.focus();
  } else {
    closeKafkaCidMenu();
  }
}

function onKafkaCidSelectInput(event) {
  _kafkaCidFilter = String(event?.target?.value || '').trim();
  updateKafkaConsumerControls();
  openKafkaCidMenu();
}

function handleKafkaCidInputKeydown(event) {
  if (!event) return;
  if (event.key === 'Escape') {
    closeKafkaCidMenu();
    event.preventDefault();
    return;
  }
  if (event.key === 'ArrowDown') {
    openKafkaCidMenu();
  }
}

function rebuildKafkaCidSelect() {
  const input = document.getElementById('kafka-cid-select');
  if (!input) return;
  const prev = _getKafkaCidFilter();
  _syncKafkaCidPlaceholder();
  input.value = prev;
  _renderKafkaCidMenu();
  updateKafkaConsumerControls();
  renderKafkaPage();
}

function onKafkaCidSelectChange() {
  _kafkaPage = 1;
  renderKafkaPage();
}

function _getKafkaFiltered() {
  const filter = _getSelectedKafkaConversationId();
  if (!filter) return _kafkaAllMessages;
  return _kafkaAllMessages.filter(m => {
    const cid = m.value?.metaData?.conversationId || m.key || '';
    return String(cid) === filter;
  });
}

function updateKafkaExpandButtonLabel() {
  const btn = document.getElementById('btn-kafka-toggle-expand');
  if (!btn) return;
  btn.textContent = kafkaExpandAll ? 'Collapse All' : 'Expand All';
  btn.title = kafkaExpandAll ? 'Collapse currently visible messages' : 'Expand currently visible messages';
}

function toggleAllKafkaMessages(forceExpanded) {
  const container = document.getElementById('kafka-messages');
  if (!container) return;
  kafkaExpandAll = typeof forceExpanded === 'boolean' ? forceExpanded : !kafkaExpandAll;
  for (const el of container.children) {
    if (!(el instanceof HTMLElement)) continue;
    if (el.style.display === 'none') continue;
    el.classList.toggle('expanded', kafkaExpandAll);
  }
  updateKafkaExpandButtonLabel();
}

async function copySingleKafkaJson(btnEl) {
  const card = btnEl && btnEl.closest ? btnEl.closest('.kafka-msg') : null;
  if (!card) return;
  const raw = card.dataset.rawJson;
  if (!raw) return;
  const prev = btnEl.textContent;
  try {
    const obj = JSON.parse(raw);
    await navigator.clipboard.writeText(JSON.stringify(obj, null, 2));
    btnEl.textContent = 'Copied';
  } catch (e) {
    btnEl.textContent = 'Failed';
  }
  setTimeout(() => { btnEl.textContent = prev; }, 900);
}

async function replayKafkaMessage(btnEl) {
  const card = btnEl && btnEl.closest ? btnEl.closest('.kafka-msg') : null;
  if (!card) return;
  const raw = card.dataset.rawJson;
  if (!raw) return;
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch (e) {
    return;
  }
  const cid = payload?.metaData?.conversationId;
  const wsUrl = document.getElementById('ws-url')?.value || '';
  if (!cid || !wsUrl) return;
  const url = `${wsUrl}?conversationId=${encodeURIComponent(cid)}`;
  btnEl.disabled = true;
  const prev = btnEl.textContent;
  btnEl.textContent = 'Replaying...';
  try {
    const ws = new WebSocket(url);
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timeout')), 8000);
      ws.onopen = () => ws.send(JSON.stringify(payload));
      ws.onmessage = () => {
        clearTimeout(timer);
        ws.close();
        resolve(null);
      };
      ws.onerror = () => {
        clearTimeout(timer);
        try { ws.close(); } catch (e) {}
        reject(new Error('ws'));
      };
    });
    btnEl.textContent = 'Replayed';
  } catch (e) {
    btnEl.textContent = 'Failed';
  } finally {
    setTimeout(() => {
      btnEl.disabled = false;
      btnEl.textContent = prev || 'Replay';
    }, 900);
  }
}

async function startKafka() {
  const bs = document.getElementById('kafka-bootstrap').value.trim();
  const topic = document.getElementById('kafka-topic').value.trim();
  const conversationId = _getSelectedKafkaConversationId();
  const btn = document.getElementById('btn-kafka-start');
  if (!conversationId) {
    updateKafkaConsumerControls();
    return;
  }
  btn.disabled = true;
  btn.textContent = 'Connecting...';
  _setKafkaStatus('○ Connecting...', 'var(--yellow)');
  clearKafkaRenderedMessagesOnly();
  try {
    const resp = await fetch(
      `${API}/api/kafka/start?bootstrap=${encodeURIComponent(bs)}&topic=${encodeURIComponent(topic)}&conversation_id=${encodeURIComponent(conversationId)}`,
      { method: 'POST' },
    );
    const data = await resp.json();
    if (data.status === 'kafka_consumer_failed') {
      _kafkaConsumerConnected = false;
      _kafkaActiveConversationId = '';
      _setKafkaStatus('✗ ' + data.error, 'var(--red)');
    } else if (data.status === 'kafka_consumer_started') {
      _kafkaConsumerConnected = true;
      _kafkaActiveConversationId = conversationId;
      _setKafkaStatus(`● Connected ${(data.topic || topic)} · ${conversationId}`, 'var(--green)');
    }
  } catch (e) {
    _kafkaConsumerConnected = false;
    _kafkaActiveConversationId = '';
    _setKafkaStatus('✗ Request failed', 'var(--red)');
  } finally {
    updateKafkaConsumerControls();
  }
}

async function stopKafka() {
  _setKafkaStatus('○ Disconnecting...', 'var(--yellow)');
  await fetch(`${API}/api/kafka/stop`, { method: 'POST' });
}

function clearKafkaRenderedMessagesOnly() {
  _kafkaAllMessages.length = 0;
  _kafkaPage = 1;
  kafkaExpandAll = false;
  updateKafkaExpandButtonLabel();
  renderKafkaPage();
}

function clearKafkaMessagesOnly() {
  _kafkaAllMessages.length = 0;
  _kafkaPage = 1;
  _kafkaCidFilter = '';
  _kafkaInventoryKey = '';
  _kafkaInventoryError = '';
  _kafkaInventoryLoading = false;
  _kafkaInventoryPromise = null;
  _kafkaConsumerConnected = false;
  _kafkaActiveConversationId = '';
  knownConversationIds.clear();
  closeKafkaCidMenu();
  kafkaExpandAll = false;
  updateKafkaExpandButtonLabel();
  rebuildKafkaCidSelect();
  renderKafkaPage();
}

function addKafkaMessage(msg) {
  _kafkaAllMessages.push(msg);
  const cidRaw = msg.value?.metaData?.conversationId || msg.key || '-';

  const filtered = _getKafkaFiltered();
  const totalPages = Math.max(1, Math.ceil(filtered.length / _kafkaPageSize));
  const wasOnLast = (_kafkaPage >= totalPages - 1) || (_kafkaPage >= totalPages);
  if (wasOnLast) {
    _kafkaPage = totalPages;
    renderKafkaPage();
  } else {
    _updateKafkaPager(filtered);
  }
}

function _buildKafkaMsgEl(msg) {
  const div = document.createElement('div');
  div.className = 'kafka-msg';
  div.onclick = () => div.classList.toggle('expanded');

  const cidRaw = msg.value?.metaData?.conversationId || msg.key || '-';
  const cidDisp = cidRaw === '-' ? '-' : cidRaw;
  div.dataset.cid = String(cidRaw === '-' ? '' : cidRaw);

  const sn = msg.value?.payload?.sequenceNumber;
  const hasSeq = sn !== null && sn !== undefined && sn !== '' && Number.isFinite(Number(sn));
  const seqLabel = hasSeq ? `seq ${Number(sn)}` : 'seq —';
  const seqClass = hasSeq ? 'kafka-meta-seq' : 'kafka-meta-seq kafka-meta-seq--na';
  const jsonText = JSON.stringify(msg.value, null, 2);
  const escapedJson = jsonText
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
  const highlightedJson = escapedJson.replace(
    /("(?:\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*")(\s*:)?|\b(true|false|null)\b|-?\b\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?\b/g,
    (m, strToken, isKey, boolOrNull) => {
      if (isKey) return `<span class="json-key">${strToken}</span>:`;
      if (strToken) return `<span class="json-string">${strToken}</span>`;
      if (boolOrNull === 'true' || boolOrNull === 'false') return `<span class="json-bool">${boolOrNull}</span>`;
      if (boolOrNull === 'null') return `<span class="json-null">null</span>`;
      return `<span class="json-number">${m}</span>`;
    },
  );

  div.innerHTML = `
    <div class="meta">
      <span class="${seqClass}" title="payload.sequenceNumber">${seqLabel}</span>
      <span>P${msg.partition}:${msg.offset}</span>
      <span>${cidDisp}</span>
      <span>${new Date(msg.timestamp).toLocaleTimeString()}</span>
      <button type="button" class="panel-header-action" onclick="event.stopPropagation(); replayKafkaMessage(this)" title="Replay this JSON through the current Scenario WebSocket URL">Replay</button>
      <button type="button" class="panel-header-action" onclick="event.stopPropagation(); copySingleKafkaJson(this)" title="Copy this message JSON">Copy</button>
    </div>
    <pre class="body"><code>${highlightedJson}</code></pre>
  `;
  div.dataset.rawJson = JSON.stringify(msg.value ?? {});
  if (kafkaExpandAll) div.classList.add('expanded');
  return div;
}

function renderKafkaPage() {
  const container = document.getElementById('kafka-messages');
  container.innerHTML = '';
  const filtered = _getKafkaFiltered();
  const totalPages = Math.max(1, Math.ceil(filtered.length / _kafkaPageSize));
  if (_kafkaPage > totalPages) _kafkaPage = totalPages;
  if (_kafkaPage < 1) _kafkaPage = 1;

  const start = (_kafkaPage - 1) * _kafkaPageSize;
  const end = Math.min(start + _kafkaPageSize, filtered.length);
  const frag = document.createDocumentFragment();
  for (let i = start; i < end; i++) {
    frag.appendChild(_buildKafkaMsgEl(filtered[i]));
  }
  container.appendChild(frag);
  container.scrollTop = 0;

  _updateKafkaPager(filtered);
}

function _updateKafkaPager(filtered) {
  if (!filtered) filtered = _getKafkaFiltered();
  const total = _kafkaAllMessages.length;
  const fLen = filtered.length;
  const totalPages = Math.max(1, Math.ceil(fLen / _kafkaPageSize));
  const hasFilter = Boolean(_getKafkaCidFilter());

  const summary = document.getElementById('kafka-pager-summary');
  if (summary) {
    if (hasFilter) {
      summary.innerHTML = `Filtered <span class="kafka-pager-total">${fLen}</span> records (total ${total})`;
    } else {
      summary.innerHTML = `Total <span class="kafka-pager-total">${total}</span> records`;
    }
  }
  document.getElementById('kafka-count').textContent = fLen;

  const pos = document.getElementById('kafka-pager-pos');
  if (pos) pos.textContent = `${_kafkaPage} / ${totalPages}`;

  const pager = document.getElementById('kafka-pager');
  if (pager) {
    const btns = pager.querySelectorAll('button');
    btns[0].disabled = _kafkaPage <= 1;
    btns[1].disabled = _kafkaPage <= 1;
    btns[2].disabled = _kafkaPage >= totalPages;
    btns[3].disabled = _kafkaPage >= totalPages;
  }

  _syncKafkaCidCounts();
}

function _syncKafkaCidCounts() {
  _syncKafkaCidPlaceholder();
  _renderKafkaCidMenu();
}

function kafkaPageGo(action) {
  const filtered = _getKafkaFiltered();
  const totalPages = Math.max(1, Math.ceil(filtered.length / _kafkaPageSize));
  switch (action) {
    case 'first': _kafkaPage = 1; break;
    case 'prev':  _kafkaPage = Math.max(1, _kafkaPage - 1); break;
    case 'next':  _kafkaPage = Math.min(totalPages, _kafkaPage + 1); break;
    case 'last':  _kafkaPage = totalPages; break;
  }
  renderKafkaPage();
}

function onKafkaPageSizeChange(val) {
  _kafkaPageSize = Math.max(10, Number(val) || 50);
  _kafkaPage = 1;
  renderKafkaPage();
}

function clearKafka() {
  _kafkaAllMessages.length = 0;
  _kafkaPage = 1;
  _kafkaCidFilter = '';
  _kafkaInventoryKey = '';
  _kafkaInventoryError = '';
  _kafkaInventoryLoading = false;
  _kafkaInventoryPromise = null;
  _kafkaConsumerConnected = false;
  _kafkaActiveConversationId = '';
  knownConversationIds.clear();
  const input = document.getElementById('kafka-cid-select');
  if (input) input.value = '';
  closeKafkaCidMenu();
  kafkaExpandAll = false;
  updateKafkaExpandButtonLabel();
  rebuildKafkaCidSelect();
  renderKafkaPage();
}

function extractErrorCode(e) {
  const c = e?.server_resp?.error?.code;
  if (c) return String(c);
  const m = (e?.detail || '').match(/\bE\d{4}\b/);
  return m ? m[0] : 'UNKNOWN';
}

function renderHeatstrip() {
  const el = document.getElementById('latency-heatstrip');
  if (!el) return;
  el.innerHTML = '';
  const vals = latencyHistory.map((x) => Number(x.v)).filter((n) => Number.isFinite(n));
  if (vals.length === 0) {
    el.innerHTML = '<span class="heatstrip-empty">Waiting for load-test data...</span>';
    return;
  }
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const range = Math.max(1, max - min);
  latencyHistory.forEach((p) => {
    const v = Number.isFinite(Number(p.v)) ? Number(p.v) : 0;
    const bar = document.createElement('span');
    bar.className = 'bar';
    const rel = (v - min) / range;
    const abs = Math.min(v / 80, 1);
    const score = 0.68 * rel + 0.32 * abs;
    const h = 8 + Math.round(score * 52);
    bar.style.height = `${h}px`;
    bar.style.background = p.s ? 'rgba(198,120,221,0.55)' : 'rgba(97,175,239,0.55)';
    bar.title = `${p.s ? 'server' : 'e2e'} P95=${v} ms`;
    el.appendChild(bar);
  });
}

function renderErrorRank(errs) {
  const el = document.getElementById('error-rank-list');
  if (!el) return;
  const rank = {};
  (errs || []).forEach((e) => {
    const code = extractErrorCode(e);
    rank[code] = (rank[code] || 0) + 1;
  });
  const rows = Object.entries(rank).sort((a, b) => b[1] - a[1]).slice(0, 8);
  el.innerHTML = rows.length
    ? rows.map(([c, n]) => `<li><span>${c}</span><strong>${n}</strong></li>`).join('')
    : '<li><span>No errors</span><strong>0</strong></li>';
}

function renderLifecycle(d) {
  const el = document.getElementById('lifecycle-line');
  if (!el) return;
  el.innerHTML = `
    <span class="lifecycle-dot"></span><span>Connect ${d.active_connections ?? 0}</span>
    <span>→</span><span>Send ${d.sent ?? 0}</span>
    <span>→</span><span>Ack ${d.ack ?? 0}</span>
    <span>→</span><span>Error ${d.error ?? 0}</span>
  `;
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------
function updateStats(d) {
  const fmtInt = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? Math.round(n) : 0;
  };
  document.getElementById('stat-sent').textContent = d.sent ?? 0;
  document.getElementById('stat-ack').textContent = d.ack ?? 0;
  document.getElementById('stat-error').textContent = d.error ?? 0;
  document.getElementById('stat-active').textContent = d.active_connections ?? 0;
  document.getElementById('stat-tps').textContent = fmtInt(d.ack_tps ?? d.tps);
  document.getElementById('stat-p50').textContent = fmtInt(d.p50_ms);
  document.getElementById('stat-p95').textContent = fmtInt(d.p95_ms);
  document.getElementById('stat-p99').textContent = fmtInt(d.p99_ms);
  document.getElementById('stat-server-p50').textContent = fmtInt(d.server_p50_ms);
  document.getElementById('stat-server-p95').textContent = fmtInt(d.server_p95_ms);
  document.getElementById('stat-server-p99').textContent = fmtInt(d.server_p99_ms);

  /* Only append while the current load-test UI is running. Do not key off d.load_running, because the server may briefly emit load_running after load_done and keep the heatstrip alive. */
  if (_loadUiRunning) {
    const e2e = Number(d.p95_ms || 0);
    const srv = Number(d.server_p95_ms || 0);
    const hasSample = (Number(d.sent || 0) > 0 || Number(d.ack || 0) > 0);
    if (hasSample || e2e > 0 || srv > 0) {
      latencyHistory.push({ v: e2e, s: 0 });
      latencyHistory.push({ v: srv, s: 1 });
      while (latencyHistory.length > 60) latencyHistory.shift();
    }
  }
  renderHeatstrip();
  renderErrorRank(d.recent_errors || []);
  renderLifecycle(d);

  const panel = document.getElementById('load-errors-panel');
  const pre = document.getElementById('load-errors-pre');
  const errs = d.recent_errors;
  if (panel && pre) {
    if (errs && errs.length > 0) {
      panel.style.display = '';
      pre.textContent = errs.map((e) => {
        const seq = e.seq !== undefined && e.seq !== null ? ` seq=${e.seq}` : '';
        const et = e.eventType != null ? ` eventType=${e.eventType}` : '';
        let lines = `[${e.stage}] ${e.cid}${seq}${et}\n  ${e.detail || ''}`;
        if (e.server_resp) {
          lines += `\n  ┗ Server raw response:\n  ${JSON.stringify(e.server_resp, null, 2).replace(/\n/g, '\n  ')}`;
        }
        return lines;
      }).join('\n\n');
    } else {
      panel.style.display = 'none';
      pre.textContent = '';
    }
  }
}

