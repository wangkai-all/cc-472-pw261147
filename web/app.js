/* Pointwise DQ console — vanilla JS SPA, no build step. */
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
function unescapeHtml(s) {
  const map = { "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&#39;": "'" };
  return String(s).replace(/&(amp|lt|gt|quot|#39);/g, (m) => map[m]);
}
const pill = (v) => `<span class="pill ${esc(v)}">${esc(v)}</span>`;
const pretty = (o) => esc(JSON.stringify(o, null, 2));

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) {
    const detail = data?.detail;
    const msg = typeof detail === "string" ? detail : JSON.stringify(detail);
    throw new Error(`${res.status}: ${msg}`);
  }
  return data;
}

function toast(msg, kind = "") {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = msg;
  $("#toast-host").appendChild(el);
  setTimeout(() => el.remove(), 4200);
}

function modal(html) {
  const host = $("#modal-host");
  host.innerHTML = `<div class="modal-backdrop"><div class="modal">${html}
    <div class="actions"><button class="btn ghost" id="modal-close">关闭</button></div></div></div>`;
  $("#modal-close").onclick = () => (host.innerHTML = "");
  $(".modal-backdrop", host).onclick = (e) => {
    if (e.target.classList.contains("modal-backdrop")) host.innerHTML = "";
  };
  return host;
}

const state = { tab: "ingest", dataset: "orders", selectedRoot: null };

/* ============================================================== ① ingest */
async function renderIngest() {
  const batches = await api("/api/batches");
  const view = $("#view");
  view.innerHTML = `
    <div class="grid cols-3" style="margin-bottom:18px">
      <div class="card"><div class="muted">批次总数</div><div class="stat">${batches.length}</div></div>
      <div class="card"><div class="muted">接入批次</div><div class="stat amber">${batches.filter(b => b.source === "ingest").length}</div></div>
      <div class="card"><div class="muted">回放批次</div><div class="stat" style="color:var(--purple)">${batches.filter(b => b.source === "replay").length}</div></div>
    </div>
    <div class="card">
      <h2>接入新数据批次</h2>
      <div class="row" style="margin-bottom:12px">
        <div style="max-width:260px"><label>数据集</label>
          <input id="in-dataset" value="${esc(state.dataset)}"></div>
        <div style="max-width:200px"><label>空批次测试</label>
          <button class="btn ghost" id="btn-empty">提交空批次 (应被拒绝)</button></div>
      </div>
      <label>记录 JSON 数组</label>
      <textarea id="in-rows" style="min-height:200px">[
  {"order_id": "O-1001", "customer_id": "C-001", "amount": 120.5, "quantity": 2, "status": "paid"},
  {"order_id": "O-1002", "customer_id": "C-999", "amount": -5, "quantity": 1, "status": "paid"}
]</textarea>
      <div style="margin-top:12px"><button class="btn" id="btn-ingest">执行质量检查并接入</button></div>
    </div>
    <div class="card section-gap">
      <h2>批次列表</h2>
      ${batches.length ? batchTable(batches) : '<div class="empty">暂无批次</div>'}
    </div>`;

  $("#btn-empty").onclick = async () => {
    try {
      await api("/api/ingest", { method: "POST", body: JSON.stringify({ dataset: $("#in-dataset").value, rows: [] }) });
      toast("未预期：空批次被接受", "error");
    } catch (e) { toast(`空批次已被拒绝 ✔ ${e.message}`, "ok"); }
  };
  $("#btn-ingest").onclick = async () => {
    let rows;
    try { rows = JSON.parse($("#in-rows").value); }
    catch (e) { return toast("JSON 解析失败: " + e.message, "error"); }
    state.dataset = $("#in-dataset").value;
    try {
      const r = await api("/api/ingest", { method: "POST", body: JSON.stringify({ dataset: state.dataset, rows }) });
      toast(`批次 ${r.batch.batch_id} 已检查：失败 ${r.execution.failed} 条`, "ok");
      state.selectedRoot = r.batch.root_batch_id;
      go("gate");
    } catch (e) { toast(e.message, "error"); }
  };
  document.querySelectorAll("[data-gate]").forEach((btn) => {
    btn.onclick = () => { state.selectedRoot = btn.dataset.gate; go("gate"); };
  });
}

function batchTable(batches) {
  return `<div class="scrollx"><table><thead><tr>
    <th>批次 ID</th><th>数据集</th><th>来源</th><th>状态</th><th>记录数</th>
    <th>父批次</th><th>根批次</th><th>创建时间</th><th></th></tr></thead><tbody>
    ${batches.map((b) => `<tr>
      <td class="mono">${esc(b.batch_id)}</td><td>${esc(b.dataset)}</td>
      <td>${pill(b.source)}</td><td>${pill(b.status)}</td><td>${b.record_count}</td>
      <td class="mono muted">${esc(b.parent_batch_id || "—")}</td>
      <td class="mono muted">${esc(b.root_batch_id)}</td>
      <td class="muted">${esc(b.created_at)}</td>
      <td><button class="btn ghost" data-gate="${esc(b.root_batch_id)}">门禁</button></td>
    </tr>`).join("")}</tbody></table></div>`;
}

/* ============================================================== ② rules */
const RULE_TEMPLATES = {
  primary_key: { fields: ["order_id"] },
  unique: { fields: ["email"] },
  range: { field: "amount", min: 0, max: 100000, min_inclusive: true, max_inclusive: true, allow_null: false },
  cross_table: { field: "customer_id", ref_dataset: "customers", ref_field: "id", allow_null: false },
  expression: { expr: "amount >= 0 and quantity >= 1" },
};

async function renderRules() {
  const rules = await api("/api/rules?dataset=" + encodeURIComponent(state.dataset));
  $("#view").innerHTML = `
    <div class="card">
      <h2>新建规则（草稿 → 发布后生效）</h2>
      <div class="row">
        <div style="max-width:180px"><label>数据集</label><input id="r-dataset" value="${esc(state.dataset)}"></div>
        <div style="max-width:200px"><label>规则名称</label><input id="r-name" placeholder="例如：订单金额范围"></div>
        <div style="max-width:200px"><label>规则类型</label>
          <select id="r-type">${["primary_key", "unique", "range", "cross_table", "expression"]
            .map((t) => `<option value="${t}">${t}</option>`).join("")}</select></div>
      </div>
      <h3>规则配置（JSON）</h3>
      <textarea id="r-config" style="min-height:130px">${pretty(RULE_TEMPLATES.range)}</textarea>
      <div style="margin-top:10px"><button class="btn" id="btn-create-rule">创建草稿规则</button></div>
    </div>
    <div class="card section-gap">
      <h2>规则与版本 <span class="muted" style="font-weight:400">（同一规则升级保留全部历史版本）</span></h2>
      ${rules.length ? rules.map(ruleCard).join("") : '<div class="empty">该数据集暂无规则</div>'}
    </div>`;

  $("#r-type").onchange = (e) =>
    ($("#r-config").value = JSON.stringify(RULE_TEMPLATES[e.target.value], null, 2));
  $("#btn-create-rule").onclick = async () => {
    let config;
    try { config = JSON.parse($("#r-config").value); }
    catch (e) { return toast("配置 JSON 解析失败: " + e.message, "error"); }
    try {
      const r = await api("/api/rules", {
        method: "POST",
        body: JSON.stringify({ dataset: $("#r-dataset").value, name: $("#r-name").value || "未命名规则", type: $("#r-type").value, config }),
      });
      toast(`规则 ${r.rule_id} 已创建为草稿`, "ok");
      state.dataset = $("#r-dataset").value;
      renderRules();
    } catch (e) { toast(e.message, "error"); }
  };
  bindRuleActions();
}

function ruleCard(r) {
  return `<div class="card" style="background:var(--panel2);margin-bottom:12px">
    <div class="row" style="align-items:center">
      <div><b>${esc(r.name)}</b> ${pill(r.type)} <span class="mono muted">${esc(r.rule_id)}</span></div>
      <span style="flex:1"></span>
      <button class="btn green" data-publish="${esc(r.rule_id)}" ${r.versions.some(v => v.status === "draft") ? "" : "disabled"}>发布草稿</button>
      <button class="btn" data-upgrade="${esc(r.rule_id)}">升级规则（新版本）</button>
    </div>
    <div class="scrollx" style="margin-top:10px"><table><thead><tr>
      <th>版本</th><th>状态</th><th>配置</th><th>变更说明</th><th>时间</th></tr></thead><tbody>
      ${r.versions.map((v) => `<tr>
        <td>v${v.version}</td><td>${pill(v.status)}</td>
        <td><pre style="margin:0;padding:8px">${pretty(v.config)}</pre></td>
        <td class="muted">${esc(v.change_note || "—")}</td><td class="muted">${esc(v.created_at)}</td>
      </tr>`).join("")}
    </tbody></table></div></div>`;
}

function bindRuleActions() {
  document.querySelectorAll("[data-publish]").forEach((btn) => {
    btn.onclick = async () => {
      try { await api(`/api/rules/${btn.dataset.publish}/publish`, { method: "POST" }); toast("规则已发布 ✔", "ok"); renderRules(); }
      catch (e) { toast(e.message, "error"); }
    };
  });
  document.querySelectorAll("[data-upgrade]").forEach((btn) => {
    btn.onclick = () => upgradeModal(btn.dataset.upgrade);
  });
}

async function upgradeModal(ruleId) {
  const rule = await api("/api/rules/" + ruleId);
  const current = rule.versions.filter((v) => v.status === "published").slice(-1)[0] || rule.versions.at(-1);
  const host = modal(`
    <h2>升级规则：${esc(rule.name)} <span class="muted mono">${esc(ruleId)}</span></h2>
    <p class="muted" style="margin-bottom:10px">当前 v${current.version} (${current.status})，提交后生成新的草稿版本，历史版本保留。</p>
    <label>新配置（JSON）</label>
    <textarea id="up-config" style="min-height:160px">${pretty(current.config)}</textarea>
    <label style="margin-top:10px">变更说明</label>
    <input id="up-note" placeholder="例如：金额上限调整为 50000">
    <div class="actions">
      <button class="btn ghost" id="modal-close2">取消</button>
      <button class="btn" id="up-submit">创建新版本</button>
    </div>`);
  $("#modal-close2", host).onclick = () => ($("#modal-host").innerHTML = "");
  $("#up-submit", host).onclick = async () => {
    let config;
    try { config = JSON.parse($("#up-config", host).value); }
    catch (e) { return toast("JSON 解析失败: " + e.message, "error"); }
    try {
      await api(`/api/rules/${ruleId}/upgrade`, {
        method: "POST",
        body: JSON.stringify({ config, change_note: $("#up-note", host).value || "规则升级" }),
      });
      $("#modal-host").innerHTML = "";
      toast("新版本已创建（草稿），发布后对后续回放生效", "ok");
      renderRules();
    } catch (e) { toast(e.message, "error"); }
  };
}

/* ============================================================== ③ quarantine */
async function renderQuarantine() {
  const entries = await api("/api/quarantine");
  const open = entries.filter((e) => e.status === "open").length;
  $("#view").innerHTML = `
    <div class="grid cols-3" style="margin-bottom:18px">
      <div class="card"><div class="muted">隔离条目（当前状态）</div><div class="stat">${entries.length}</div></div>
      <div class="card"><div class="muted">待修复 open</div><div class="stat red">${open}</div></div>
      <div class="card"><div class="muted">已修复 fixed</div><div class="stat green">${entries.length - open}</div></div>
    </div>
    <div class="card">
      <h2>隔离分区 <span class="muted" style="font-weight:400">— 失败记录进入隔离；每条记录保留完整修复历史</span></h2>
      ${entries.length ? `<div class="scrollx"><table><thead><tr>
        <th>记录键</th><th>规则</th><th>状态</th><th>失败原因</th>
        <th>当前载荷（含暂存修复）</th><th>首次发现</th><th>历史轮次</th><th>操作</th>
      </tr></thead><tbody>
        ${entries.map((e) => `<tr>
          <td class="mono">${esc(e.record_key)}</td>
          <td class="mono muted">${esc(e.rule_id.slice(0, 14))}…</td>
          <td>${pill(e.status)}</td>
          <td style="max-width:240px">${esc(e.reason)}</td>
          <td><pre style="margin:0;padding:6px;max-width:260px;max-height:110px;overflow:auto">${pretty(e.payload)}</pre></td>
          <td class="muted">${esc(e.first_seen)}</td>
          <td><button class="btn ghost" data-history="${e.entry_id}">${e.history.length} 个事件</button></td>
          <td>${e.status === "open" ? `<button class="btn" data-repair='${esc(JSON.stringify({ ds: e.dataset, key: e.record_key, payload: e.payload }))}'>修复</button>` : "—"}</td>
        </tr>`).join("")}
      </tbody></table></div>` : '<div class="empty">隔离分区为空 🎉</div>'}
    </div>`;
  document.querySelectorAll("[data-history]").forEach((btn) =>
    (btn.onclick = async () => {
      const { history } = await api(`/api/quarantine/${btn.dataset.history}/history`);
      modal(`<h2>修复历史（append-only，不可变）</h2>
        <ul class="timeline">${history.map((h) => `<li>
          ${pill(h.event_type)} <b>v${h.rule_version}</b>
          <div class="muted">${esc(h.created_at)} · 批次 <span class="mono">${esc(h.batch_id)}</span></div>
          <div>${esc(h.reason)}</div>
          <pre style="margin:6px 0;padding:8px;max-height:120px;overflow:auto">${pretty(h.payload)}</pre>
        </li>`).join("")}</ul>`);
    }));
  document.querySelectorAll("[data-repair]").forEach((btn) =>
    (btn.onclick = () => repairModal(JSON.parse(unescapeHtml(btn.dataset.repair)))));
}

function repairModal({ ds, key, payload }) {
  const host = modal(`
    <h2>修复记录 <span class="mono">${esc(key)}</span></h2>
    <p class="muted" style="margin-bottom:10px">暂存修复后的载荷，之后在「修复批次」页发起回放，仅回放仍处于 open 的失败数据。</p>
    <textarea id="fix-payload" style="min-height:200px">${pretty(payload)}</textarea>
    <div class="actions">
      <button class="btn ghost" id="modal-close2">取消</button>
      <button class="btn green" id="fix-submit">暂存修复</button>
    </div>`);
  $("#modal-close2", host).onclick = () => ($("#modal-host").innerHTML = "");
  $("#fix-submit", host).onclick = async () => {
    let next;
    try { next = JSON.parse($("#fix-payload", host).value); }
    catch (e) { return toast("JSON 解析失败: " + e.message, "error"); }
    try {
      await api(`/api/quarantine/${encodeURIComponent(ds)}/${encodeURIComponent(key)}/repair`, {
        method: "PUT", body: JSON.stringify({ payload: next }),
      });
      $("#modal-host").innerHTML = "";
      toast("修复已暂存，等待回放 ✔", "ok");
      renderQuarantine();
    } catch (e) { toast(e.message, "error"); }
  };
}

/* ============================================================== ④ batches / replay */
async function renderBatches() {
  const batches = await api("/api/batches");
  const rows = [];
  for (const b of batches) {
    const execs = await api(`/api/batches/${b.batch_id}/executions`);
    const fails = await api(`/api/batches/${b.batch_id}/failures`);
    rows.push({ b, execs, fails });
  }
  $("#view").innerHTML = `
    <div class="card">
      <h2>修复批次与回放</h2>
      <p class="muted" style="margin-bottom:12px">回放只取根批次血缘下仍 open 的隔离记录，用暂存载荷生成新的子批次，并按<b>当前已发布的规则版本</b>重新检查。没有 open 记录时回放会被拒绝（空回放）。</p>
      ${rows.length ? `<div class="scrollx"><table><thead><tr>
        <th>批次</th><th>来源/状态</th><th>执行结果</th><th>失败样本</th><th>操作</th>
      </tr></thead><tbody>
        ${rows.map(({ b, execs, fails }) => `<tr>
          <td class="mono">${esc(b.batch_id)}<br><span class="muted">根: ${esc(b.root_batch_id)}</span></td>
          <td>${pill(b.source)} ${pill(b.status)}</td>
          <td>${execs.map((e) => `<div class="mono">总数 ${e.total} / 通过 ${e.passed} / 失败 <b style="color:var(--red)">${e.failed}</b></div>
             <div class="muted" style="font-size:11.5px">规则: ${e.rule_versions_snapshot.map(s => esc(s.name) + "@v" + s.version).join(", ") || "无已发布规则"}</div>`).join("") || '<span class="muted">无执行</span>'}</td>
          <td style="max-width:280px">${fails.length ? fails.map(f => `<div class="mono" style="margin-bottom:3px">${esc(f.record_key)} <span class="muted">— ${esc(f.reason)}</span></div>`).join("") : '<span class="muted">无</span>'}</td>
          <td style="white-space:nowrap">
            <button class="btn" data-replay="${esc(b.batch_id)}">回放失败数据</button>
            <button class="btn ghost" data-gate="${esc(b.root_batch_id)}">查看门禁</button>
          </td>
        </tr>`).join("")}
      </tbody></table></div>` : '<div class="empty">暂无批次</div>'}
    </div>`;
  document.querySelectorAll("[data-replay]").forEach((btn) => {
    btn.onclick = async () => {
      try {
        const r = await api(`/api/batches/${btn.dataset.replay}/replay`, { method: "POST" });
        toast(`回放批次 ${r.batch.batch_id} 生成：${r.execution.failed} 条仍失败`, "ok");
        renderBatches();
      } catch (e) {
        if (e.message.includes("NOTHING_TO_REPLAY")) toast("空回放已被拒绝：没有 open 的隔离记录 ✔", "ok");
        else toast(e.message, "error");
      }
    };
  });
  document.querySelectorAll("[data-gate]").forEach((btn) => {
    btn.onclick = () => { state.selectedRoot = btn.dataset.gate; go("gate"); };
  });
}

/* ============================================================== ⑤ gate */
async function renderGate() {
  const batches = await api("/api/batches");
  const roots = batches.filter((b) => b.root_batch_id === b.batch_id);
  if (!state.selectedRoot && roots[0]) state.selectedRoot = roots[0].batch_id;
  $("#view").innerHTML = `
    <div class="card">
      <h2>发布门禁</h2>
      <div class="row" style="margin-bottom:14px">
        <div style="max-width:360px"><label>选择根批次（覆盖整条修复血缘）</label>
          <select id="gate-select">${roots.map((b) =>
            `<option value="${esc(b.batch_id)}" ${b.batch_id === state.selectedRoot ? "selected" : ""}>${esc(b.batch_id)} · ${esc(b.dataset)}</option>`).join("")}</select>
        </div>
      </div>
      <div id="gate-detail"><div class="empty">选择根批次查看门禁评估</div></div>
    </div>`;
  $("#gate-select").onchange = (e) => { state.selectedRoot = e.target.value; loadGateDetail(); };
  if (state.selectedRoot) loadGateDetail();
}

async function loadGateDetail() {
  const root = state.selectedRoot;
  let g;
  try { g = await api(`/api/gate/${root}`); }
  catch (e) { return toast(e.message, "error"); }
  const color = g.decision === "pass" ? "green" : g.decision === "warn" ? "amber" : "red";
  $("#gate-detail").innerHTML = `
    <div class="gate-banner ${g.decision}">
      <div class="stat ${color}" style="font-size:30px;text-transform:uppercase">${g.decision}</div>
      <div><div class="big">${esc(g.reason)}</div>
        <div class="muted">open 隔离 ${g.open_quarantine} 条 · 隔离条目合计 ${g.total_quarantine_entries} 条 · 血缘批次 ${g.lineage.length} 个</div></div>
      <span style="flex:1"></span>
      <button class="btn ${g.decision === "block" ? "red" : "green"}" id="btn-publish" ${g.decision === "block" ? "" : ""}>发布整个血缘</button>
    </div>
    <h3>血缘批次</h3>
    ${batchTable(g.lineage)}
    <h3 class="section-gap">规则汇总（跨所有执行，按规则累计）</h3>
    <div class="scrollx"><table><thead><tr><th>规则</th><th>类型</th><th>最新版本</th><th>累计评估</th><th>累计失败</th></tr></thead><tbody>
      ${g.rule_rollup.map((r) => `<tr><td>${esc(r.rule_name)}</td><td>${pill(r.rule_type)}</td><td>v${r.latest_version}</td><td>${r.evaluated}</td><td style="color:${r.failed ? "var(--red)" : "var(--green)"}">${r.failed}</td></tr>`).join("") || '<tr><td colspan="5" class="empty">无执行记录</td></tr>'}
    </tbody></table></div>
    <h3 class="section-gap">执行历史（含规则版本快照）</h3>
    <div class="scrollx"><table><thead><tr><th>执行</th><th>批次</th><th>总/过/败</th><th>规则版本快照</th><th>时间</th></tr></thead><tbody>
      ${g.executions.map((e) => `<tr>
        <td class="mono">${esc(e.execution_id)}</td><td class="mono">${esc(e.batch_id)}</td>
        <td>${e.total} / ${e.passed} / <b style="color:var(--red)">${e.failed}</b></td>
        <td>${e.snapshot.map((s) => `<span class="pill type">${esc(s.name)} v${s.version}</span>`).join(" ") || '<span class="muted">空（无规则）</span>'}</td>
        <td class="muted">${esc(e.created_at)}</td></tr>`).join("")}
    </tbody></table></div>`;
  $("#btn-publish").onclick = async () => {
    try {
      const r = await api(`/api/gate/${root}/publish`, { method: "POST" });
      toast(`已发布：血缘 ${r.lineage.length} 个批次全部 published ✔`, "ok");
      loadGateDetail();
    } catch (e) {
      if (e.message.includes("GATE_BLOCKED")) toast("门禁拦截：仍有 open 隔离记录，需修复后回放", "error");
      else toast(e.message, "error");
    }
  };
}

/* ============================================================== demo data */
$("#btn-demo").onclick = async () => {
  try {
    const ds = "customers";
    // reference dataset: clean customer rows
    await api("/api/ingest", { method: "POST", body: JSON.stringify({
      dataset: ds,
      rows: [
        { id: "C-001", name: "Alice", email: "alice@example.com", tier: "gold" },
        { id: "C-002", name: "Bob", email: "bob@example.com", tier: "silver" },
      ],
    }) });
    const mk = async (body) => (await api("/api/rules", { method: "POST", body: JSON.stringify(body) })).rule_id;
    const pk = await mk({ dataset: "orders", name: "订单主键", type: "primary_key", config: { fields: ["order_id"] } });
    const uniq = await mk({ dataset: "orders", name: "邮箱唯一", type: "unique", config: { fields: ["email"] } });
    const rng = await mk({ dataset: "orders", name: "金额范围", type: "range", config: { field: "amount", min: 0, max: 10000, allow_null: false } });
    const cross = await mk({ dataset: "orders", name: "客户外键一致", type: "cross_table", config: { field: "customer_id", ref_dataset: "customers", ref_field: "id", allow_null: false } });
    const expr = await mk({ dataset: "orders", name: "数量与金额表达式", type: "expression", config: { expr: "quantity >= 1 and amount >= 0" } });
    for (const id of [pk, uniq, rng, cross, expr]) await api(`/api/rules/${id}/publish`, { method: "POST" });
    await api("/api/ingest", { method: "POST", body: JSON.stringify({
      dataset: "orders",
      rows: [
        { order_id: "O-1", email: "o1@x.com", customer_id: "C-001", amount: 100, quantity: 2, status: "paid" },
        { order_id: "O-2", email: "o2@x.com", customer_id: "C-999", amount: 50, quantity: 1, status: "paid" },          // cross-table fail
        { order_id: "O-3", email: "o3@x.com", customer_id: "C-002", amount: -10, quantity: 1, status: "paid" },          // range + expr fail
        { order_id: "O-4", email: "o3@x.com", customer_id: "C-001", amount: 200, quantity: 0, status: "paid" },           // unique + expr fail
      ],
    }) });
    state.dataset = "orders";
    toast("演示数据已载入：customers 参照表 + orders 五类规则 + 含失败的接入批次", "ok");
    go("rules");
  } catch (e) { toast("演示载入失败（可能已存在）: " + e.message, "error"); }
};

/* ============================================================== shell */
const TABS = ["ingest", "rules", "quarantine", "batches", "gate"];
const RENDERERS = { ingest: renderIngest, rules: renderRules, quarantine: renderQuarantine, batches: renderBatches, gate: renderGate };

function go(tab) {
  state.tab = tab;
  document.querySelectorAll("#tabs button").forEach((b) => b.classList.toggle("active", b.dataset.tab === tab));
  RENDERERS[tab]().catch((e) => ($("#view").innerHTML = `<div class="card empty">加载失败: ${esc(e.message)}</div>`));
}
$("#tabs").addEventListener("click", (e) => {
  const btn = e.target.closest("button");
  if (btn) go(btn.dataset.tab);
});
$("#btn-refresh").onclick = () => go(state.tab);
go("ingest");
