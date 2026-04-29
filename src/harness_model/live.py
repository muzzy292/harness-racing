# live.py — Local live-betting page: paste TAB odds, compute edge, track bets.
#
# Serves at http://127.0.0.1:8001 with:
#   GET  /         → live HTML page (bet tracking via localStorage)
#   POST /api/ocr  → Claude API image→odds extraction (requires ANTHROPIC_API_KEY)

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .odds import load_feature_rows, load_weights, score_race_rows

_MIN_EDGE = 0.25
_KELLY_FRACTION = 0.25
_MAX_STAKE_PCT = 0.04
_STARTING_BANK = 100.0

# ---------------------------------------------------------------------------
# Data: score all races from the feature CSV
# ---------------------------------------------------------------------------

def _score_all_races(csv_path: str, weights_path: str | None) -> list[dict]:
    rows = load_feature_rows(csv_path)
    w = load_weights(weights_path) if weights_path and Path(weights_path).exists() else None

    combos: set[tuple[str, int]] = set()
    for row in rows:
        mc = row.get("meeting_code", "")
        rn = row.get("race_number")
        if mc and rn:
            try:
                combos.add((mc, int(rn)))
            except (ValueError, TypeError):
                pass

    races: list[dict] = []
    for mc, rn in sorted(combos, reverse=True):   # most recent first
        scored = score_race_rows(rows, mc, rn, weights=w)
        if not scored:
            continue
        races.append({
            "meeting_code": mc,
            "race_number": rn,
            "runners": [
                {
                    "runner_number": r.get("runner_number"),
                    "horse_name": r.get("horse_name", ""),
                    "barrier": r.get("barrier", ""),
                    "fair_odds": r.get("fair_odds"),
                }
                for r in scored
                if not r.get("scratched")
            ],
        })
    return races


# ---------------------------------------------------------------------------
# HTML page
# ---------------------------------------------------------------------------

# Use %%PLACEHOLDER%% substitution to avoid f-string escaping of JS braces.
_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MuzzyBet Live</title>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@700;800;900&family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;600&display=swap" rel="stylesheet"/>
<style>
:root {
  --bg:#0a0a0a; --card-bg:#101010; --surface:#131313;
  --primary-text:#f0f0f0; --secondary:#808080;
  --accent:#00d4ff; --accent-dark:#00aacc;
  --border:#1f1f1f; --highlight:#1a1a1a;
  --winner:#22d3a0; --danger:#ef4444;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--primary-text);font-family:Inter,sans-serif;font-size:13px}

nav{background:var(--card-bg);border-bottom:1px solid var(--border);padding:12px 20px;display:flex;align-items:center;gap:20px}
.nav-brand{font-family:"Barlow Condensed",sans-serif;font-size:22px;font-weight:900;color:var(--accent);letter-spacing:1px}
nav a{color:var(--secondary);text-decoration:none;font-size:13px}
nav a:hover{color:var(--primary-text)}
nav a.active{color:var(--accent)}

.page{max-width:1400px;margin:0 auto;padding:20px;display:grid;grid-template-columns:1fr 1fr;gap:16px}
.full{grid-column:1/-1}

.card{background:var(--card-bg);border:1px solid var(--border);border-radius:8px;padding:16px}
.card-title{font-family:"Barlow Condensed",sans-serif;font-size:16px;font-weight:800;color:var(--accent);letter-spacing:1px;text-transform:uppercase;margin-bottom:14px}

.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
select{background:var(--surface);color:var(--primary-text);border:1px solid var(--border);border-radius:6px;padding:7px 10px;font-size:13px;cursor:pointer}
select:focus{outline:none;border-color:var(--accent)}

table{width:100%;border-collapse:collapse}
th{font-family:"Barlow Condensed",sans-serif;font-size:12px;font-weight:700;letter-spacing:.5px;color:var(--secondary);text-transform:uppercase;text-align:left;padding:6px 8px;border-bottom:1px solid var(--border)}
td{padding:7px 8px;border-bottom:1px solid var(--border);font-family:"Roboto Mono",monospace}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--highlight)}
td.name{font-family:Inter,sans-serif;font-weight:600;font-size:13px}
td.g{color:var(--winner)} td.r{color:var(--danger)} td.c{color:var(--accent)}

textarea{width:100%;background:var(--surface);color:var(--primary-text);border:1px solid var(--border);border-radius:6px;padding:10px;font-family:"Roboto Mono",monospace;font-size:12px;resize:vertical;min-height:130px}
textarea:focus{outline:none;border-color:var(--accent)}
textarea::placeholder{color:var(--secondary)}

.btn{padding:8px 16px;border-radius:6px;border:none;cursor:pointer;font-size:12px;font-weight:600;transition:opacity .15s;font-family:Inter,sans-serif}
.btn:hover{opacity:.8}
.btn:disabled{opacity:.4;cursor:default}
.btn-accent{background:var(--accent);color:#000}
.btn-surface{background:var(--surface);color:var(--primary-text);border:1px solid var(--border)}
.btn-win{background:rgba(34,211,160,.15);color:var(--winner);border:1px solid rgba(34,211,160,.3)}
.btn-loss{background:rgba(239,68,68,.15);color:var(--danger);border:1px solid rgba(239,68,68,.3)}
.btn-muted{background:var(--surface);color:var(--secondary);border:1px solid var(--border)}
.btn-sm{padding:4px 10px;font-size:11px}

.bank-bar{display:flex;align-items:center;gap:20px;margin-bottom:14px;padding:12px 16px;background:var(--surface);border-radius:6px;border:1px solid var(--border);flex-wrap:wrap}
.bank-lbl{color:var(--secondary);font-size:11px;margin-bottom:2px;font-family:"Barlow Condensed",sans-serif;letter-spacing:.5px;text-transform:uppercase}
.bank-val{font-family:"Roboto Mono",monospace;font-size:22px;font-weight:600;color:var(--winner)}
.bank-stat{font-family:"Roboto Mono",monospace;font-size:14px}
.bank-stat.pos{color:var(--winner)} .bank-stat.neg{color:var(--danger)}

.edge-pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;font-family:"Roboto Mono",monospace}
.edge-q{background:rgba(34,211,160,.15);color:var(--winner)}
.edge-n{background:var(--surface);color:var(--secondary)}

.match-ok{color:var(--winner);font-size:11px}
.match-~{color:#f59e0b;font-size:11px}
.match-x{color:var(--danger);font-size:11px}

tr.r-won td{background:rgba(34,211,160,.05)}
tr.r-lost td{background:rgba(239,68,68,.04)}
tr.r-void{opacity:.4}

.empty{color:var(--secondary);text-align:center;padding:24px;font-style:italic;font-size:12px}
.hint{color:var(--secondary);font-size:11px;margin-top:6px}

/* Settings overlay */
.overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:100;align-items:center;justify-content:center}
.overlay.open{display:flex}
.settings-box{background:var(--card-bg);border:1px solid var(--border);border-radius:10px;padding:24px;width:420px;max-width:95vw}
.settings-box h3{font-family:"Barlow Condensed",sans-serif;font-size:18px;font-weight:800;color:var(--accent);margin-bottom:16px;letter-spacing:1px;text-transform:uppercase}
.settings-box label{display:block;color:var(--secondary);font-size:11px;margin-bottom:4px;font-family:"Barlow Condensed",sans-serif;letter-spacing:.5px;text-transform:uppercase}
.settings-box input{width:100%;background:var(--surface);color:var(--primary-text);border:1px solid var(--border);border-radius:6px;padding:8px 10px;font-family:"Roboto Mono",monospace;font-size:12px;margin-bottom:12px}
.settings-box input:focus{outline:none;border-color:var(--accent)}
.settings-note{color:var(--secondary);font-size:11px;line-height:1.5;margin-bottom:14px}
.key-ok{color:var(--winner);font-size:11px;margin-top:-8px;margin-bottom:10px}
</style>
</head>
<body>

<nav>
  <span class="nav-brand">MUZZYBET LIVE</span>
  <a href="/">Races</a>
  <a href="/stats.html">Stats</a>
  <a href="/betting.html">Betting</a>
  <a href="/diagnose.html">Diagnose</a>
  <a class="active" href="#">Live</a>
  <button class="btn btn-muted btn-sm" style="margin-left:auto" onclick="openSettings()">⚙ OCR Settings</button>
</nav>

<!-- Settings overlay -->
<div class="overlay" id="settings-overlay" onclick="if(event.target===this)closeSettings()">
  <div class="settings-box">
    <h3>OCR Settings</h3>
    <label>Anthropic API Key</label>
    <input type="password" id="api-key-input" placeholder="sk-ant-..." autocomplete="off">
    <div id="key-status"></div>
    <p class="settings-note">Your key is stored only in this browser (localStorage). It is never sent anywhere except directly to Anthropic's API for screenshot OCR.<br><br>Get a key at <a href="https://console.anthropic.com" target="_blank" style="color:var(--accent)">console.anthropic.com</a></p>
    <div class="row" style="gap:8px">
      <button class="btn btn-accent" onclick="saveKey()">Save Key</button>
      <button class="btn btn-muted" onclick="clearKey()">Clear Key</button>
      <button class="btn btn-surface" onclick="closeSettings()" style="margin-left:auto">Close</button>
    </div>
  </div>
</div>

<div class="page">

  <!-- Race selector -->
  <div class="card full">
    <div class="card-title">Select Race</div>
    <div class="row">
      <select id="mc-sel" onchange="onMcChange()"><option value="">-- Meeting --</option></select>
      <select id="rn-sel" onchange="onRnChange()"><option value="">-- Race --</option></select>
    </div>
  </div>

  <!-- Model prices -->
  <div class="card">
    <div class="card-title">Model Prices</div>
    <div id="model-panel"><div class="empty">Select a race above</div></div>
  </div>

  <!-- TAB odds input -->
  <div class="card">
    <div class="card-title">TAB Odds</div>
    <textarea id="tab-input" placeholder="Paste TAB odds here, e.g.&#10;1 HORSE NAME $4.50&#10;2 ANOTHER HORSE $7.00&#10;&#10;Accepts most copy-paste formats."></textarea>
    <div class="row" style="margin-top:10px">
      <button class="btn btn-accent" onclick="calcBets()">Calculate Bets</button>
      <button class="btn btn-surface" id="ocr-btn" onclick="document.getElementById('img-input').click()">Screenshot OCR</button>
      <input type="file" id="img-input" accept="image/*" hidden onchange="runOCR(this)">
    </div>
    <p class="hint">Accepts TAB, Betfair, or any format with horse name + decimal odds</p>
  </div>

  <!-- Qualifying bets -->
  <div class="card full">
    <div class="card-title">Qualifying Bets</div>
    <div class="bank-bar">
      <div><div class="bank-lbl">Bank</div><div class="bank-val" id="bank-val">$100.00</div></div>
      <div><div class="bank-lbl">P&amp;L</div><div class="bank-stat" id="pnl-val">+$0.00</div></div>
      <div><div class="bank-lbl">Record</div><div class="bank-stat" id="rec-val">0W / 0L / 0P</div></div>
      <button class="btn btn-muted btn-sm" onclick="if(confirm('Reset bank to $100 and clear all bets?'))resetAll()" style="margin-left:auto">Reset Bank</button>
    </div>
    <div id="qual-panel"><div class="empty">Paste TAB odds and click Calculate Bets</div></div>
  </div>

  <!-- Bet tracker -->
  <div class="card full">
    <div class="card-title">Bet Tracker</div>
    <div id="hist-panel"><div class="empty">No bets recorded yet</div></div>
  </div>

</div>

<script>
// ── Constants ─────────────────────────────────────────────────────────────────
const RACES       = %%RACES_JSON%%;
const MIN_EDGE    = %%MIN_EDGE%%;
const KELLY       = %%KELLY%%;
const MAX_STK_PCT = %%MAX_STK%%;
const START_BANK  = %%START_BANK%%;
const LS_KEY      = 'muzzybet_live_v1';

// ── Race data helpers ─────────────────────────────────────────────────────────
function getMeetings() {
  const seen = new Set(), out = [];
  for (const r of RACES) { if (!seen.has(r.meeting_code)) { seen.add(r.meeting_code); out.push(r.meeting_code); } }
  return out;
}
function getRaces(mc) { return RACES.filter(r => r.meeting_code === mc).sort((a,b)=>a.race_number-b.race_number); }
function getRace(mc, rn) { return RACES.find(r => r.meeting_code === mc && r.race_number === +rn); }

// ── Race selector ─────────────────────────────────────────────────────────────
function populateMeetings() {
  const sel = document.getElementById('mc-sel');
  for (const mc of getMeetings()) {
    const o = document.createElement('option'); o.value = mc; o.textContent = mc; sel.appendChild(o);
  }
}

function onMcChange() {
  const mc = document.getElementById('mc-sel').value;
  const rSel = document.getElementById('rn-sel');
  rSel.innerHTML = '<option value="">-- Race --</option>';
  document.getElementById('model-panel').innerHTML = '<div class="empty">Select a race</div>';
  document.getElementById('qual-panel').innerHTML  = '<div class="empty">Paste TAB odds and click Calculate Bets</div>';
  if (!mc) return;
  for (const r of getRaces(mc)) {
    const o = document.createElement('option'); o.value = r.race_number; o.textContent = 'Race ' + r.race_number; rSel.appendChild(o);
  }
}

function onRnChange() {
  const mc = document.getElementById('mc-sel').value;
  const rn = document.getElementById('rn-sel').value;
  if (mc && rn) renderModelTable(mc, +rn);
}

function renderModelTable(mc, rn) {
  const race = getRace(mc, rn);
  if (!race) return;
  let h = '<table><thead><tr><th>#</th><th>Horse</th><th>Bar</th><th>Model $</th></tr></thead><tbody>';
  for (const r of race.runners) {
    h += `<tr><td>${r.runner_number??'-'}</td><td class="name">${r.horse_name}</td><td>${r.barrier??'-'}</td><td class="c">${r.fair_odds?'$'+r.fair_odds.toFixed(2):'-'}</td></tr>`;
  }
  h += '</tbody></table>';
  document.getElementById('model-panel').innerHTML = h;
}

// ── TAB odds parser ───────────────────────────────────────────────────────────
function parseTabText(text) {
  const out = [];
  for (const raw of text.split(/\n/)) {
    const line = raw.trim().replace(/[  ]/g, ' ');  // normalise non-breaking spaces
    if (!line) continue;
    // Strip leading runner number: "1." "1)" "1 " "No. 1"
    const stripped = line.replace(/^(?:No[.]?\s*)?\d+[.):\s]+/, '').trim();
    // Decimal odds at end: "HORSE NAME $4.50" or "HORSE NAME 4.50"
    let m = stripped.match(/^(.+?)\s+\$?([\d]+\.[\d]+)\s*$/);
    if (m) { const name=m[1].trim().toUpperCase().replace(/\s+/g,' '); const odds=+m[2]; if(odds>1) out.push({name,odds}); continue; }
    // Fractional: "HORSE NAME 5/2"
    m = stripped.match(/^(.+?)\s+(\d+)\/(\d+)\s*$/);
    if (m) { const name=m[1].trim().toUpperCase().replace(/\s+/g,' '); const odds=+m[2]/+m[3]+1; if(odds>1) out.push({name,odds}); }
  }
  return out;
}

// ── Fuzzy name matching ───────────────────────────────────────────────────────
function lev(a, b) {
  const m=a.length, n=b.length;
  const d=Array.from({length:m+1},(_,i)=>Array.from({length:n+1},(_,j)=>i===0?j:j===0?i:0));
  for(let i=1;i<=m;i++) for(let j=1;j<=n;j++)
    d[i][j]=a[i-1]===b[j-1]?d[i-1][j-1]:1+Math.min(d[i-1][j],d[i][j-1],d[i-1][j-1]);
  return d[m][n];
}

function bestMatch(tabName, runners) {
  let best=null, bestD=Infinity;
  for (const r of runners) {
    const mn = r.horse_name.toUpperCase();
    if (tabName===mn) return {runner:r, dist:0};
    const d = lev(tabName, mn);
    if (d<bestD) { bestD=d; best=r; }
  }
  return best ? {runner:best, dist:bestD} : null;
}

// ── Edge + Kelly ──────────────────────────────────────────────────────────────
function edge(tab, fair) { return fair>0 ? tab/fair-1 : null; }

function kellyStake(e, tabOdds, bank) {
  if (e<=0 || tabOdds<=1) return 0;
  const k = (e/(tabOdds-1))*KELLY;
  return Math.min(Math.max(k*bank, 0.50), bank*MAX_STK_PCT);
}

// ── Calculate + render bets ───────────────────────────────────────────────────
let _curMc='', _curRn=0;

function calcBets() {
  const mc = document.getElementById('mc-sel').value;
  const rn = +document.getElementById('rn-sel').value;
  if (!mc||!rn) { alert('Select a meeting and race first'); return; }
  const race = getRace(mc, rn);
  const tabOdds = parseTabText(document.getElementById('tab-input').value);
  if (!tabOdds.length) { alert('Could not parse any odds — check the format'); return; }
  _curMc=mc; _curRn=rn;
  const bank = currentBank();
  const rows = tabOdds.map(t => {
    const m = bestMatch(t.name, race.runners);
    const e_val = m ? edge(t.odds, m.runner.fair_odds) : null;
    const stake = (e_val!==null && e_val>=MIN_EDGE) ? kellyStake(e_val, t.odds, bank) : null;
    return {tabName:t.name, tabOdds:t.odds, runner:m?.runner??null, dist:m?.dist??999, edge:e_val, stake};
  });
  renderQual(mc, rn, rows, bank);
}

function renderQual(mc, rn, rows, bank) {
  let h = '<table><thead><tr><th>Horse</th><th>Match</th><th>Model $</th><th>TAB $</th><th>Edge</th><th>Stake</th><th></th></tr></thead><tbody>';
  let anyQ = false;
  for (const r of rows) {
    const matchLabel = !r.runner ? '<span class="match-x">✗ no match</span>'
      : r.dist===0 ? '<span class="match-ok">✓ exact</span>'
      : r.dist<=3  ? `<span class="match-~">~ ${r.runner.horse_name}</span>`
      : r.dist<=6  ? `<span class="match-~">? ${r.runner.horse_name}</span>`
      : '<span class="match-x">✗ poor match</span>';
    const modelStr = r.runner?.fair_odds ? '$'+r.runner.fair_odds.toFixed(2) : '-';
    const edgeStr  = r.edge!==null ? (r.edge*100).toFixed(1)+'%' : '-';
    const eClass   = r.edge!==null && r.edge>=MIN_EDGE ? 'edge-q' : 'edge-n';
    if (r.edge!==null && r.edge>=MIN_EDGE && r.stake!==null) {
      anyQ = true;
      const betJson = encodeURIComponent(JSON.stringify({mc,rn,horse:r.runner.horse_name,model_odds:r.runner.fair_odds,tab_odds:r.tabOdds,edge:r.edge,stake:parseFloat(r.stake.toFixed(2))}));
      h += `<tr>
        <td class="name">${r.runner.horse_name}</td>
        <td>${matchLabel}</td>
        <td class="c">${modelStr}</td>
        <td>${r.tabOdds.toFixed(2)}</td>
        <td><span class="edge-pill ${eClass}">${edgeStr}</span></td>
        <td class="g">$${r.stake.toFixed(2)}</td>
        <td><button class="btn btn-accent btn-sm" onclick="placeBet(this,'${betJson}')">Place</button></td>
      </tr>`;
    } else {
      h += `<tr style="opacity:.4">
        <td class="name">${r.tabName}</td>
        <td>${matchLabel}</td>
        <td class="c">${modelStr}</td>
        <td>${r.tabOdds.toFixed(2)}</td>
        <td><span class="edge-pill ${eClass}">${edgeStr}</span></td>
        <td>—</td><td></td>
      </tr>`;
    }
  }
  if (!anyQ) h += `<tr><td colspan="7"><div class="empty">No qualifying bets (edge &lt; ${(MIN_EDGE*100).toFixed(0)}%)</div></td></tr>`;
  h += '</tbody></table>';
  document.getElementById('qual-panel').innerHTML = h;
}

// ── localStorage bet store ────────────────────────────────────────────────────
function loadBets() { try { return JSON.parse(localStorage.getItem(LS_KEY)||'[]'); } catch { return []; } }
function saveBets(b) { localStorage.setItem(LS_KEY, JSON.stringify(b)); }

function currentBank() {
  let bank = START_BANK;
  for (const b of loadBets()) {
    if (b.result==='won')  bank += b.payout - b.stake;
    if (b.result==='lost') bank -= b.stake;
  }
  return Math.max(bank, 0.50);
}

function placeBet(btn, enc) {
  const d = JSON.parse(decodeURIComponent(enc));
  const bank = currentBank();
  const stake = parseFloat(Math.min(d.stake, bank*MAX_STK_PCT).toFixed(2));
  const bets = loadBets();
  bets.push({id:Date.now(), placed_at:new Date().toISOString(), mc:d.mc, rn:d.rn, horse:d.horse, model_odds:d.model_odds, tab_odds:d.tab_odds, edge:d.edge, stake, payout:parseFloat((stake*d.tab_odds).toFixed(2)), result:'pending'});
  saveBets(bets);
  refreshBank();
  renderHistory();
  btn.textContent='Recorded ✓'; btn.disabled=true;
  setTimeout(()=>{btn.textContent='Place';btn.disabled=false;},2000);
}

function setResult(id, result) {
  const bets = loadBets();
  const b = bets.find(x=>x.id===id);
  if (b) { b.result=result; saveBets(bets); refreshBank(); renderHistory(); }
}

function deleteBet(id) {
  if (!confirm('Delete this bet?')) return;
  saveBets(loadBets().filter(b=>b.id!==id));
  refreshBank(); renderHistory();
}

function resetAll() { localStorage.removeItem(LS_KEY); refreshBank(); renderHistory(); }

// ── Bankroll display ──────────────────────────────────────────────────────────
function refreshBank() {
  const bets = loadBets();
  let bank=START_BANK, wins=0, losses=0, pending=0;
  for (const b of bets) {
    if (b.result==='won')  { bank+=b.payout-b.stake; wins++; }
    else if (b.result==='lost') { bank-=b.stake; losses++; }
    else if (b.result==='pending') pending++;
  }
  bank = Math.max(bank, 0);
  const pnl = bank - START_BANK;
  document.getElementById('bank-val').textContent = '$'+bank.toFixed(2);
  const pnlEl = document.getElementById('pnl-val');
  pnlEl.textContent = (pnl>=0?'+$':'−$')+Math.abs(pnl).toFixed(2);
  pnlEl.className = 'bank-stat '+(pnl>=0?'pos':'neg');
  document.getElementById('rec-val').textContent = `${wins}W / ${losses}L / ${pending}P`;
}

// ── Bet history ───────────────────────────────────────────────────────────────
function renderHistory() {
  const bets = loadBets();
  if (!bets.length) { document.getElementById('hist-panel').innerHTML='<div class="empty">No bets recorded yet</div>'; return; }
  let h=`<table><thead><tr><th>Date</th><th>Race</th><th>Horse</th><th>Model $</th><th>TAB $</th><th>Edge</th><th>Stake</th><th>Return</th><th>Result</th><th></th></tr></thead><tbody>`;
  for (const b of [...bets].reverse()) {
    const dt = new Date(b.placed_at).toLocaleDateString('en-AU',{day:'2-digit',month:'short'});
    const rClass = b.result==='won'?'r-won':b.result==='lost'?'r-lost':b.result==='void'?'r-void':'';
    const retStr = b.result==='won'  ? `<span class="g">+$${(b.payout-b.stake).toFixed(2)}</span>`
                 : b.result==='lost' ? `<span class="r">−$${b.stake.toFixed(2)}</span>` : '—';
    const resBtns = b.result==='pending'
      ? `<div class="row" style="gap:4px">
           <button class="btn btn-win btn-sm"  onclick="setResult(${b.id},'won')">Won</button>
           <button class="btn btn-loss btn-sm" onclick="setResult(${b.id},'lost')">Lost</button>
           <button class="btn btn-muted btn-sm" onclick="setResult(${b.id},'void')">Void</button>
         </div>`
      : `<span style="color:${b.result==='won'?'var(--winner)':b.result==='lost'?'var(--danger)':'var(--secondary)'};font-weight:700;text-transform:uppercase;font-size:11px">${b.result}</span>`;
    h += `<tr class="${rClass}">
      <td>${dt}</td>
      <td>${b.mc} R${b.rn}</td>
      <td class="name">${b.horse}</td>
      <td class="c">$${(b.model_odds??0).toFixed(2)}</td>
      <td>$${(b.tab_odds??0).toFixed(2)}</td>
      <td><span class="edge-pill edge-q">${(b.edge*100).toFixed(1)}%</span></td>
      <td>$${b.stake.toFixed(2)}</td>
      <td>${retStr}</td>
      <td>${resBtns}</td>
      <td><button class="btn btn-muted btn-sm" onclick="deleteBet(${b.id})">✕</button></td>
    </tr>`;
  }
  h += '</tbody></table>';
  document.getElementById('hist-panel').innerHTML = h;
}

// ── OCR ───────────────────────────────────────────────────────────────────────
const OCR_MODE = '%%OCR_MODE%%';  // 'local' or 'hosted'
const OCR_PROMPT = 'Extract horse names and decimal odds from this TAB/betting screenshot. Return a JSON array only: [{"name":"HORSE NAME","odds":3.50},...]. Uppercase names, decimal odds only. No explanation, just the JSON array.';

async function runOCR(input) {
  if (!input.files.length) return;
  const btn = document.getElementById('ocr-btn');
  btn.textContent = 'Analysing...'; btn.disabled = true;
  try {
    const file = input.files[0];
    const b64 = await new Promise((res,rej)=>{ const r=new FileReader(); r.onload=()=>res(r.result.split(',')[1]); r.onerror=rej; r.readAsDataURL(file); });
    let data;
    if (OCR_MODE === 'local') {
      const resp = await fetch('/api/ocr', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({image:b64, media_type:file.type})});
      if (!resp.ok) throw new Error(await resp.text());
      data = await resp.json();
    } else {
      const apiKey = localStorage.getItem('muzzybet_anthropic_key');
      if (!apiKey) { openSettings(); throw new Error('Enter your Anthropic API key in the OCR Settings panel'); }
      const resp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
          'anthropic-version': '2023-06-01',
          'anthropic-dangerous-direct-browser-access': 'true',
        },
        body: JSON.stringify({
          model: 'claude-haiku-4-5',
          max_tokens: 1024,
          messages: [{role:'user', content:[
            {type:'image', source:{type:'base64', media_type:file.type, data:b64}},
            {type:'text', text:OCR_PROMPT}
          ]}]
        })
      });
      if (!resp.ok) { const e=await resp.json(); throw new Error(e.error?.message||resp.statusText); }
      const result = await resp.json();
      data = JSON.parse(result.content[0].text);
    }
    if (data.error) throw new Error(data.error);
    document.getElementById('tab-input').value = data.map(h=>`${h.name} ${h.odds}`).join('\n');
    btn.textContent = `OCR: ${data.length} horses found`;
  } catch(e) {
    alert('OCR failed: '+e.message);
    btn.textContent = 'Screenshot OCR';
  } finally {
    btn.disabled = false; input.value = '';
  }
}

// ── Settings ──────────────────────────────────────────────────────────────────
function openSettings() {
  const saved = localStorage.getItem('muzzybet_anthropic_key');
  document.getElementById('api-key-input').value = saved || '';
  document.getElementById('key-status').textContent = saved ? '✓ Key saved' : '';
  document.getElementById('key-status').className = saved ? 'key-ok' : '';
  document.getElementById('settings-overlay').classList.add('open');
}
function closeSettings() { document.getElementById('settings-overlay').classList.remove('open'); }
function saveKey() {
  const key = document.getElementById('api-key-input').value.trim();
  if (!key) { alert('Enter an API key first'); return; }
  localStorage.setItem('muzzybet_anthropic_key', key);
  document.getElementById('key-status').textContent = '✓ Key saved';
  document.getElementById('key-status').className = 'key-ok';
}
function clearKey() {
  localStorage.removeItem('muzzybet_anthropic_key');
  document.getElementById('api-key-input').value = '';
  document.getElementById('key-status').textContent = '';
}

// ── Init ──────────────────────────────────────────────────────────────────────
populateMeetings();
refreshBank();
renderHistory();
</script>
</body>
</html>"""


def _build_html(races: list[dict], mode: str = "local") -> str:
    """Build the live page HTML.

    mode='local'  — OCR calls /api/ocr on the local server.
    mode='hosted' — OCR calls Anthropic API directly from the browser
                    using a key the user stores in localStorage.
    """
    return (
        _HTML_TEMPLATE
        .replace("%%RACES_JSON%%", json.dumps(races))
        .replace("%%OCR_MODE%%", mode)
        .replace("%%MIN_EDGE%%", str(_MIN_EDGE))
        .replace("%%KELLY%%", str(_KELLY_FRACTION))
        .replace("%%MAX_STK%%", str(_MAX_STAKE_PCT))
        .replace("%%START_BANK%%", str(_STARTING_BANK))
    )


def build_live_site(
    csv_path: str,
    weights_path: str | None = None,
    out_dir: str = "docs",
) -> Path:
    """Build docs/live.html for GitHub Pages hosting.

    Embeds all scored races as JSON. OCR calls Anthropic directly from
    the browser using a key the user saves in their browser's localStorage.
    """
    print("Loading and scoring races for live page...")
    races = _score_all_races(csv_path, weights_path)
    html = _build_html(races, mode="hosted")
    out_path = Path(out_dir) / "live.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"  {len(races)} races embedded -> {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Local HTTP server
# ---------------------------------------------------------------------------

class _LiveHandler(BaseHTTPRequestHandler):
    """Serves the live page and handles /api/ocr POST."""

    _html: bytes = b""

    def log_message(self, fmt, *args):  # suppress request logs
        pass

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self._send(200, "text/html; charset=utf-8", self._html)
        else:
            self._send(404, "text/plain", b"Not found")

    def do_POST(self):
        if self.path == "/api/ocr":
            self._handle_ocr()
        else:
            self._send(404, "text/plain", b"Not found")

    def do_OPTIONS(self):
        self._send(200, "text/plain", b"")

    def _handle_ocr(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        image_b64 = body.get("image", "")
        media_type = body.get("media_type", "image/png")
        try:
            import anthropic
            client = anthropic.Anthropic()
            response = client.messages.create(
                model="claude-opus-4-5",
                max_tokens=1024,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": media_type, "data": image_b64},
                        },
                        {
                            "type": "text",
                            "text": (
                                "Extract horse names and decimal odds from this TAB/betting screenshot. "
                                "Return a JSON array only, like: "
                                '[{"name": "HORSE NAME", "odds": 3.50}, ...]. '
                                "Use decimal odds (convert fractional: 5/2 → 3.50). "
                                "Uppercase horse names. No explanation, just the JSON array."
                            ),
                        },
                    ],
                }],
            )
            result = json.loads(response.content[0].text)
            self._send(200, "application/json", json.dumps(result).encode())
        except Exception as exc:
            self._send(200, "application/json", json.dumps({"error": str(exc)}).encode())

    def _send(self, status: int, content_type: str, body: bytes):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def serve_live(
    csv_path: str,
    weights_path: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8001,
) -> None:
    print("Loading and scoring races...")
    races = _score_all_races(csv_path, weights_path)
    html = _build_html(races, mode="local").encode("utf-8")
    print(f"  {len(races)} races loaded")

    handler = type("LiveHandler", (_LiveHandler,), {"_html": html})
    server = ThreadingHTTPServer((host, port), handler)

    has_anthropic = bool(os.environ.get("ANTHROPIC_API_KEY"))
    print(f"\nMuzzyBet Live: http://{host}:{port}")
    print(f"  OCR: {'enabled (ANTHROPIC_API_KEY found)' if has_anthropic else 'disabled (set ANTHROPIC_API_KEY to enable)'}")
    print("  Bet tracking: browser localStorage (persists across sessions)")
    print("  Starting bank: $100.00")
    print("\nPress Ctrl+C to stop.\n")

    try:
        server.serve_forever()
    finally:
        server.server_close()
