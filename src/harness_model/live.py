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

def _score_all_races(csv_path: str, weights_path: str | None, weights: dict | None = None) -> list[dict]:
    rows = load_feature_rows(csv_path)
    if weights is not None:
        w = weights
    elif weights_path and Path(weights_path).exists():
        w = load_weights(weights_path)
    else:
        w = None

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
        def _f(v):
            try: return round(float(v), 3) if v not in (None, "") else None
            except (TypeError, ValueError): return None

        races.append({
            "meeting_code": mc,
            "race_number": rn,
            "runners": [
                {
                    "runner_number": r.get("runner_number"),
                    "horse_name": r.get("horse_name", ""),
                    "barrier": r.get("barrier", ""),
                    "fair_odds": r.get("fair_odds"),
                    "stage1_score": r.get("stage1_score"),
                    "stage2_score": r.get("stage2_score"),
                    # Key features for AI analysis
                    "feat": {
                        "last_5_win_rate":   _f(r.get("last_5_win_rate")),
                        "season_wins":        _f(r.get("season_wins")),
                        "season_starts":      _f(r.get("season_starts")),
                        "career_wins":        _f(r.get("career_wins")),
                        "career_starts":      _f(r.get("career_starts")),
                        "career_win_rate":    _f(r.get("career_win_rate")),
                        "days_since_last_run": _f(r.get("days_since_last_run")),
                        "nr_rating":          _f(r.get("nr_rating")),
                        "nr_headroom":        _f(r.get("nr_headroom")),
                        "race_nr_ceiling":    _f(r.get("race_nr_ceiling")),
                        "nr_grade_delta":     _f(r.get("nr_grade_delta")),
                        "class_delta":        _f(r.get("class_delta")),
                        "consistency":        _f(r.get("recent_line_avg_class_adj_margin")),
                        "ceiling":            _f(r.get("recent_line_best_class_adj_margin")),
                        "driver_last_30_starts":   _f(r.get("driver_last_30_starts")),
                        "driver_last_30_wins":     _f(r.get("driver_last_30_wins")),
                        "driver_last_30_win_rate": _f(r.get("driver_last_30_win_rate")),
                        "trainer_last_30_starts":  _f(r.get("trainer_last_30_starts")),
                        "trainer_last_30_wins":    _f(r.get("trainer_last_30_wins")),
                        "trainer_last_30_win_rate":_f(r.get("trainer_last_30_win_rate")),
                    },
                    # Top components by absolute value for the prompt
                    "components": {
                        k: round(v, 3) for k, v in
                        sorted((r.get("components") or {}).items(),
                               key=lambda x: abs(x[1]), reverse=True)[:10]
                    },
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
th.sortable{cursor:pointer;user-select:none;white-space:nowrap}
th.sortable:hover{color:var(--primary-text)}
th.sort-asc,th.sort-desc{color:var(--accent)}
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

.ai-flag{margin-top:4px;padding:8px 12px;border-radius:6px;font-size:12px;line-height:1.5;border-left:3px solid var(--accent)}
.ai-flag.loading{color:var(--secondary);border-color:var(--secondary)}
.ai-flag.result{background:rgba(0,212,255,.06);color:var(--primary-text)}
.ai-flag.error{background:rgba(239,68,68,.08);color:var(--danger);border-color:var(--danger)}
.ai-flag .ai-category{font-weight:700;color:var(--accent);margin-bottom:3px}
.btn-analyse{background:rgba(0,212,255,.1);color:var(--accent);border:1px solid rgba(0,212,255,.3);border-radius:4px;padding:3px 8px;font-size:11px;cursor:pointer;white-space:nowrap}
.btn-analyse:hover{background:rgba(0,212,255,.2)}
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

.import-preview table{font-size:11px;width:100%}
.import-dup td{opacity:.4}
.import-won .imp-res{color:var(--winner);font-weight:700}
.import-lost .imp-res{color:var(--danger);font-weight:700}
</style>
</head>
<body>

<nav>
  <span class="nav-brand">MUZZYBET LIVE</span>
  <a href="index.html">Races</a>
  <a href="stats.html">Stats</a>
  <a href="betting.html">Betting</a>
  <a href="diagnose.html">Diagnose</a>
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

<!-- Import TAB Statement overlay -->
<div class="overlay" id="import-overlay" onclick="if(event.target===this)closeImport()">
  <div class="settings-box" style="width:780px;max-width:96vw;max-height:88vh;overflow-y:auto">
    <h3>Import TAB Statement</h3>
    <p class="settings-note">Copy rows directly from your TAB account history page and paste below. Any row containing <code style="color:var(--accent);font-size:10px">@Price:</code> is parsed as a harness bet. Already-recorded bets (same race + horse) are skipped automatically.</p>
    <label>Statement Text</label>
    <textarea id="import-txt" style="min-height:160px;font-size:11px;margin-bottom:10px" placeholder="Wed 6 May   19:00:16  FIXED ODDS   BATHURST R04 06/05 KENZIE SHANNON @Price:$23.00  23.00  Lost  $2.00  $0.00  $150.72&#10;Wed 6 May   18:00:00  FIXED ODDS   MENANGLE R05 02/05 OUR HORSE @Price:$4.50=Return:$9.00  4.50  Credit  $2.00  $9.00  $154.00&#10;..."></textarea>
    <div class="row" style="gap:8px;margin-bottom:12px">
      <button class="btn btn-accent" onclick="parseImport()">Parse Statement</button>
      <button class="btn btn-surface" onclick="closeImport()">Cancel</button>
    </div>
    <div id="import-preview"></div>
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
    <textarea id="tab-input" oninput="saveTabOdds()" placeholder="Paste TAB odds here, e.g.&#10;1 HORSE NAME $4.50&#10;2 ANOTHER HORSE $7.00&#10;&#10;Accepts most copy-paste formats."></textarea>
    <div class="row" style="margin-top:10px">
      <button class="btn btn-accent" onclick="calcBets()">Calculate Bets</button>
      <button class="btn btn-surface" id="ocr-btn" onclick="document.getElementById('img-input').click()">Screenshot OCR</button>
      <input type="file" id="img-input" accept="image/*" hidden onchange="runOCR(this)">
    </div>
    <p class="hint">Paste TAB text odds above, or press Ctrl+V anywhere on the page to paste a screenshot directly</p>
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
    <div style="display:flex;align-items:center;margin-bottom:14px">
      <span class="card-title" style="margin-bottom:0">Bet Tracker</span>
      <button class="btn btn-muted btn-sm" style="margin-left:auto" onclick="openImport()">+ Import TAB Statement</button>
    </div>
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
function mcToDate(mc) {
  // Meeting codes: 2-letter track prefix + DDMMYY  e.g. NR290426 = 29 Apr 2026
  if (mc.length < 8) return null;
  const dd = mc.slice(2, 4), mm = mc.slice(4, 6), yy = mc.slice(6, 8);
  const d = new Date(`20${yy}-${mm}-${dd}`);
  return isNaN(d) ? null : d;
}

function getMeetings() {
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const seen = new Set(), out = [];
  for (const r of RACES) {
    if (seen.has(r.meeting_code)) continue;
    const d = mcToDate(r.meeting_code);
    if (d === null || d >= today) { seen.add(r.meeting_code); out.push(r.meeting_code); }
  }
  return out;
}
function getRaces(mc) { return RACES.filter(r => r.meeting_code === mc).sort((a,b)=>a.race_number-b.race_number); }
function getRace(mc, rn) { return RACES.find(r => r.meeting_code === mc && r.race_number === +rn); }

// ── Race selector ─────────────────────────────────────────────────────────────
function populateMeetings() {
  const sel = document.getElementById('mc-sel');
  const meetings = getMeetings();
  if (!meetings.length) {
    const o = document.createElement('option');
    o.disabled = true; o.textContent = 'No upcoming meetings — run build-features and republish-all';
    sel.appendChild(o);
    return;
  }
  for (const mc of meetings) {
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

function tabOddsKey(mc, rn) { return `muzzybet_tab_${mc}_${rn}`; }

function onRnChange() {
  const mc = document.getElementById('mc-sel').value;
  const rn = document.getElementById('rn-sel').value;
  if (!mc || !rn) return;
  renderModelTable(mc, +rn);
  // Restore any previously saved TAB odds for this race
  const saved = localStorage.getItem(tabOddsKey(mc, rn));
  document.getElementById('tab-input').value = saved || '';
  document.getElementById('qual-panel').innerHTML = '<div class="empty">Paste TAB odds and click Calculate Bets</div>';
}

function saveTabOdds() {
  const mc = document.getElementById('mc-sel').value;
  const rn = document.getElementById('rn-sel').value;
  if (!mc || !rn) return;
  const val = document.getElementById('tab-input').value;
  if (val.trim()) localStorage.setItem(tabOddsKey(mc, rn), val);
  else localStorage.removeItem(tabOddsKey(mc, rn));
}

let _modelSort = 'number';  // 'number' or 'odds'

function renderModelTable(mc, rn) {
  const race = getRace(mc, rn);
  if (!race) return;
  const runners = [...race.runners];
  if (_modelSort === 'number') {
    runners.sort((a,b) => (a.runner_number??99) - (b.runner_number??99));
  }
  // 'odds' keeps original order (already sorted by probability desc from scoring)
  const numActive = _modelSort==='number';
  let h = `<div style="display:flex;gap:6px;margin-bottom:10px">
    <button class="btn btn-sm ${numActive?'btn-accent':'btn-surface'}" onclick="_modelSort='number';renderModelTable('${mc}',${rn})"># Runner No.</button>
    <button class="btn btn-sm ${!numActive?'btn-accent':'btn-surface'}" onclick="_modelSort='odds';renderModelTable('${mc}',${rn})">Model Price</button>
  </div>`;
  h += '<table><thead><tr><th>#</th><th>Horse</th><th>Bar</th><th>Model $</th></tr></thead><tbody>';
  for (const r of runners) {
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
    // Decimal or integer odds at end: "HORSE NAME $4.50" or "HORSE NAME 10"
    let m = stripped.match(/^(.+?)\s+\$?(\d+(?:[.]\d+)?)\s*$/);
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

function roundStake(raw) {
  // Round to nearest $0.50, minimum $1.00
  return Math.max(1.00, Math.round(raw * 2) / 2);
}

function kellyStake(e, tabOdds, bank) {
  if (e<=0 || tabOdds<=1) return 0;
  const k = (e/(tabOdds-1))*KELLY;
  const raw = Math.min(k*bank, bank*MAX_STK_PCT);
  return roundStake(raw);
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
    // Show Analyse button for big divergences: model >> market (big value) or market >> model (model missing favourite)
    const bigDivergence = r.runner && r.edge !== null && (r.edge >= 1.0 || r.edge <= -0.5);
    const runnerEnc = r.runner ? encodeURIComponent(JSON.stringify(r.runner)) : null;
    const analyseBtn = bigDivergence
      ? `<button class="btn-analyse" onclick="analyseRunner(this,${runnerEnc},'${mc}',${rn},${r.tabOdds},${r.edge})">AI</button>`
      : '';
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
        <td style="display:flex;gap:4px;align-items:center">
          <button class="btn btn-accent btn-sm" onclick="placeBet(this,'${betJson}')">Place</button>
          ${analyseBtn}
        </td>
      </tr>
      <tr class="ai-row-${r.runner.horse_name.replace(/\s+/g,'_')}"><td colspan="7" style="padding:0 8px 6px"></td></tr>`;
    } else {
      h += `<tr style="opacity:.4">
        <td class="name">${r.tabName}</td>
        <td>${matchLabel}</td>
        <td class="c">${modelStr}</td>
        <td>${r.tabOdds.toFixed(2)}</td>
        <td><span class="edge-pill ${eClass}">${edgeStr}</span></td>
        <td>—</td>
        <td>${analyseBtn}</td>
      </tr>
      ${bigDivergence ? `<tr class="ai-row-${r.runner.horse_name.replace(/\s+/g,'_')}"><td colspan="7" style="padding:0 8px 6px"></td></tr>` : ''}`;
    }
  }
  if (!anyQ) h += `<tr><td colspan="7"><div class="empty">No qualifying bets (edge &lt; ${(MIN_EDGE*100).toFixed(0)}%)</div></td></tr>`;
  h += '</tbody></table>';
  document.getElementById('qual-panel').innerHTML = h;
}

// ── AI race narrative analysis ────────────────────────────────────────────────
function buildAnalysisPrompt(runner, mc, rn, tabOdds, edgeVal) {
  const f = runner.feat || {};
  const comps = runner.components || {};
  const edgePct = (edgeVal * 100).toFixed(1);
  const direction = edgeVal >= 0 ? `Model ${edgePct}% longer than market (potential value)` : `Market ${(-edgeVal*100).toFixed(1)}% shorter than model (model missing favourite)`;

  const featLines = [
    `Last 5 win rate: ${f.last_5_win_rate!=null ? (f.last_5_win_rate*100).toFixed(0)+'%' : 'n/a'}`,
    `Season: ${f.season_wins??'?'}/${f.season_starts??'?'} wins`,
    `Career: ${f.career_wins??'?'}/${f.career_starts??'?'} (${f.career_win_rate!=null?(f.career_win_rate*100).toFixed(0)+'%':'n/a'})`,
    `Days since last run: ${f.days_since_last_run??'n/a'}`,
    `NR rating: ${f.nr_rating??'n/a'} | Race ceiling: ${f.race_nr_ceiling??'n/a'} | Headroom: ${f.nr_headroom!=null?(f.nr_headroom>0?'+':'')+f.nr_headroom:'n/a'}`,
    `Grade delta: ${f.nr_grade_delta!=null?(f.nr_grade_delta>0?'+':'')+f.nr_grade_delta.toFixed(1)+' (positive = dropping in grade)':'n/a'}`,
    `Class delta: ${f.class_delta!=null?'$'+(f.class_delta>0?'+':'')+f.class_delta.toFixed(0)+' (positive = higher purse race)':'n/a'}`,
    `Avg class-adj margin: ${f.consistency!=null?f.consistency.toFixed(1)+'m':'n/a'} | Best: ${f.ceiling!=null?f.ceiling.toFixed(1)+'m':'n/a'}`,
    `Driver last 30: ${f.driver_last_30_wins??'?'}/${f.driver_last_30_starts??'?'} (${f.driver_last_30_win_rate!=null?(f.driver_last_30_win_rate*100).toFixed(0)+'%':'n/a'})`,
    `Trainer last 30: ${f.trainer_last_30_wins??'?'}/${f.trainer_last_30_starts??'?'} (${f.trainer_last_30_win_rate!=null?(f.trainer_last_30_win_rate*100).toFixed(0)+'%':'n/a'})`,
  ].join('\n');

  const compLines = Object.entries(comps).slice(0,8)
    .map(([k,v]) => `  ${k}: ${v>0?'+':''}${v.toFixed(3)}`).join('\n');

  return `You are a harness racing analyst. A pricing model has significantly disagreed with the market. Identify the most likely reason from the data below.

Horse: ${runner.horse_name} | Meeting: ${mc} R${rn}
Model: $${runner.fair_odds?.toFixed(2)??'?'} | Market: $${tabOdds.toFixed(2)} | ${direction}
S1 score: ${runner.stage1_score??'n/a'} | S2 score: ${runner.stage2_score??'n/a'}

FORM:
${featLines}

TOP MODEL COMPONENTS (by magnitude):
${compLines}

Respond in EXACTLY this format, nothing else:
FLAG: [one of: Grade drop underpriced | Declining form | Cold operator | Fitness concern | Thin data | Class mismatch | Market overreaction | No clear reason]
REASON: [1-2 sentences identifying the specific issue in the data]`;
}

async function analyseRunner(btn, runner, mc, rn, tabOdds, edgeVal) {
  const key = localStorage.getItem('muzzybet_anthropic_key');
  if (!key) { alert('No API key — open Settings and enter your Anthropic API key'); return; }

  const rowKey = runner.horse_name.replace(/\s+/g,'_');
  const aiRow = document.querySelector(`.ai-row-${rowKey} td`);
  if (!aiRow) return;

  btn.disabled = true;
  btn.textContent = '...';
  aiRow.innerHTML = '<div class="ai-flag loading">Analysing...</div>';

  try {
    const prompt = buildAnalysisPrompt(runner, mc, rn, tabOdds, edgeVal);
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
        'anthropic-dangerous-direct-browser-access': 'true',
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5',
        max_tokens: 200,
        messages: [{ role: 'user', content: prompt }],
      }),
    });
    if (!resp.ok) throw new Error(`API error ${resp.status}`);
    const data = await resp.json();
    const text = data.content?.[0]?.text?.trim() ?? '';
    const flagMatch   = text.match(/FLAG:\s*(.+)/i);
    const reasonMatch = text.match(/REASON:\s*(.+)/i);
    const flag   = flagMatch?.[1]?.trim() ?? 'Unknown';
    const reason = reasonMatch?.[1]?.trim() ?? text;
    aiRow.innerHTML = `<div class="ai-flag result"><div class="ai-category">${flag}</div>${reason}</div>`;
    btn.textContent = 'AI';
    btn.disabled = false;
  } catch(e) {
    aiRow.innerHTML = `<div class="ai-flag error">Analysis failed: ${e.message}</div>`;
    btn.textContent = 'AI';
    btn.disabled = false;
  }
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
  const stake = roundStake(Math.min(d.stake, bank*MAX_STK_PCT));
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
let _histSort = { col: 'date', dir: 'desc' };

function sortHistory(col) {
  if (_histSort.col === col) {
    _histSort.dir = _histSort.dir === 'asc' ? 'desc' : 'asc';
  } else {
    _histSort.col = col;
    _histSort.dir = col === 'date' ? 'desc' : 'asc';
  }
  renderHistory();
}

function _betSortKey(b, col) {
  switch (col) {
    case 'date':   return b.placed_at || '';
    case 'race':   return (b.mc || '') + String(b.rn || 0).padStart(3, '0');
    case 'horse':  return b.horse || '';
    case 'model':  return b.model_odds ?? 9999;
    case 'tab':    return b.tab_odds ?? 0;
    case 'edge':   return b.edge ?? -999;
    case 'stake':  return b.stake ?? 0;
    case 'return': {
      if (b.result === 'won')  return (b.payout ?? 0) - (b.stake ?? 0);
      if (b.result === 'lost') return -(b.stake ?? 0);
      return 0;
    }
    case 'result': return b.result || 'z';
    default:       return 0;
  }
}

function _thSort(col, label) {
  const active = _histSort.col === col;
  const arrow  = active ? (_histSort.dir === 'asc' ? ' ▲' : ' ▼') : '';
  const cls    = active ? (_histSort.dir === 'asc' ? 'sort-asc' : 'sort-desc') : '';
  return `<th class="sortable ${cls}" onclick="sortHistory('${col}')">${label}${arrow}</th>`;
}

function renderHistory() {
  const raw = loadBets();
  if (!raw.length) {
    document.getElementById('hist-panel').innerHTML = '<div class="empty">No bets recorded yet</div>';
    return;
  }

  // Sort a copy — default (date desc) = newest first
  const bets = [...raw].sort((a, b) => {
    const ka = _betSortKey(a, _histSort.col);
    const kb = _betSortKey(b, _histSort.col);
    const cmp = ka < kb ? -1 : ka > kb ? 1 : 0;
    return _histSort.dir === 'asc' ? cmp : -cmp;
  });

  let h = `<table><thead><tr>
    ${_thSort('date',   'Date')}
    ${_thSort('race',   'Race')}
    ${_thSort('horse',  'Horse')}
    ${_thSort('model',  'Model $')}
    ${_thSort('tab',    'TAB $')}
    ${_thSort('edge',   'Edge')}
    ${_thSort('stake',  'Stake')}
    ${_thSort('return', 'Return')}
    ${_thSort('result', 'Result')}
    <th></th>
  </tr></thead><tbody>`;

  for (const b of bets) {
    const dt = new Date(b.placed_at).toLocaleDateString('en-AU', {day:'2-digit', month:'short'});
    const rClass  = b.result==='won' ? 'r-won' : b.result==='lost' ? 'r-lost' : b.result==='void' ? 'r-void' : '';
    const retStr  = b.result==='won'  ? `<span class="g">+$${((b.payout??0)-(b.stake??0)).toFixed(2)}</span>`
                  : b.result==='lost' ? `<span class="r">−$${(b.stake??0).toFixed(2)}</span>` : '—';
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
      <td class="c">${b.model_odds!=null?'$'+b.model_odds.toFixed(2):'—'}</td>
      <td>$${(b.tab_odds??0).toFixed(2)}</td>
      <td><span class="edge-pill ${b.edge!=null?'edge-q':'edge-n'}">${b.edge!=null?(b.edge*100).toFixed(1)+'%':'—'}</span></td>
      <td>$${(b.stake??0).toFixed(2)}</td>
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
const OCR_PROMPT = 'Extract horse names and decimal odds from this TAB/betting screenshot. Return a JSON array only: [{"name":"HORSE NAME","odds":3.50},...]. Uppercase names, decimal odds only. If you cannot find any odds, return an empty array []. Never return text — always return a valid JSON array.';

async function runOCRFromFile(file) {
  const btn = document.getElementById('ocr-btn');
  btn.textContent = 'Analysing...'; btn.disabled = true;
  try {
    const b64 = await new Promise((res,rej)=>{ const r=new FileReader(); r.onload=()=>res(r.result.split(',')[1]); r.onerror=rej; r.readAsDataURL(file); });
    let data;
    if (OCR_MODE === 'local') {
      const resp = await fetch('/api/ocr', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({image:b64, media_type:file.type})});
      if (!resp.ok) throw new Error(await resp.text());
      data = await resp.json();
    } else {
      const apiKey = localStorage.getItem('muzzybet_anthropic_key');
      if (!apiKey) { openSettings(); throw new Error('Enter your Anthropic API key in OCR Settings'); }
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
      const raw = result.content[0].text.replace(/^```[\w]*\n?/, '').replace(/\n?```$/, '').trim();
      try { data = JSON.parse(raw); }
      catch { throw new Error('Could not read odds from image — make sure the screenshot shows TAB odds clearly'); }
    }
    if (data.error) throw new Error(data.error);
    if (!Array.isArray(data) || !data.length) throw new Error('No odds found in image — make sure the screenshot shows TAB odds clearly');
    document.getElementById('tab-input').value = data.map(h=>`${h.name} ${h.odds}`).join('\n');
    saveTabOdds();
    btn.textContent = `OCR: ${data.length} horses found`;
  } catch(e) {
    alert('OCR failed: '+e.message);
    btn.textContent = 'Screenshot OCR';
  } finally {
    btn.disabled = false;
  }
}

// File picker trigger
function runOCR(input) {
  if (!input.files.length) return;
  runOCRFromFile(input.files[0]).finally(() => { input.value = ''; });
}

// Paste anywhere on the page — intercepts clipboard images (Ctrl+V / Cmd+V)
document.addEventListener('paste', (e) => {
  const items = e.clipboardData?.items;
  if (!items) return;
  for (const item of items) {
    if (item.type.startsWith('image/')) {
      e.preventDefault();
      runOCRFromFile(item.getAsFile());
      return;
    }
  }
});

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

// ── TAB Statement Import ──────────────────────────────────────────────────────
// Venue name → 2-letter meeting-code prefix mapping
const VENUE_PREFIX = {
  'WAGGA WAGGA':    'WG',
  'PORT MACQUARIE': 'PM',
  'PORT KEMBLA':    'PK',
  'HUNTER VALLEY':  'HV',
  'PENRITH PARK':   'PE',
  'BATHURST':       'BH',
  'PENRITH':        'PE',
  'TAMWORTH':       'TA',
  'WAGGA':          'WG',
  'MENANGLE':       'MN',
  'NEWCASTLE':      'NR',
  'ALBURY':         'AL',
  'CANBERRA':       'CB',
  'GOULBURN':       'LM',
  'NARROMINE':      'NA',
  'PARKES':         'PK',
  'DUBBO':          'DU',
  'GOSFORD':        'GO',
  'MAITLAND':       'MA',
  'CESSNOCK':       'CK',
  'YOUNG':          'YG',
  'ORANGE':         'OG',
  'GRIFFITH':       'GR',
};

// Build regex — sort longest venue names first so "WAGGA WAGGA" matches before "WAGGA"
const _venueKeys = Object.keys(VENUE_PREFIX).sort((a, b) => b.length - a.length);
const _venuePat  = _venueKeys.map(v => v.replace(/\s+/g, '\\s+')).join('|');
// Matches: VENUE R## DD/MM HORSE NAME @Price:$X.XX[=Return:$Y.YY]
const _descRe = new RegExp(
  `(${_venuePat})\\s+R0*(\\d+)\\s+(\\d{2})\\/(\\d{2})\\s+(.+?)\\s+@Price:\\$(\\d+\\.?\\d*)(?:=Return:\\$(\\d+\\.?\\d*))?`,
  'i'
);

let _importParsed = [];

function _venuePrefix(name) {
  const u = name.toUpperCase();
  for (const k of _venueKeys) {
    if (u.replace(/\s+/g,' ').startsWith(k)) return VENUE_PREFIX[k];
  }
  return u.replace(/\s+/g,'').slice(0,2);
}

function parseTabStatement(text) {
  // The TAB statement is tab-separated with columns:
  //   Date | Time | Type | Description | Detail | Odds | Result | Debit | Credit | Balance
  //
  // IMPORTANT: =Return:$X in the Description is the POTENTIAL return printed on every bet
  // regardless of outcome — it is NOT the actual result. Result is in col[6] ("Lost"/"Credit"/blank).
  // Winning bets receive a separate "SGL" credit row with the actual payout amount.
  // We match SGL rows to bet rows by their shared Transaction number.

  const lines = text.split(/\r?\n/).map(l => l.trim()).filter(Boolean);

  // Pass 1: collect SGL credit rows → Map<transaction_number, actual_payout>
  const sglCredits = new Map();
  for (const line of lines) {
    if (line.includes('@Price:')) continue;   // skip bet rows
    const cols = line.split('\t');
    // SGL credit rows have "SGL" in the Detail column (col[4]) and credit > 0 in col[8]
    if (cols.length < 9) continue;
    const detail = (cols[4] || '').trim().toUpperCase();
    if (detail !== 'SGL') continue;
    const txnM = (cols[3] || '').match(/Transaction number:\s*(\d+)/);
    if (!txnM) continue;
    const credit = parseFloat((cols[8] || '').replace(/[^0-9.]/g, ''));
    if (!isNaN(credit) && credit > 0) sglCredits.set(txnM[1], credit);
  }

  // Pass 2: parse bet rows
  const bets = [];
  for (const line of lines) {
    if (!line.includes('@Price:')) continue;

    const cols = line.split('\t');
    if (cols.length < 8) continue;

    const time      = (cols[1] || '12:00:00').trim();
    const desc      = (cols[3] || '').trim();
    const resultCol = (cols[6] || '').trim().toLowerCase();  // "lost" / "credit" / ""

    const m = desc.match(_descRe);
    if (!m) continue;

    const venueRaw = m[1];
    const rn       = parseInt(m[2]);
    const dd       = m[3], mm = m[4];
    const horse    = m[5].trim().replace(/\s+Deductions\s+Apply\s*/i, '').trim().toUpperCase();
    const tabOdds  = parseFloat(m[6]);
    // m[7] = potential return shown on all bets — only used as payout fallback

    const prefix   = _venuePrefix(venueRaw);
    const mc       = `${prefix}${dd}${mm}26`;
    const placedAt = `2026-${mm}-${dd}T${time}+10:00`;

    // Stake from Debit column (col[7])
    let stake = 1.0;
    const debitStr = parseFloat((cols[7] || '').replace(/[^0-9.]/g, ''));
    if (!isNaN(debitStr) && debitStr > 0) stake = debitStr;

    // Match to SGL credit row by transaction number
    const txnM = desc.match(/Transaction number:\s*(\d+)/);
    const txn  = txnM ? txnM[1] : null;
    const sglCredit = txn !== null ? sglCredits.get(txn) : undefined;

    // Determine result:
    //   SGL credit > stake  → won (actual payout = SGL credit, may differ from stated return due to deductions)
    //   SGL credit ≈ stake  → void (stake refunded)
    //   resultCol = "lost"  → lost
    //   resultCol = "credit" but no SGL match → treat as won using stated potential return
    //   blank               → pending
    let result = 'pending', payout = null;
    if (sglCredit !== undefined) {
      if (sglCredit > stake * 1.001) {
        result = 'won';
        payout = sglCredit;
      } else {
        result = 'void';   // credit ≈ stake = refund
      }
    } else if (resultCol === 'lost') {
      result = 'lost';
    } else if (resultCol === 'credit') {
      // Credit result but no SGL row in this paste — use potential return as estimate
      result = 'won';
      payout = m[7] ? parseFloat(m[7]) : null;
    }
    if (!payout) payout = parseFloat((stake * tabOdds).toFixed(2));

    // Auto-fill model odds if race is in current RACES data
    const raceData = getRace(mc, rn);
    let modelOdds = null;
    if (raceData) {
      const rm = bestMatch(horse, raceData.runners);
      if (rm && rm.dist <= 4) modelOdds = rm.runner.fair_odds ?? null;
    }
    const edgeVal = (modelOdds && tabOdds) ? edge(tabOdds, modelOdds) : null;

    bets.push({ mc, rn, horse, tab_odds: tabOdds, model_odds: modelOdds,
                edge: edgeVal, stake, payout, result, placed_at: placedAt });
  }
  return bets;
}

function openImport() {
  document.getElementById('import-txt').value = '';
  document.getElementById('import-preview').innerHTML = '';
  document.getElementById('import-overlay').classList.add('open');
}
function closeImport() { document.getElementById('import-overlay').classList.remove('open'); }

function parseImport() {
  const text = document.getElementById('import-txt').value;
  _importParsed = parseTabStatement(text);
  if (!_importParsed.length) {
    document.getElementById('import-preview').innerHTML =
      '<div class="empty">No bets found — paste rows that contain @Price: (e.g. copy from TAB account history)</div>';
    return;
  }
  renderImportPreview(_importParsed);
}

function renderImportPreview(bets) {
  const existing = loadBets();
  const isDup = b => existing.some(e =>
    e.mc === b.mc && e.rn === b.rn &&
    e.horse.toUpperCase() === b.horse.toUpperCase());

  const newCount = bets.filter(b => !isDup(b)).length;
  const dupCount = bets.length - newCount;

  let h = `<div style="margin-bottom:8px;color:var(--secondary);font-size:11px">
    ${bets.length} bets parsed &nbsp;·&nbsp;
    <span style="color:var(--winner)">${newCount} new</span> &nbsp;·&nbsp;
    ${dupCount} already recorded (will be skipped)
  </div>`;

  h += '<div class="import-preview"><table><thead><tr>';
  h += '<th>Race</th><th>Horse</th><th>Model $</th><th>TAB $</th><th>Edge %</th><th>Stake</th><th>Result</th><th>Status</th>';
  h += '</tr></thead><tbody>';

  for (let i = 0; i < bets.length; i++) {
    const b      = bets[i];
    const dup    = isDup(b);
    const rowCls = dup ? 'import-dup' : b.result === 'won' ? 'import-won' : b.result === 'lost' ? 'import-lost' : '';
    const modStr = b.model_odds ? '$' + b.model_odds.toFixed(2) : '—';
    const resTxt = b.result === 'won'  ? '<span class="imp-res">WON</span>'
                 : b.result === 'lost' ? '<span class="imp-res">LOST</span>'
                 : b.result === 'void' ? '<span style="color:var(--secondary)">VOID</span>'
                 : '<span style="color:var(--secondary)">PENDING</span>';
    const status = dup ? '<span style="color:var(--secondary)">skip (dup)</span>'
                       : '<span class="match-ok">import</span>';

    // Edge: editable input for new bets (auto-filled where model data exists),
    //       static text for duplicates (they won't be imported)
    const edgePct = b.edge != null ? (b.edge * 100).toFixed(1) : '';
    const edgeCell = dup
      ? `<td style="color:var(--secondary)">${edgePct ? edgePct + '%' : '—'}</td>`
      : `<td><input class="imp-edge-in" data-idx="${i}" type="number" step="0.1"
            value="${edgePct}" placeholder="—"
            style="width:64px;background:var(--surface);color:var(--primary-text);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px;font-family:'Roboto Mono',monospace"
            title="Edge % — auto-filled where model data available; type manually if needed"></td>`;

    h += `<tr class="${rowCls}">
      <td>${b.mc} R${b.rn}</td>
      <td class="name">${b.horse}</td>
      <td class="c">${modStr}</td>
      <td>$${b.tab_odds.toFixed(2)}</td>
      ${edgeCell}
      <td>$${b.stake.toFixed(2)}</td>
      <td>${resTxt}</td>
      <td>${status}</td>
    </tr>`;
  }
  h += '</tbody></table></div>';

  if (newCount > 0) {
    h += `<div style="margin-top:12px;display:flex;align-items:center;gap:10px">
      <button class="btn btn-accent" onclick="confirmImport()">Import ${newCount} New Bet${newCount !== 1 ? 's' : ''}</button>
      <span style="color:var(--secondary);font-size:11px">Duplicates skipped · Edge auto-filled where model data available — edit any cell to override</span>
    </div>`;
  } else {
    h += '<div style="margin-top:10px;color:var(--secondary);font-size:11px">All parsed bets are already in the tracker.</div>';
  }

  document.getElementById('import-preview').innerHTML = h;
}

function confirmImport() {
  // Read any manually entered / overridden edge values from the preview inputs
  document.querySelectorAll('.imp-edge-in').forEach(input => {
    const idx = parseInt(input.dataset.idx);
    const val = parseFloat(input.value);
    _importParsed[idx].edge = isNaN(val) ? null : val / 100;  // convert % → decimal
  });

  const existing = loadBets();
  const isDup = b => existing.some(e =>
    e.mc === b.mc && e.rn === b.rn &&
    e.horse.toUpperCase() === b.horse.toUpperCase());

  const toImport = _importParsed.filter(b => !isDup(b));
  let nextId = Date.now();
  for (const b of toImport) {
    existing.push({
      id:          nextId++,
      placed_at:   b.placed_at,
      mc:          b.mc,
      rn:          b.rn,
      horse:       b.horse,
      model_odds:  b.model_odds,
      tab_odds:    b.tab_odds,
      edge:        b.edge,
      stake:       b.stake,
      payout:      b.payout,
      result:      b.result,
    });
  }
  saveBets(existing);
  refreshBank();
  renderHistory();
  closeImport();
  alert(`Imported ${toImport.length} bet${toImport.length !== 1 ? 's' : ''} successfully.`);
}

// ── Init ──────────────────────────────────────────────────────────────────────

// Migrate any bets recorded before $0.50 rounding was introduced
(function migrateStakes() {
  const bets = loadBets();
  let changed = false;
  for (const b of bets) {
    const rounded = roundStake(b.stake);
    if (rounded !== b.stake) {
      b.payout = parseFloat((rounded * b.tab_odds).toFixed(2));
      b.stake = rounded;
      changed = true;
    }
  }
  if (changed) saveBets(bets);
})();

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
    weights: dict | None = None,
    out_dir: str = "docs",
) -> Path:
    """Build docs/live.html for GitHub Pages hosting.

    Embeds all scored races as JSON. OCR calls Anthropic directly from
    the browser using a key the user saves in their browser's localStorage.
    """
    print("Loading and scoring races for live page...")
    races = _score_all_races(csv_path, weights_path, weights=weights)
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
            import re
            raw = re.sub(r'^```[\w]*\n?', '', response.content[0].text.strip())
            raw = re.sub(r'\n?```$', '', raw).strip()
            result = json.loads(raw)
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
