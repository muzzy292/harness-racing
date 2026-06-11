from __future__ import annotations

import html
import json
import os
import sqlite3
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .odds import load_feature_rows, load_market_rows, load_weights, score_meeting_rows
from .storage import connect, init_db

_QLD_PREFIXES: frozenset[str] = frozenset({"AP", "RE", "UG"})


def _meeting_state(meeting_code: str) -> str:
    """Return 'QLD' for QLD meetings, 'NSW' for all others."""
    return "QLD" if str(meeting_code)[:2].upper() in _QLD_PREFIXES else "NSW"


def _load_state_weights(base: dict | None) -> dict[str, dict | None]:
    """Resolve per-state weight sets for the web pipeline.

    weights-nsw.json / weights-qld.json (repo root, alongside weights.json)
    override the base weights for meetings in that state. A state without its
    own file falls back to the base weights, so behaviour is unchanged until
    a state file is created. NSW and QLD have structurally different class
    systems (NR bands vs win restrictions) and need independent calibration.
    """
    resolved: dict[str, dict | None] = {}
    for st in ("NSW", "QLD"):
        path = Path(f"weights-{st.lower()}.json")
        resolved[st] = load_weights(path) if path.exists() else base
    return resolved


def build_meeting_site(
    meeting_code: str,
    csv_path: str | Path = "data/features/runner_features.csv",
    db_path: str | Path = "data/harness.db",
    out_dir: str | Path = "data/site",
    market_csv: str | None = None,
    min_probability: float = 0.0,
    max_probability: float = 1.0,
    model_weight: float | None = None,
    market_weight: float | None = None,
    temperature: float | None = None,
    weights: dict | None = None,
) -> Path:
    rows = load_feature_rows(csv_path)
    market_rows = load_market_rows(market_csv) if market_csv else None
    meeting_scores = score_meeting_rows(
        rows,
        meeting_code,
        min_probability=min_probability,
        max_probability=max_probability,
        market_rows=market_rows,
        model_weight=model_weight,
        market_weight=market_weight,
        temperature=temperature,
        weights=weights,
    )

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    conn = connect(db_path)
    init_db(conn)
    meeting_meta = _load_meeting_metadata(conn, meeting_code)
    result_rows = _load_results(conn, meeting_code)
    conn.close()

    races_scored = sum(1 for rows in meeting_scores.values() if rows)
    winners_count = _count_top_pick_winners(meeting_scores, result_rows)
    page_path = out_path / f"{meeting_code}.html"
    page_path.write_text(
        _render_meeting_html(meeting_code, meeting_scores, meeting_meta, result_rows),
        encoding="utf-8",
    )
    _write_index(out_path, meeting_meta=meeting_meta, races=races_scored, winners=winners_count)
    return page_path


def serve_site(site_dir: str | Path = "data/site", host: str = "127.0.0.1", port: int = 8000) -> None:
    root = Path(site_dir)
    root.mkdir(parents=True, exist_ok=True)
    os.chdir(root)
    server = ThreadingHTTPServer((host, port), SimpleHTTPRequestHandler)
    print(f"Serving {root.resolve()} at http://{host}:{port}")
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _track_from_raw_title(raw_title: str | None) -> str | None:
    if not raw_title:
        return None
    prefix = "form guide for "
    lower = raw_title.lower()
    if prefix not in lower:
        return None
    name = raw_title[lower.index(prefix) + len(prefix):]
    for sep in ("\xa0", "  "):
        if sep in name:
            name = name[: name.index(sep)]
    if " at " in name:
        name = name.split(" at ")[0]
    return name.strip() or None


def _load_meeting_metadata(conn: sqlite3.Connection, meeting_code: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT meeting_code, meeting_date, track_name, state, raw_title
        FROM meetings
        WHERE meeting_code = ?
        LIMIT 1
        """,
        (meeting_code,),
    ).fetchone()
    if row is None:
        return {"meeting_code": meeting_code, "meeting_date": None, "track_name": None, "state": None, "raw_title": None}
    result = dict(row)
    if not result.get("track_name"):
        result["track_name"] = _track_from_raw_title(result.get("raw_title"))
    return result


def _count_top_pick_winners(
    meeting_scores: dict[int, list[dict[str, object]]],
    result_rows: dict[int, dict[str, dict[str, Any]]],
) -> int:
    """Count races where the model's top pick (highest win_probability) finished first."""
    count = 0
    for race_number, rows in meeting_scores.items():
        if not rows:
            continue
        race_results = result_rows.get(race_number, {})
        if not race_results:
            continue
        top_pick = max(rows, key=lambda r: float(r.get("win_probability") or 0.0))
        top_pick_key = _normalise_name(str(top_pick.get("horse_name") or ""))
        result = race_results.get(top_pick_key)
        if result and result.get("finish_position") == 1:
            count += 1
    return count


def _load_results(conn: sqlite3.Connection, meeting_code: str) -> dict[int, dict[str, dict[str, Any]]]:
    results: dict[int, dict[str, dict[str, Any]]] = {}
    rows = conn.execute(
        """
        SELECT race_number, horse_name, finish_position, margin, starting_price, steward_comment
        FROM race_results
        WHERE meeting_code = ?
        ORDER BY race_number, finish_position
        """,
        (meeting_code,),
    ).fetchall()
    for row in rows:
        race_number = int(row["race_number"])
        race_bucket = results.setdefault(race_number, {})
        race_bucket[_normalise_name(str(row["horse_name"]))] = dict(row)
    return results


def _write_index(
    out_dir: Path,
    meeting_meta: dict[str, Any] | None = None,
    races: int | None = None,
    winners: int | None = None,
) -> None:
    manifest_path = out_dir / "meetings.json"
    if manifest_path.exists():
        manifest: list[dict[str, Any]] = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = []

    if meeting_meta:
        code = meeting_meta.get("meeting_code", "")
        existing = next((m for m in manifest if m.get("meeting_code") == code), None)
        if existing:
            existing.update({k: meeting_meta[k] for k in ("track_name", "meeting_date") if meeting_meta.get(k)})
            if races is not None:
                existing["races"] = races
            if winners is not None:
                existing["winners"] = winners
        else:
            manifest.append({
                "meeting_code": code,
                "track_name": meeting_meta.get("track_name"),
                "meeting_date": meeting_meta.get("meeting_date"),
                "races": races,
                "winners": winners,
                "state": _meeting_state(code),
            })
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Fall back to filesystem scan for any HTML files not yet in manifest
    known_codes = {m["meeting_code"] for m in manifest}
    for page in sorted(out_dir.glob("*.html")):
        if page.stem.upper() in known_codes or page.name.lower() == "index.html":
            continue
        manifest.append({"meeting_code": page.stem.upper(), "track_name": None, "meeting_date": None})

    def _date_sort_key(m: dict) -> datetime:
        try:
            return datetime.strptime(m.get("meeting_date") or "", "%d %b %Y")
        except ValueError:
            return datetime.min

    manifest_sorted = sorted(manifest, key=_date_sort_key, reverse=True)
    cards: list[str] = []
    for m in manifest_sorted:
        code = m["meeting_code"]
        track = html.escape(m.get("track_name") or "")
        date = html.escape(m.get("meeting_date") or "")
        meta_line = " · ".join(part for part in [date, track] if part)
        races_val = m.get("races")
        winners_val = m.get("winners")
        stats_parts = []
        if races_val is not None:
            stats_parts.append(f"{races_val} races")
        if winners_val is not None:
            stats_parts.append(f"{winners_val} winners")
        stats_html = f'<span class="meeting-stats">{html.escape(" · ".join(stats_parts))}</span>' if stats_parts else ""
        state_attr = html.escape(m.get("state") or _meeting_state(code))
        cards.append(
            f"""
            <a class="meeting-card" data-state="{state_attr}" href="{html.escape(code)}.html">
              <span class="meeting-code">{html.escape(code)}</span>
              {f'<span class="meeting-meta">{meta_line}</span>' if meta_line else ''}
              {stats_html}
              <span class="meeting-link">Open meeting page</span>
            </a>
            """
        )

    index_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Harness Racing Scores</title>
  <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@700;800;900&family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;600&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg:           #0a0a0a;
      --card-bg:      #101010;
      --surface:      #131313;
      --primary-text: #f0f0f0;
      --secondary:    #808080;
      --accent:       #00d4ff;
      --accent-dark:  #00aacc;
      --border:       #1f1f1f;
      --highlight:    #1a1a1a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      background-color: var(--bg);
      color: var(--primary-text);
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }}
    .top-nav {{
      background: #060606;
      padding: 10px 20px;
      display: flex;
      gap: 4px;
      align-items: center;
      border-bottom: 1px solid var(--border);
    }}
    .top-nav a {{
      color: var(--secondary);
      text-decoration: none;
      font-size: 13px;
      font-weight: 600;
      padding: 6px 14px;
      border-radius: 5px;
      transition: all 0.15s;
      letter-spacing: 0.02em;
    }}
    .top-nav a:hover {{ background: var(--highlight); color: var(--primary-text); }}
    .top-nav a.active {{ background: rgba(0,212,255,0.08); color: var(--accent); }}
    .hero {{
      background: #060606;
      color: var(--primary-text);
      padding: 40px 20px 36px;
      text-align: center;
      border-bottom: 1px solid var(--border);
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 36px;
      font-weight: 900;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      font-family: 'Barlow Condensed', 'Inter', sans-serif;
    }}
    .hero p {{
      margin: 0;
      color: var(--secondary);
      font-size: 14px;
    }}
    .wrap {{ max-width: 960px; margin: 0 auto; padding: 32px 20px 60px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 10px;
    }}
    .meeting-card {{
      display: flex;
      flex-direction: column;
      gap: 5px;
      padding: 18px 20px;
      text-decoration: none;
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      transition: border-color 0.15s, background 0.15s;
    }}
    .meeting-card:hover {{
      border-color: var(--accent);
      background: rgba(0,212,255,0.04);
    }}
    .meeting-code {{
      font-size: 16px;
      font-weight: 800;
      color: var(--primary-text);
      letter-spacing: 0.06em;
      font-family: 'Barlow Condensed', 'Inter', sans-serif;
      text-transform: uppercase;
    }}
    .meeting-meta {{ color: var(--secondary); font-size: 12px; }}
    .meeting-stats {{ color: var(--primary-text); font-size: 12px; font-weight: 600; }}
    .meeting-link {{ margin-top: 4px; color: var(--accent); font-size: 13px; font-weight: 600; }}
    .filter-btn{{background:var(--surface);color:var(--secondary);border:1px solid var(--border);border-radius:5px;padding:6px 16px;font-size:13px;font-weight:600;cursor:pointer;font-family:'Inter',sans-serif}}
    .filter-btn.active{{background:rgba(0,212,255,0.12);color:var(--accent);border-color:var(--accent)}}
  </style>
</head>
<body>
  <nav class="top-nav">
    <a href="index.html" class="active">Meetings</a>
    <a href="stats.html">Stats</a>
    <a href="betting.html">Betting</a>
    <a href="betting-nsw.html" style="font-size:11px;padding:3px 8px">NSW</a>
    <a href="betting-qld.html" style="font-size:11px;padding:3px 8px">QLD</a>
    <a href="diagnose.html">Diagnose</a>
    <a href="diagnose-nsw.html" style="font-size:11px;padding:3px 8px">NSW</a>
    <a href="diagnose-qld.html" style="font-size:11px;padding:3px 8px">QLD</a>
    <a href="live.html">Live</a>
  </nav>
  <div class="hero">
    <h1>Harness Racing Scores</h1>
    <p>Fair odds and probabilities — select a meeting below</p>
  </div>
  <div class="wrap">
    <div class="filter-bar" style="display:flex;gap:8px;margin-bottom:20px">
      <button class="filter-btn active" onclick="filterState('all')">All</button>
      <button class="filter-btn" onclick="filterState('NSW')">NSW</button>
      <button class="filter-btn" onclick="filterState('QLD')">QLD</button>
    </div>
    <div class="grid">
      {''.join(cards) if cards else '<p>No meeting pages built yet.</p>'}
    </div>
  </div>
<script>
function filterState(s) {{
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.toggle('active', b.textContent===s||(s==='all'&&b.textContent==='All')));
  document.querySelectorAll('.meeting-card').forEach(c => {{
    c.style.display = (s==='all' || c.dataset.state===s) ? '' : 'none';
  }});
}}
</script>
</body>
</html>
"""
    (out_dir / "index.html").write_text(index_html, encoding="utf-8")


def _render_meeting_html(
    meeting_code: str,
    meeting_scores: dict[int, list[dict[str, object]]],
    meeting_meta: dict[str, Any],
    result_rows: dict[int, dict[str, dict[str, Any]]],
) -> str:
    meeting_title = " — ".join(part for part in [meeting_meta.get("track_name"), meeting_meta.get("meeting_date")] if part) or meeting_code
    generated = datetime.now().strftime("%d %b %Y %H:%M")
    race_nav = "".join(
        f'<a href="#race-{race_number}">Race {race_number}</a>'
        for race_number, rows in meeting_scores.items() if rows
    )

    races_scored = sum(1 for rows in meeting_scores.values() if rows)
    winners_count = _count_top_pick_winners(meeting_scores, result_rows)
    summary_cards = [
        ("Meeting", meeting_code),
        ("Track", meeting_meta.get("track_name") or "Unknown"),
        ("Date", meeting_meta.get("meeting_date") or "Unknown"),
        ("Races", str(races_scored)),
        ("Results Loaded", str(len(result_rows))),
        ("Winners", str(winners_count) if result_rows else "—"),
        ("Generated", generated),
    ]
    summary_html = "".join(
        f"""
        <div class="summary-card">
          <span class="label">{html.escape(label)}</span>
          <span class="value">{html.escape(value)}</span>
        </div>
        """
        for label, value in summary_cards
    )

    sections: list[str] = []
    for race_number, rows in meeting_scores.items():
        if not rows:
            continue
        sections.append(_render_race_section(race_number, rows, result_rows.get(race_number, {})))

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(meeting_title)}</title>
  <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@700;800;900&family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;600&display=swap" rel="stylesheet" />
  <style>
    :root {{
      /* MuzzyBet Stealth Theme */
      --bg:            #0a0a0a;
      --card-bg:       #101010;
      --surface:       #131313;
      --primary-text:  #f0f0f0;
      --secondary:     #808080;
      --accent:        #00d4ff;
      --accent-dark:   #00aacc;
      --accent-soft:   rgba(0,212,255,0.08);
      --winner:        #22d3a0;
      --winner-soft:   rgba(34,211,160,0.08);
      --pick-bg:       rgba(192,132,252,0.08);
      --pick-text:     #c084fc;
      --border:        #1f1f1f;
      --border-strong: #2e2e2e;
      --highlight:     #1a1a1a;
      --winner-bg:     rgba(34,211,160,0.06);
    }}

    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}

    body {{
      margin: 0;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      color: var(--primary-text);
      background-color: var(--bg);
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }}

    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 28px 20px;
    }}

    /* Hero Section */
    .hero {{
      background: #060606;
      color: var(--primary-text);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 28px 32px;
      margin-bottom: 16px;
      position: relative;
      overflow: hidden;
    }}

    .hero::after {{
      content: "";
      position: absolute;
      top: -60px; right: -60px;
      width: 280px; height: 280px;
      background: radial-gradient(circle, rgba(0,212,255,0.06) 0%, transparent 70%);
      pointer-events: none;
    }}

    .eyebrow {{
      display: inline-block;
      padding: 3px 10px;
      border-radius: 3px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      margin-bottom: 12px;
      font-family: 'Roboto Mono', monospace;
    }}

    h1 {{
      margin: 0;
      font-size: 32px;
      font-weight: 900;
      letter-spacing: 0.03em;
      font-family: 'Barlow Condensed', 'Inter', sans-serif;
      text-transform: uppercase;
    }}

    .sub {{ color: var(--secondary); font-size: 14px; margin-top: 6px; }}

    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 10px;
      margin-top: 20px;
    }}

    .summary-card {{
      background: rgba(255,255,255,0.03);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 12px 14px;
    }}

    .summary-card .label {{
      display: block;
      font-size: 10px;
      color: var(--secondary);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 3px;
    }}

    .summary-card .value {{
      font-size: 16px;
      font-weight: 700;
      color: var(--primary-text);
      font-family: 'Roboto Mono', monospace;
    }}

    /* Navigation */
    .race-nav {{
      position: sticky;
      top: 0;
      z-index: 50;
      margin-bottom: 16px;
      padding: 6px;
      display: flex;
      gap: 4px;
      overflow-x: auto;
      background: rgba(10,10,10,0.95);
      backdrop-filter: blur(12px);
      border-bottom: 1px solid var(--border);
      scrollbar-width: none;
    }}

    .race-nav::-webkit-scrollbar {{ display: none; }}

    .race-nav a {{
      text-decoration: none;
      white-space: nowrap;
      color: var(--secondary);
      background: transparent;
      border-radius: 5px;
      padding: 6px 14px;
      font-size: 13px;
      font-weight: 600;
      transition: all 0.15s;
      letter-spacing: 0.02em;
    }}

    .race-nav a:hover {{
      background: var(--highlight);
      color: var(--primary-text);
    }}

    /* Race Cards */
    .race-card {{
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0;
      margin-bottom: 10px;
      overflow: hidden;
    }}

    .race-head {{
      padding: 12px 20px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      background: var(--surface);
    }}

    .race-head h2 {{
      margin: 0;
      font-size: 20px;
      font-weight: 900;
      font-family: 'Barlow Condensed', 'Inter', sans-serif;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--primary-text);
    }}

    .race-head .top-pick {{
      font-size: 12px;
      background: var(--pick-bg);
      color: var(--pick-text);
      padding: 4px 12px;
      border-radius: 4px;
      font-weight: 700;
      letter-spacing: 0.03em;
      border: 1px solid rgba(192,132,252,0.2);
    }}

    /* Tables */
    table {{
      width: 100%;
      border-collapse: collapse;
    }}

    th, td {{
      padding: 9px 12px;
      text-align: left;
      font-size: 13px;
      border-bottom: 1px solid var(--border);
    }}

    th {{
      background: var(--surface);
      color: var(--secondary);
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    tr:last-child td {{ border-bottom: none; }}
    tr:hover {{ background-color: var(--highlight); }}

    /* Special Rows */
    tr.top-pick-row {{ background: var(--pick-bg); }}
    tr.top-pick-row:hover {{ background: rgba(192,132,252,0.12); }}
    tr.result-winner {{ background: var(--winner-bg); }}

    .horse-cell {{
      display: flex;
      flex-direction: column;
      gap: 2px;
    }}

    .horse-cell strong {{
      font-size: 14px;
      color: var(--primary-text);
      font-family: 'Barlow Condensed', 'Inter', sans-serif;
      font-weight: 700;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }}
    .horse-cell .meta {{ color: var(--secondary); font-size: 12px; }}

    .pill {{
      display: inline-block;
      margin-top: 4px;
      padding: 2px 7px;
      border-radius: 3px;
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .pill.pick {{ background: var(--pick-bg); color: var(--pick-text); border: 1px solid rgba(192,132,252,0.25); }}
    .pill.win {{ background: var(--winner-soft); color: var(--winner); border: 1px solid rgba(34,211,160,0.25); }}
    .pill.fs {{ background: rgba(124,58,237,0.15); color: #a78bfa; border: 1px solid rgba(124,58,237,0.25); }}
    .pill.ns {{ background: rgba(234,88,12,0.15); color: #fb923c; border: 1px solid rgba(234,88,12,0.25); }}

    .driver-change {{ color: #f59e0b; font-weight: 600; }}

    .muted {{ color: var(--secondary); opacity: 0.5; }}

    /* ── Portrait mobile ── */
    @media (max-width: 820px) and (orientation: portrait) {{
      .wrap {{ padding: 12px 8px; }}
      h1 {{ font-size: 26px; }}
      .hero {{ padding: 20px 16px; border-radius: 16px; margin-bottom: 16px; }}
      .summary-grid {{ display: none; }}
      .race-nav {{ margin-bottom: 16px; }}
      .race-card {{ border-radius: 12px; margin-bottom: 16px; overflow-x: auto; }}
      .race-head {{ padding: 12px 16px; flex-direction: column; align-items: flex-start; gap: 6px; }}
      .race-head h2 {{ font-size: 18px; }}
      .race-head .top-pick {{ font-size: 12px; }}
      th, td {{ padding: 8px 10px; font-size: 12px; white-space: nowrap; }}
      .horse-cell .meta {{ display: none; }}
      .horse-cell strong {{ font-size: 13px; }}
      th:nth-child(4), td:nth-child(4),
      th:nth-child(6), td:nth-child(6),
      th:nth-child(7), td:nth-child(7),
      th:nth-child(8), td:nth-child(8),
      th:nth-child(9), td:nth-child(9),
      th:nth-child(12), td:nth-child(12) {{ display: none; }}
    }}

    /* ── Landscape mobile / small tablet ── */
    @media (max-width: 1024px) and (orientation: landscape) {{
      .wrap {{ padding: 12px 12px; }}
      .hero {{ padding: 20px 24px; border-radius: 16px; margin-bottom: 16px; }}
      h1 {{ font-size: 28px; }}
      .race-card {{ margin-bottom: 16px; overflow-x: auto; }}
      .race-head {{ padding: 12px 20px; }}
      .race-head h2 {{ font-size: 20px; }}
      th, td {{ padding: 8px 10px; font-size: 12px; white-space: nowrap; }}
      .horse-cell .meta {{ display: none; }}
      table {{ min-width: 700px; }}
    }}

    /* ── Diagnostics panel ── */
    .diag-row td {{ padding: 0; background: #0d0d0d; border-bottom: 2px solid var(--border); }}
    .diag-panel {{ padding: 14px 18px 6px; display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; }}
    .diag-group {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 10px 14px; }}
    .diag-group h4 {{ margin: 0 0 7px; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--secondary); }}
    .diag-kv {{ display: flex; justify-content: space-between; gap: 8px; padding: 2px 0; border-bottom: 1px solid var(--border); font-size: 11.5px; }}
    .diag-kv:last-child {{ border-bottom: none; }}
    .diag-kv .k {{ color: var(--secondary); white-space: nowrap; }}
    .diag-kv .v {{ font-weight: 600; font-variant-numeric: tabular-nums; text-align: right; }}
    .diag-kv .v.warn {{ color: #dc2626; }}
    .diag-kv .v.ok   {{ color: #059669; }}
    .diag-kv .v.muted {{ color: var(--secondary); font-weight: 400; }}
    .diag-comps {{ display: flex; gap: 20px; flex-wrap: wrap; padding: 10px 18px 14px; }}
    .diag-comp-table {{ flex: 1; min-width: 190px; border-collapse: collapse; }}
    .diag-comp-table caption {{ font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--secondary); text-align: left; padding-bottom: 4px; caption-side: top; }}
    .diag-comp-table td {{ padding: 1px 5px; font-size: 11.5px; border: none; }}
    .comp-name {{ color: var(--secondary); }}
    .comp-pos  {{ color: #16a34a; font-weight: 600; font-variant-numeric: tabular-nums; text-align: right; }}
    .comp-neg  {{ color: #dc2626; font-weight: 600; font-variant-numeric: tabular-nums; text-align: right; }}
    .comp-zero {{ color: var(--secondary); opacity: 0.45; font-variant-numeric: tabular-nums; text-align: right; }}
    .diag-toggle {{ background: none; border: 1px solid var(--border-strong); border-radius: 3px; cursor: pointer; font-size: 10px; padding: 1px 5px; margin-left: 5px; color: var(--secondary); transition: transform 0.15s; line-height: 1.4; vertical-align: middle; }}
    .diag-toggle.open {{ transform: rotate(90deg); color: var(--accent); border-color: var(--accent); }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <span class="eyebrow">{html.escape(meeting_code)}</span>
      <h1>{html.escape(meeting_title)}</h1>
      <p class="sub">Harness Racing Model — Fair Odds &amp; Probabilities</p>
      <div class="summary-grid">{summary_html}</div>
    </section>
    <nav class="race-nav">
      <a href="index.html">Meetings</a>
      <a href="stats.html">Stats</a>
      <a href="betting.html">Betting</a>
      <a href="betting-nsw.html" style="font-size:11px;padding:3px 8px">NSW</a>
      <a href="betting-qld.html" style="font-size:11px;padding:3px 8px">QLD</a>
      <a href="diagnose.html">Diagnose</a>
      <a href="diagnose-nsw.html" style="font-size:11px;padding:3px 8px">NSW</a>
      <a href="diagnose-qld.html" style="font-size:11px;padding:3px 8px">QLD</a>
      <a href="live.html">Live</a>
      {race_nav}
    </nav>
    {''.join(sections) if sections else '<div class="race-card"><p>No races found for this meeting.</p></div>'}
  </div>
  <script>
    function toggleDiag(id) {{
      var row = document.getElementById(id);
      var btn = document.querySelector('[onclick="toggleDiag(\\'' + id + '\\')"]');
      if (!row) return;
      var hidden = row.style.display === 'none' || row.style.display === '';
      row.style.display = hidden ? 'table-row' : 'none';
      if (btn) btn.classList.toggle('open', hidden);
    }}
  </script>
</body>
</html>
"""


def _render_diag_row(row_id: str, row: dict[str, object]) -> str:
    """Return a hidden <tr> with the full diagnostics panel for one horse."""
    components = row.get("components") or {}

    def _fv(key: str) -> float | None:
        v = row.get(key)
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    def _iv(key: str) -> int | None:
        v = _fv(key)
        return int(v) if v is not None else None

    def kv(label: str, display: str, css: str = "") -> str:
        css_attr = f' class="{css}"' if css else ""
        return f'<div class="diag-kv"><span class="k">{label}</span><span class="v"{css_attr}>{display}</span></div>'

    def _fmt_m(v: float | None) -> tuple[str, str]:
        if v is None:
            return "—", "muted"
        return f"{v:+.1f}m", ("ok" if v < 0 else ("warn" if v > 10 else ""))

    def _fmt_pct(v: float | None, warn_above: float | None = None, ok_below: float | None = None) -> tuple[str, str]:
        if v is None:
            return "—", "muted"
        s = f"{v:.1%}"
        css = ""
        if warn_above is not None and v > warn_above:
            css = "warn"
        elif ok_below is not None and v <= ok_below:
            css = "ok"
        return s, css

    def _fmt_f(v: float | None, fmt: str = ".2f") -> tuple[str, str]:
        if v is None:
            return "—", "muted"
        return format(v, fmt), ""

    def _fmt_days(v: float | None) -> tuple[str, str]:
        if v is None:
            return "—", "muted"
        d = int(v)
        css = "warn" if d >= 43 else ("ok" if d <= 14 else "")
        return f"{d}d", css

    # --- Group A: Form Quality ---
    avg_ca, avg_ca_css = _fmt_m(_fv("recent_line_avg_class_adj_margin"))
    best_ca, best_ca_css = _fmt_m(_fv("recent_line_best_class_adj_margin"))
    wr5, wr5_css = _fmt_pct(_fv("last_5_win_rate"), warn_above=0.50)
    t3, t3_css = _fmt_pct(_fv("last_5_top3_rate"))
    comp, comp_css = _fmt_pct(_fv("last_5_competitive_rate"))
    sec_delta_v = _fv("last_3_avg_sectional_delta")
    sec_delta_s = (f"{sec_delta_v:+.2f}s" if sec_delta_v is not None else "—")
    sec_delta_css = ("ok" if sec_delta_v is not None and sec_delta_v < -0.5 else
                     ("warn" if sec_delta_v is not None and sec_delta_v > 0.3 else
                      ("muted" if sec_delta_v is None else "")))
    sec_count = _iv("recent_sectional_count")
    career_s = _iv("career_starts") or 0
    sec_count_css = "warn" if (sec_count == 0 and career_s > 0) else ("muted" if sec_count is None else "")
    null_flags = _iv("recent_line_null_flags") or 0
    null_css = "warn" if null_flags > 1 else ("muted" if null_flags == 0 else "")

    grp_a = (
        kv("Avg Class Adj", avg_ca, avg_ca_css)
        + kv("Best Class Adj", best_ca, best_ca_css)
        + kv("Last 5 Win Rate", wr5, wr5_css)
        + kv("Last 5 Top3 Rate", t3, t3_css)
        + kv("Last 5 Competitive", comp, comp_css)
        + kv("Sectional Δ (L3)", sec_delta_s, sec_delta_css)
        + kv("Sectional Count", str(sec_count) if sec_count is not None else "—", sec_count_css)
        + kv("Null Runs Excl.", str(null_flags), null_css)
    )

    # --- Group B: Class & NR ---
    nr_v = _fv("nr_rating")
    ceil_v = _fv("race_nr_ceiling")
    delta_v = _fv("nr_grade_delta")
    avg_ceil_v = _fv("avg_recent_nr_ceiling")
    headroom_v = _fv("nr_headroom")
    purse_v = _fv("avg_recent_run_purse")
    class_delta_v = _fv("class_delta")

    delta_s = (f"{delta_v:+.1f}" if delta_v is not None else "—")
    delta_css = ("ok" if delta_v is not None and delta_v < -5 else
                 ("warn" if delta_v is not None and delta_v > 5 else
                  ("muted" if delta_v is None else "")))
    purse_s = (f"${purse_v:,.0f}" if purse_v is not None else "—")
    cd_s = (f"${class_delta_v:+,.0f}" if class_delta_v is not None else "—")
    cd_css = "warn" if (class_delta_v is not None and class_delta_v > 5000) else ("muted" if class_delta_v is None else "")

    grp_b = (
        kv("NR Rating", str(int(nr_v)) if nr_v is not None else "—", "muted" if nr_v is None else "")
        + kv("Race NR Ceiling", str(int(ceil_v)) if ceil_v is not None else "—", "muted" if ceil_v is None else "")
        + kv("NR Grade Delta", delta_s, delta_css)
        + kv("Avg Recent NR Ceil", f"{avg_ceil_v:.1f}" if avg_ceil_v is not None else "—", "muted" if avg_ceil_v is None else "")
        + kv("NR Headroom", f"{headroom_v:+.1f}" if headroom_v is not None else "—", "muted" if headroom_v is None else "")
        + kv("Avg Recent Purse", purse_s, "muted" if purse_v is None else "")
        + kv("Class Delta", cd_s, cd_css)
    )

    # --- Group C: Race-day Factors ---
    sp_v = _fv("last_5_avg_sp")
    days_v = _fv("days_since_last_run")
    days_s, days_css = _fmt_days(days_v)
    lead_v = _fv("style_lead_rate")
    relief_v = _fv("barrier_relief_score")
    soft_v = _fv("map_soft_trip_score")
    wide_v = _fv("map_wide_risk_score")
    dsr_v = _fv("dist_strike_rate_ratio")
    dsr_starts = _iv("dist_rge_starts") or 0

    grp_c = (
        kv("Last 5 Avg SP", f"${sp_v:.2f}" if sp_v is not None else "—", "muted" if sp_v is None else "")
        + kv("Days Since Run", days_s, days_css)
        + kv("Lead Rate", f"{lead_v:.1%}" if lead_v is not None else "—", "muted" if lead_v is None else "")
        + kv("Barrier Relief", f"{relief_v:+.3f}" if relief_v is not None else "—", "muted" if relief_v is None else "")
        + kv("Map Soft Score", f"{soft_v:.3f}" if soft_v is not None else "—", "muted" if soft_v is None else "")
        + kv("Map Wide Risk", f"{wide_v:.3f}" if wide_v is not None else "—", "muted" if wide_v is None else "")
        + kv("Dist Strike Rate", (f"{dsr_v:.2f} ({dsr_starts}st)" if dsr_v is not None else f"— ({dsr_starts}st)"), "muted" if dsr_v is None else "")
    )

    # --- Group D: Career / Season ---
    c_starts = _iv("career_starts")
    c_wins = _iv("career_wins")
    c_wr = _fv("career_win_rate")
    s_starts = _iv("season_starts")
    s_wins = _iv("season_wins")
    l10_wr = _fv("last_10_win_rate")
    csr_v = _fv("ceiling_support_rate")
    cbr_v = _iv("ceiling_best_run_index")

    grp_d = (
        kv("Career Starts", str(c_starts) if c_starts is not None else "—", "muted" if c_starts is None else "")
        + kv("Career Wins", str(c_wins) if c_wins is not None else "—", "muted" if c_wins is None else "")
        + kv("Career Win Rate", f"{c_wr:.1%}" if c_wr is not None else "—", "muted" if c_wr is None else "")
        + kv("Season Starts", str(s_starts) if s_starts is not None else "—", "muted" if s_starts is None else "")
        + kv("Season Wins", str(s_wins) if s_wins is not None else "—", "muted" if s_wins is None else "")
        + kv("Last 10 Win Rate", f"{l10_wr:.1%}" if l10_wr is not None else "—", "muted" if l10_wr is None else "")
        + kv("Ceiling Support", f"{csr_v:.1%}" if csr_v is not None else "—", "muted" if csr_v is None else "")
        + kv("Best Run Index", str(cbr_v) if cbr_v is not None else "—", "muted" if cbr_v is None else "")
    )

    # --- Component tables ---
    s1_keys = [
        "consistency", "ceiling", "late_speed", "tempo_adj", "tempo_flags", "null_flags",
        "market", "win_rate", "career_win_rate", "top3_rate", "competitive_rate",
        "nr", "class_pos", "stake_class", "class_delta",
        "sp_class", "sp_trend", "sp_market_ceiling", "sp_class_ceiling", "sp_avg_market",
        "sp_reliability", "debut_adj",
    ]
    s2_keys = [
        "barrier", "barrier_relief", "map_lead", "map_soft", "map_soft_context",
        "pace_backmarker", "map_wide", "map_death", "dist_strike_rate",
        "nr_grade_delta", "fitness", "second_up", "driver_form", "trainer_form",
        "hot_driver", "hot_trainer", "cold_driver", "cold_trainer",
    ]

    def comp_rows(keys: list[str]) -> str:
        out = []
        for k in keys:
            v = components.get(k, 0.0) or 0.0
            if abs(v) < 0.0005:
                css = "comp-zero"
            elif v > 0:
                css = "comp-pos"
            else:
                css = "comp-neg"
            out.append(f'<tr><td class="comp-name">{html.escape(k)}</td><td class="{css}">{v:+.3f}</td></tr>')
        return "".join(out)

    return f"""<tr id="{row_id}" class="diag-row" style="display:none">
      <td colspan="12">
        <div class="diag-panel">
          <div class="diag-group"><h4>Form Quality</h4>{grp_a}</div>
          <div class="diag-group"><h4>Class &amp; NR</h4>{grp_b}</div>
          <div class="diag-group"><h4>Race-day</h4>{grp_c}</div>
          <div class="diag-group"><h4>Career / Season</h4>{grp_d}</div>
        </div>
        <div class="diag-comps">
          <table class="diag-comp-table"><caption>S1 Components</caption><tbody>{comp_rows(s1_keys)}</tbody></table>
          <table class="diag-comp-table"><caption>S2 Components</caption><tbody>{comp_rows(s2_keys)}</tbody></table>
        </div>
      </td>
    </tr>"""


def _render_race_section(
    race_number: int,
    rows: list[dict[str, object]],
    results_for_race: dict[str, dict[str, Any]],
) -> str:
    ordered = sorted(
        rows,
        key=lambda row: (
            row.get("runner_number") is None,
            row.get("runner_number") if row.get("runner_number") is not None else 999,
        ),
    )
    top_pick = max(ordered, key=lambda row: float(row.get("win_probability") or 0.0))
    body_rows: list[str] = []
    for i, row in enumerate(ordered):
        horse_name = str(row.get("horse_name") or "")
        result = results_for_race.get(_normalise_name(horse_name))
        classes = []
        if row is top_pick:
            classes.append("top-pick-row")
        if result and result.get("finish_position") == 1:
            classes.append("result-winner")
        class_attr = f' class="{" ".join(classes)}"' if classes else ""
        finish = result.get("finish_position") if result else None
        sp = result.get("starting_price") if result else None
        margin = result.get("margin") if result else None
        badges = []
        if row is top_pick:
            badges.append('<span class="pill pick">Top Pick</span>')
        if finish == 1:
            badges.append('<span class="pill win">Winner</span>')
        if row.get("career_starts") == 0:
            badges.append('<span class="pill fs">FS</span>')
        try:
            if int(row.get("recent_sectional_count") or 0) == 0 and int(row.get("career_starts") or 0) > 0:
                badges.append('<span class="pill ns">NS</span>')
        except (TypeError, ValueError):
            pass
        row_id = f"diag_{race_number}_{i}"
        body_rows.append(
            f"""
            <tr{class_attr}>
              <td data-label="No">{_fmt_runner_number(row.get('runner_number'))}</td>
              <td data-label="Horse">
                <div class="horse-cell">
                  <strong>{html.escape(horse_name)}</strong>
                  <span class="meta">{html.escape(str(row.get('nominated_trainer') or ''))} / {'<span class="driver-change">' + html.escape(str(row.get('nominated_driver') or '')) + '</span>' if str(row.get('driver_change_flag')) == '1' else html.escape(str(row.get('nominated_driver') or ''))}</span>
                  <span>{''.join(badges)}<button class="diag-toggle" onclick="toggleDiag('{row_id}')" title="Show diagnostics">&#9658;</button></span>
                </div>
              </td>
              <td data-label="Barrier">{html.escape(str(row.get('barrier') or ''))}</td>
              <td data-label="Prob">{_fmt_prob(row.get('win_probability'))}</td>
              <td data-label="Fair Odds">{_fmt_decimal(row.get('fair_odds'))}</td>
              <td data-label="S1">{_fmt_signed(row.get('stage1_score'))}</td>
              <td data-label="S2">{_fmt_signed(row.get('stage2_score'))}</td>
              <td data-label="Score">{_fmt_decimal(row.get('score'), places=4)}</td>
              <td data-label="Rel">{_fmt_signed(row.get('relative_score'))}</td>
              <td data-label="Result">{finish if finish is not None else '<span class="muted">-</span>'}</td>
              <td data-label="SP">{_fmt_decimal(sp) if sp is not None else '<span class="muted">-</span>'}</td>
              <td data-label="Margin">{_fmt_decimal(margin) if margin is not None else '<span class="muted">-</span>'}</td>
            </tr>
            {_render_diag_row(row_id, row)}"""
        )

    top_pick_name = str(top_pick.get("horse_name") or "")
    top_pick_prob = _fmt_prob(top_pick.get("win_probability"))
    return f"""
    <section id="race-{race_number}" class="race-card">
      <div class="race-head">
        <h2>Race {race_number}</h2>
        <div class="top-pick">Top pick: <strong>{html.escape(top_pick_name)}</strong> at {top_pick_prob}</div>
      </div>
      <table>
        <thead>
          <tr>
            <th>No.</th>
            <th>Horse</th>
            <th>Barrier</th>
            <th>Prob</th>
            <th>Fair Odds</th>
            <th>S1</th>
            <th>S2</th>
            <th>Score</th>
            <th>Rel</th>
            <th>Result</th>
            <th>SP</th>
            <th>Margin</th>
          </tr>
        </thead>
        <tbody>
          {''.join(body_rows)}
        </tbody>
      </table>
    </section>
    """


def _fmt_prob(value: object) -> str:
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return "-"


def _fmt_decimal(value: object, places: int = 2) -> str:
    try:
        return f"{float(value):.{places}f}"
    except (TypeError, ValueError):
        return "-"


def _fmt_signed(value: object) -> str:
    try:
        return f"{float(value):+0.3f}"
    except (TypeError, ValueError):
        return "-"


def _fmt_runner_number(value: object) -> str:
    if value is None:
        return "-"
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def _normalise_name(value: str) -> str:
    return " ".join(value.upper().split())


# ---------------------------------------------------------------------------
# Stats page — driver / trainer performance tables
# ---------------------------------------------------------------------------

def _compute_person_stats(conn: sqlite3.Connection, field_name: str) -> list[dict]:
    """Aggregate driver or trainer stats from race_results joined to meetings.

    field_name: 'driver_name' or 'trainer_name'
    Returns list of dicts sorted by L30 win% descending. Requires >= 3 overall starts.
    """
    rows = conn.execute(
        f"""
        SELECT
            ru.{field_name}    AS name,
            rr.starting_price  AS sp,
            rr.finish_position AS pos,
            m.meeting_date     AS meeting_date
        FROM race_results rr
        JOIN meetings m ON rr.meeting_code = m.meeting_code
        JOIN race_runners ru
          ON  ru.meeting_code = rr.meeting_code
          AND ru.race_number  = rr.race_number
          AND ru.horse_id     = rr.horse_id
        WHERE ru.{field_name} IS NOT NULL
          AND rr.starting_price IS NOT NULL
        """,
    ).fetchall()

    today = datetime.today()
    cutoff_30 = today - timedelta(days=30)
    cutoff_14 = today - timedelta(days=14)

    def _parse_date(s: str | None) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.strptime(s.strip(), "%d %b %Y")
        except ValueError:
            return None

    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        buckets[str(row["name"])].append({
            "sp": float(row["sp"]),
            "pos": row["pos"],
            "date": _parse_date(row["meeting_date"]),
        })

    result: list[dict] = []
    for name, runs in buckets.items():
        overall_starts = len(runs)
        if overall_starts < 3:
            continue
        overall_wins = sum(1 for r in runs if r["pos"] == 1)

        l30_runs = [r for r in runs if r["date"] and r["date"] >= cutoff_30]
        l30_starts = len(l30_runs)
        l30_wins = sum(1 for r in l30_runs if r["pos"] == 1)
        l30_places = sum(1 for r in l30_runs if r["pos"] is not None and int(r["pos"]) <= 3)

        if l30_starts >= 3:
            l30_win_pct: float | None = l30_wins / l30_starts
            l30_place_pct: float | None = l30_places / l30_starts
            l30_roi: float | None = sum(r["sp"] - 1 if r["pos"] == 1 else -1 for r in l30_runs) / l30_starts
            l30_win_sps = [r["sp"] for r in l30_runs if r["pos"] == 1]
            l30_avg_win_sp: float | None = sum(l30_win_sps) / len(l30_win_sps) if l30_win_sps else None
        else:
            l30_win_pct = None
            l30_place_pct = None
            l30_roi = None
            l30_avg_win_sp = None

        l14_runs = [r for r in runs if r["date"] and r["date"] >= cutoff_14]
        l14_starts = len(l14_runs)
        l14_wins = sum(1 for r in l14_runs if r["pos"] == 1)

        result.append({
            "name": name,
            "overall_wins": overall_wins,
            "overall_starts": overall_starts,
            "l30_starts": l30_starts,
            "l30_wins": l30_wins,
            "l30_places": l30_places,
            "l30_win_pct": l30_win_pct,
            "l30_place_pct": l30_place_pct,
            "l30_roi": l30_roi,
            "l30_avg_win_sp": l30_avg_win_sp,
            "l14_starts": l14_starts,
            "l14_wins": l14_wins,
        })

    result.sort(key=lambda r: (r["l30_win_pct"] is None, -(r["l30_win_pct"] or 0.0)))
    return result


def _render_stats_table(rows: list[dict]) -> str:
    """Render one driver or trainer stats table body."""
    tr_parts: list[str] = []
    for r in rows:
        name = html.escape(r["name"])
        highlight = (
            r["l30_win_pct"] is not None
            and r["l30_starts"] >= 3
            and r["l30_win_pct"] >= 0.25
        )
        row_class = ' class="hot-row"' if highlight else ""

        def _dash(v: object, fmt: str = "") -> str:
            if v is None:
                return '<span class="muted">–</span>'
            try:
                return format(float(v), fmt)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return str(v)

        l30_win_pct_str = f"{r['l30_win_pct'] * 100:.1f}%" if r["l30_win_pct"] is not None else '<span class="muted">–</span>'
        l30_place_pct_str = f"{r['l30_place_pct'] * 100:.1f}%" if r["l30_place_pct"] is not None else '<span class="muted">–</span>'
        l30_roi_str = f"{r['l30_roi']:+.2f}" if r["l30_roi"] is not None else '<span class="muted">–</span>'
        l30_avg_sp_str = f"{r['l30_avg_win_sp']:.2f}" if r["l30_avg_win_sp"] is not None else '<span class="muted">–</span>'
        l14_str = f"{r['l14_wins']}-{r['l14_starts']}" if r["l14_starts"] > 0 else '<span class="muted">–</span>'
        overall_str = f"{r['overall_wins']}-{r['overall_starts']}"

        l30_starts_str = str(r["l30_starts"]) if r["l30_starts"] > 0 else '<span class="muted">–</span>'
        l30_wins_str = str(r["l30_wins"]) if r["l30_starts"] >= 3 else '<span class="muted">–</span>'
        l30_places_str = str(r["l30_places"]) if r["l30_starts"] >= 3 else '<span class="muted">–</span>'

        tr_parts.append(
            f"""<tr{row_class}>
              <td>{name}</td>
              <td class="num">{l30_starts_str}</td>
              <td class="num">{l30_wins_str}</td>
              <td class="num sort-col">{l30_win_pct_str}</td>
              <td class="num">{l30_places_str}</td>
              <td class="num">{l30_place_pct_str}</td>
              <td class="num">{l30_roi_str}</td>
              <td class="num">{l30_avg_sp_str}</td>
              <td class="num">{l14_str}</td>
              <td class="num">{overall_str}</td>
            </tr>"""
        )
    return "\n".join(tr_parts)


def _render_stats_html(driver_rows: list[dict], trainer_rows: list[dict], meta: dict) -> str:
    """Render the standalone stats page HTML."""
    generated = html.escape(meta.get("generated_at", ""))
    total = meta.get("total_results", 0)
    driver_tbody = _render_stats_table(driver_rows)
    trainer_tbody = _render_stats_table(trainer_rows)

    table_html = """<table class="stats-table" id="{tid}">
        <thead>
          <tr>
            <th onclick="sortTable('{tid}',0)">Name</th>
            <th class="num" onclick="sortTable('{tid}',1)">Starts (L30)</th>
            <th class="num" onclick="sortTable('{tid}',2)">Wins (L30)</th>
            <th class="num sorted-asc" onclick="sortTable('{tid}',3)">Win % (L30)</th>
            <th class="num" onclick="sortTable('{tid}',4)">Places (L30)</th>
            <th class="num" onclick="sortTable('{tid}',5)">Place % (L30)</th>
            <th class="num" onclick="sortTable('{tid}',6)">ROI (L30)</th>
            <th class="num" onclick="sortTable('{tid}',7)">Ave Win SP (L30)</th>
            <th class="num" onclick="sortTable('{tid}',8)">L14 W-S</th>
            <th class="num" onclick="sortTable('{tid}',9)">Overall W-S</th>
          </tr>
        </thead>
        <tbody>
          {tbody}
        </tbody>
      </table>"""

    driver_table = table_html.format(tid="drivers-table", tbody=driver_tbody)
    trainer_table = table_html.format(tid="trainers-table", tbody=trainer_tbody)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Driver &amp; Trainer Stats — Harness Racing</title>
  <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@700;800;900&family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;600&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg: #0a0a0a;
      --card-bg: #101010;
      --surface: #131313;
      --primary: #f0f0f0;
      --secondary: #808080;
      --accent: #00d4ff;
      --accent-dark: #00aacc;
      --accent-soft: rgba(0,212,255,0.08);
      --border: #1f1f1f;
      --highlight: #1a1a1a;
      --hot-bg: rgba(240,180,41,0.08);
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      color: var(--primary);
      background-color: var(--bg);
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }}
    .top-nav {{
      background: #060606;
      padding: 10px 20px;
      display: flex;
      gap: 4px;
      align-items: center;
      border-bottom: 1px solid var(--border);
    }}
    .top-nav a {{
      color: var(--secondary);
      text-decoration: none;
      font-size: 13px;
      font-weight: 600;
      padding: 6px 14px;
      border-radius: 5px;
      transition: all 0.15s;
    }}
    .top-nav a:hover {{ background: var(--highlight); color: var(--primary); }}
    .top-nav a.active {{ background: rgba(0,212,255,0.08); color: var(--accent); }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 36px 20px 60px; }}
    .hero {{
      background: #060606;
      color: var(--primary);
      padding: 36px 20px 28px;
      text-align: center;
      border-bottom: 1px solid var(--border);
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: 32px; font-weight: 900; letter-spacing: 0.04em; text-transform: uppercase; font-family: 'Barlow Condensed', 'Inter', sans-serif; }}
    .hero p {{ margin: 0; color: var(--secondary); font-size: 14px; }}
    .section-title {{
      font-size: 20px;
      font-weight: 900;
      color: var(--primary);
      margin: 32px 0 12px;
      padding-bottom: 8px;
      border-bottom: 1px solid var(--border);
      display: inline-block;
      font-family: 'Barlow Condensed', 'Inter', sans-serif;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}
    .table-wrap {{
      overflow-x: auto;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--card-bg);
    }}
    table.stats-table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 700px;
    }}
    th, td {{
      padding: 9px 12px;
      text-align: left;
      font-size: 13px;
      border-bottom: 1px solid var(--border);
    }}
    th {{
      background: var(--surface);
      color: var(--secondary);
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    th:hover {{ color: var(--primary); background: var(--highlight); }}
    th.sorted-asc::after {{ content: " ▼"; color: var(--accent); }}
    th.sorted-desc::after {{ content: " ▲"; color: var(--accent); }}
    td.num {{ text-align: right; font-family: 'Roboto Mono', monospace; font-size: 12px; }}
    th.num {{ text-align: right; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover {{ background: var(--highlight); }}
    tr.hot-row {{ background: var(--hot-bg); }}
    tr.hot-row:hover {{ background: rgba(240,180,41,0.12); }}
    .muted {{ color: var(--secondary); opacity: 0.5; }}
    .footer {{
      margin-top: 48px;
      text-align: center;
      color: var(--secondary);
      font-size: 13px;
    }}
  </style>
</head>
<body>
  <nav class="top-nav">
    <a href="index.html">Meetings</a>
    <a href="stats.html" class="active">Stats</a>
    <a href="betting.html">Betting</a>
    <a href="betting-nsw.html" style="font-size:11px;padding:3px 8px">NSW</a>
    <a href="betting-qld.html" style="font-size:11px;padding:3px 8px">QLD</a>
    <a href="diagnose.html">Diagnose</a>
    <a href="diagnose-nsw.html" style="font-size:11px;padding:3px 8px">NSW</a>
    <a href="diagnose-qld.html" style="font-size:11px;padding:3px 8px">QLD</a>
    <a href="live.html">Live</a>
  </nav>
  <div class="hero">
    <h1>Driver &amp; Trainer Stats</h1>
    <p>Based on ingested race results — primary metrics are last 30 days</p>
  </div>
  <div class="wrap">
    <div class="section-title">Drivers</div>
    <div class="table-wrap">{driver_table}</div>

    <div class="section-title">Trainers</div>
    <div class="table-wrap">{trainer_table}</div>

    <div class="footer">Generated {generated} &nbsp;·&nbsp; {total:,} results ingested</div>
  </div>
  <script>
    function parseSortVal(text) {{
      var t = text.replace(/[%+]/g, '').trim();
      if (t === '' || t === '–' || t === '-') return null;
      // W-S format like "3-12" → sort by win count descending
      var ws = t.match(/^(\\d+)-(\\d+)$/);
      if (ws) return parseInt(ws[1], 10);
      var n = parseFloat(t);
      return isNaN(n) ? t.toLowerCase() : n;
    }}

    var _sortState = {{}};

    function sortTable(tableId, colIdx) {{
      var table = document.getElementById(tableId);
      var tbody = table.querySelector('tbody');
      var rows = Array.from(tbody.querySelectorAll('tr'));
      var ths = table.querySelectorAll('thead th');

      var prev = _sortState[tableId] || {{}};
      var asc = !(prev.col === colIdx && prev.asc);
      _sortState[tableId] = {{ col: colIdx, asc: asc }};

      ths.forEach(function(th) {{
        th.classList.remove('sorted-asc', 'sorted-desc');
      }});
      ths[colIdx].classList.add(asc ? 'sorted-asc' : 'sorted-desc');

      rows.sort(function(a, b) {{
        var av = parseSortVal(a.cells[colIdx].textContent);
        var bv = parseSortVal(b.cells[colIdx].textContent);
        if (av === null && bv === null) return 0;
        if (av === null) return 1;
        if (bv === null) return -1;
        if (av < bv) return asc ? 1 : -1;
        if (av > bv) return asc ? -1 : 1;
        return 0;
      }});
      rows.forEach(function(r) {{ tbody.appendChild(r); }});
    }}

    // Initial sort: L30 Win% (col 3) descending on page load
    window.addEventListener('DOMContentLoaded', function() {{
      sortTable('drivers-table', 3);
      sortTable('trainers-table', 3);
    }});
  </script>
</body>
</html>
"""


def build_stats_site(
    db_path: str | Path,
    out_dir: str | Path,
) -> Path:
    """Build driver and trainer stats page and write to out_dir/stats.html."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = connect(db_path)
    init_db(conn)
    driver_rows = _compute_person_stats(conn, "driver_name")
    trainer_rows = _compute_person_stats(conn, "trainer_name")
    total_rows = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
    conn.close()

    meta = {
        "generated_at": datetime.now().strftime("%d %b %Y %H:%M"),
        "total_results": total_rows,
    }
    stats_html = _render_stats_html(driver_rows, trainer_rows, meta)
    out_path = out_dir / "stats.html"
    out_path.write_text(stats_html, encoding="utf-8")
    print(f"Stats page written -> {out_path}  ({len(driver_rows)} drivers, {len(trainer_rows)} trainers)")
    return out_path


# ---------------------------------------------------------------------------
# Betting / bankroll backtest page — fractional Kelly staking
# ---------------------------------------------------------------------------

_KELLY_FRACTION  = 0.25   # quarter-Kelly
_MAX_BET_PCT     = 0.04   # never more than 4% of current bankroll
_MAX_WIN         = 500.0  # max payout per bet — realistic bookmaker limit for country harness
_MAX_SP          = 60.0   # never bet horses longer than this SP (bookmaker / realism cap)
# Edge filter: bet whenever market_odds / fair_odds - 1 >= 25%, up to the
# _MAX_SP cap above. There is intentionally NO upper edge bound — the strategy
# takes any size of model-vs-market disagreement within the SP cap.
_MIN_ODDS_EDGE   = 0.25   # market must be at least 25% longer than fair odds


def _kelly_stake_pct(model_prob: float, market_odds: float, fraction: float = _KELLY_FRACTION) -> float | None:
    """Fractional Kelly stake as a fraction of bankroll, or None if no bet."""
    if market_odds <= 1.0 or model_prob <= 0:
        return None
    b = market_odds - 1.0
    full_kelly = (b * model_prob - (1.0 - model_prob)) / b
    if full_kelly <= 0:
        return None
    return min(full_kelly * fraction, _MAX_BET_PCT)


def _bankroll_svg(records: list[dict], starting_bankroll: float) -> str:
    values = [starting_bankroll] + [r["bankroll"] for r in records]
    if len(values) < 2:
        return ""
    min_v = min(values) * 0.97
    max_v = max(values) * 1.03
    W, H = 800, 160
    rng = max_v - min_v if max_v > min_v else 1.0

    def px(i: int) -> float:
        return i / (len(values) - 1) * W

    def py(v: float) -> float:
        return H - (v - min_v) / rng * H

    pts = " ".join(f"{px(i):.1f},{py(v):.1f}" for i, v in enumerate(values))
    colour = "#10b981" if values[-1] >= starting_bankroll else "#ef4444"
    baseline = py(starting_bankroll)
    return (
        f'<svg viewBox="0 0 {W} {H}" preserveAspectRatio="none" '
        f'style="width:100%;height:160px;display:block;">'
        f'<line x1="0" y1="{baseline:.1f}" x2="{W}" y2="{baseline:.1f}" '
        f'stroke="#64748b" stroke-width="1" stroke-dasharray="4,4"/>'
        f'<polyline points="{pts}" fill="none" stroke="{colour}" stroke-width="2.5"/>'
        f'</svg>'
    )


def _compute_bet_records(
    conn: sqlite3.Connection,
    csv_path: str | Path,
    weights: dict | None,
    starting_bankroll: float = 1000.0,
    state: str | None = None,
    from_date: datetime | None = None,
    exclude_states: tuple[str, ...] = (),
    per_state_weights: bool = False,
) -> tuple[list[dict], dict]:
    """Score all meetings in the CSV, join with race_results, apply fractional Kelly.

    Returns (records, summary). Records are in chronological order.
    from_date: if set, only meetings on or after this date are included.
    exclude_states: states skipped entirely (e.g. ("QLD",) while QLD pricing
    is unreliable — see NR-ceiling parsing gap).
    per_state_weights: when True, weights-<state>.json overrides ``weights``
    per meeting. Off by default so sweep/fitting scripts that pass candidate
    weight dicts keep full control of what is scored.
    """
    state_weights = _load_state_weights(weights) if per_state_weights else None
    # Results lookup: (meeting_code, race_number, normalised_name) → row
    results_lookup: dict[tuple, dict] = {}
    for r in conn.execute(
        "SELECT meeting_code, race_number, horse_name, finish_position, starting_price "
        "FROM race_results WHERE starting_price IS NOT NULL AND finish_position IS NOT NULL"
    ).fetchall():
        key = (r["meeting_code"], int(r["race_number"]), _normalise_name(str(r["horse_name"])))
        results_lookup[key] = dict(r)

    # Meeting metadata for sorting and display
    meeting_meta: dict[str, dict] = {}
    for r in conn.execute("SELECT meeting_code, meeting_date, track_name FROM meetings").fetchall():
        meeting_meta[r["meeting_code"]] = dict(r)

    def _date_key(mc: str) -> datetime:
        d = (meeting_meta.get(mc) or {}).get("meeting_date", "")
        try:
            return datetime.strptime(d, "%d %b %Y")
        except ValueError:
            return datetime.min

    # Score all meetings
    feature_rows = load_feature_rows(csv_path)
    meeting_codes = list(dict.fromkeys(r.get("meeting_code", "") for r in feature_rows if r.get("meeting_code")))
    meeting_codes.sort(key=_date_key)

    bankroll = starting_bankroll
    unit_halved = False
    records: list[dict] = []

    for mc in meeting_codes:
        if state and _meeting_state(mc) != state:
            continue
        if _meeting_state(mc) in exclude_states:
            continue
        if from_date and _date_key(mc) < from_date:
            continue
        mc_weights = state_weights[_meeting_state(mc)] if state_weights else weights
        scored = score_meeting_rows(feature_rows, mc, weights=mc_weights)
        for race_number, race_rows in sorted(scored.items()):
            for row in race_rows:
                horse = str(row.get("horse_name") or "")
                model_prob_raw = row.get("win_probability")
                try:
                    model_prob = float(model_prob_raw or 0)
                except (TypeError, ValueError):
                    continue
                if model_prob <= 0:
                    continue

                result = results_lookup.get((mc, race_number, _normalise_name(horse)))
                if result is None:
                    continue  # no result yet

                market_odds = float(result["starting_price"])
                finish_pos = result["finish_position"]

                # Odds-based edge: how much longer is the market than our fair price?
                fair_odds_raw = row.get("fair_odds")
                if fair_odds_raw is None or float(fair_odds_raw) <= 1.0:
                    continue
                fair_odds = float(fair_odds_raw)
                odds_edge = market_odds / fair_odds - 1.0

                # Only bet when edge >= 25%.
                if odds_edge < _MIN_ODDS_EDGE:
                    continue

                # Skip horses priced above the SP ceiling (too long for realistic bets).
                if market_odds > _MAX_SP:
                    continue

                # Halve units once if bankroll drops 25% from start
                if not unit_halved and bankroll < starting_bankroll * 0.75:
                    unit_halved = True

                fraction = _KELLY_FRACTION * (0.5 if unit_halved else 1.0)
                stake_pct = _kelly_stake_pct(model_prob, market_odds, fraction)
                if stake_pct is None:
                    continue

                stake = round(bankroll * stake_pct, 2)
                # Cap stake so potential payout never exceeds bookmaker limit
                if market_odds > 0 and stake * market_odds > _MAX_WIN:
                    stake = round(_MAX_WIN / market_odds, 2)
                stake = max(0.50, stake)
                won = int(finish_pos) == 1
                profit = round(stake * (market_odds - 1), 2) if won else round(-stake, 2)
                bankroll = round(bankroll + profit, 2)

                edge_pct = odds_edge * 100
                tier = "Strong" if edge_pct >= 50 else "Value"
                meta = meeting_meta.get(mc, {})
                fair_odds = row.get("fair_odds")

                records.append({
                    "meeting_code": mc,
                    "meeting_date": meta.get("meeting_date", ""),
                    "track_name": meta.get("track_name", ""),
                    "race_number": race_number,
                    "horse_name": horse,
                    "model_prob": round(model_prob * 100, 1),
                    "fair_odds": fair_odds,
                    "market_odds": market_odds,
                    "edge_pct": round(edge_pct, 1),
                    "tier": tier,
                    "stake": stake,
                    "won": won,
                    "finish_pos": finish_pos,
                    "profit": profit,
                    "bankroll": bankroll,
                })

    total_staked = sum(r["stake"] for r in records)
    total_profit = sum(r["profit"] for r in records)
    winners = sum(1 for r in records if r["won"])
    n = len(records)
    summary = {
        "starting_bankroll": starting_bankroll,
        "current_bankroll": bankroll,
        "total_bets": n,
        "winners": winners,
        "win_rate": round(winners / n * 100, 1) if n else 0.0,
        "total_staked": round(total_staked, 2),
        "total_profit": round(total_profit, 2),
        "roi": round(total_profit / total_staked * 100, 2) if total_staked else 0.0,
        "avg_edge": round(sum(r["edge_pct"] for r in records) / n, 1) if n else 0.0,
        "unit_halved": unit_halved,
        "state": state or ("All excl. " + ", ".join(exclude_states) if exclude_states else "All"),
        "from_date": from_date.strftime("%d %b %Y").lstrip("0") if from_date else None,
    }
    return records, summary


def _render_betting_html(records: list[dict], summary: dict, generated_at: str) -> str:
    import json as _json

    start_bank = summary["starting_bankroll"]
    curr_bank  = summary["current_bankroll"]
    profit     = summary["total_profit"]
    profit_colour = "#10b981" if profit >= 0 else "#ef4444"
    profit_sign   = "+" if profit >= 0 else ""

    chart_svg = _bankroll_svg(records, start_bank)

    def _sc(label: str, value: str, sub: str = "", colour: str = "") -> str:
        style = f' style="color:{colour}"' if colour else ""
        return f"""<div class="sc"><span class="sc-label">{html.escape(label)}</span>
          <span class="sc-value"{style}>{html.escape(value)}</span>
          {f'<span class="sc-sub">{html.escape(sub)}</span>' if sub else ''}
        </div>"""

    summary_html = "".join([
        _sc("Starting Bank",  f"${start_bank:,.0f}"),
        _sc("Current Bank",   f"${curr_bank:,.2f}",
            sub=f"{profit_sign}${abs(profit):,.2f}", colour=profit_colour),
        _sc("Total Bets",     str(summary["total_bets"]),
            sub=f"{summary['winners']} winners"),
        _sc("Win Rate",       f"{summary['win_rate']:.1f}%"),
        _sc("ROI",            f"{profit_sign}{summary['roi']:.1f}%", colour=profit_colour),
        _sc("Avg Edge",       f"{summary['avg_edge']:.1f}%"),
        _sc("Total Staked",   f"${summary['total_staked']:,.2f}"),
    ])

    unit_note = " · Units halved (bank fell 25%)" if summary.get("unit_halved") else ""
    _from_note = f" · From {summary['from_date']}" if summary.get("from_date") else ""

    # Add a numeric date_sort key so JS can sort chronologically without parsing
    _MONTHS = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    def _date_sort_key(d: str) -> int:
        parts = str(d).split()
        if len(parts) == 3:
            try:
                return int(parts[2]) * 10000 + _MONTHS.get(parts[1], 0) * 100 + int(parts[0])
            except (ValueError, KeyError):
                pass
        return 0

    serialisable = []
    for r in records:
        serialisable.append({
            "date":        r["meeting_date"],
            "date_sort":   _date_sort_key(r["meeting_date"]),
            "track":       r["track_name"] or r["meeting_code"],
            "race":        r["race_number"],
            "horse":       r["horse_name"],
            "model_prob":  r["model_prob"],
            "sp":          r["market_odds"],
            "edge":        r["edge_pct"],
            "tier":        r["tier"],
            "stake":       r["stake"],
            "pos":         r["finish_pos"],
            "won":         r["won"],
            "profit":      r["profit"],
            "bank":        r["bankroll"],
        })
    records_json = _json.dumps(serialisable)

    _bet_state = summary.get("state", "All")
    _sub = "style=\"font-size:11px;padding:3px 8px\""
    _bet_nav = (
        f'<a href="betting.html"{" class=\"active\"" if _bet_state=="All" else ""}>Betting</a>'
        f'<a href="betting-nsw.html" {_sub}{" class=\"active\"" if _bet_state=="NSW" else ""}>NSW</a>'
        f'<a href="betting-qld.html" {_sub}{" class=\"active\"" if _bet_state=="QLD" else ""}>QLD</a>'
    )
    _diag_nav = (
        '<a href="diagnose.html">Diagnose</a>'
        f'<a href="diagnose-nsw.html" {_sub}>NSW</a>'
        f'<a href="diagnose-qld.html" {_sub}>QLD</a>'
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Betting Backtest — Harness Racing</title>
  <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@700;800;900&family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;600&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg: #0a0a0a; --card-bg: #101010; --surface: #131313; --primary: #f0f0f0;
      --secondary: #808080; --accent: #00d4ff; --accent-dark: #00aacc;
      --border: #1f1f1f; --highlight: #1a1a1a;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{ margin:0; font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;
      color:var(--primary); background:var(--bg); line-height:1.5; -webkit-font-smoothing:antialiased; }}
    .top-nav {{ background:#060606; padding:10px 20px; display:flex; gap:4px; align-items:center; border-bottom:1px solid var(--border); }}
    .top-nav a {{ color:var(--secondary); text-decoration:none; font-size:13px; font-weight:600;
      padding:6px 14px; border-radius:5px; transition:all 0.15s; }}
    .top-nav a:hover {{ background:var(--highlight); color:var(--primary); }}
    .top-nav a.active {{ background:rgba(0,212,255,0.08); color:var(--accent); }}
    .hero {{ background:#060606; color:var(--primary); padding:32px 20px; text-align:center; border-bottom:1px solid var(--border); }}
    .hero h1 {{ margin:0 0 8px; font-size:32px; font-weight:900; letter-spacing:0.04em; text-transform:uppercase; font-family:'Barlow Condensed','Inter',sans-serif; }}
    .hero p {{ margin:0; color:var(--secondary); font-size:14px; }}
    .wrap {{ max-width:1300px; margin:0 auto; padding:32px 20px 60px; }}
    .summary-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:14px; margin-bottom:28px; }}
    .sc {{ background:var(--card-bg); border:1px solid var(--border); border-radius:14px; padding:16px; }}
    .sc-label {{ display:block; font-size:11px; color:var(--secondary); text-transform:uppercase;
      letter-spacing:0.05em; margin-bottom:4px; }}
    .sc-value {{ display:block; font-size:20px; font-weight:700; }}
    .sc-sub {{ display:block; font-size:12px; color:var(--secondary); margin-top:2px; }}
    .chart-card {{ background:var(--card-bg); border:1px solid var(--border); border-radius:16px;
      padding:20px; margin-bottom:28px; box-shadow:0 4px 6px -1px rgba(0,0,0,0.05); }}
    .chart-card h3 {{ margin:0 0 12px; font-size:14px; color:var(--secondary); font-weight:600;
      text-transform:uppercase; letter-spacing:0.05em; }}
    .params {{ background:var(--card-bg); border:1px solid var(--border); border-radius:12px;
      padding:14px 20px; margin-bottom:28px; font-size:13px; color:var(--secondary);
      display:flex; gap:24px; flex-wrap:wrap; align-items:center; }}
    .params strong {{ color:var(--primary); }}
    .section-title {{ font-size:20px; font-weight:700; margin:0 0 14px;
      padding-bottom:6px; border-bottom:2px solid var(--accent); display:inline-block; }}
    .table-toolbar {{ display:flex; justify-content:space-between; align-items:center;
      margin-bottom:10px; flex-wrap:wrap; gap:10px; }}
    .page-info {{ font-size:13px; color:var(--secondary); }}
    .page-controls {{ display:flex; gap:6px; align-items:center; }}
    .page-btn {{ background:var(--surface); border:1px solid var(--border); color:var(--primary);
      padding:5px 14px; border-radius:6px; font-size:13px; cursor:pointer; transition:all 0.15s; }}
    .page-btn:hover:not(:disabled) {{ background:var(--highlight); border-color:var(--accent); }}
    .page-btn:disabled {{ opacity:0.35; cursor:default; }}
    .table-wrap {{ overflow-x:auto; border:1px solid var(--border); border-radius:16px;
      background:var(--card-bg); box-shadow:0 4px 6px -1px rgba(0,0,0,0.05); }}
    table {{ width:100%; border-collapse:collapse; min-width:900px; }}
    th,td {{ padding:9px 12px; text-align:left; font-size:13px; border-bottom:1px solid var(--border); }}
    th {{ background:var(--surface); color:var(--secondary); font-size:11px; font-weight:700;
      text-transform:uppercase; letter-spacing:0.05em; white-space:nowrap;
      cursor:pointer; user-select:none; transition:color 0.15s; }}
    th:hover {{ color:var(--primary); }}
    th.sort-asc::after  {{ content:' ▲'; color:var(--accent); font-size:9px; }}
    th.sort-desc::after {{ content:' ▼'; color:var(--accent); font-size:9px; }}
    tr:last-child td {{ border-bottom:none; }}
    td.num {{ text-align:right; }}
    th.num {{ text-align:right; }}
    tr.bet-win {{ background:rgba(34,211,160,0.07); }}
    tr.bet-win:hover {{ background:rgba(34,211,160,0.12); }}
    tr.bet-loss {{ background:var(--card-bg); }}
    tr.bet-loss:hover {{ background:var(--highlight); }}
    .tier {{ display:inline-block; padding:2px 8px; border-radius:4px;
      font-size:11px; font-weight:700; text-transform:uppercase; }}
    .tier-value    {{ background:#ecfdf5; color:#059669; }}
    .tier-strong   {{ background:#fef3c7; color:#b45309; }}
    .footer {{ margin-top:40px; text-align:center; color:var(--secondary); font-size:13px; }}
  </style>
</head>
<body>
  <nav class="top-nav">
    <a href="index.html">Meetings</a>
    <a href="stats.html">Stats</a>
    {_bet_nav}
    {_diag_nav}
    <a href="live.html">Live</a>
  </nav>
  <div class="hero">
    <h1>Betting Backtest{' — ' + html.escape(summary['state']) if summary.get('state') and summary['state'] != 'All' else ''}</h1>
    <p>Quarter-Kelly staking · {html.escape(summary.get('state') or 'All')} tracks · Starting bank ${start_bank:,.0f}{(' · From ' + html.escape(summary['from_date'])) if summary.get('from_date') else ''}</p>
  </div>
  <div class="wrap">
    <div class="summary-grid">{summary_html}</div>
    <div class="params">
      <span><strong>Strategy:</strong> Quarter-Kelly (0.25×) &middot; Edge filter: &ge;25% odds</span>
      <span><strong>SP cap:</strong> $60 max</span>
      <span><strong>Max Bet:</strong> 4% of bank</span>
      <span><strong>Halve Units:</strong> if bank falls 25%</span>
      <span><strong>Tracks:</strong> {html.escape(summary.get('state') or 'All')}</span>
      {f'<span style="color:#f59e0b;font-weight:600;">⚠ Units halved</span>' if summary.get("unit_halved") else ''}
    </div>
    <div class="chart-card">
      <h3>Bankroll progression — {summary['total_bets']} bets{_from_note}{unit_note}</h3>
      {chart_svg if chart_svg else '<p style="color:var(--secondary)">No bets yet.</p>'}
    </div>
    <div class="section-title">Bet History</div>
    <div class="table-toolbar">
      <span class="page-info" id="pageInfo"></span>
      <div class="page-controls">
        <button class="page-btn" id="btnFirst" onclick="goPage(0)">«</button>
        <button class="page-btn" id="btnPrev"  onclick="goPage(_page-1)">‹ Prev</button>
        <span class="page-info" id="pageNum"></span>
        <button class="page-btn" id="btnNext"  onclick="goPage(_page+1)">Next ›</button>
        <button class="page-btn" id="btnLast"  onclick="goPage(_totalPages-1)">»</button>
      </div>
    </div>
    <div class="table-wrap">
      <table id="betTable">
        <thead>
          <tr>
            <th data-col="date_sort">Date</th>
            <th data-col="track">Track</th>
            <th data-col="race" class="num">Race</th>
            <th data-col="horse">Horse</th>
            <th data-col="model_prob" class="num">Model%</th>
            <th data-col="sp" class="num">SP</th>
            <th data-col="edge" class="num">Edge%</th>
            <th data-col="tier" class="num">Tier</th>
            <th data-col="stake" class="num">Stake</th>
            <th data-col="pos" class="num">Result</th>
            <th data-col="profit" class="num">P&amp;L</th>
            <th data-col="bank" class="num">Bank</th>
          </tr>
        </thead>
        <tbody id="betBody"></tbody>
      </table>
    </div>
    <div class="table-toolbar" style="margin-top:10px;">
      <span class="page-info" id="pageInfo2"></span>
      <div class="page-controls">
        <button class="page-btn" id="btnFirst2" onclick="goPage(0)">«</button>
        <button class="page-btn" id="btnPrev2"  onclick="goPage(_page-1)">‹ Prev</button>
        <span class="page-info" id="pageNum2"></span>
        <button class="page-btn" id="btnNext2"  onclick="goPage(_page+1)">Next ›</button>
        <button class="page-btn" id="btnLast2"  onclick="goPage(_totalPages-1)">»</button>
      </div>
    </div>
    <div class="footer">Generated {html.escape(generated_at)}</div>
  </div>
<script>
const _RECORDS = {records_json};
const _PAGE_SIZE = 200;
let _sortCol = 'date_sort';
let _sortDir = -1;   // -1 = desc (newest first by default)
let _page = 0;
let _sorted = [];
let _totalPages = 1;

function _esc(s) {{
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}}

function _sortKey(r, col) {{
  const v = r[col];
  if (typeof v === 'string') return v.toLowerCase();
  return v ?? -Infinity;
}}

function _doSort() {{
  _sorted = [..._RECORDS];
  _sorted.sort((a, b) => {{
    const ka = _sortKey(a, _sortCol), kb = _sortKey(b, _sortCol);
    if (ka < kb) return _sortDir;
    if (ka > kb) return -_sortDir;
    // secondary: date desc, then race asc
    if (_sortCol !== 'date_sort') {{
      if (a.date_sort !== b.date_sort) return b.date_sort - a.date_sort;
    }}
    return a.race - b.race;
  }});
  _totalPages = Math.max(1, Math.ceil(_sorted.length / _PAGE_SIZE));
  if (_page >= _totalPages) _page = _totalPages - 1;
}}

function _renderPage() {{
  const start = _page * _PAGE_SIZE;
  const slice = _sorted.slice(start, start + _PAGE_SIZE);
  const rows = slice.map(r => {{
    const won = r.won;
    const rowCls = won ? 'bet-win' : 'bet-loss';
    const profStr = won ? '+$' + r.profit.toFixed(2) : '-$' + Math.abs(r.profit).toFixed(2);
    const profCol = won ? '#10b981' : '#ef4444';
    const resultStr = won ? '1st' : String(r.pos);
    const tierCls = r.tier === 'Strong' ? 'tier-strong' : 'tier-value';
    return `<tr class="${{rowCls}}">
      <td>${{_esc(r.date)}}</td>
      <td>${{_esc(r.track)}}</td>
      <td class="num">R${{r.race}}</td>
      <td>${{_esc(r.horse)}}</td>
      <td class="num">${{r.model_prob.toFixed(1)}}%</td>
      <td class="num">$${{r.sp.toFixed(2)}}</td>
      <td class="num">${{r.edge.toFixed(1)}}%</td>
      <td class="num"><span class="tier ${{tierCls}}">${{_esc(r.tier)}}</span></td>
      <td class="num">$${{r.stake.toFixed(2)}}</td>
      <td class="num">${{resultStr}}</td>
      <td class="num" style="color:${{profCol}}">${{profStr}}</td>
      <td class="num">$${{r.bank.toLocaleString('en-AU', {{minimumFractionDigits:2, maximumFractionDigits:2}})}}</td>
    </tr>`;
  }}).join('');
  document.getElementById('betBody').innerHTML = rows ||
    '<tr><td colspan="12" style="text-align:center;color:var(--secondary);padding:32px;">No bets recorded yet.</td></tr>';

  // Update pagination controls (top + bottom)
  const from = _sorted.length ? start + 1 : 0;
  const to   = Math.min(start + _PAGE_SIZE, _sorted.length);
  const info = `Showing ${{from}}–${{to}} of ${{_sorted.length}} bets`;
  const pg   = `Page ${{_page + 1}} / ${{_totalPages}}`;
  ['pageInfo','pageInfo2'].forEach(id => {{ const el=document.getElementById(id); if(el) el.textContent=info; }});
  ['pageNum','pageNum2'].forEach(id  => {{ const el=document.getElementById(id);  if(el) el.textContent=pg;   }});

  const atFirst = _page === 0;
  const atLast  = _page >= _totalPages - 1;
  [['btnFirst','btnFirst2'], ['btnPrev','btnPrev2']].forEach(([a,b]) => {{
    [a,b].forEach(id => {{ const el=document.getElementById(id); if(el) el.disabled=atFirst; }});
  }});
  [['btnNext','btnNext2'], ['btnLast','btnLast2']].forEach(([a,b]) => {{
    [a,b].forEach(id => {{ const el=document.getElementById(id); if(el) el.disabled=atLast; }});
  }});

  // Update header sort indicators
  document.querySelectorAll('#betTable thead th').forEach(th => {{
    th.classList.remove('sort-asc','sort-desc');
    if (th.dataset.col === _sortCol) {{
      th.classList.add(_sortDir === 1 ? 'sort-asc' : 'sort-desc');
    }}
  }});
}}

function goPage(n) {{
  _page = Math.max(0, Math.min(n, _totalPages - 1));
  _renderPage();
  document.getElementById('betTable').scrollIntoView({{behavior:'smooth', block:'start'}});
}}

// Wire up header clicks
document.querySelectorAll('#betTable thead th[data-col]').forEach(th => {{
  th.addEventListener('click', () => {{
    const col = th.dataset.col;
    if (_sortCol === col) {{
      _sortDir = -_sortDir;
    }} else {{
      _sortCol = col;
      // numeric cols default desc, text cols default asc
      const numCols = new Set(['date_sort','race','model_prob','sp','edge','stake','pos','profit','bank']);
      _sortDir = numCols.has(col) ? -1 : 1;
    }}
    _page = 0;
    _doSort();
    _renderPage();
  }});
}});

// Initial render
_doSort();
_renderPage();
</script>
</body>
</html>
"""


def build_betting_site(
    db_path: str | Path,
    csv_path: str | Path,
    out_dir: str | Path,
    starting_bankroll: float = 1000.0,
    state: str | None = None,
    from_date: datetime | None = None,
    exclude_states: tuple[str, ...] = (),
) -> Path:
    """Build fractional-Kelly backtest page and write to out_dir/betting[-state].html."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = connect(db_path)
    init_db(conn)
    weights_path = Path("weights.json")
    weights = load_weights(weights_path) if weights_path.exists() else None
    records, summary = _compute_bet_records(
        conn, csv_path, weights, starting_bankroll,
        state=state, from_date=from_date, exclude_states=exclude_states,
        per_state_weights=True,
    )
    conn.close()

    betting_html = _render_betting_html(
        records, summary, datetime.now().strftime("%d %b %Y %H:%M")
    )
    filename = f"betting-{state.lower()}.html" if state else "betting.html"
    out_path = out_dir / filename
    out_path.write_text(betting_html, encoding="utf-8")
    print(f"Betting page written -> {out_path}  ({summary['total_bets']} bets, ROI {summary['roi']:+.1f}%)")
    return out_path


def publish_scored_meeting(
    meeting_code: str,
    meeting_scores: dict[int, list[dict[str, object]]],
    db_path: str | Path = "data/harness.db",
    docs_dir: str | Path = "docs",
) -> None:
    """Write scored meeting HTML to docs/ and push to GitHub Pages.

    Takes already-scored data from score_meeting_rows so there is no
    double-scoring. Loads meeting metadata and result rows from the DB,
    generates the HTML, updates the index, then runs git add/commit/push.
    """
    repo_root = Path(__file__).parents[2]
    docs = Path(docs_dir) if Path(docs_dir).is_absolute() else (repo_root / docs_dir)
    docs.mkdir(parents=True, exist_ok=True)

    conn = connect(db_path)
    init_db(conn)
    meeting_meta = _load_meeting_metadata(conn, meeting_code)
    result_rows = _load_results(conn, meeting_code)
    conn.close()

    races_scored = sum(1 for rows in meeting_scores.values() if rows)
    winners_count = _count_top_pick_winners(meeting_scores, result_rows)
    page_path = docs / f"{meeting_code}.html"
    page_path.write_text(
        _render_meeting_html(meeting_code, meeting_scores, meeting_meta, result_rows),
        encoding="utf-8",
    )
    _write_index(docs, meeting_meta=meeting_meta, races=races_scored, winners=winners_count)

    print(f"  Written {page_path.relative_to(repo_root)}")

    try:
        subprocess.run(["git", "add", str(docs)], check=True, cwd=repo_root)
        commit = subprocess.run(
            ["git", "commit", "-m", f"Publish {meeting_code} scores"],
            cwd=repo_root,
        )
        if commit.returncode not in (0, 1):
            print(f"  Warning: git commit exited with code {commit.returncode}")
            return
        push = subprocess.run(["git", "push"], cwd=repo_root)
        if push.returncode == 0:
            print(f"  Pushed to GitHub. Your page will be live at:")
            print(f"  https://<your-username>.github.io/<repo-name>/{meeting_code}.html")
        else:
            print(f"  Warning: git push failed (exit {push.returncode}). Check remote is configured.")
    except FileNotFoundError:
        print("  Warning: git not found on PATH — HTML written but not pushed.")


def republish_all_meetings(
    csv_path: str | Path = "data/features/runner_features.csv",
    db_path: str | Path = "data/harness.db",
    docs_dir: str | Path = "docs",
    weights: dict | None = None,
) -> int:
    """Re-score and re-render every meeting in docs/meetings.json in one batch.

    Loads feature rows and opens the DB once, writes all HTMLs, then does a
    single git add/commit/push. Returns the number of meetings republished.
    """
    repo_root = Path(__file__).parents[2]
    docs = Path(docs_dir) if Path(docs_dir).is_absolute() else (repo_root / docs_dir)
    docs.mkdir(parents=True, exist_ok=True)

    manifest_path = docs / "meetings.json"
    if not manifest_path.exists():
        print("No meetings.json found — nothing to republish.")
        return 0

    manifest: list[dict[str, Any]] = json.loads(manifest_path.read_text(encoding="utf-8"))
    meeting_codes = [m["meeting_code"] for m in manifest if m.get("meeting_code")]
    if not meeting_codes:
        print("meetings.json is empty — nothing to republish.")
        return 0

    print(f"Loading features from {csv_path} ...")
    feature_rows = load_feature_rows(csv_path)

    conn = connect(db_path)
    init_db(conn)

    published = 0
    state_weights = _load_state_weights(weights)
    for code in meeting_codes:
        try:
            meeting_scores = score_meeting_rows(feature_rows, code, weights=state_weights[_meeting_state(code)])
            meeting_meta = _load_meeting_metadata(conn, code)
            result_rows = _load_results(conn, code)

            races_scored = sum(1 for rows in meeting_scores.values() if rows)
            winners_count = _count_top_pick_winners(meeting_scores, result_rows)

            page_path = docs / f"{code}.html"
            page_path.write_text(
                _render_meeting_html(code, meeting_scores, meeting_meta, result_rows),
                encoding="utf-8",
            )
            # Update the manifest entry in-place (races/winners counts may have changed)
            entry = next((m for m in manifest if m.get("meeting_code") == code), None)
            if entry:
                entry["races"] = races_scored
                entry["winners"] = winners_count
                if meeting_meta:
                    for k in ("track_name", "meeting_date"):
                        if meeting_meta.get(k):
                            entry[k] = meeting_meta[k]

            print(f"  {code}: {races_scored} races, {winners_count} winners")
            published += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  {code}: ERROR — {exc}")

    conn.close()

    # Write updated manifest and rebuild index
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    _write_index(docs)

    # Rebuild betting and diagnose pages (all + per-state).
    # Betting pages start from 1 May 2026 — the first date with clean (non-leaky)
    # per-meeting form snapshots and calibrated weights.
    _bet_from = datetime(2026, 5, 1)
    try:
        # Headline page excludes QLD: 44% of QLD races lack a parseable NR
        # ceiling, producing unreliable prices (-44.8% ROI). The QLD-only page
        # is still built below for monitoring, but QLD bets stay out of the
        # main ledger until NR gating / state-specific weights are in place.
        build_betting_site(db_path, csv_path, docs, starting_bankroll=1000.0, from_date=_bet_from, exclude_states=("QLD",))
        build_betting_site(db_path, csv_path, docs, starting_bankroll=1000.0, state="NSW", from_date=_bet_from)
        build_betting_site(db_path, csv_path, docs, starting_bankroll=1000.0, state="QLD", from_date=_bet_from)
    except Exception as exc:  # noqa: BLE001
        print(f"  Warning: betting page rebuild failed — {exc}")
    try:
        build_diagnose_site(csv_path, db_path, docs, weights=weights)
        build_diagnose_site(csv_path, db_path, docs, weights=weights, state="NSW")
        build_diagnose_site(csv_path, db_path, docs, weights=weights, state="QLD")
    except Exception as exc:  # noqa: BLE001
        print(f"  Warning: diagnose page rebuild failed — {exc}")

    # Rebuild live page so hosted race data stays current
    try:
        from .live import build_live_site
        build_live_site(str(csv_path), weights=weights, out_dir=str(docs))
    except Exception as exc:  # noqa: BLE001
        print(f"  Warning: live page rebuild failed — {exc}")

    print(f"\nRepublished {published}/{len(meeting_codes)} meetings.")

    try:
        subprocess.run(["git", "add", str(docs)], check=True, cwd=repo_root)
        commit = subprocess.run(
            ["git", "commit", "-m", f"Republish all meetings ({published} pages)"],
            cwd=repo_root,
        )
        if commit.returncode not in (0, 1):
            print(f"  Warning: git commit exited with code {commit.returncode}")
            return published
        push = subprocess.run(["git", "push"], cwd=repo_root)
        if push.returncode == 0:
            print("  Pushed to GitHub Pages.")
        else:
            print(f"  Warning: git push failed (exit {push.returncode}).")
    except FileNotFoundError:
        print("  Warning: git not found on PATH — HTMLs written but not pushed.")

    return published


# ---------------------------------------------------------------------------
# Diagnostic page
# ---------------------------------------------------------------------------

def _fv_web(row: dict, key: str) -> float | None:
    """Read a CSV passthrough field as float, silently returning None if absent/non-numeric."""
    val = row.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def build_diagnose_site(
    csv_path: str | Path,
    db_path: str | Path,
    out_dir: str | Path = "docs",
    weights: dict | None = None,
    state: str | None = None,
) -> Path:
    """Score all meetings with stored results, build the diagnostic truth-set page."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_feature_rows(csv_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    meetings = [r[0] for r in conn.execute(
        "SELECT DISTINCT meeting_code FROM race_results ORDER BY meeting_code"
    ).fetchall()]

    # Prefix map: fallback when meetings.track_name is NULL or the meeting isn't
    # in the meetings table (e.g. manually-added results from older meetings).
    _PREFIX_TRACK = {
        "BH": "Bathurst", "CB": "Canberra", "CL": "Canberra",
        "DU": "Dubbo", "EY": "Wagga", "FB": "Forbes",
        "JU": "Junee", "LE": "Leeton", "LM": "Goulburn",
        "NA": "Narrabri", "NR": "Newcastle", "PC": "Menangle",
        "PE": "Penrith", "PK": "Parkes", "TA": "Tamworth",
        "TM": "Temora", "UG": "Marburg", "YU": "Young",
        "AP": "Albion Park", "RE": "Redcliffe",
    }
    meeting_track: dict[str, str] = {}
    for r in conn.execute("SELECT meeting_code, track_name FROM meetings").fetchall():
        track = r["track_name"] or _PREFIX_TRACK.get(r["meeting_code"][:2], r["meeting_code"])
        meeting_track[r["meeting_code"]] = track

    results_lookup: dict[tuple, dict] = {}
    for r in conn.execute(
        "SELECT meeting_code, race_number, horse_name, finish_position, starting_price "
        "FROM race_results"
    ).fetchall():
        key = (r["meeting_code"], r["race_number"], r["horse_name"].upper())
        results_lookup[key] = {"pos": r["finish_position"], "sp": r["starting_price"]}
    conn.close()

    # Build race_groups identical to the diagnose CLI handler
    race_groups: dict[tuple, list[dict]] = {}
    state_weights = _load_state_weights(weights)
    for meeting_code in meetings:
        if state and _meeting_state(meeting_code) != state:
            continue
        try:
            scored = score_meeting_rows(rows, meeting_code, weights=state_weights[_meeting_state(meeting_code)])
        except Exception:
            continue
        for race_number, horse_rows in scored.items():
            group = []
            for h in horse_rows:
                key = (meeting_code, race_number, h["horse_name"].upper())
                if key not in results_lookup:
                    continue
                res = results_lookup[key]
                group.append({
                    "meeting": meeting_code,
                    "race": race_number,
                    "horse": h["horse_name"],
                    "finish_pos": res["pos"],
                    "sp": res["sp"],
                    "stage1": h.get("stage1_score"),
                    "stage2": h.get("stage2_score"),
                    "total_score": h.get("score"),
                    "model_prob": h.get("adjusted_probability"),
                    "model_odds": h.get("adjusted_fair_odds"),
                    "race_nr_ceiling": _fv_web(h, "race_nr_ceiling"),
                    "nr_grade_delta": _fv_web(h, "nr_grade_delta"),
                    "sp_class_ceiling": _fv_web(h, "sp_best_prob_similar_grade"),
                    "map_lead_score": _fv_web(h, "map_lead_score"),
                })
            if len(group) < 2:
                continue
            group.sort(key=lambda x: -(x["model_prob"] or 0))
            for i, h in enumerate(group):
                h["model_rank"] = i + 1
            group_sp = sorted(group, key=lambda x: x["sp"] if x["sp"] else 9999)
            for i, h in enumerate(group_sp):
                h["sp_rank"] = i + 1
            group.sort(key=lambda x: -(x["stage1"] or 0))
            for i, h in enumerate(group):
                h["s1_rank"] = i + 1
            race_groups[(meeting_code, race_number)] = group

    generated_at = datetime.now().strftime("%d %b %Y %H:%M")
    html_str = _render_diagnose_html(race_groups, generated_at, meeting_track=meeting_track, state=state)
    filename = f"diagnose-{state.lower()}.html" if state else "diagnose.html"
    out_path = out_dir / filename
    out_path.write_text(html_str, encoding="utf-8")
    return out_path


def _render_diagnose_html(race_groups: dict, generated_at: str, meeting_track: dict | None = None, state: str | None = None) -> str:
    """Render the full diagnostic page as an HTML string."""

    def _avg(lst: list) -> float | None:
        return sum(lst) / len(lst) if lst else None

    # ---- Build per-race JSON rows for client-side filtering ----
    race_rows: list[dict] = []
    map_rows: list[dict] = []

    for key in sorted(race_groups.keys()):
        horses = race_groups[key]
        meeting, race = key
        winner    = next((h for h in horses if h["finish_pos"] == 1), None)
        model_top = next((h for h in horses if h["model_rank"] == 1), None)
        sp_top    = next((h for h in horses if h["sp_rank"] == 1), None)
        s1_top    = next((h for h in horses if h["s1_rank"] == 1), None)
        model_ranked = sorted(horses, key=lambda h: h["model_rank"])
        best_map  = max(horses, key=lambda h: (h.get("map_lead_score") or -99))

        if not (winner and model_top and sp_top):
            continue

        model_correct = winner["horse"] == model_top["horse"]
        sp_correct    = winner["horse"] == sp_top["horse"]
        is_sp_knew    = sp_correct and (winner.get("model_rank") or 0) >= 4
        s1_changed    = s1_top and model_top and s1_top["horse"] != model_top["horse"]

        _track = (meeting_track or {}).get(meeting) or _PREFIX_TRACK.get(meeting[:2], meeting)
        race_rows.append({
            "meeting": meeting,
            "track": _track,
            "race": race,
            # Winner
            "winner": winner["horse"],
            "winner_sp": winner["sp"],
            "winner_model_rank": winner.get("model_rank"),
            "winner_sp_rank": winner.get("sp_rank"),
            "winner_model_odds": winner.get("model_odds"),
            "winner_s1": winner.get("stage1"),
            "winner_s2": winner.get("stage2"),
            "winner_total": winner.get("total_score"),
            "winner_nr_grade_delta": winner.get("nr_grade_delta"),
            "winner_race_nr_ceiling": winner.get("race_nr_ceiling"),
            "winner_sp_class_ceiling": winner.get("sp_class_ceiling"),
            "winner_map": winner.get("map_lead_score"),
            # Model top pick (may differ from winner)
            "model_top1": model_top["horse"],
            "model_top1_s1": model_top.get("stage1"),
            "model_top1_s2": model_top.get("stage2"),
            "model_top1_total": model_top.get("total_score"),
            "model_top2": model_ranked[1]["horse"] if len(model_ranked) > 1 else None,
            "model_top3": model_ranked[2]["horse"] if len(model_ranked) > 2 else None,
            # Flags
            "model_correct": model_correct,
            "sp_correct": sp_correct,
            "is_sp_knew": is_sp_knew,
            "s2_changed_top": s1_changed,
        })

        # Map override: best-map horse not top pick AND placed top 3
        bm_finish = best_map.get("finish_pos")
        if best_map["horse"] != model_top["horse"] and bm_finish and bm_finish <= 3:
            map_rows.append({
                "meeting": meeting,
                "track": _track,
                "race": race,
                "bm_horse": best_map["horse"],
                "bm_model_rank": best_map.get("model_rank"),
                "bm_sp_rank": best_map.get("sp_rank"),
                "bm_finish": bm_finish,
                "bm_map": best_map.get("map_lead_score"),
                "winner": winner["horse"],
                "winner_map": winner.get("map_lead_score"),
            })

    # ---- Pre-compute global + per-meeting summary stats ----
    def _compute_stats(rows: list[dict]) -> dict:
        n = len(rows)
        if n == 0:
            return {}
        failures = [r for r in rows if not r["model_correct"]]
        return {
            "total_races": n,
            "model_correct": sum(1 for r in rows if r["model_correct"]),
            "sp_correct": sum(1 for r in rows if r["sp_correct"]),
            "top3_hit": sum(1 for r in rows if (r["winner_model_rank"] or 0) <= 3),
            "rank2": sum(1 for r in rows if r["winner_model_rank"] == 2),
            "rank3": sum(1 for r in rows if r["winner_model_rank"] == 3),
            "rank4plus": sum(1 for r in rows if (r["winner_model_rank"] or 0) >= 4),
            "both_correct": sum(1 for r in rows if r["model_correct"] and r["sp_correct"]),
            "model_edge": sum(1 for r in rows if r["model_correct"] and not r["sp_correct"]),
            "sp_failure": sum(1 for r in rows if not r["model_correct"] and r["sp_correct"]),
            "both_wrong": sum(1 for r in rows if not r["model_correct"] and not r["sp_correct"]),
            "sp_knew_count": sum(1 for r in rows if r.get("is_sp_knew")),
            "s2_swung": sum(1 for r in rows if r.get("s2_changed_top")),
            "big_miss_count": sum(1 for r in rows if (r["winner_model_rank"] or 0) >= 5),
            # S1/S2 on failures: model top pick vs winner
            "avg_top_s1": _avg([r["model_top1_s1"] for r in failures if r["model_top1_s1"] is not None]),
            "avg_top_s2": _avg([r["model_top1_s2"] for r in failures if r["model_top1_s2"] is not None]),
            "avg_top_tot": _avg([r["model_top1_total"] for r in failures if r["model_top1_total"] is not None]),
            "avg_win_s1": _avg([r["winner_s1"] for r in failures if r["winner_s1"] is not None]),
            "avg_win_s2": _avg([r["winner_s2"] for r in failures if r["winner_s2"] is not None]),
            "avg_win_tot": _avg([r["winner_total"] for r in failures if r["winner_total"] is not None]),
            "avg_winner_rank": _avg([r["winner_model_rank"] for r in failures if r["winner_model_rank"] is not None]),
        }

    global_stats = _compute_stats(race_rows)
    meetings_list = sorted({r["meeting"] for r in race_rows})
    by_meeting = {m: _compute_stats([r for r in race_rows if r["meeting"] == m]) for m in meetings_list}
    tracks_list = sorted({r["track"] for r in race_rows})
    by_track = {t: _compute_stats([r for r in race_rows if r["track"] == t]) for t in tracks_list}

    data_json = json.dumps({
        "global": global_stats,
        "by_meeting": by_meeting,
        "by_track": by_track,
        "tracks": tracks_list,
        "races": race_rows,
        "map_overrides": map_rows,
    }, separators=(",", ":"))

    n_races = global_stats.get("total_races", 0)

    _sub = "style=\"font-size:11px;padding:3px 8px\""
    _bet_nav = (
        '<a href="betting.html">Betting</a>'
        f'<a href="betting-nsw.html" {_sub}>NSW</a>'
        f'<a href="betting-qld.html" {_sub}>QLD</a>'
    )
    _diag_nav = (
        f'<a href="diagnose.html"{" class=\"active\"" if not state else ""}>Diagnose</a>'
        f'<a href="diagnose-nsw.html" {_sub}{" class=\"active\"" if state=="NSW" else ""}>NSW</a>'
        f'<a href="diagnose-qld.html" {_sub}{" class=\"active\"" if state=="QLD" else ""}>QLD</a>'
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Model Diagnostic — Harness Racing</title>
  <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@700;800;900&family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;600&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg:#0a0a0a; --card-bg:#101010; --surface:#131313; --primary:#f0f0f0;
      --secondary:#808080; --accent:#00d4ff; --accent-dark:#00aacc;
      --border:#1f1f1f; --highlight:#1a1a1a;
      --red:#ef4444; --amber:#f59e0b; --blue:#3b82f6;
    }}
    *{{box-sizing:border-box;}}
    html{{scroll-behavior:smooth;}}
    body{{margin:0;font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;
      color:var(--primary);background:var(--bg);line-height:1.5;-webkit-font-smoothing:antialiased;}}
    .top-nav{{background:#060606;padding:10px 20px;display:flex;gap:4px;align-items:center;border-bottom:1px solid var(--border);}}
    .top-nav a{{color:var(--secondary);text-decoration:none;font-size:13px;font-weight:600;
      padding:6px 14px;border-radius:5px;transition:all 0.15s;}}
    .top-nav a:hover{{background:var(--highlight);color:var(--primary);}}
    .top-nav a.active{{background:rgba(0,212,255,0.08);color:var(--accent);}}
    .hero{{background:#060606;color:var(--primary);padding:28px 20px;border-bottom:1px solid var(--border);}}
    .hero-inner{{max-width:1300px;margin:0 auto;display:flex;align-items:center;gap:24px;flex-wrap:wrap;}}
    .hero h1{{margin:0;font-size:26px;font-weight:900;letter-spacing:0.04em;text-transform:uppercase;font-family:'Barlow Condensed','Inter',sans-serif;}}
    .hero p{{margin:4px 0 0;color:var(--secondary);font-size:13px;}}
    .meeting-select{{margin-left:auto;display:flex;align-items:center;gap:10px;}}
    .meeting-select label{{color:var(--secondary);font-size:13px;font-weight:600;white-space:nowrap;}}
    .meeting-select select{{padding:8px 12px;border-radius:6px;border:1px solid var(--border);
      background:var(--surface);color:var(--primary);font-size:13px;font-weight:600;cursor:pointer;
      appearance:none;-webkit-appearance:none;min-width:180px;}}
    .meeting-select select option{{background:#131313;color:var(--primary);}}
    .wrap{{max-width:1300px;margin:0 auto;padding:28px 20px 60px;}}
    /* Summary cards */
    .cards-row{{display:grid;gap:14px;margin-bottom:24px;}}
    .cards-row.two{{grid-template-columns:1fr 1fr;}}
    .cards-row.three{{grid-template-columns:repeat(3,1fr);}}
    @media(max-width:700px){{.cards-row.two,.cards-row.three{{grid-template-columns:1fr;}}}}
    .card{{background:var(--card-bg);border:1px solid var(--border);border-radius:14px;
      padding:18px 20px;box-shadow:0 2px 4px rgba(0,0,0,0.04);}}
    .card-title{{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;
      color:var(--secondary);margin:0 0 12px;padding-bottom:8px;border-bottom:1px solid var(--border);}}
    /* Mini stat rows */
    .stat-row{{display:flex;justify-content:space-between;align-items:baseline;padding:4px 0;
      font-size:13px;border-bottom:1px solid var(--highlight);}}
    .stat-row:last-child{{border-bottom:none;}}
    .stat-label{{color:var(--secondary);}}
    .stat-val{{font-weight:700;font-size:14px;}}
    .stat-val.green{{color:var(--accent-dark);}}
    .stat-val.red{{color:var(--red);}}
    .stat-val.amber{{color:var(--amber);}}
    /* Quadrant grid */
    .quad-grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}
    .quad{{border-radius:10px;padding:12px;text-align:center;}}
    .quad .q-val{{font-size:22px;font-weight:800;}}
    .quad .q-pct{{font-size:12px;font-weight:600;color:var(--secondary);}}
    .quad .q-label{{font-size:11px;color:var(--secondary);margin-top:2px;}}
    .quad.both-right{{background:rgba(34,211,160,0.10);border:1px solid rgba(34,211,160,0.25);}}
    .quad.model-edge{{background:rgba(0,212,255,0.07);border:1px solid rgba(0,212,255,0.2);}}
    .quad.model-fail{{background:rgba(239,68,68,0.07);border:1px solid rgba(239,68,68,0.2);}}
    .quad.both-wrong{{background:var(--surface);border:1px solid var(--border);}}
    /* Section header */
    .section-header{{display:flex;align-items:baseline;gap:12px;margin:28px 0 12px;}}
    .section-title{{font-size:16px;font-weight:700;}}
    .section-count{{font-size:13px;color:var(--secondary);}}
    /* Tables */
    .table-wrap{{overflow-x:auto;border:1px solid var(--border);border-radius:14px;
      background:var(--card-bg);box-shadow:0 2px 4px rgba(0,0,0,0.04);}}
    table{{width:100%;border-collapse:collapse;min-width:700px;}}
    th,td{{padding:8px 12px;text-align:left;font-size:12px;border-bottom:1px solid var(--border);}}
    th{{background:var(--surface);color:var(--secondary);font-size:10px;font-weight:700;
      text-transform:uppercase;letter-spacing:0.05em;white-space:nowrap;}}
    tr:last-child td{{border-bottom:none;}}
    td.num{{text-align:right;font-variant-numeric:tabular-nums;}}
    th.num{{text-align:right;}}
    tr.model-win{{background:rgba(34,211,160,0.07);}}
    tr.model-win:hover{{background:rgba(34,211,160,0.12);}}
    tr.model-loss:hover{{background:var(--highlight);}}
    .badge{{display:inline-block;padding:1px 7px;border-radius:4px;font-size:10px;font-weight:700;}}
    .badge.win{{background:#dcfce7;color:#15803d;}}
    .badge.loss{{background:#fee2e2;color:#b91c1c;}}
    .hidden{{display:none!important;}}
    .no-data{{padding:24px;text-align:center;color:var(--secondary);font-size:13px;}}
    .footer{{margin-top:40px;text-align:center;color:var(--secondary);font-size:12px;}}
  </style>
</head>
<body>
  <nav class="top-nav">
    <a href="index.html">Meetings</a>
    <a href="stats.html">Stats</a>
    {_bet_nav}
    {_diag_nav}
  </nav>
  <div class="hero">
    <div class="hero-inner">
      <div>
        <h1>Model Diagnostic{' — ' + html.escape(state) if state else ''}</h1>
        <p>Per-race truth set &middot; {n_races} races with results &middot; Generated {html.escape(generated_at)}</p>
      </div>
      <div class="meeting-select">
        <label for="track-sel">Track:</label>
        <select id="track-sel">
          <option value="all">All tracks ({n_races} races)</option>
          {''.join(f'<option value="{html.escape(t)}">{html.escape(t)}</option>' for t in tracks_list)}
        </select>
      </div>
    </div>
  </div>

  <div class="wrap">
    <!-- Row 1: Hit Rate + Quadrants -->
    <div class="cards-row two">
      <div class="card">
        <div class="card-title">Hit Rate</div>
        <div class="stat-row"><span class="stat-label">Top pick won (model)</span><span class="stat-val" id="s-model-correct"></span></div>
        <div class="stat-row"><span class="stat-label">Top pick won (SP)</span><span class="stat-val" id="s-sp-correct"></span></div>
        <div class="stat-row"><span class="stat-label">Top 3 contained winner</span><span class="stat-val" id="s-top3"></span></div>
        <div class="stat-row"><span class="stat-label">Winner was model 2nd</span><span class="stat-val" id="s-rank2"></span></div>
        <div class="stat-row"><span class="stat-label">Winner was model 3rd</span><span class="stat-val" id="s-rank3"></span></div>
        <div class="stat-row"><span class="stat-label">Winner was model 4th+</span><span class="stat-val red" id="s-rank4plus"></span></div>
      </div>
      <div class="card">
        <div class="card-title">Failure Quadrants</div>
        <div class="quad-grid">
          <div class="quad both-right">
            <div class="q-val" id="q-both-correct"></div>
            <div class="q-pct" id="q-both-correct-pct"></div>
            <div class="q-label">Both right</div>
          </div>
          <div class="quad model-edge">
            <div class="q-val" id="q-model-edge"></div>
            <div class="q-pct" id="q-model-edge-pct"></div>
            <div class="q-label">Model edge</div>
          </div>
          <div class="quad model-fail">
            <div class="q-val" id="q-sp-failure"></div>
            <div class="q-pct" id="q-sp-failure-pct"></div>
            <div class="q-label">Model failure</div>
          </div>
          <div class="quad both-wrong">
            <div class="q-val" id="q-both-wrong"></div>
            <div class="q-pct" id="q-both-wrong-pct"></div>
            <div class="q-label">Both wrong</div>
          </div>
        </div>
      </div>
    </div>

    <!-- Row 2: S1 vs S2 breakdown -->
    <div class="cards-row three">
      <div class="card">
        <div class="card-title">S1 vs S2 on Failures</div>
        <div class="stat-row"><span class="stat-label"></span><span class="stat-label" style="font-weight:700">Top pick &nbsp;&nbsp; Winner</span></div>
        <div class="stat-row"><span class="stat-label">Avg S1 score</span><span class="stat-val" id="s-s1-compare"></span></div>
        <div class="stat-row"><span class="stat-label">Avg S2 score</span><span class="stat-val" id="s-s2-compare"></span></div>
        <div class="stat-row"><span class="stat-label">Avg total score</span><span class="stat-val" id="s-tot-compare"></span></div>
        <div class="stat-row"><span class="stat-label">Avg winner model rank</span><span class="stat-val" id="s-avg-rank"></span></div>
        <div class="stat-row"><span class="stat-label">S2 changed top pick</span><span class="stat-val" id="s-s2-swung"></span></div>
      </div>
      <div class="card">
        <div class="card-title">Flagged Patterns</div>
        <div class="stat-row"><span class="stat-label">SP knew, model didn't (rank 4+)</span><span class="stat-val red" id="s-sp-knew"></span></div>
        <div class="stat-row"><span class="stat-label">Map override placed top 3</span><span class="stat-val amber" id="s-map-override"></span></div>
        <div class="stat-row"><span class="stat-label">Big miss (winner rank 5+)</span><span class="stat-val red" id="s-big-miss"></span></div>
      </div>
      <div class="card">
        <div class="card-title">Score Distribution</div>
        <div class="stat-row"><span class="stat-label">Total races</span><span class="stat-val" id="s-total-races"></span></div>
        <div class="stat-row"><span class="stat-label">Model correct</span><span class="stat-val green" id="s-model-pct"></span></div>
        <div class="stat-row"><span class="stat-label">SP correct</span><span class="stat-val" id="s-sp-pct"></span></div>
        <div class="stat-row"><span class="stat-label">Model vs SP gap</span><span class="stat-val" id="s-gap"></span></div>
      </div>
    </div>

    <!-- Section 4: Per-Race Detail -->
    <div class="section-header">
      <div class="section-title">Per-Race Detail</div>
      <div class="section-count" id="detail-count"></div>
    </div>
    <div class="table-wrap">
      <table id="detail-table">
        <thead><tr>
          <th>Meeting</th><th class="num">R</th>
          <th>Winner</th><th class="num">SP$</th>
          <th class="num">Mdl#</th><th class="num">SP#</th>
          <th>Model top 3</th>
          <th class="num">W.S1</th><th class="num">W.S2</th>
        </tr></thead>
        <tbody id="detail-tbody"></tbody>
      </table>
    </div>

    <!-- Section 5: SP Knew, Model Didn't -->
    <div class="section-header">
      <div class="section-title">SP Knew, Model Didn't</div>
      <div class="section-count" id="sp-knew-count"></div>
    </div>
    <div class="table-wrap">
      <table id="sp-knew-table">
        <thead><tr>
          <th>Meeting</th><th class="num">R</th>
          <th>Winner</th>
          <th class="num">Mdl#</th><th class="num">SP#</th>
          <th class="num">Model$</th><th class="num">SP$</th>
          <th class="num">nr_grade_delta</th>
          <th class="num">sp_sim_grade_prob</th>
        </tr></thead>
        <tbody id="sp-knew-tbody"></tbody>
      </table>
    </div>

    <!-- Section 6: Map Overrides -->
    <div class="section-header">
      <div class="section-title">Map Override Opportunities</div>
      <div class="section-count" id="map-count"></div>
    </div>
    <div class="table-wrap">
      <table id="map-table">
        <thead><tr>
          <th>Meeting</th><th class="num">R</th>
          <th>Best-map horse</th>
          <th class="num">Mdl#</th><th class="num">SP#</th><th class="num">Finish</th>
          <th class="num">Map score</th>
          <th>Winner</th><th class="num">Winner map</th>
        </tr></thead>
        <tbody id="map-tbody"></tbody>
      </table>
    </div>

    <!-- Section 7: Biggest Misses -->
    <div class="section-header">
      <div class="section-title">Biggest Model Misses</div>
      <div class="section-count" id="miss-count"></div>
    </div>
    <div class="table-wrap">
      <table id="miss-table">
        <thead><tr>
          <th>Meeting</th><th class="num">R</th>
          <th>Winner</th>
          <th class="num">Mdl#</th><th class="num">SP#</th>
          <th class="num">Model$</th><th class="num">SP$</th>
          <th class="num">S1</th><th class="num">S2</th>
          <th class="num">nr_grade_delta</th><th class="num">NR ceil</th>
        </tr></thead>
        <tbody id="miss-tbody"></tbody>
      </table>
    </div>

    <div class="footer">Generated {html.escape(generated_at)}</div>
  </div>

<script>
const DATA = {data_json};

function fmt(v, decimals, prefix) {{
  if (v === null || v === undefined) return '<span style="color:#94a3b8">n/a</span>';
  const s = (v >= 0 && prefix ? '+' : '') + v.toFixed(decimals);
  return s;
}}
function fmtOdds(v) {{
  return v ? '$' + v.toFixed(2) : '<span style="color:#94a3b8">n/a</span>';
}}
function pct(n, total) {{
  return total ? (n / total * 100).toFixed(1) + '%' : '—';
}}
function el(id) {{ return document.getElementById(id); }}

function updateSummary(stats) {{
  const n = stats.total_races || 0;
  el('s-total-races').textContent = n;
  el('s-model-correct').textContent = stats.model_correct + '/' + n + ' (' + pct(stats.model_correct, n) + ')';
  el('s-sp-correct').textContent    = stats.sp_correct + '/' + n + ' (' + pct(stats.sp_correct, n) + ')';
  el('s-top3').textContent          = stats.top3_hit + '/' + n + ' (' + pct(stats.top3_hit, n) + ')';
  el('s-rank2').textContent         = stats.rank2 + '/' + n;
  el('s-rank3').textContent         = stats.rank3 + '/' + n;
  el('s-rank4plus').textContent     = stats.rank4plus + '/' + n;

  el('q-both-correct').textContent     = stats.both_correct;
  el('q-both-correct-pct').textContent = pct(stats.both_correct, n);
  el('q-model-edge').textContent       = stats.model_edge;
  el('q-model-edge-pct').textContent   = pct(stats.model_edge, n);
  el('q-sp-failure').textContent       = stats.sp_failure;
  el('q-sp-failure-pct').textContent   = pct(stats.sp_failure, n);
  el('q-both-wrong').textContent       = stats.both_wrong;
  el('q-both-wrong-pct').textContent   = pct(stats.both_wrong, n);

  const f = n => n !== null && n !== undefined ? n.toFixed(3) : 'n/a';
  el('s-s1-compare').innerHTML  = f(stats.avg_top_s1) + ' &nbsp; ' + f(stats.avg_win_s1);
  el('s-s2-compare').innerHTML  = f(stats.avg_top_s2) + ' &nbsp; ' + f(stats.avg_win_s2);
  el('s-tot-compare').innerHTML = f(stats.avg_top_tot) + ' &nbsp; ' + f(stats.avg_win_tot);
  el('s-avg-rank').textContent  = stats.avg_winner_rank !== null ? stats.avg_winner_rank.toFixed(1) : 'n/a';
  el('s-s2-swung').textContent  = stats.s2_swung + ' races';

  el('s-sp-knew').textContent   = stats.sp_knew_count + ' races';
  el('s-map-override').textContent = '—'; // recomputed below after filtering
  el('s-big-miss').textContent  = stats.big_miss_count + ' races';

  const modelPct = n ? (stats.model_correct / n * 100).toFixed(1) : '—';
  const spPct    = n ? (stats.sp_correct    / n * 100).toFixed(1) : '—';
  el('s-model-pct').textContent = modelPct + '%';
  el('s-sp-pct').textContent    = spPct + '%';
  const gap = n ? ((stats.model_correct - stats.sp_correct) / n * 100).toFixed(1) : '—';
  el('s-gap').textContent = (parseFloat(gap) >= 0 ? '+' : '') + gap + '%';
  el('s-gap').className = 'stat-val ' + (parseFloat(gap) >= 0 ? 'green' : 'red');
}}

function buildDetailRows(races) {{
  return races.map(r => {{
    const cls = r.model_correct ? 'model-win' : 'model-loss';
    const badge = r.model_correct
      ? '<span class="badge win">WIN</span>'
      : '<span class="badge loss">LOSS</span>';
    const top3 = [r.model_top1, r.model_top2, r.model_top3].filter(Boolean)
      .map(n => n.length > 12 ? n.slice(0,12) : n).join(', ');
    const sp = r.winner_sp ? '$' + r.winner_sp.toFixed(2) : 'n/a';
    return `<tr class="${{cls}}" data-meeting="${{r.meeting}}">
      <td>${{r.meeting}}</td><td class="num">${{r.race}}</td>
      <td>${{r.winner}} ${{badge}}</td>
      <td class="num">${{sp}}</td>
      <td class="num">${{r.winner_model_rank ?? '?'}}</td>
      <td class="num">${{r.winner_sp_rank ?? '?'}}</td>
      <td style="font-size:11px;color:var(--secondary)">${{top3}}</td>
      <td class="num">${{fmt(r.winner_s1, 2, true)}}</td>
      <td class="num">${{fmt(r.winner_s2, 2, true)}}</td>
    </tr>`;
  }}).join('');
}}

function buildSpKnewRows(races) {{
  const rows = races.filter(r => r.is_sp_knew);
  return rows.map(r => {{
    return `<tr data-meeting="${{r.meeting}}">
      <td>${{r.meeting}}</td><td class="num">${{r.race}}</td>
      <td>${{r.winner}}</td>
      <td class="num" style="color:var(--red);font-weight:700">${{r.winner_model_rank ?? '?'}}</td>
      <td class="num">${{r.winner_sp_rank ?? '?'}}</td>
      <td class="num">${{fmtOdds(r.winner_model_odds)}}</td>
      <td class="num">${{r.winner_sp ? '$' + r.winner_sp.toFixed(2) : 'n/a'}}</td>
      <td class="num">${{fmt(r.winner_nr_grade_delta, 1, true)}}</td>
      <td class="num">${{fmt(r.winner_sp_class_ceiling, 3, true)}}</td>
    </tr>`;
  }}).join('') || '<tr><td colspan="9" class="no-data">No SP-knew failures in this selection.</td></tr>';
}}

function buildMapRows(mapRows) {{
  return mapRows.map(r => {{
    return `<tr data-meeting="${{r.meeting}}">
      <td>${{r.meeting}}</td><td class="num">${{r.race}}</td>
      <td style="font-weight:600">${{r.bm_horse}}</td>
      <td class="num">${{r.bm_model_rank ?? '?'}}</td>
      <td class="num">${{r.bm_sp_rank ?? '?'}}</td>
      <td class="num">${{r.bm_finish}}</td>
      <td class="num">${{fmt(r.bm_map, 2, false)}}</td>
      <td>${{r.winner}}</td>
      <td class="num">${{fmt(r.winner_map, 2, false)}}</td>
    </tr>`;
  }}).join('') || '<tr><td colspan="9" class="no-data">No map override opportunities in this selection.</td></tr>';
}}

function buildMissRows(races) {{
  const rows = races.filter(r => (r.winner_model_rank || 0) >= 5)
    .sort((a, b) => (b.winner_model_rank || 0) - (a.winner_model_rank || 0));
  return rows.map(r => {{
    return `<tr data-meeting="${{r.meeting}}">
      <td>${{r.meeting}}</td><td class="num">${{r.race}}</td>
      <td>${{r.winner}}</td>
      <td class="num" style="color:var(--red);font-weight:700">${{r.winner_model_rank ?? '?'}}</td>
      <td class="num">${{r.winner_sp_rank ?? '?'}}</td>
      <td class="num">${{fmtOdds(r.winner_model_odds)}}</td>
      <td class="num">${{r.winner_sp ? '$' + r.winner_sp.toFixed(2) : 'n/a'}}</td>
      <td class="num">${{fmt(r.winner_s1, 3, true)}}</td>
      <td class="num">${{fmt(r.winner_s2, 3, true)}}</td>
      <td class="num">${{fmt(r.winner_nr_grade_delta, 1, true)}}</td>
      <td class="num">${{r.winner_race_nr_ceiling !== null && r.winner_race_nr_ceiling !== undefined ? r.winner_race_nr_ceiling : '<span style="color:#94a3b8">n/a</span>'}}</td>
    </tr>`;
  }}).join('') || '<tr><td colspan="11" class="no-data">No big misses in this selection.</td></tr>';
}}

function render(track) {{
  const isAll = track === 'all';
  const stats = isAll ? DATA.global : (DATA.by_track[track] || {{}});
  updateSummary(stats);

  const races   = isAll ? DATA.races         : DATA.races.filter(r => r.track === track);
  const mapData = isAll ? DATA.map_overrides : DATA.map_overrides.filter(r => r.track === track);

  el('detail-tbody').innerHTML = buildDetailRows(races);
  el('detail-count').textContent = races.length + ' races';

  const spKnew = races.filter(r => r.is_sp_knew);
  el('sp-knew-tbody').innerHTML = buildSpKnewRows(races);
  el('sp-knew-count').textContent = spKnew.length + ' races';

  el('map-tbody').innerHTML = buildMapRows(mapData);
  el('map-count').textContent = mapData.length + ' races';
  el('s-map-override').textContent = mapData.length + ' races';

  const misses = races.filter(r => (r.winner_model_rank || 0) >= 5);
  el('miss-tbody').innerHTML = buildMissRows(races);
  el('miss-count').textContent = misses.length + ' races';
}}

document.getElementById('track-sel').addEventListener('change', e => render(e.target.value));
render('all');
</script>
</body>
</html>"""
