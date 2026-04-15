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

from .odds import load_feature_rows, load_market_rows, score_meeting_rows
from .storage import connect, init_db


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
        cards.append(
            f"""
            <a class="meeting-card" href="{html.escape(code)}.html">
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
  <style>
    :root {{
      --bg: #f8fafc;
      --card-bg: #ffffff;
      --primary: #0f172a;
      --secondary: #64748b;
      --accent: #10b981;
      --accent-dark: #059669;
      --border: #e2e8f0;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background-color: var(--bg);
      color: var(--primary);
      line-height: 1.5;
    }}
    .top-nav {{
      background: var(--primary);
      padding: 12px 20px;
      display: flex;
      gap: 8px;
      align-items: center;
    }}
    .top-nav a {{
      color: var(--secondary);
      text-decoration: none;
      font-size: 14px;
      font-weight: 600;
      padding: 6px 14px;
      border-radius: 8px;
      transition: all 0.2s;
    }}
    .top-nav a:hover {{ background: rgba(255,255,255,0.1); color: white; }}
    .top-nav a.active {{ background: rgba(255,255,255,0.12); color: white; }}
    .hero {{
      background: var(--primary);
      color: white;
      padding: 32px 20px 40px;
      text-align: center;
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 40px;
      font-weight: 800;
      letter-spacing: -0.02em;
    }}
    .hero p {{
      margin: 0;
      color: var(--secondary);
      font-size: 16px;
    }}
    .wrap {{ max-width: 960px; margin: 0 auto; padding: 36px 20px 60px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
    }}
    .meeting-card {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      padding: 24px;
      text-decoration: none;
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 16px;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
      transition: transform 0.15s ease, box-shadow 0.15s ease;
    }}
    .meeting-card:hover {{
      transform: translateY(-2px);
      box-shadow: 0 10px 20px rgba(0, 0, 0, 0.08);
    }}
    .meeting-code {{ font-size: 20px; font-weight: 700; color: var(--primary); letter-spacing: 0.02em; }}
    .meeting-meta {{ color: var(--secondary); font-size: 13px; }}
    .meeting-stats {{ color: var(--primary); font-size: 13px; font-weight: 600; }}
    .meeting-link {{ margin-top: 6px; color: var(--accent-dark); font-size: 14px; font-weight: 600; }}
  </style>
</head>
<body>
  <nav class="top-nav">
    <a href="index.html" class="active">Meetings</a>
    <a href="stats.html">Stats</a>
  </nav>
  <div class="hero">
    <h1>Harness Racing Scores</h1>
    <p>Fair odds and probabilities — select a meeting below</p>
  </div>
  <div class="wrap">
    <div class="grid">
      {''.join(cards) if cards else '<p>No meeting pages built yet.</p>'}
    </div>
  </div>
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
  <style>
    :root {{
      /* Modern Deep & Crisp Palette */
      --bg: #f8fafc;
      --card-bg: #ffffff;
      --primary: #0f172a;    /* Slate 900 */
      --secondary: #64748b;  /* Slate 500 */
      --accent: #10b981;     /* Emerald 500 */
      --accent-dark: #059669;
      --accent-soft: #ecfdf5;
      --border: #e2e8f0;     /* Slate 200 */
      --pick-bg: #f0f9ff;    /* Sky 50 */
      --pick-text: #0369a1;  /* Sky 700 */
      --winner-bg: #f0fdf4;
      --highlight: #f1f5f9;
    }}

    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}

    body {{
      margin: 0;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      color: var(--primary);
      background-color: var(--bg);
      line-height: 1.5;
    }}

    .wrap {{ 
      max-width: 1200px; 
      margin: 0 auto; 
      padding: 40px 20px; 
    }}

    /* Hero Section */
    .hero {{
      background: var(--primary);
      color: white;
      border-radius: 24px;
      padding: 40px;
      margin-bottom: 32px;
      box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1);
      position: relative;
      overflow: hidden;
    }}

    .hero::after {{
      content: "";
      position: absolute;
      top: 0; right: 0;
      width: 300px; height: 300px;
      background: radial-gradient(circle, rgba(16, 185, 129, 0.15) 0%, transparent 70%);
      pointer-events: none;
    }}

    .eyebrow {{
      display: inline-block;
      padding: 4px 12px;
      border-radius: 6px;
      background: rgba(255,255,255,0.1);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      margin-bottom: 16px;
    }}

    h1 {{ margin: 0; font-size: 42px; font-weight: 800; letter-spacing: -0.02em; }}
    .sub {{ color: var(--secondary); font-size: 16px; margin-top: 8px; font-weight: 400; }}

    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 16px;
      margin-top: 32px;
    }}

    .summary-card {{
      background: rgba(255,255,255,0.05);
      border: 1px solid rgba(255,255,255,0.1);
      border-radius: 12px;
      padding: 16px;
    }}

    .summary-card .label {{ 
      display: block;
      font-size: 11px; 
      color: var(--secondary); 
      text-transform: uppercase; 
      letter-spacing: 0.05em;
      margin-bottom: 4px;
    }}
    .summary-card .value {{ font-size: 18px; font-weight: 600; color: white; }}

    /* Navigation */
    .race-nav {{
      position: sticky;
      top: 16px;
      z-index: 50;
      margin-bottom: 32px;
      padding: 8px;
      display: flex;
      gap: 8px;
      overflow-x: auto;
      background: rgba(255, 255, 255, 0.8);
      backdrop-filter: blur(12px);
      border: 1px solid var(--border);
      border-radius: 16px;
      scrollbar-width: none;
    }}

    .race-nav::-webkit-scrollbar {{ display: none; }}

    .race-nav a {{
      text-decoration: none;
      white-space: nowrap;
      color: var(--secondary);
      background: transparent;
      border-radius: 10px;
      padding: 8px 16px;
      font-size: 14px;
      font-weight: 600;
      transition: all 0.2s;
    }}

    .race-nav a:hover {{
      background: var(--highlight);
      color: var(--primary);
    }}

    /* Race Cards */
    .race-card {{
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 0;
      margin-bottom: 20px;
      overflow: hidden;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }}

    .race-head {{
      padding: 14px 24px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      background: #fafafa;
    }}

    .race-head h2 {{ margin: 0; font-size: 24px; font-weight: 700; color: var(--primary); }}
    .race-head .top-pick {{ 
      font-size: 14px; 
      background: var(--accent-soft); 
      color: var(--accent-dark);
      padding: 6px 14px;
      border-radius: 99px;
      font-weight: 600;
    }}

    /* Tables */
    table {{
      width: 100%;
      border-collapse: collapse;
    }}

    th, td {{
      padding: 9px 12px;
      text-align: left;
      font-size: 14px;
      border-bottom: 1px solid var(--border);
    }}

    th {{
      background: white;
      color: var(--secondary);
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}

    tr:last-child td {{ border-bottom: none; }}
    tr:hover {{ background-color: var(--highlight); }}

    /* Special Rows */
    tr.top-pick-row {{ background: var(--pick-bg); }}
    tr.top-pick-row:hover {{ background: #e0f2fe; }}
    tr.result-winner {{ background: var(--winner-bg); }}

    .horse-cell {{
      display: flex;
      flex-direction: column;
      gap: 2px;
    }}

    .horse-cell strong {{ font-size: 15px; color: var(--primary); }}
    .horse-cell .meta {{ color: var(--secondary); font-size: 12px; }}

    .pill {{
      display: inline-block;
      margin-top: 4px;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
    }}
    .pill.pick {{ background: var(--pick-text); color: white; }}
    .pill.win {{ background: var(--accent); color: white; }}
    .pill.fs {{ background: #7c3aed; color: white; }}
    .pill.ns {{ background: #ea580c; color: white; }}

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
      {race_nav}
    </nav>
    {''.join(sections) if sections else '<div class="race-card"><p>No races found for this meeting.</p></div>'}
  </div>
</body>
</html>
"""


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
    for row in ordered:
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
        body_rows.append(
            f"""
            <tr{class_attr}>
              <td data-label="No">{_fmt_runner_number(row.get('runner_number'))}</td>
              <td data-label="Horse">
                <div class="horse-cell">
                  <strong>{html.escape(horse_name)}</strong>
                  <span class="meta">{html.escape(str(row.get('nominated_trainer') or ''))} / {'<span class="driver-change">' + html.escape(str(row.get('nominated_driver') or '')) + '</span>' if str(row.get('driver_change_flag')) == '1' else html.escape(str(row.get('nominated_driver') or ''))}</span>
                  <span>{''.join(badges)}</span>
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
            """
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
  <style>
    :root {{
      --bg: #f8fafc;
      --card-bg: #ffffff;
      --primary: #0f172a;
      --secondary: #64748b;
      --accent: #10b981;
      --accent-dark: #059669;
      --accent-soft: #ecfdf5;
      --border: #e2e8f0;
      --highlight: #f1f5f9;
      --hot-bg: rgba(245, 158, 11, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      color: var(--primary);
      background-color: var(--bg);
      line-height: 1.5;
    }}
    .top-nav {{
      background: var(--primary);
      padding: 12px 20px;
      display: flex;
      gap: 8px;
      align-items: center;
    }}
    .top-nav a {{
      color: var(--secondary);
      text-decoration: none;
      font-size: 14px;
      font-weight: 600;
      padding: 6px 14px;
      border-radius: 8px;
      transition: all 0.2s;
    }}
    .top-nav a:hover {{ background: rgba(255,255,255,0.1); color: white; }}
    .top-nav a.active {{ background: rgba(255,255,255,0.12); color: white; }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 36px 20px 60px; }}
    .hero {{
      background: var(--primary);
      color: white;
      padding: 40px 20px 32px;
      text-align: center;
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: 36px; font-weight: 800; letter-spacing: -0.02em; }}
    .hero p {{ margin: 0; color: var(--secondary); font-size: 15px; }}
    .section-title {{
      font-size: 22px;
      font-weight: 700;
      color: var(--primary);
      margin: 40px 0 16px;
      padding-bottom: 8px;
      border-bottom: 2px solid var(--accent);
      display: inline-block;
    }}
    .table-wrap {{
      overflow-x: auto;
      border: 1px solid var(--border);
      border-radius: 16px;
      background: var(--card-bg);
      box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
    }}
    table.stats-table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 700px;
    }}
    th, td {{
      padding: 10px 14px;
      text-align: left;
      font-size: 14px;
      border-bottom: 1px solid var(--border);
    }}
    th {{
      background: #fafafa;
      color: var(--secondary);
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    th:hover {{ color: var(--primary); background: var(--highlight); }}
    th.sorted-asc::after {{ content: " ▼"; color: var(--accent-dark); }}
    th.sorted-desc::after {{ content: " ▲"; color: var(--accent-dark); }}
    td.num {{ text-align: right; }}
    th.num {{ text-align: right; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover {{ background: var(--highlight); }}
    tr.hot-row {{ background: var(--hot-bg); }}
    tr.hot-row:hover {{ background: rgba(245, 158, 11, 0.14); }}
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

    <div class="footer">
      Generated {generated} &nbsp;·&nbsp; {total:,} results ingested
    </div>
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
