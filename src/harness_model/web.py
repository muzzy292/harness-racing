from __future__ import annotations

import html
import os
import sqlite3
from datetime import datetime
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

    page_path = out_path / f"{meeting_code}.html"
    page_path.write_text(
        _render_meeting_html(meeting_code, meeting_scores, meeting_meta, result_rows),
        encoding="utf-8",
    )
    _write_index(out_path)
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
    return dict(row)


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


def _write_index(out_dir: Path) -> None:
    meeting_pages = sorted(path for path in out_dir.glob("*.html") if path.name.lower() != "index.html")
    cards: list[str] = []
    for page in meeting_pages:
        code = page.stem.upper()
        cards.append(
            f"""
            <a class="meeting-card" href="{html.escape(page.name)}">
              <span class="meeting-code">{html.escape(code)}</span>
              <span class="meeting-link">Open meeting page</span>
            </a>
            """
        )

    index_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Harness Odds Viewer</title>
  <style>
    :root {{
      --bg: #f5efe4;
      --panel: #fffaf2;
      --ink: #1f2a37;
      --muted: #637082;
      --line: #d8ccb7;
      --accent: #0f6a73;
      --accent-soft: #d7ecee;
      --gold: #c4872f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: radial-gradient(circle at top left, #fff7ea, var(--bg));
      color: var(--ink);
    }}
    .wrap {{ max-width: 960px; margin: 0 auto; padding: 40px 20px 60px; }}
    h1 {{ margin: 0 0 8px; font-size: 44px; }}
    p {{ color: var(--muted); font-size: 18px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-top: 28px;
    }}
    .meeting-card {{
      display: flex;
      flex-direction: column;
      gap: 10px;
      padding: 20px;
      text-decoration: none;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 12px 30px rgba(97, 76, 43, 0.08);
      transition: transform 0.18s ease, box-shadow 0.18s ease;
    }}
    .meeting-card:hover {{
      transform: translateY(-3px);
      box-shadow: 0 16px 36px rgba(97, 76, 43, 0.14);
    }}
    .meeting-code {{ font-size: 24px; font-weight: 700; letter-spacing: 0.04em; }}
    .meeting-link {{ color: var(--accent); font-size: 15px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Harness Odds Viewer</h1>
    <p>Static meeting pages built from your local score-meeting data. Open a meeting below, or host this folder anywhere static files can be served.</p>
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
    meeting_title = " ".join(part for part in [meeting_meta.get("track_name"), meeting_meta.get("meeting_date")] if part) or meeting_code
    generated = datetime.now().strftime("%d %b %Y %H:%M")
    race_nav = "".join(
        f'<a href="#race-{race_number}">Race {race_number}</a>'
        for race_number, rows in meeting_scores.items() if rows
    )

    summary_cards = [
        ("Meeting", meeting_code),
        ("Track", meeting_meta.get("track_name") or "Unknown"),
        ("Date", meeting_meta.get("meeting_date") or "Unknown"),
        ("Races", str(sum(1 for rows in meeting_scores.values() if rows))),
        ("Results Loaded", str(len(result_rows))),
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
  <title>{html.escape(meeting_code)} - Harness Odds Viewer</title>
  <style>
    :root {{
      --bg: #f3ecdf;
      --panel: #fffaf1;
      --panel-strong: #fffdf8;
      --ink: #1d2530;
      --muted: #677385;
      --line: #d9ccb8;
      --accent: #0d6770;
      --accent-soft: #d7ecee;
      --gold: #be832c;
      --winner: #e7f6e8;
      --top: #eef8fb;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff7ea 0%, var(--bg) 48%),
        linear-gradient(180deg, #efe6d7 0%, var(--bg) 100%);
    }}
    .wrap {{ max-width: 1240px; margin: 0 auto; padding: 28px 18px 80px; }}
    .hero {{
      background: linear-gradient(135deg, rgba(255,255,255,0.82), rgba(255,248,236,0.96));
      border: 1px solid var(--line);
      border-radius: 28px;
      padding: 28px;
      box-shadow: 0 16px 42px rgba(97, 76, 43, 0.10);
    }}
    .eyebrow {{
      display: inline-block;
      padding: 8px 12px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 13px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    h1 {{ margin: 14px 0 8px; font-size: 48px; line-height: 1; }}
    .sub {{ color: var(--muted); font-size: 18px; margin: 0; }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 12px;
      margin-top: 22px;
    }}
    .summary-card {{
      background: var(--panel-strong);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    .summary-card .label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; }}
    .summary-card .value {{ font-size: 20px; font-weight: 700; }}
    .race-nav {{
      position: sticky;
      top: 0;
      z-index: 5;
      margin: 18px 0 22px;
      padding: 12px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      background: rgba(255,250,241,0.92);
      backdrop-filter: blur(8px);
      border: 1px solid var(--line);
      border-radius: 18px;
    }}
    .race-nav a {{
      text-decoration: none;
      color: var(--accent);
      background: white;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 14px;
      font-size: 14px;
    }}
    .race-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 18px;
      box-shadow: 0 14px 34px rgba(97, 76, 43, 0.08);
      margin-bottom: 18px;
    }}
    .race-head {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 16px;
      margin-bottom: 14px;
      flex-wrap: wrap;
    }}
    .race-head h2 {{ margin: 0; font-size: 28px; }}
    .race-head .top-pick {{ color: var(--muted); font-size: 16px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 16px;
    }}
    th, td {{
      padding: 12px 10px;
      border-bottom: 1px solid #e8ddcb;
      text-align: left;
      vertical-align: middle;
      font-size: 15px;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      background: #f9f2e7;
    }}
    tr.top-pick-row {{ background: var(--top); }}
    tr.result-winner {{ background: var(--winner); }}
    .horse-cell {{
      display: flex;
      flex-direction: column;
      gap: 4px;
    }}
    .horse-cell .meta {{ color: var(--muted); font-size: 12px; }}
    .pill {{
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      background: #f4ead5;
      color: #6f4f18;
    }}
    .pill.win {{ background: var(--winner); color: #2f6a30; }}
    .pill.pick {{ background: var(--accent-soft); color: var(--accent); }}
    .muted {{ color: var(--muted); }}
    @media (max-width: 820px) {{
      .wrap {{ padding: 18px 12px 60px; }}
      h1 {{ font-size: 36px; }}
      .race-card {{ padding: 14px; }}
      table, thead, tbody, th, td, tr {{ display: block; }}
      thead {{ display: none; }}
      tr {{
        padding: 10px 0;
        border-bottom: 1px solid #e8ddcb;
      }}
      td {{
        border: 0;
        padding: 6px 0;
      }}
      td::before {{
        content: attr(data-label);
        display: block;
        font-size: 11px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: var(--muted);
        margin-bottom: 2px;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <span class="eyebrow">Meeting Viewer</span>
      <h1>{html.escape(meeting_title)}</h1>
      <p class="sub">Light shareable page for score-meeting output and any official results already stored locally.</p>
      <div class="summary-grid">{summary_html}</div>
    </section>
    <nav class="race-nav">
      <a href="index.html">All meetings</a>
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
        body_rows.append(
            f"""
            <tr{class_attr}>
              <td data-label="No">{_fmt_runner_number(row.get('runner_number'))}</td>
              <td data-label="Horse">
                <div class="horse-cell">
                  <strong>{html.escape(horse_name)}</strong>
                  <span class="meta">{html.escape(str(row.get('nominated_trainer') or ''))} / {html.escape(str(row.get('nominated_driver') or ''))}</span>
                  <span>{''.join(badges)}</span>
                </div>
              </td>
              <td data-label="Barrier">{html.escape(str(row.get('barrier') or ''))}</td>
              <td data-label="Prob">{_fmt_prob(row.get('win_probability'))}</td>
              <td data-label="Fair Odds">{_fmt_decimal(row.get('fair_odds'))}</td>
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
