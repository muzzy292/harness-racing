from __future__ import annotations

import logging
from pathlib import Path
import time
from datetime import date, datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover — zoneinfo is stdlib on 3.9+
    ZoneInfo = None  # type: ignore[assignment]

# NSW racing date used to match meetings.meeting_date; Melbourne shares this
# offset, so the calendar date is identical either way.
_LOCAL_TZ = "Australia/Sydney"
log = logging.getLogger("harness_model.pipeline")

from .features import build_runner_feature_rows, generate_track_pars_from_db, install_sqlite_helpers, write_feature_csv, write_track_pars
from .parsers import (
    parse_driver_page_html,
    parse_excluded_driver_rows,
    parse_horse_profile_html,
    parse_hrnsw_results_index,
    parse_hrnsw_track_options,
    parse_hrnsw_upcoming_meetings,
    parse_meeting_html,
    parse_results_html,
    parse_trainer_links_from_fields_html,
    parse_trainer_page_html,
)
from .scraper import (
    build_fields_url,
    build_driver_url,
    build_horse_url,
    build_hrnsw_results_index_url,
    build_meeting_url,
    build_results_url,
    build_trainer_url,
    driver_name_to_slug,
    fetch_hrnsw_results_search_html,
    fetch_rendered_html,
    is_rate_limited_html,
    is_valid_driver_html,
    is_valid_horse_html,
    is_valid_meeting_html,
    is_valid_results_html,
    is_valid_trainer_html,
    save_html,
    trainer_name_to_slug,
)
from .storage import (
    connect,
    driver_stats_are_fresh,
    cleanup_form_entries_for_horse,
    horse_has_runs,
    init_db,
    sync_runner_recent_lines_to_horse_runs,
    upsert_driver_stats,
    upsert_excluded_driver_runs,
    upsert_horse_profile,
    upsert_meeting,
    upsert_results,
    upsert_runners,
    upsert_trainer_stats,
    trainer_stats_are_fresh,
)
from .track_pars import load_track_pars


def fetch_meeting(meeting_code: str, output_dir: str | Path) -> Path:
    html = fetch_rendered_html(build_meeting_url(meeting_code))
    if not is_valid_meeting_html(html):
        raise RuntimeError(
            f"Meeting page for {meeting_code} did not return a valid form page. "
            "The meeting code may be wrong, unavailable, or temporarily blocked."
        )
    return save_html(html, Path(output_dir) / f"meeting_{meeting_code}.html")


def fetch_results(meeting_code: str, output_dir: str | Path, wait_ms: int = 6000, retries: int = 3) -> Path:
    """Fetch a meeting's results page, validating that results actually rendered.

    The results table loads after the fields table, so a too-early capture sees a
    fields-only page and parses to zero results. Earlier this was saved silently;
    now we validate (is_valid_results_html) and retry with escalating wait, and
    raise a clear error if no results table appears — so the meeting is retried on
    a later run instead of being stranded with zero results.
    """
    html = ""
    for attempt in range(1, retries + 1):
        # Wait for the results table itself to render, rather than guessing a fixed
        # delay — some meetings (e.g. Wagga) render results notably slower.
        html = fetch_rendered_html(
            build_results_url(meeting_code),
            wait_ms=wait_ms,
            wait_selector="table.resultTable",
        )
        if is_valid_results_html(html):
            return save_html(html, Path(output_dir) / f"results_{meeting_code}.html")
    # Diagnostic: describe what our headless fetch actually received, so a
    # persistent failure (e.g. Wagga) can be characterised from the run log
    # without reproducing locally — is it a fields-only page, a challenge/error
    # page, or a genuinely different structure?
    low = html.lower()
    title = ""
    t0 = low.find("<title")
    if t0 != -1:
        ts, te = html.find(">", t0), low.find("</title>", t0)
        if ts != -1 and te != -1:
            title = html[ts + 1:te].strip()[:80]
    print(
        f"    {meeting_code} diagnostic: len={len(html)} title={title!r} "
        f"racefieldtable={'racefieldtable' in low} resulttable={'resulttable' in low} "
        f"rate_limited={is_rate_limited_html(html)} error={'an error has occurred' in low}",
        flush=True,
    )
    # The old site is migrating results to harness.au and now redirects some
    # meetings there. If we can spot the redirect target, probe it (real browser
    # from the runner) so we can see the new site's structure and confirm the
    # runner can even reach it. Diagnostic only — safe to remove once the new
    # source is built.
    import re as _re
    tgt = _re.search(r"https?://[^\s\"'<>]+harness\.au[^\s\"'<>]*", html)
    if tgt:
        target_url = tgt.group(0).replace("&amp;", "&")
        print(f"    {meeting_code} redirect target: {target_url}", flush=True)
        try:
            nhtml = fetch_rendered_html(target_url, wait_ms=8000, wait_until="networkidle", retries=1)
            nlow = nhtml.lower()
            nt0 = nlow.find("<title")
            ntitle = ""
            if nt0 != -1:
                nts, nte = nhtml.find(">", nt0), nlow.find("</title>", nt0)
                if nts != -1 and nte != -1:
                    ntitle = nhtml[nts + 1:nte].strip()[:80]
            print(
                f"    {meeting_code} harness.au probe: len={len(nhtml)} title={ntitle!r} "
                f"finish={'finish' in nlow} position={'position' in nlow} "
                f"price={'price' in nlow or 'odds' in nlow} margin={'margin' in nlow} "
                f"json_blob={'application/json' in nlow or '__next_data__' in nlow}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001 — probe must never break the run
            print(f"    {meeting_code} harness.au probe failed: {exc}", flush=True)
    raise RuntimeError(
        f"Results page for {meeting_code} showed no results table after {retries} attempts "
        "(race may not be official yet, or the page was slow/blocked) — will retry next run."
    )


def fetch_results_history(
    output_dir: str | Path,
    tracks: list[str] | None = None,
    limit: int | None = None,
    force_refresh: bool = False,
) -> list[Path]:
    index_html = fetch_rendered_html(build_hrnsw_results_index_url(), wait_ms=3000)
    requested_tracks = {_normalize_person_key(track) for track in (tracks or [])}
    track_options = parse_hrnsw_track_options(index_html)
    options_in_scope = [
        option
        for option in track_options
        if not requested_tracks or _normalize_person_key(option["label"]) in requested_tracks
    ]
    if options_in_scope:
        entries: list[dict[str, str]] = []
        for option in options_in_scope:
            print(f"Searching HRNSW results for track: {option['label']}", flush=True)
            search_html = fetch_hrnsw_results_search_html(option["value"], wait_ms=3000)
            entries.extend(parse_hrnsw_results_index(search_html))
            time.sleep(0.5)
    else:
        entries = parse_hrnsw_results_index(index_html)

    deduped_entries: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    for entry in entries:
        code = entry["meeting_code"]
        if code in seen_codes:
            continue
        seen_codes.add(code)
        deduped_entries.append(entry)
    entries = deduped_entries

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if requested_tracks:
        entries = [
            entry for entry in entries
            if _normalize_person_key(entry["track_name"]) in requested_tracks
        ]
    if limit is not None:
        entries = entries[:limit]

    saved_paths: list[Path] = []
    failures: list[str] = []
    for entry in entries:
        target = output_path / f"results_{entry['meeting_code']}.html"
        if target.exists() and not force_refresh:
            print(f"Skipping existing results HTML: {target.name}", flush=True)
            saved_paths.append(target)
            continue
        print(
            f"Fetching results history: {entry['track_name']} {entry['meeting_date']} "
            f"({entry['meeting_code']})",
            flush=True,
        )
        try:
            html = fetch_rendered_html(entry["results_url"], wait_ms=3000)
        except Exception as exc:
            print(
                f"  First attempt failed for {entry['meeting_code']}: {exc}",
                flush=True,
            )
            time.sleep(2.0)
            try:
                html = fetch_rendered_html(entry["results_url"], wait_ms=5000)
            except Exception as retry_exc:
                print(
                    f"  Skipping {entry['meeting_code']} after retry failure: {retry_exc}",
                    flush=True,
                )
                failures.append(entry["meeting_code"])
                continue
        saved_paths.append(save_html(html, target))
        time.sleep(1.0)

    print(
        "\nResults history summary:\n"
        f"  Meetings in scope:    {len(entries)}\n"
        f"  Files ready:          {len(saved_paths)}\n"
        f"  Failed meetings:      {len(failures)}",
        flush=True,
    )
    return saved_paths


_HRNSW_UPCOMING_URL = "https://www.hrnsw.com.au/racing/upcomingmeetings"
_HRNSW_RESULTS_URL = "https://www.hrnsw.com.au/racing/results"


def sync_upcoming_meetings(
    db_path: str | Path,
    output_dir: str | Path = "data/raw",
    delay_s: float = 2.0,
) -> tuple[int, int]:
    """Fetch and ingest all upcoming NSW meetings not already in the DB.

    Returns (fetched, skipped).
    """
    print("Fetching HRNSW upcoming meetings index...", flush=True)
    html = fetch_rendered_html(_HRNSW_UPCOMING_URL, wait_ms=4000)
    entries = parse_hrnsw_upcoming_meetings(html)
    if not entries:
        print("No upcoming meetings found on HRNSW page.", flush=True)
        return 0, 0

    conn = connect(db_path)
    init_db(conn)
    existing = {row[0] for row in conn.execute("SELECT meeting_code FROM meetings").fetchall()}
    conn.close()

    fetched = 0
    skipped = 0
    for entry in entries:
        code = entry["meeting_code"]
        label = f"{entry['track_name']} {entry['meeting_date']} ({code})"
        if code in existing:
            print(f"  Skip {label} — already in DB", flush=True)
            skipped += 1
            continue
        print(f"  Fetching {label}...", flush=True)
        try:
            path = fetch_meeting(code, output_dir)
            ingest_meeting_html(db_path, path)
            fetched += 1
            existing.add(code)
        except Exception as exc:
            print(f"  Warning: {code} failed — {exc}", flush=True)
        time.sleep(delay_s)

    print(
        f"\nUpcoming meetings sync summary:\n"
        f"  Meetings found:  {len(entries)}\n"
        f"  Fetched:         {fetched}\n"
        f"  Skipped (known): {skipped}",
        flush=True,
    )
    return fetched, skipped


def _local_today() -> date:
    """Today's date in the Australian racing timezone (matches meeting_date).

    Cron fires at ~6am AEDT, which is still the previous calendar day in UTC —
    so the date must be computed in local time, not UTC, to pick today's
    meetings. Falls back to the system local date if zoneinfo is unavailable
    (e.g. Windows without tzdata), which on an Australian machine is correct.
    """
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(_LOCAL_TZ)).date()
        except Exception:  # noqa: BLE001 — missing tz database
            pass
    return datetime.now().date()


def refresh_today_meetings(
    db_path: str | Path,
    output_dir: str | Path = "data/raw",
    delay_s: float = 2.0,
    today: date | None = None,
) -> tuple[int, int]:
    """Re-fetch and re-ingest the form pages for meetings running today.

    sync-upcoming imports each meeting once (often days early) and skips it
    thereafter, so late scratchings and field changes are never picked up.
    This re-ingests today's meetings so scoring sees the current fields. The
    runner_recent_lines upsert (ON CONFLICT) makes re-ingest idempotent — no
    duplicate rows. Returns (refreshed, failed).
    """
    target = today or _local_today()
    conn = connect(db_path)
    init_db(conn)
    rows = conn.execute("SELECT meeting_code, meeting_date FROM meetings").fetchall()
    conn.close()

    todays = []
    for row in rows:
        code, meeting_date = row[0], row[1]
        try:
            if datetime.strptime(meeting_date, "%d %b %Y").date() == target:
                todays.append(code)
        except (TypeError, ValueError):
            continue

    if not todays:
        print(f"No meetings dated {target:%d %b %Y} to refresh.", flush=True)
        return 0, 0

    refreshed = 0
    failed = 0
    for code in todays:
        print(f"  Refreshing {code} (today's fields)...", flush=True)
        try:
            path = fetch_meeting(code, output_dir)
            ingest_meeting_html(db_path, path)
            refreshed += 1
        except Exception as exc:  # noqa: BLE001 — keep refreshing other meetings
            print(f"  Warning: refresh {code} failed — {exc}", flush=True)
            failed += 1
        time.sleep(delay_s)

    print(f"\nRefresh-today summary: {refreshed} refreshed, {failed} failed.", flush=True)
    return refreshed, failed


def run_daily(
    db_path: str | Path = "data/harness.db",
    csv_path: str | Path = "data/features/runner_features.csv",
    track_pars_path: str | Path = "data/track_pars.json",
    docs_dir: str | Path = "docs",
    weights_path: str | Path = "weights.json",
    delay_s: float = 2.0,
) -> dict:
    """Full unattended daily pipeline for cloud (GitHub Actions) runs.

    sync-results → sync-upcoming → refresh-today → build-features → republish-all.
    Scrape steps are wrapped so a transient failure is logged but still lets the
    site rebuild and publish from whatever is already in the DB. republish-all
    handles its own git commit/push (GitHub Pages). Returns a summary dict.
    """
    from .web import republish_all_meetings
    from .odds import load_weights

    summary: dict = {"errors": []}

    def _step(name: str, fn):
        log.info("START %s", name)
        try:
            result = fn()
            log.info("DONE  %s -> %s", name, result)
            return result
        except Exception as exc:  # noqa: BLE001 — publish even if a step fails
            log.exception("FAILED %s", name)
            summary["errors"].append(f"{name}: {exc}")
            return None

    summary["sync_results"] = _step("sync-results", lambda: sync_recent_results(db_path, delay_s=delay_s))
    summary["sync_upcoming"] = _step("sync-upcoming", lambda: sync_upcoming_meetings(db_path, delay_s=delay_s))
    summary["refresh_today"] = _step("refresh-today", lambda: refresh_today_meetings(db_path, delay_s=delay_s))

    _tp = track_pars_path if Path(track_pars_path).exists() else None
    if track_pars_path and _tp is None:
        log.warning("track-pars file not found (%s) — sectional deltas will be empty.", track_pars_path)
    summary["build_features"] = _step("build-features", lambda: build_feature_dataset(db_path, csv_path, _tp))

    _weights = load_weights(weights_path) if Path(weights_path).exists() else None
    summary["republish"] = _step("republish-all", lambda: republish_all_meetings(csv_path, db_path, docs_dir, weights=_weights))

    if summary["errors"]:
        log.warning("Daily run completed with %d step error(s): %s", len(summary["errors"]), summary["errors"])
    else:
        log.info("Daily run completed cleanly.")
    return summary


def sync_recent_results(
    db_path: str | Path,
    output_dir: str | Path = "data/raw",
    delay_s: float = 2.0,
) -> tuple[int, int]:
    """Fetch and ingest results for recent NSW meetings with incomplete results.

    Re-fetches any meeting on the HRNSW recent-results index whose stored
    results don't yet cover all of its scoreable races (so meetings scraped
    mid-card get completed later). For meetings not yet in the meetings table
    the form page is ingested first. Returns (fetched, skipped).
    """
    print("Fetching HRNSW recent results index...", flush=True)
    html = fetch_rendered_html(_HRNSW_RESULTS_URL, wait_ms=4000)
    entries = parse_hrnsw_results_index(html)
    if not entries:
        print("No results found on HRNSW page.", flush=True)
        return 0, 0

    conn = connect(db_path)
    init_db(conn)
    existing_meetings = {row[0] for row in conn.execute("SELECT meeting_code FROM meetings").fetchall()}
    # A meeting is re-fetched unless EVERY scoreable race already has a result.
    # Skipping on "has any result" stranded meetings scraped mid-card (one race
    # official, the rest not) — the partial result marked them done forever.
    # Comparing the set of runner race_numbers to result race_numbers means a
    # partially-resulted meeting gets completed on a later run. The HRNSW index
    # only lists recent meetings, so re-fetching self-bounds.
    runner_races: dict[str, set] = {}
    for row in conn.execute(
        "SELECT DISTINCT meeting_code, race_number FROM race_runners WHERE COALESCE(scratched, 0) = 0"
    ).fetchall():
        runner_races.setdefault(row[0], set()).add(row[1])
    result_races: dict[str, set] = {}
    for row in conn.execute(
        "SELECT DISTINCT meeting_code, race_number FROM race_results WHERE finish_position IS NOT NULL"
    ).fetchall():
        result_races.setdefault(row[0], set()).add(row[1])
    meeting_dates: dict[str, str] = {
        row[0]: row[1]
        for row in conn.execute("SELECT meeting_code, meeting_date FROM meetings").fetchall()
    }
    conn.close()

    _today = _local_today()

    def _meeting_age_days(code: str) -> int | None:
        try:
            return (_today - datetime.strptime(meeting_dates.get(code, ""), "%d %b %Y").date()).days
        except (TypeError, ValueError):
            return None

    def _results_complete(code: str) -> bool:
        needed = runner_races.get(code, set())
        have = result_races.get(code, set())
        # Unknown meetings (no runners yet) are treated as incomplete so they fetch.
        if needed and needed.issubset(have):
            return True
        # Grace: a meeting that has SOME results and is >2 days old is treated as
        # final — remaining races are abandoned/never-posted (e.g. Penrith R9/R10
        # abandoned), not pending, so stop re-fetching it. A meeting with ZERO
        # results keeps retrying (that's a fetch failure, not abandonment).
        age = _meeting_age_days(code)
        return bool(have) and age is not None and age > 2

    fetched = 0
    skipped = 0
    for entry in entries:
        code = entry["meeting_code"]
        label = f"{entry['track_name']} {entry['meeting_date']} ({code})"
        if _results_complete(code):
            print(f"  Skip {label} — all races have results", flush=True)
            skipped += 1
            continue
        print(f"  Fetching results for {label}...", flush=True)
        try:
            if code not in existing_meetings:
                form_path = fetch_meeting(code, output_dir)
                ingest_meeting_html(db_path, form_path)
                existing_meetings.add(code)
                time.sleep(delay_s)
            results_path = fetch_results(code, output_dir)
            n_results = ingest_results_html(db_path, results_path)
            print(f"    {code}: ingested {n_results} result rows", flush=True)
            fetched += 1
        except Exception as exc:
            print(f"  Warning: {code} failed — {exc}", flush=True)
        time.sleep(delay_s)

    print(
        f"\nRecent results sync summary:\n"
        f"  Meetings found:  {len(entries)}\n"
        f"  Fetched:         {fetched}\n"
        f"  Skipped (known): {skipped}",
        flush=True,
    )
    return fetched, skipped


def fetch_horse_pages_from_meeting_html(
    meeting_html_path: str | Path,
    output_dir: str | Path,
    race_number: int | None = None,
    horse_library_dir: str | Path | None = None,
    db_path: str | Path | None = None,
    force_refresh: bool = False,
) -> list[Path]:
    html_path = Path(meeting_html_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    library_path = Path(horse_library_dir) if horse_library_dir else None
    if library_path:
        library_path.mkdir(parents=True, exist_ok=True)
    html = html_path.read_text(encoding="utf-8", errors="replace")
    meeting_code = _infer_meeting_code(html_path)
    _, runners = parse_meeting_html(html, meeting_code)
    conn = None
    if db_path:
        conn = connect(db_path)
        init_db(conn)
    if race_number is not None:
        runners = [runner for runner in runners if runner.race_number == race_number]

    saved_paths: list[Path] = []
    dedupe_seen: set[str] = set()
    unique_runners = []
    for runner in runners:
        if runner.horse_id in dedupe_seen:
            continue
        dedupe_seen.add(runner.horse_id)
        unique_runners.append(runner)

    scope = f"race {race_number}" if race_number is not None else "meeting"
    print(f"Found {len(unique_runners)} unique horses in {scope} {meeting_code}")

    failures: list[str] = []
    reused_count = 0
    fetched_count = 0
    skipped_existing_count = 0
    skipped_known_count = 0
    for index, runner in enumerate(unique_runners, start=1):
        if conn is not None and not force_refresh and horse_has_runs(conn, runner.horse_id):
            skipped_known_count += 1
            print(
                f"[{index}/{len(unique_runners)}] Skipping known horse with DB history for "
                f"{runner.horse_name} ({runner.horse_id})",
                flush=True,
            )
            continue
        target_path = output_path / f"{_safe_name(runner.horse_name)}_{runner.horse_id}.html"
        library_target = _library_target_path(library_path, runner.horse_id, runner.horse_name) if library_path else None
        library_existing = _find_existing_library_file(library_path, runner.horse_id, preferred=library_target)

        if target_path.exists():
            existing_html = target_path.read_text(encoding="utf-8", errors="replace")
            if is_valid_horse_html(existing_html):
                skipped_existing_count += 1
                print(
                    f"[{index}/{len(unique_runners)}] Reusing local cached profile for "
                    f"{runner.horse_name} ({runner.horse_id})",
                    flush=True,
                )
                if library_target and not library_target.exists():
                    save_html(existing_html, library_target)
                saved_paths.append(target_path)
                continue
            print(f"[{index}/{len(unique_runners)}] Replacing invalid existing file for {runner.horse_name} ({runner.horse_id})", flush=True)
        elif library_existing:
            existing_html = library_existing.read_text(encoding="utf-8", errors="replace")
            if is_valid_horse_html(existing_html):
                reused_count += 1
                print(
                    f"[{index}/{len(unique_runners)}] Reusing library cached profile for "
                    f"{runner.horse_name} ({runner.horse_id})",
                    flush=True,
                )
                save_html(existing_html, target_path)
                if library_target and library_existing != library_target:
                    save_html(existing_html, library_target)
                saved_paths.append(target_path)
                continue

        print(
            f"[{index}/{len(unique_runners)}] Fetching new horse profile for "
            f"{runner.horse_name} ({runner.horse_id})...",
            flush=True,
        )
        horse_html = _fetch_horse_with_retry(runner.horse_id, runner.horse_name)
        if horse_html is None:
            failures.append(f"{runner.horse_name} ({runner.horse_id})")
            print(f"[{index}/{len(unique_runners)}] Failed after retries", flush=True)
            continue
        saved_path = save_html(horse_html, target_path)
        fetched_count += 1
        if library_target:
            save_html(horse_html, library_target)
        saved_paths.append(saved_path)
        print(f"[{index}/{len(unique_runners)}] Saved to {saved_path}", flush=True)
        time.sleep(2.5)

    print(
        "\nHorse fetch summary:\n"
        f"  Total in scope:      {len(unique_runners)}\n"
        f"  Skipped known:       {skipped_known_count}\n"
        f"  Reused from library: {reused_count}\n"
        f"  Skipped existing:    {skipped_existing_count}\n"
        f"  Freshly fetched:     {fetched_count}\n"
        f"  Failed:              {len(failures)}",
        flush=True,
    )
    if conn is not None:
        conn.close()
    if failures:
        print(f"Completed with {len(failures)} failures due to repeated blocking or invalid pages.", flush=True)
        for failed in failures:
            print(f"  - {failed}", flush=True)
    return saved_paths


def ingest_meeting_html(db_path: str | Path, html_path: str | Path) -> tuple[int, int]:
    path = Path(html_path)
    html = path.read_text(encoding="utf-8", errors="replace")
    meeting_code = _infer_meeting_code(path)
    meeting, runners = parse_meeting_html(html, meeting_code)
    conn = connect(db_path)
    init_db(conn)
    upsert_meeting(conn, meeting)
    upsert_runners(conn, runners)
    sync_runner_recent_lines_to_horse_runs(conn, runners)
    conn.close()
    return 1, len(runners)


def ingest_results_html(db_path: str | Path, html_path: str | Path) -> int:
    path = Path(html_path)
    html = path.read_text(encoding="utf-8", errors="replace")
    meeting_code = _infer_meeting_code(path)
    results = parse_results_html(html, meeting_code)
    conn = connect(db_path)
    init_db(conn)
    upsert_results(conn, results)
    # Also capture driver/trainer results from excluded races (trotters, 2YO)
    # so rolling win-rate stats reflect all race types, not just pacing.
    meeting_date = (
        conn.execute("SELECT meeting_date FROM meetings WHERE meeting_code = ?", (meeting_code,))
        .fetchone()
    )
    excluded_rows = parse_excluded_driver_rows(html, meeting_date[0] if meeting_date else None)
    upsert_excluded_driver_runs(conn, excluded_rows)
    conn.close()
    return len(results)


def ingest_results_dir(db_path: str | Path, results_dir: str | Path) -> int:
    count = 0
    for path in sorted(Path(results_dir).glob("results_*.html")):
        count += ingest_results_html(db_path, path)
    return count


def ingest_horse_html(db_path: str | Path, html_path: str | Path) -> str:
    path = Path(html_path)
    html = path.read_text(encoding="utf-8", errors="replace")
    profile = parse_horse_profile_html(html, horse_id=_infer_horse_id(path), source_name=path.name)
    conn = connect(db_path)
    init_db(conn)
    upsert_horse_profile(conn, profile)
    cleanup_form_entries_for_horse(conn, profile.horse_id)
    conn.close()
    return profile.horse_id


def ingest_horse_dir(db_path: str | Path, horse_dir: str | Path) -> int:
    count = 0
    for path in sorted(Path(horse_dir).glob("*.html")):
        ingest_horse_html(db_path, path)
        count += 1
    return count


def fetch_driver_stats_for_meeting(
    db_path: str | Path,
    meeting_code: str,
    force_refresh: bool = False,
    max_age_days: int = 7,
    driver_library_dir: str | Path = "data/driver_library",
) -> int:
    library_dir = Path(driver_library_dir)
    library_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    drivers = conn.execute(
        """
        SELECT DISTINCT driver_name, driver_link
        FROM race_runners
        WHERE meeting_code = ? AND driver_name IS NOT NULL AND COALESCE(scratched, 0) = 0
        """,
        (meeting_code,),
    ).fetchall()
    conn.close()

    count = 0
    skipped_fresh_count = 0
    for row in drivers:
        driver_name = row["driver_name"]
        slug = driver_name_to_slug(driver_name)
        driver_link = row["driver_link"]
        cache_path = library_dir / f"{slug}.html"
        conn = connect(db_path)
        init_db(conn)
        is_fresh = driver_stats_are_fresh(conn, slug, max_age_days=max_age_days)
        conn.close()
        if is_fresh and not force_refresh:
            skipped_fresh_count += 1
            print(f"Skipping driver with fresh DB stats: {driver_name}", flush=True)
            continue
        primary_url = _absolute_driver_url(driver_link)
        fallback_url = build_driver_url(driver_name)
        url = primary_url or fallback_url
        html = None
        if not force_refresh and _cache_is_fresh(cache_path, max_age_days=max_age_days):
            html = cache_path.read_text(encoding="utf-8", errors="replace")
            if is_valid_driver_html(html):
                print(f"Using cached driver HTML: {driver_name}", flush=True)
            else:
                html = None
        if force_refresh:
            print(f"Force refreshing driver stats: {driver_name} ({url})", flush=True)
        elif html is None:
            print(f"Fetching driver stats: {driver_name} ({url})", flush=True)
        if html is None:
            try:
                html = fetch_rendered_html(url, wait_ms=3000)
            except Exception as exc:
                if primary_url and primary_url != fallback_url:
                    print(f"  Primary driver link failed for {driver_name}: {exc}", flush=True)
                    print(f"  Retrying with fallback slug URL: {fallback_url}", flush=True)
                    try:
                        html = fetch_rendered_html(fallback_url, wait_ms=3000)
                    except Exception as fallback_exc:
                        print(f"  Failed to fetch {driver_name}: {fallback_exc}", flush=True)
                        continue
                else:
                    print(f"  Failed to fetch {driver_name}: {exc}", flush=True)
                    continue
        if not is_valid_driver_html(html):
            if primary_url and primary_url != fallback_url and url != fallback_url:
                print(f"  Primary driver page invalid for {driver_name}, retrying fallback slug URL", flush=True)
                try:
                    html = fetch_rendered_html(fallback_url, wait_ms=3000)
                except Exception as fallback_exc:
                    print(f"  Failed to fetch {driver_name}: {fallback_exc}", flush=True)
                    continue
                if not is_valid_driver_html(html):
                    print(f"  No valid stats page for {driver_name}", flush=True)
                    continue
            else:
                print(f"  No valid stats page for {driver_name}", flush=True)
                continue
        save_html(html, cache_path)
        stats = parse_driver_page_html(html, driver_name)
        if stats is None:
            print(f"  Could not parse stats for {driver_name}", flush=True)
            continue
        conn = connect(db_path)
        upsert_driver_stats(conn, slug, stats)
        conn.close()
        print(
            f"  Stored: season {stats.get('season_wins')}/{stats.get('season_starts')} "
            f"({int((stats.get('season_win_rate') or 0) * 100)}%)",
            flush=True,
        )
        count += 1
        time.sleep(2.0)

    print(
        "\nDriver stats summary:\n"
        f"  Drivers in scope:     {len(drivers)}\n"
        f"  Skipped fresh:        {skipped_fresh_count}\n"
        f"  Freshly fetched:      {count}",
        flush=True,
    )
    return count


def fetch_trainer_stats_for_meeting(
    db_path: str | Path,
    meeting_code: str,
    force_refresh: bool = False,
    max_age_days: int = 7,
    trainer_library_dir: str | Path = "data/trainer_library",
) -> int:
    library_dir = Path(trainer_library_dir)
    library_dir.mkdir(parents=True, exist_ok=True)
    fields_links = _fetch_trainer_links_for_meeting(meeting_code)
    conn = connect(db_path)
    init_db(conn)
    trainers = conn.execute(
        """
        SELECT DISTINCT trainer_name, trainer_link
        FROM race_runners
        WHERE meeting_code = ? AND trainer_name IS NOT NULL AND COALESCE(scratched, 0) = 0
        """,
        (meeting_code,),
    ).fetchall()
    conn.close()

    count = 0
    skipped_fresh_count = 0
    for row in trainers:
        trainer_name = row["trainer_name"]
        slug = trainer_name_to_slug(trainer_name)
        trainer_link = row["trainer_link"] or fields_links.get(_normalize_person_key(trainer_name))
        cache_path = library_dir / f"{slug}.html"
        conn = connect(db_path)
        init_db(conn)
        is_fresh = trainer_stats_are_fresh(conn, slug, max_age_days=max_age_days)
        conn.close()
        if is_fresh and not force_refresh:
            skipped_fresh_count += 1
            print(f"Skipping trainer with fresh DB stats: {trainer_name}", flush=True)
            continue

        primary_url = _absolute_trainer_url(trainer_link)
        fallback_url = build_trainer_url(trainer_name)
        url = primary_url or fallback_url
        html = None
        if not force_refresh and _cache_is_fresh(cache_path, max_age_days=max_age_days):
            html = cache_path.read_text(encoding="utf-8", errors="replace")
            if is_valid_trainer_html(html):
                print(f"Using cached trainer HTML: {trainer_name}", flush=True)
            else:
                html = None
        if force_refresh:
            print(f"Force refreshing trainer stats: {trainer_name} ({url})", flush=True)
        elif html is None:
            print(f"Fetching trainer stats: {trainer_name} ({url})", flush=True)
        if html is None:
            try:
                html = fetch_rendered_html(url, wait_ms=3000)
            except Exception as exc:
                if primary_url and primary_url != fallback_url:
                    print(f"  Primary trainer link failed for {trainer_name}: {exc}", flush=True)
                    print(f"  Retrying with fallback slug URL: {fallback_url}", flush=True)
                    try:
                        html = fetch_rendered_html(fallback_url, wait_ms=3000)
                    except Exception as fallback_exc:
                        print(f"  Failed to fetch {trainer_name}: {fallback_exc}", flush=True)
                        continue
                else:
                    print(f"  Failed to fetch {trainer_name}: {exc}", flush=True)
                    continue

        if not is_valid_trainer_html(html):
            if primary_url and primary_url != fallback_url and url != fallback_url:
                print(f"  Primary trainer page invalid for {trainer_name}, retrying fallback slug URL", flush=True)
                try:
                    html = fetch_rendered_html(fallback_url, wait_ms=3000)
                except Exception as fallback_exc:
                    print(f"  Failed to fetch {trainer_name}: {fallback_exc}", flush=True)
                    continue
                if not is_valid_trainer_html(html):
                    print(f"  No valid stats page for {trainer_name}", flush=True)
                    continue
            else:
                print(f"  No valid stats page for {trainer_name}", flush=True)
                continue

        save_html(html, cache_path)
        stats = parse_trainer_page_html(html, trainer_name)
        if stats is None:
            print(f"  Could not parse stats for {trainer_name}", flush=True)
            continue
        conn = connect(db_path)
        init_db(conn)
        upsert_trainer_stats(conn, slug, stats)
        conn.close()
        print(
            f"  Stored: season {stats.get('season_wins')}/{stats.get('season_starts')} "
            f"({int((stats.get('season_win_rate') or 0) * 100)}%)",
            flush=True,
        )
        count += 1
        time.sleep(2.0)

    print(
        "\nTrainer stats summary:\n"
        f"  Trainers in scope:    {len(trainers)}\n"
        f"  Skipped fresh:        {skipped_fresh_count}\n"
        f"  Freshly fetched:      {count}",
        flush=True,
    )
    return count


def calibrate_temperature(
    feature_csv: str | Path,
    db_path: str | Path,
    meeting_codes: list[str] | None = None,
    temperatures: list[float] | None = None,
) -> list[dict[str, object]]:
    """Sweep softmax temperatures and report log loss against stored race results.

    Queries race_results for finish_position = 1, scores each race at every
    candidate temperature, and returns rows sorted by log_loss ascending.

    Requires at least ~20 races of results to be meaningful.
    """
    from .odds import load_feature_rows, sweep_temperature

    rows = load_feature_rows(feature_csv)
    conn = connect(db_path)
    init_db(conn)

    if meeting_codes:
        placeholders = ",".join("?" * len(meeting_codes))
        result_rows = conn.execute(
            f"SELECT meeting_code, race_number, horse_name FROM race_results "
            f"WHERE finish_position = 1 AND meeting_code IN ({placeholders})",
            meeting_codes,
        ).fetchall()
    else:
        result_rows = conn.execute(
            "SELECT meeting_code, race_number, horse_name FROM race_results "
            "WHERE finish_position = 1"
        ).fetchall()
    conn.close()

    winners = {
        (row["meeting_code"], int(row["race_number"])): row["horse_name"]
        for row in result_rows
    }
    if not winners:
        print("No race results found in DB — run ingest-results first.", flush=True)
        return []

    print(f"Calibrating temperature across {len(winners)} races...", flush=True)
    return sweep_temperature(rows, winners, temperatures=temperatures)


def build_track_par_database(db_path: str | Path, output_path: str | Path) -> Path:
    """Generate track_pars.json from last_half sectionals stored in runner_recent_lines.

    Uses median per track/distance/condition (min 10 samples).  The output
    file is in the same format as track_pars.json expected by build-features.
    """
    conn = connect(db_path)
    init_db(conn)
    pars = generate_track_pars_from_db(conn)
    conn.close()
    out = write_track_pars(pars, output_path)
    print(
        f"  Track pars written to {out}\n"
        f"  Tracks: {len(pars['pars'])}  "
        f"Cells: {pars['total_cells']}  "
        f"Runs used: {pars['total_runs']}",
        flush=True,
    )
    return out


def calibrate_nr_factor(db_path: str | Path) -> dict:
    """Run within-horse NR margin factor calibration and return the result dict."""
    from .features import calibrate_nr_margin_factor
    conn = connect(db_path)
    init_db(conn)
    result = calibrate_nr_margin_factor(conn)
    conn.close()
    return result


def build_feature_dataset(db_path: str | Path, csv_path: str | Path, track_pars_path: str | Path | None = None) -> Path:
    conn = connect(db_path)
    init_db(conn)
    install_sqlite_helpers(conn)
    track_pars = load_track_pars(track_pars_path) if track_pars_path else None
    rows = build_runner_feature_rows(conn, track_pars=track_pars)
    conn.close()
    return write_feature_csv(rows, csv_path)


def snapshot_meeting(
    meeting_code: str,
    snapshots_root: str | Path,
    race_number: int | None = None,
    horse_library_dir: str | Path | None = None,
) -> dict[str, Path | list[Path] | int]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir = Path(snapshots_root) / meeting_code / (f"R{race_number}" if race_number is not None else "meeting") / timestamp / "pre_race"
    raw_dir = base_dir / "raw"
    horses_dir = base_dir / "horses"
    raw_dir.mkdir(parents=True, exist_ok=True)
    horses_dir.mkdir(parents=True, exist_ok=True)

    meeting_path = fetch_meeting(meeting_code, raw_dir)
    horse_paths = fetch_horse_pages_from_meeting_html(
        meeting_path,
        horses_dir,
        race_number=race_number,
        horse_library_dir=horse_library_dir,
    )
    return {
        "snapshot_dir": base_dir,
        "meeting_path": meeting_path,
        "horse_paths": horse_paths,
        "horse_count": len(horse_paths),
    }


def _infer_meeting_code(path: Path) -> str:
    for part in path.stem.split("_"):
        if len(part) >= 6 and part[:2].isalpha() and any(char.isdigit() for char in part):
            return part.upper()
    return path.stem.upper()


def _infer_horse_id(path: Path) -> str | None:
    parts = path.stem.split("_")
    return parts[-1] if parts and parts[-1].isdigit() else None


def _safe_name(name: str) -> str:
    safe = "".join(char if char.isalnum() else "_" for char in name.upper())
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")


def _library_target_path(library_path: Path | None, horse_id: str, horse_name: str) -> Path | None:
    if library_path is None:
        return None
    return library_path / f"{horse_id}_{_safe_name(horse_name)}.html"


def _find_existing_library_file(library_path: Path | None, horse_id: str, preferred: Path | None = None) -> Path | None:
    if library_path is None:
        return None
    if preferred and preferred.exists():
        return preferred

    patterns = [
        f"{horse_id}.html",
        f"{horse_id}_*.html",
        f"*_{horse_id}.html",
        f"*{horse_id}*.html",
    ]
    seen: set[Path] = set()
    for pattern in patterns:
        for path in library_path.glob(pattern):
            if path in seen or not path.is_file():
                continue
            seen.add(path)
            return path
    return None


def _fetch_horse_with_retry(horse_id: str, horse_name: str, max_attempts: int = 4) -> str | None:
    for attempt in range(1, max_attempts + 1):
        try:
            html = fetch_rendered_html(build_horse_url(horse_id))
        except Exception as exc:
            wait_seconds = min(15 * attempt, 60)
            print(
                f"    Fetch error for {horse_name} on attempt {attempt}/{max_attempts}: {exc}. "
                f"Waiting {wait_seconds}s before retrying...",
                flush=True,
            )
            time.sleep(wait_seconds)
            continue

        if is_valid_horse_html(html):
            return html

        if is_rate_limited_html(html):
            wait_seconds = min(20 * attempt, 90)
            print(
                f"    Rate limit hit for {horse_name} on attempt {attempt}/{max_attempts}. "
                f"Waiting {wait_seconds}s before retrying...",
                flush=True,
            )
            time.sleep(wait_seconds)
            continue

        print(
            f"    Invalid horse page returned for {horse_name} on attempt {attempt}/{max_attempts}.",
            flush=True,
        )
        time.sleep(5)

    return None


def _absolute_driver_url(driver_link: str | None) -> str | None:
    if not driver_link:
        return None
    text = str(driver_link).strip()
    if not text:
        return None
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if text.startswith("/"):
        return f"https://www.harness.org.au{text}"
    return f"https://www.harness.org.au/{text.lstrip('/')}"


def _absolute_trainer_url(trainer_link: str | None) -> str | None:
    if not trainer_link:
        return None
    text = str(trainer_link).strip()
    if not text:
        return None
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if text.startswith("/"):
        return f"https://www.harness.org.au{text}"
    return f"https://www.harness.org.au/{text.lstrip('/')}"


def _fetch_trainer_links_for_meeting(meeting_code: str) -> dict[str, str]:
    try:
        html = fetch_rendered_html(build_fields_url(meeting_code), wait_ms=3000)
    except Exception as exc:
        print(f"Could not fetch fields page for trainer links: {exc}", flush=True)
        return {}
    return parse_trainer_links_from_fields_html(html)


def _normalize_person_key(name: str) -> str:
    return " ".join(str(name).upper().split())


def _cache_is_fresh(path: Path, max_age_days: int) -> bool:
    if not path.exists():
        return False
    modified = datetime.fromtimestamp(path.stat().st_mtime)
    age_days = (datetime.now() - modified).days
    return age_days <= max_age_days
