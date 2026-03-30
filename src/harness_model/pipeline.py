from __future__ import annotations

from pathlib import Path
import time
from datetime import datetime

from .features import build_runner_feature_rows, install_sqlite_helpers, write_feature_csv
from .parsers import parse_horse_profile_html, parse_meeting_html, parse_results_html
from .scraper import (
    build_horse_url,
    build_meeting_url,
    build_results_url,
    fetch_rendered_html,
    is_rate_limited_html,
    is_valid_horse_html,
    is_valid_meeting_html,
    save_html,
)
from .storage import connect, init_db, upsert_horse_profile, upsert_meeting, upsert_results, upsert_runners
from .track_pars import load_track_pars


def fetch_meeting(meeting_code: str, output_dir: str | Path) -> Path:
    html = fetch_rendered_html(build_meeting_url(meeting_code))
    if not is_valid_meeting_html(html):
        raise RuntimeError(
            f"Meeting page for {meeting_code} did not return a valid form page. "
            "The meeting code may be wrong, unavailable, or temporarily blocked."
        )
    return save_html(html, Path(output_dir) / f"meeting_{meeting_code}.html")


def fetch_results(meeting_code: str, output_dir: str | Path) -> Path:
    html = fetch_rendered_html(build_results_url(meeting_code))
    return save_html(html, Path(output_dir) / f"results_{meeting_code}.html")


def fetch_horse_pages_from_meeting_html(
    meeting_html_path: str | Path,
    output_dir: str | Path,
    race_number: int | None = None,
    horse_library_dir: str | Path | None = None,
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
    for index, runner in enumerate(unique_runners, start=1):
        target_path = output_path / f"{_safe_name(runner.horse_name)}_{runner.horse_id}.html"
        library_target = _library_target_path(library_path, runner.horse_id, runner.horse_name) if library_path else None
        library_existing = _find_existing_library_file(library_path, runner.horse_id, preferred=library_target)

        if target_path.exists():
            existing_html = target_path.read_text(encoding="utf-8", errors="replace")
            if is_valid_horse_html(existing_html):
                skipped_existing_count += 1
                print(f"[{index}/{len(unique_runners)}] Skipping existing valid file for {runner.horse_name} ({runner.horse_id})", flush=True)
                if library_target and not library_target.exists():
                    save_html(existing_html, library_target)
                saved_paths.append(target_path)
                continue
            print(f"[{index}/{len(unique_runners)}] Replacing invalid existing file for {runner.horse_name} ({runner.horse_id})", flush=True)
        elif library_existing:
            existing_html = library_existing.read_text(encoding="utf-8", errors="replace")
            if is_valid_horse_html(existing_html):
                reused_count += 1
                print(f"[{index}/{len(unique_runners)}] Reusing library file for {runner.horse_name} ({runner.horse_id})", flush=True)
                save_html(existing_html, target_path)
                if library_target and library_existing != library_target:
                    save_html(existing_html, library_target)
                saved_paths.append(target_path)
                continue

        print(f"[{index}/{len(unique_runners)}] Fetching {runner.horse_name} ({runner.horse_id})...", flush=True)
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
        f"  Reused from library: {reused_count}\n"
        f"  Skipped existing:    {skipped_existing_count}\n"
        f"  Freshly fetched:     {fetched_count}\n"
        f"  Failed:              {len(failures)}",
        flush=True,
    )
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
    conn.close()
    return len(results)


def ingest_horse_html(db_path: str | Path, html_path: str | Path) -> str:
    path = Path(html_path)
    html = path.read_text(encoding="utf-8", errors="replace")
    profile = parse_horse_profile_html(html, horse_id=_infer_horse_id(path), source_name=path.name)
    conn = connect(db_path)
    init_db(conn)
    upsert_horse_profile(conn, profile)
    conn.close()
    return profile.horse_id


def ingest_horse_dir(db_path: str | Path, horse_dir: str | Path) -> int:
    count = 0
    for path in sorted(Path(horse_dir).glob("*.html")):
        ingest_horse_html(db_path, path)
        count += 1
    return count


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
