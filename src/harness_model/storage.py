from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from datetime import date

from .models import HorseProfile, MeetingInfo, RunnerInfo


SCHEMA = """
CREATE TABLE IF NOT EXISTS meetings (
    meeting_code TEXT PRIMARY KEY,
    meeting_date TEXT,
    track_name TEXT,
    state TEXT,
    raw_title TEXT
);

CREATE TABLE IF NOT EXISTS race_runners (
    meeting_code TEXT NOT NULL,
    race_number INTEGER NOT NULL,
    horse_id TEXT NOT NULL,
    runner_number INTEGER,
    horse_name TEXT NOT NULL,
    barrier TEXT,
    driver_name TEXT,
    driver_link TEXT,
    trainer_name TEXT,
    trainer_link TEXT,
    scratched INTEGER NOT NULL DEFAULT 0,
    race_name TEXT,
    race_distance INTEGER,
    race_type TEXT,
    class_name TEXT,
    raw_price REAL,
    form_career_summary TEXT,
    form_this_season_summary TEXT,
    form_last_season_summary TEXT,
    form_bmr TEXT,
    form_bmr_dist_rge TEXT,
    race_purse REAL,
    PRIMARY KEY (meeting_code, race_number, horse_id)
);

CREATE TABLE IF NOT EXISTS runner_recent_lines (
    meeting_code TEXT NOT NULL,
    race_number INTEGER NOT NULL,
    horse_id TEXT NOT NULL,
    line_index INTEGER NOT NULL,
    run_date TEXT,
    track_name TEXT,
    track_code TEXT,
    distance INTEGER,
    condition TEXT,
    last_half REAL,
    mile_rate TEXT,
    first_half REAL,
    q1 REAL,
    q2 REAL,
    q3 REAL,
    q4 REAL,
    raw_comment TEXT,
    finish_position INTEGER,
    raw_margin REAL,
    run_purse REAL,
    comment_adjustment REAL,
    tempo_adjustment REAL,
    null_run INTEGER NOT NULL DEFAULT 0,
    adjusted_margin REAL,
    PRIMARY KEY (meeting_code, race_number, horse_id, line_index)
);

CREATE TABLE IF NOT EXISTS horse_profiles (
    horse_id TEXT PRIMARY KEY,
    horse_name TEXT NOT NULL,
    nr_rating INTEGER,
    career_summary TEXT,
    this_season_summary TEXT,
    last_season_summary TEXT,
    career_bmr TEXT,
    this_season_bmr TEXT,
    last_season_bmr TEXT
);

CREATE TABLE IF NOT EXISTS horse_runs (
    horse_id TEXT NOT NULL,
    run_date TEXT,
    track_code TEXT,
    finish_position INTEGER,
    barrier TEXT,
    margin REAL,
    mile_rate TEXT,
    driver_name TEXT,
    trainer_name TEXT,
    stake REAL,
    distance INTEGER,
    distance_code TEXT,
    race_name TEXT,
    start_price REAL,
    comment_codes TEXT,
    comment_adjustment REAL,
    null_run INTEGER NOT NULL DEFAULT 0,
    adjusted_margin REAL,
    race_type TEXT,
    PRIMARY KEY (horse_id, run_date, race_name, distance_code)
);

CREATE TABLE IF NOT EXISTS race_results (
    meeting_code TEXT NOT NULL,
    race_number INTEGER NOT NULL,
    horse_id TEXT,
    horse_name TEXT NOT NULL,
    finish_position INTEGER,
    margin REAL,
    starting_price REAL,
    PRIMARY KEY (meeting_code, race_number, horse_name)
);

CREATE TABLE IF NOT EXISTS driver_stats (
    driver_slug TEXT PRIMARY KEY,
    driver_name TEXT NOT NULL,
    season_starts INTEGER,
    season_wins INTEGER,
    season_win_rate REAL,
    career_win_rate REAL,
    fetched_date TEXT
);

CREATE TABLE IF NOT EXISTS trainer_stats (
    trainer_slug TEXT PRIMARY KEY,
    trainer_name TEXT NOT NULL,
    season_starts INTEGER,
    season_wins INTEGER,
    season_win_rate REAL,
    career_win_rate REAL,
    fetched_date TEXT
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    _ensure_columns(
        conn,
        "race_runners",
        {
            "form_nr": "INTEGER",
            "trainer_change_manual": "INTEGER",
            "form_career_summary": "TEXT",
            "form_this_season_summary": "TEXT",
            "form_last_season_summary": "TEXT",
            "form_dist_rge_summary": "TEXT",
            "form_bmr": "TEXT",
            "form_bmr_dist_rge": "TEXT",
            "race_purse": "REAL",
            "driver_link": "TEXT",
            "trainer_link": "TEXT",
        },
    )
    _ensure_columns(
        conn,
        "runner_recent_lines",
        {
            "first_half": "REAL",
            "q1": "REAL",
            "q2": "REAL",
            "q3": "REAL",
            "q4": "REAL",
            "raw_comment": "TEXT",
            "finish_position": "INTEGER",
            "raw_margin": "REAL",
            "comment_adjustment": "REAL",
            "tempo_adjustment": "REAL",
            "null_run": "INTEGER NOT NULL DEFAULT 0",
            "adjusted_margin": "REAL",
            "run_purse": "REAL",
            "line_nr_ceiling": "INTEGER",
            "line_race_age": "TEXT",
            "run_sp": "REAL",
        },
    )
    _ensure_columns(
        conn,
        "race_results",
        {
            "horse_id": "TEXT",
            "margin": "REAL",
            "starting_price": "REAL",
        },
    )
    conn.commit()


def upsert_meeting(conn: sqlite3.Connection, meeting: MeetingInfo) -> None:
    conn.execute(
        """
        INSERT INTO meetings(meeting_code, meeting_date, track_name, state, raw_title)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(meeting_code) DO UPDATE SET
            meeting_date = excluded.meeting_date,
            track_name = excluded.track_name,
            state = excluded.state,
            raw_title = excluded.raw_title
        """,
        (meeting.meeting_code, meeting.meeting_date, meeting.track_name, meeting.state, meeting.raw_title),
    )
    conn.commit()


def upsert_runners(conn: sqlite3.Connection, runners: list[RunnerInfo]) -> None:
    conn.executemany(
        """
        INSERT INTO race_runners(
            meeting_code, race_number, horse_id, runner_number, horse_name,
            barrier, driver_name, driver_link, trainer_name, trainer_link, scratched, race_name,
            race_distance, race_type, class_name, raw_price,
            form_nr, form_career_summary, form_this_season_summary, form_last_season_summary,
            form_dist_rge_summary, form_bmr, form_bmr_dist_rge, race_purse
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(meeting_code, race_number, horse_id) DO UPDATE SET
            runner_number = excluded.runner_number,
            horse_name = excluded.horse_name,
            barrier = excluded.barrier,
            driver_name = excluded.driver_name,
            driver_link = excluded.driver_link,
            trainer_name = excluded.trainer_name,
            trainer_link = excluded.trainer_link,
            scratched = excluded.scratched,
            race_name = excluded.race_name,
            race_distance = excluded.race_distance,
            race_type = excluded.race_type,
            class_name = excluded.class_name,
            raw_price = excluded.raw_price,
            form_nr = excluded.form_nr,
            form_career_summary = excluded.form_career_summary,
            form_this_season_summary = excluded.form_this_season_summary,
            form_last_season_summary = excluded.form_last_season_summary,
            form_dist_rge_summary = excluded.form_dist_rge_summary,
            form_bmr = excluded.form_bmr,
            form_bmr_dist_rge = excluded.form_bmr_dist_rge,
            race_purse = excluded.race_purse
        """,
        [
            (
                runner.meeting_code,
                runner.race_number,
                runner.horse_id,
                runner.runner_number,
                runner.horse_name,
                runner.barrier,
                runner.driver_name,
                runner.driver_link,
                runner.trainer_name,
                runner.trainer_link,
                int(runner.scratched),
                runner.race_name,
                runner.race_distance,
                runner.race_type,
                runner.class_name,
                runner.raw_price,
                runner.form_nr,
                _summary_to_text(runner.form_career_summary),
                _summary_to_text(runner.form_this_season_summary),
                _summary_to_text(runner.form_last_season_summary),
                _summary_to_text(runner.form_dist_rge_summary),
                runner.form_bmr,
                runner.form_bmr_dist_rge,
                runner.race_purse,
            )
            for runner in runners
        ],
    )
    conn.executemany(
        """
        INSERT INTO runner_recent_lines(
            meeting_code, race_number, horse_id, line_index, run_date,
            track_name, track_code, distance, condition, last_half, mile_rate,
            first_half, q1, q2, q3, q4, raw_comment, finish_position,
            raw_margin, run_purse, line_nr_ceiling, line_race_age, run_sp, comment_adjustment, tempo_adjustment, null_run, adjusted_margin
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(meeting_code, race_number, horse_id, line_index) DO UPDATE SET
            run_date = excluded.run_date,
            track_name = excluded.track_name,
            track_code = excluded.track_code,
            distance = excluded.distance,
            condition = excluded.condition,
            last_half = excluded.last_half,
            mile_rate = excluded.mile_rate,
            first_half = excluded.first_half,
            q1 = excluded.q1,
            q2 = excluded.q2,
            q3 = excluded.q3,
            q4 = excluded.q4,
            raw_comment = excluded.raw_comment,
            finish_position = excluded.finish_position,
            raw_margin = excluded.raw_margin,
            run_purse = excluded.run_purse,
            line_nr_ceiling = excluded.line_nr_ceiling,
            line_race_age = excluded.line_race_age,
            run_sp = excluded.run_sp,
            comment_adjustment = excluded.comment_adjustment,
            tempo_adjustment = excluded.tempo_adjustment,
            null_run = excluded.null_run,
            adjusted_margin = excluded.adjusted_margin
        """,
        [
            (
                runner.meeting_code,
                runner.race_number,
                runner.horse_id,
                line.line_index,
                line.run_date,
                line.track_name,
                line.track_code,
                line.distance,
                line.condition,
                line.last_half,
                line.mile_rate,
                line.first_half,
                line.q1,
                line.q2,
                line.q3,
                line.q4,
                line.raw_comment,
                line.finish_position,
                line.raw_margin,
                line.run_purse,
                line.line_nr_ceiling,
                line.line_race_age,
                line.run_sp,
                line.comment_adjustment,
                line.tempo_adjustment,
                int(line.null_run),
                line.adjusted_margin,
            )
            for runner in runners
            for line in runner.recent_lines
        ],
    )
    conn.commit()


def horse_has_runs(conn: sqlite3.Connection, horse_id: str) -> bool:
    """Return True only if the horse has real profile-sourced runs (not FORM/RESULT placeholders)."""
    row = conn.execute(
        """
        SELECT 1
        FROM horse_runs
        WHERE horse_id = ?
          AND race_name NOT LIKE 'FORM:%'
          AND race_name NOT LIKE 'RESULT:%'
        LIMIT 1
        """,
        (horse_id,),
    ).fetchone()
    return row is not None


def cleanup_form_entries_for_horse(conn: sqlite3.Connection, horse_id: str) -> int:
    """Delete FORM and RESULT placeholder runs now covered by real profile data for this horse."""
    # FORM entries: match on (run_date, track_code) — both are populated from form-page parsing.
    c1 = conn.execute(
        """
        DELETE FROM horse_runs
        WHERE horse_id = ?
          AND race_name LIKE 'FORM:%'
          AND (run_date, track_code) IN (
              SELECT run_date, track_code
              FROM horse_runs
              WHERE horse_id = ?
                AND race_name NOT LIKE 'FORM:%'
                AND race_name NOT LIKE 'RESULT:%'
          )
        """,
        (horse_id, horse_id),
    )
    # RESULT entries: match on run_date alone (track_code from meeting code prefix differs
    # from the 7-char profile page track codes, so we can't join on it).
    c2 = conn.execute(
        """
        DELETE FROM horse_runs
        WHERE horse_id = ?
          AND race_name LIKE 'RESULT:%'
          AND run_date IN (
              SELECT run_date
              FROM horse_runs
              WHERE horse_id = ?
                AND race_name NOT LIKE 'FORM:%'
                AND race_name NOT LIKE 'RESULT:%'
          )
        """,
        (horse_id, horse_id),
    )
    conn.commit()
    return c1.rowcount + c2.rowcount


def driver_stats_are_fresh(conn: sqlite3.Connection, driver_slug: str, max_age_days: int) -> bool:
    row = conn.execute(
        """
        SELECT fetched_date
        FROM driver_stats
        WHERE driver_slug = ?
        LIMIT 1
        """,
        (driver_slug,),
    ).fetchone()
    if row is None or not row["fetched_date"]:
        return False
    try:
        fetched = date.fromisoformat(str(row["fetched_date"]))
    except ValueError:
        return False
    age_days = (date.today() - fetched).days
    return age_days <= max_age_days


def trainer_stats_are_fresh(conn: sqlite3.Connection, trainer_slug: str, max_age_days: int) -> bool:
    row = conn.execute(
        """
        SELECT fetched_date
        FROM trainer_stats
        WHERE trainer_slug = ?
        LIMIT 1
        """,
        (trainer_slug,),
    ).fetchone()
    if row is None or not row["fetched_date"]:
        return False
    try:
        fetched = date.fromisoformat(str(row["fetched_date"]))
    except ValueError:
        return False
    age_days = (date.today() - fetched).days
    return age_days <= max_age_days


def sync_runner_recent_lines_to_horse_runs(conn: sqlite3.Connection, runners: list[RunnerInfo]) -> int:
    synced_rows: list[tuple] = []
    for runner in runners:
        # Skip form lines that are already covered by a fetched horse profile.
        # Matching on (run_date, track_code) is sufficient — a horse cannot race
        # twice at the same track on the same day.
        existing_keys = {
            (row["run_date"], row["track_code"])
            for row in conn.execute(
                "SELECT run_date, track_code FROM horse_runs WHERE horse_id = ?",
                (runner.horse_id,),
            ).fetchall()
        }
        for line in runner.recent_lines:
            if (line.run_date, line.track_code) in existing_keys:
                continue
            race_name = f"FORM:{line.track_name or line.track_code or 'UNK'}:{line.run_date or 'UNK'}"
            distance_code = str(line.distance or "")
            synced_rows.append(
                (
                    runner.horse_id,
                    line.run_date,
                    line.track_code,
                    line.finish_position,
                    None,
                    line.raw_margin,
                    line.mile_rate,
                    None,
                    None,
                    line.run_purse,
                    line.distance,
                    distance_code,
                    race_name,
                    None,
                    line.raw_comment,
                    line.comment_adjustment,
                    int(line.null_run),
                    line.adjusted_margin,
                    "RACE",
                )
            )

    if not synced_rows:
        return 0

    conn.executemany(
        """
        INSERT INTO horse_runs(
            horse_id, run_date, track_code, finish_position, barrier, margin,
            mile_rate, driver_name, trainer_name, stake, distance, distance_code,
            race_name, start_price, comment_codes, comment_adjustment, null_run,
            adjusted_margin, race_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(horse_id, run_date, race_name, distance_code) DO UPDATE SET
            track_code = excluded.track_code,
            finish_position = COALESCE(excluded.finish_position, horse_runs.finish_position),
            margin = COALESCE(excluded.margin, horse_runs.margin),
            mile_rate = COALESCE(excluded.mile_rate, horse_runs.mile_rate),
            stake = COALESCE(excluded.stake, horse_runs.stake),
            distance = COALESCE(excluded.distance, horse_runs.distance),
            comment_codes = COALESCE(excluded.comment_codes, horse_runs.comment_codes),
            comment_adjustment = COALESCE(excluded.comment_adjustment, horse_runs.comment_adjustment),
            null_run = excluded.null_run,
            adjusted_margin = COALESCE(excluded.adjusted_margin, horse_runs.adjusted_margin),
            race_type = COALESCE(excluded.race_type, horse_runs.race_type)
        """,
        synced_rows,
    )
    conn.commit()
    return len(synced_rows)


def upsert_horse_profile(conn: sqlite3.Connection, profile: HorseProfile) -> None:
    conn.execute(
        """
        INSERT INTO horse_profiles(
            horse_id, horse_name, nr_rating, career_summary, this_season_summary,
            last_season_summary, career_bmr, this_season_bmr, last_season_bmr
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(horse_id) DO UPDATE SET
            horse_name = excluded.horse_name,
            nr_rating = excluded.nr_rating,
            career_summary = excluded.career_summary,
            this_season_summary = excluded.this_season_summary,
            last_season_summary = excluded.last_season_summary,
            career_bmr = excluded.career_bmr,
            this_season_bmr = excluded.this_season_bmr,
            last_season_bmr = excluded.last_season_bmr
        """,
        (
            profile.horse_id,
            profile.horse_name,
            profile.nr_rating,
            _summary_to_text(profile.career_summary),
            _summary_to_text(profile.this_season_summary),
            _summary_to_text(profile.last_season_summary),
            profile.career_bmr,
            profile.this_season_bmr,
            profile.last_season_bmr,
        ),
    )

    conn.executemany(
        """
        INSERT INTO horse_runs(
            horse_id, run_date, track_code, finish_position, barrier, margin,
            mile_rate, driver_name, trainer_name, stake, distance, distance_code,
            race_name, start_price, comment_codes, comment_adjustment, null_run,
            adjusted_margin, race_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(horse_id, run_date, race_name, distance_code) DO UPDATE SET
            track_code = excluded.track_code,
            finish_position = excluded.finish_position,
            barrier = excluded.barrier,
            margin = excluded.margin,
            mile_rate = excluded.mile_rate,
            driver_name = excluded.driver_name,
            trainer_name = excluded.trainer_name,
            stake = excluded.stake,
            distance = excluded.distance,
            start_price = excluded.start_price,
            comment_codes = excluded.comment_codes,
            comment_adjustment = excluded.comment_adjustment,
            null_run = excluded.null_run,
            adjusted_margin = excluded.adjusted_margin,
            race_type = excluded.race_type
        """,
        [
            (
                run.horse_id,
                run.run_date,
                run.track_code,
                run.finish_position,
                run.barrier,
                run.margin,
                run.mile_rate,
                run.driver_name,
                run.trainer_name,
                run.stake,
                run.distance,
                run.distance_code,
                run.race_name,
                run.start_price,
                run.comment_codes,
                run.comment_adjustment,
                int(run.null_run),
                run.adjusted_margin,
                run.race_type,
            )
            for run in profile.runs
        ],
    )
    conn.commit()


def _normalize_run_date(meeting_date: str) -> str:
    """Zero-pad day in meeting dates like '1 Apr 2026' → '01 Apr 2026'."""
    m = re.match(r"^(\d{1,2})\s+(\w+)\s+(\d{4})$", meeting_date.strip())
    if m:
        return f"{int(m.group(1)):02d} {m.group(2)} {m.group(3)}"
    return meeting_date


def upsert_result_horse_runs(conn: sqlite3.Connection, results: list, resolved_ids: dict) -> None:
    """Write results-sourced runs into horse_runs.

    Uses race_name = 'RESULT:{meeting_code}:{race_number}' as the dedup key so
    these entries are distinguishable from real profile runs and cleaned up when
    a profile is later ingested via cleanup_form_entries_for_horse().

    resolved_ids maps horse_name.upper() → horse_id for the current batch.
    """
    meeting_dates: dict[str, str | None] = {}

    rows = []
    for result in results:
        horse_id = resolved_ids.get(result.horse_name.upper())
        if not horse_id:
            continue

        meeting_code = result.meeting_code
        if meeting_code not in meeting_dates:
            row = conn.execute(
                "SELECT meeting_date FROM meetings WHERE meeting_code = ? LIMIT 1",
                (meeting_code,),
            ).fetchone()
            meeting_dates[meeting_code] = row["meeting_date"] if row else None

        meeting_date = meeting_dates[meeting_code]
        if not meeting_date:
            continue

        run_date = _normalize_run_date(meeting_date)
        # Track code from meeting code: strip the trailing 6-digit date (DDMMYY).
        track_code = re.sub(r"\d{6}$", "", meeting_code) or None
        race_name = f"RESULT:{meeting_code}:{result.race_number}"
        distance_code = str(result.distance) if result.distance else None

        rows.append((
            horse_id,
            run_date,
            track_code,
            result.finish_position,
            result.barrier,
            result.margin,
            None,               # mile_rate — not available from results pages
            result.driver_name,
            result.trainer_name,
            result.stake,
            result.distance,
            distance_code,
            race_name,
            result.starting_price,
            result.comment_codes,
            result.comment_adjustment,
            int(result.null_run),
            result.adjusted_margin,
            "RACE",
        ))

    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO horse_runs(
            horse_id, run_date, track_code, finish_position, barrier, margin,
            mile_rate, driver_name, trainer_name, stake, distance, distance_code,
            race_name, start_price, comment_codes, comment_adjustment, null_run,
            adjusted_margin, race_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(horse_id, run_date, race_name, distance_code) DO UPDATE SET
            finish_position  = COALESCE(excluded.finish_position,  horse_runs.finish_position),
            barrier          = COALESCE(excluded.barrier,          horse_runs.barrier),
            margin           = COALESCE(excluded.margin,           horse_runs.margin),
            driver_name      = COALESCE(excluded.driver_name,      horse_runs.driver_name),
            trainer_name     = COALESCE(excluded.trainer_name,     horse_runs.trainer_name),
            stake            = COALESCE(excluded.stake,            horse_runs.stake),
            start_price      = COALESCE(excluded.start_price,      horse_runs.start_price),
            comment_codes    = COALESCE(excluded.comment_codes,    horse_runs.comment_codes),
            comment_adjustment = excluded.comment_adjustment,
            null_run         = excluded.null_run,
            adjusted_margin  = COALESCE(excluded.adjusted_margin,  horse_runs.adjusted_margin)
        """,
        rows,
    )
    conn.commit()


def upsert_results(conn: sqlite3.Connection, results: list) -> None:
    resolved_ids: dict[str, str | None] = {}
    rows = []
    for result in results:
        matched_horse_id = conn.execute(
            """
            SELECT horse_id
            FROM race_runners
            WHERE meeting_code = ?
              AND race_number = ?
              AND UPPER(horse_name) = UPPER(?)
            LIMIT 1
            """,
            (result.meeting_code, result.race_number, result.horse_name),
        ).fetchone()
        horse_id = matched_horse_id["horse_id"] if matched_horse_id else result.horse_id
        resolved_ids[result.horse_name.upper()] = horse_id
        rows.append(
            (
                result.meeting_code,
                result.race_number,
                horse_id,
                result.horse_name,
                result.finish_position,
                result.margin,
                result.starting_price,
            )
        )

    conn.executemany(
        """
        INSERT INTO race_results(
            meeting_code, race_number, horse_id, horse_name, finish_position, margin, starting_price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(meeting_code, race_number, horse_name) DO UPDATE SET
            horse_id = excluded.horse_id,
            finish_position = excluded.finish_position,
            margin = excluded.margin,
            starting_price = excluded.starting_price
        """,
        rows,
    )
    conn.commit()

    upsert_result_horse_runs(conn, results, resolved_ids)


def scratch_horse(
    conn: sqlite3.Connection,
    meeting_code: str,
    horse_name: str,
    race_number: int | None = None,
) -> list[tuple[str, int]]:
    """Mark a horse as scratched in race_runners by case-insensitive partial name match.

    Returns a list of (horse_name, race_number) tuples for every row updated.
    """
    query = """
        SELECT horse_name, race_number
        FROM race_runners
        WHERE meeting_code = ?
          AND UPPER(horse_name) LIKE UPPER(?)
          AND COALESCE(scratched, 0) = 0
    """
    params: list = [meeting_code, f"%{horse_name}%"]
    if race_number is not None:
        query += " AND race_number = ?"
        params.append(race_number)

    rows = conn.execute(query, params).fetchall()
    if not rows:
        return []

    update_query = """
        UPDATE race_runners
        SET scratched = 1
        WHERE meeting_code = ?
          AND UPPER(horse_name) LIKE UPPER(?)
    """
    update_params: list = [meeting_code, f"%{horse_name}%"]
    if race_number is not None:
        update_query += " AND race_number = ?"
        update_params.append(race_number)

    conn.execute(update_query, update_params)
    conn.commit()
    return [(row["horse_name"], row["race_number"]) for row in rows]


def set_trainer_change_manual(
    conn: sqlite3.Connection,
    meeting_code: str,
    horse_name: str,
    value: int = 1,
    race_number: int | None = None,
) -> list[tuple[str, int]]:
    """Set trainer_change_manual flag on race_runners by case-insensitive partial name match.

    value=1 sets the flag, value=0 clears it.
    Returns a list of (horse_name, race_number) tuples for every row updated.
    """
    select_query = """
        SELECT horse_name, race_number
        FROM race_runners
        WHERE meeting_code = ?
          AND UPPER(horse_name) LIKE UPPER(?)
          AND COALESCE(scratched, 0) = 0
    """
    select_params: list = [meeting_code, f"%{horse_name}%"]
    if race_number is not None:
        select_query += " AND race_number = ?"
        select_params.append(race_number)

    rows = conn.execute(select_query, select_params).fetchall()
    if not rows:
        return []

    update_query = """
        UPDATE race_runners
        SET trainer_change_manual = ?
        WHERE meeting_code = ?
          AND UPPER(horse_name) LIKE UPPER(?)
    """
    update_params: list = [value, meeting_code, f"%{horse_name}%"]
    if race_number is not None:
        update_query += " AND race_number = ?"
        update_params.append(race_number)

    conn.execute(update_query, update_params)
    conn.commit()
    return [(row["horse_name"], row["race_number"]) for row in rows]


def _summary_to_text(summary: tuple[int, int, int, int] | None) -> str | None:
    if summary is None:
        return None
    return "-".join(str(value) for value in summary)


def upsert_driver_stats(conn: sqlite3.Connection, driver_slug: str, stats: dict[str, object]) -> None:
    conn.execute(
        """
        INSERT INTO driver_stats(driver_slug, driver_name, season_starts, season_wins, season_win_rate, career_win_rate, fetched_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(driver_slug) DO UPDATE SET
            driver_name = excluded.driver_name,
            season_starts = excluded.season_starts,
            season_wins = excluded.season_wins,
            season_win_rate = excluded.season_win_rate,
            career_win_rate = excluded.career_win_rate,
            fetched_date = excluded.fetched_date
        """,
        (
            driver_slug,
            stats["driver_name"],
            stats.get("season_starts"),
            stats.get("season_wins"),
            stats.get("season_win_rate"),
            stats.get("career_win_rate"),
            date.today().isoformat(),
        ),
    )
    conn.commit()


def upsert_trainer_stats(conn: sqlite3.Connection, trainer_slug: str, stats: dict[str, object]) -> None:
    conn.execute(
        """
        INSERT INTO trainer_stats(trainer_slug, trainer_name, season_starts, season_wins, season_win_rate, career_win_rate, fetched_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trainer_slug) DO UPDATE SET
            trainer_name = excluded.trainer_name,
            season_starts = excluded.season_starts,
            season_wins = excluded.season_wins,
            season_win_rate = excluded.season_win_rate,
            career_win_rate = excluded.career_win_rate,
            fetched_date = excluded.fetched_date
        """,
        (
            trainer_slug,
            stats["trainer_name"],
            stats.get("season_starts"),
            stats.get("season_wins"),
            stats.get("season_win_rate"),
            stats.get("career_win_rate"),
            date.today().isoformat(),
        ),
    )
    conn.commit()


def _ensure_columns(conn: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
    existing_tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if table_name not in existing_tables:
        return

    existing_columns = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, column_type in columns.items():
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
