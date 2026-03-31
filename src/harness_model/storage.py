from __future__ import annotations

import sqlite3
from pathlib import Path

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
    trainer_name TEXT,
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
            "form_career_summary": "TEXT",
            "form_this_season_summary": "TEXT",
            "form_last_season_summary": "TEXT",
            "form_bmr": "TEXT",
            "form_bmr_dist_rge": "TEXT",
            "race_purse": "REAL",
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
            barrier, driver_name, trainer_name, scratched, race_name,
            race_distance, race_type, class_name, raw_price,
            form_career_summary, form_this_season_summary, form_last_season_summary,
            form_bmr, form_bmr_dist_rge, race_purse
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(meeting_code, race_number, horse_id) DO UPDATE SET
            runner_number = excluded.runner_number,
            horse_name = excluded.horse_name,
            barrier = excluded.barrier,
            driver_name = excluded.driver_name,
            trainer_name = excluded.trainer_name,
            scratched = excluded.scratched,
            race_name = excluded.race_name,
            race_distance = excluded.race_distance,
            race_type = excluded.race_type,
            class_name = excluded.class_name,
            raw_price = excluded.raw_price,
            form_career_summary = excluded.form_career_summary,
            form_this_season_summary = excluded.form_this_season_summary,
            form_last_season_summary = excluded.form_last_season_summary,
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
                runner.trainer_name,
                int(runner.scratched),
                runner.race_name,
                runner.race_distance,
                runner.race_type,
                runner.class_name,
                runner.raw_price,
                _summary_to_text(runner.form_career_summary),
                _summary_to_text(runner.form_this_season_summary),
                _summary_to_text(runner.form_last_season_summary),
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
            raw_margin, run_purse, comment_adjustment, tempo_adjustment, null_run, adjusted_margin
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def upsert_results(conn: sqlite3.Connection, results: list) -> None:
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


def _summary_to_text(summary: tuple[int, int, int, int] | None) -> str | None:
    if summary is None:
        return None
    return "-".join(str(value) for value in summary)


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
