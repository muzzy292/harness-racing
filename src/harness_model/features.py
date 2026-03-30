from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path

from .track_pars import lookup_race_par

def install_sqlite_helpers(conn: sqlite3.Connection) -> None:
    conn.create_function("_sort_run_date", 1, _sort_run_date)


def build_runner_feature_rows(conn: sqlite3.Connection, track_pars: dict | None = None) -> list[dict[str, object]]:
    runners = conn.execute(
        """
        SELECT
            rr.meeting_code,
            rr.race_number,
            rr.horse_id,
            rr.horse_name,
            rr.runner_number,
            rr.barrier,
            rr.driver_name AS nominated_driver,
            rr.trainer_name AS nominated_trainer,
            rr.scratched,
            rr.race_name,
            rr.race_distance,
            rr.class_name,
            m.meeting_date,
            m.track_name,
            hp.nr_rating,
            COALESCE(rr.form_career_summary, hp.career_summary) AS career_summary,
            COALESCE(rr.form_this_season_summary, hp.this_season_summary) AS this_season_summary,
            COALESCE(rr.form_last_season_summary, hp.last_season_summary) AS last_season_summary,
            rr.form_bmr,
            rr.form_bmr_dist_rge
        FROM race_runners rr
        LEFT JOIN meetings m ON m.meeting_code = rr.meeting_code
        LEFT JOIN horse_profiles hp ON hp.horse_id = rr.horse_id
        WHERE COALESCE(rr.scratched, 0) = 0
        ORDER BY rr.meeting_code, rr.race_number, rr.runner_number
        """
    ).fetchall()

    rows: list[dict[str, object]] = []
    for runner in runners:
        last_runs = conn.execute(
            """
            SELECT *
            FROM horse_runs
            WHERE horse_id = ?
              AND COALESCE(race_type, 'RACE') <> 'TRIAL'
              AND COALESCE(null_run, 0) = 0
            ORDER BY _sort_run_date(run_date) DESC
            LIMIT 10
            """,
            (runner["horse_id"],),
        ).fetchall()
        recent_lines = conn.execute(
            """
            SELECT *
            FROM runner_recent_lines
            WHERE meeting_code = ?
              AND race_number = ?
              AND horse_id = ?
            ORDER BY line_index
            """,
            (runner["meeting_code"], runner["race_number"], runner["horse_id"]),
        ).fetchall()

        rows.append(
            _build_feature_row(
                conn,
                dict(runner),
                [dict(row) for row in last_runs],
                [dict(row) for row in recent_lines],
                track_pars,
            )
        )
    return rows


def write_feature_csv(rows: list[dict[str, object]], output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output.write_text("", encoding="utf-8")
        return output

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return output


def _build_feature_row(
    conn: sqlite3.Connection,
    runner: dict[str, object],
    last_runs: list[dict[str, object]],
    recent_lines: list[dict[str, object]],
    track_pars: dict | None,
) -> dict[str, object]:
    adjusted = [run["adjusted_margin"] for run in last_runs if run["adjusted_margin"] is not None]
    prices = [run["start_price"] for run in last_runs if run["start_price"] is not None]
    wins = [run for run in last_runs if run["finish_position"] == 1]
    same_driver_runs = [
        run for run in last_runs
        if run["driver_name"] and runner["nominated_driver"]
        and _normalize_name(run["driver_name"]) == _normalize_name(str(runner["nominated_driver"]))
    ]

    driver_stats = _rolling_person_stats(conn, "driver_name", runner["nominated_driver"])
    trainer_stats = _rolling_person_stats(conn, "trainer_name", runner["nominated_trainer"])
    race_par = lookup_race_par(track_pars, runner["track_name"], runner["race_distance"])
    sectional_deltas = _sectional_deltas_vs_par(recent_lines, track_pars)
    valid_recent_lines = [line for line in recent_lines if not _truthy(line.get("null_run"))]
    line_comment_adjustments = [float(line["comment_adjustment"]) for line in valid_recent_lines if line.get("comment_adjustment") not in (None, "")]
    line_tempo_adjustments = [float(line["tempo_adjustment"]) for line in valid_recent_lines if line.get("tempo_adjustment") not in (None, "")]
    tempo_flag_count = sum(1 for line in recent_lines if _to_float_local(line.get("tempo_adjustment")) not in (None, 0.0))
    null_line_count = sum(1 for line in recent_lines if _truthy(line.get("null_run")))
    raw_recent_margins = [float(line["raw_margin"]) for line in valid_recent_lines if line.get("raw_margin") not in (None, "")]
    adj_recent_margins = [float(line["adjusted_margin"]) for line in valid_recent_lines if line.get("adjusted_margin") not in (None, "")]
    primary_adj_margins = adjusted if adjusted else adj_recent_margins
    primary_prices = prices
    primary_win_rate_source = wins if last_runs else [line for line in valid_recent_lines if line.get("finish_position") == 1]
    map_signals = _map_signals(valid_recent_lines, runner.get("barrier"))
    bmr_secs = _parse_bmr_secs(runner.get("form_bmr"))
    bmr_dist_rge_secs = _parse_bmr_secs(runner.get("form_bmr_dist_rge"))
    days_since_last_run = _days_since_last_run(recent_lines, runner.get("meeting_date"))
    race_nr_ceiling = _parse_race_nr_ceiling(runner.get("class_name"))
    nr_rating = runner["nr_rating"]
    nr_headroom = round(race_nr_ceiling - float(nr_rating), 1) if (race_nr_ceiling is not None and nr_rating is not None) else None
    raw_stakes = [run["stake"] for run in last_runs if run.get("stake") is not None]
    capped_stakes = _cap_outlier_stakes([float(s) for s in raw_stakes])
    last_5_avg_stake = _avg(capped_stakes[:5])

    return {
        "meeting_code": runner["meeting_code"],
        "meeting_date": runner["meeting_date"],
        "track_name": runner["track_name"],
        "race_number": runner["race_number"],
        "race_name": runner["race_name"],
        "race_distance": runner["race_distance"],
        "class_name": runner["class_name"],
        "horse_id": runner["horse_id"],
        "horse_name": runner["horse_name"],
        "runner_number": runner["runner_number"],
        "barrier": runner["barrier"],
        "nominated_driver": runner["nominated_driver"],
        "nominated_trainer": runner["nominated_trainer"],
        "nr_rating": runner["nr_rating"],
        "race_par_last_half": race_par["par_last_half"],
        "race_par_std": race_par["par_std"],
        "race_par_sample": race_par["par_sample"],
        "race_par_condition": race_par["par_condition"],
        "recent_sectional_count": len([line for line in recent_lines if line.get("last_half") is not None]),
        "last_3_avg_sectional_delta": _avg(sectional_deltas[:3]),
        "last_5_avg_sectional_delta": _avg(sectional_deltas[:5]),
        "best_recent_sectional_delta": min(sectional_deltas) if sectional_deltas else None,
        "recent_line_avg_raw_margin": _avg(raw_recent_margins[:5]),
        "recent_line_avg_adj_margin": _avg(adj_recent_margins[:5]),
        "recent_line_best_adj_margin": min(adj_recent_margins) if adj_recent_margins else None,
        "recent_line_avg_comment_adj": _avg(line_comment_adjustments),
        "recent_line_avg_tempo_adj": _avg(line_tempo_adjustments),
        "recent_line_tempo_flags": tempo_flag_count,
        "recent_line_null_flags": null_line_count,
        "style_lead_rate": map_signals["style_lead_rate"],
        "style_forward_rate": map_signals["style_forward_rate"],
        "style_restrained_rate": map_signals["style_restrained_rate"],
        "style_death_rate": map_signals["style_death_rate"],
        "style_wide_rate": map_signals["style_wide_rate"],
        "map_lead_score": map_signals["map_lead_score"],
        "map_death_score": map_signals["map_death_score"],
        "map_soft_trip_score": map_signals["map_soft_trip_score"],
        "map_wide_risk_score": map_signals["map_wide_risk_score"],
        "career_starts": _summary_part(runner["career_summary"], 0),
        "career_wins": _summary_part(runner["career_summary"], 1),
        "season_starts": _summary_part(runner["this_season_summary"], 0),
        "season_wins": _summary_part(runner["this_season_summary"], 1),
        "last_5_avg_adj_margin": _avg(primary_adj_margins[:5]),
        "last_10_avg_adj_margin": _avg(primary_adj_margins[:10]),
        "last_5_best_adj_margin": min(primary_adj_margins[:5]) if primary_adj_margins[:5] else None,
        "last_5_avg_sp": _avg(primary_prices[:5]),
        "last_5_win_rate": round(len(primary_win_rate_source[:5]) / min(len((last_runs or valid_recent_lines)[:5]), 5), 4) if (last_runs or valid_recent_lines)[:5] else None,
        "same_driver_avg_adj_margin": _avg([run["adjusted_margin"] for run in same_driver_runs if run["adjusted_margin"] is not None]),
        "same_driver_starts": len(same_driver_runs),
        "driver_last_30_starts": driver_stats["starts_30"],
        "driver_last_30_wins": driver_stats["wins_30"],
        "driver_last_30_win_rate": driver_stats["win_rate_30"],
        "driver_last_90_starts": driver_stats["starts_90"],
        "driver_last_90_wins": driver_stats["wins_90"],
        "driver_last_90_win_rate": driver_stats["win_rate_90"],
        "trainer_last_30_starts": trainer_stats["starts_30"],
        "trainer_last_30_wins": trainer_stats["wins_30"],
        "trainer_last_30_win_rate": trainer_stats["win_rate_30"],
        "trainer_last_90_starts": trainer_stats["starts_90"],
        "trainer_last_90_wins": trainer_stats["wins_90"],
        "trainer_last_90_win_rate": trainer_stats["win_rate_90"],
        "trainer_change_flag": _trainer_change_flag(last_runs, runner["nominated_trainer"]),
        "driver_change_flag": _driver_change_flag(last_runs, runner["nominated_driver"]),
        "form_bmr_secs": bmr_secs,
        "form_bmr_dist_rge_secs": bmr_dist_rge_secs,
        "days_since_last_run": days_since_last_run,
        "race_nr_ceiling": race_nr_ceiling,
        "nr_headroom": nr_headroom,
        "last_5_avg_stake": last_5_avg_stake,
    }


def _rolling_person_stats(conn: sqlite3.Connection, field_name: str, person_name: object) -> dict[str, object]:
    if not person_name:
        return {"starts_30": None, "wins_30": None, "win_rate_30": None, "starts_90": None, "wins_90": None, "win_rate_90": None}

    rows = conn.execute(
        f"""
        SELECT run_date, finish_position
        FROM horse_runs
        WHERE UPPER({field_name}) = UPPER(?)
          AND COALESCE(race_type, 'RACE') <> 'TRIAL'
        ORDER BY _sort_run_date(run_date) DESC
        LIMIT 200
        """,
        (str(person_name).strip(),),
    ).fetchall()

    starts_30 = min(len(rows), 30)
    starts_90 = min(len(rows), 90)
    wins_30 = sum(1 for row in rows[:starts_30] if row["finish_position"] == 1)
    wins_90 = sum(1 for row in rows[:starts_90] if row["finish_position"] == 1)

    return {
        "starts_30": starts_30,
        "wins_30": wins_30,
        "win_rate_30": round(wins_30 / starts_30, 4) if starts_30 else None,
        "starts_90": starts_90,
        "wins_90": wins_90,
        "win_rate_90": round(wins_90 / starts_90, 4) if starts_90 else None,
    }


def _trainer_change_flag(last_runs: list[dict[str, object]], nominated_trainer: object) -> int | None:
    if not nominated_trainer or not last_runs or not last_runs[0]["trainer_name"]:
        return None
    return int(_normalize_name(last_runs[0]["trainer_name"]) != _normalize_name(str(nominated_trainer)))


def _driver_change_flag(last_runs: list[dict[str, object]], nominated_driver: object) -> int | None:
    if not nominated_driver or not last_runs or not last_runs[0]["driver_name"]:
        return None
    return int(_normalize_name(last_runs[0]["driver_name"]) != _normalize_name(str(nominated_driver)))


def _summary_part(summary_text: object, idx: int) -> int | None:
    if not summary_text:
        return None
    parts = str(summary_text).split("-")
    if len(parts) <= idx:
        return None
    try:
        return int(parts[idx])
    except ValueError:
        return None


def _avg(values: list[float]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    return round(sum(cleaned) / len(cleaned), 4) if cleaned else None


def _normalize_name(name: str) -> str:
    return " ".join(name.upper().split())


def _sectional_deltas_vs_par(recent_lines: list[dict[str, object]], track_pars: dict | None) -> list[float]:
    deltas: list[float] = []
    for line in recent_lines:
        if _truthy(line.get("null_run")):
            continue
        if line.get("last_half") is None:
            continue
        par = lookup_race_par(track_pars, line.get("track_name"), line.get("distance"), str(line.get("condition") or "Good"))
        par_last_half = par.get("par_last_half")
        if par_last_half is None:
            continue
        adjusted_last_half = float(line["last_half"]) + float(line.get("tempo_adjustment") or 0.0)
        deltas.append(round(adjusted_last_half - float(par_last_half), 3))
    return deltas


def _truthy(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _to_float_local(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _map_signals(recent_lines: list[dict[str, object]], barrier: object) -> dict[str, float | None]:
    if not recent_lines:
        return {
            "style_lead_rate": None,
            "style_forward_rate": None,
            "style_restrained_rate": None,
            "style_death_rate": None,
            "style_wide_rate": None,
            "map_lead_score": _barrier_map_bonus(barrier, "lead"),
            "map_death_score": _barrier_map_bonus(barrier, "death"),
            "map_soft_trip_score": _barrier_map_bonus(barrier, "soft"),
            "map_wide_risk_score": _barrier_map_bonus(barrier, "wide"),
        }

    comments = [str(line.get("raw_comment") or "").lower() for line in recent_lines]
    lead = sum(1 for c in comments if " led" in f" {c}" or "leader" in c)
    forward = sum(1 for c in comments if "worked forward" in c or "pressed forward" in c or "fwd" in c)
    restrained = sum(1 for c in comments if "restrained" in c)
    death = sum(1 for c in comments if "outside leader" in c or "death seat" in c)
    wide = sum(1 for c in comments if "three wide" in c or "3 wide" in c or "wide no cover" in c)
    total = len(comments)

    lead_rate = round(lead / total, 4)
    forward_rate = round(forward / total, 4)
    restrained_rate = round(restrained / total, 4)
    death_rate = round(death / total, 4)
    wide_rate = round(wide / total, 4)

    return {
        "style_lead_rate": lead_rate,
        "style_forward_rate": forward_rate,
        "style_restrained_rate": restrained_rate,
        "style_death_rate": death_rate,
        "style_wide_rate": wide_rate,
        "map_lead_score": round((lead_rate * 1.2) + (forward_rate * 0.6) + _barrier_map_bonus(barrier, "lead"), 4),
        "map_death_score": round((death_rate * 1.1) + (forward_rate * 0.4) + _barrier_map_bonus(barrier, "death"), 4),
        "map_soft_trip_score": round((lead_rate * 0.6) - (restrained_rate * 0.15) + _barrier_map_bonus(barrier, "soft"), 4),
        "map_wide_risk_score": round((wide_rate * 1.1) + (death_rate * 0.35) + _barrier_map_bonus(barrier, "wide"), 4),
    }


def _barrier_map_bonus(barrier: object, mode: str) -> float:
    text = str(barrier or "").upper().strip()
    if not text:
        return 0.0
    num = None
    if text.startswith("FR"):
        try:
            num = int(text[2:])
        except ValueError:
            return 0.0
        if mode == "lead":
            return max(-0.35, 0.45 - 0.08 * (num - 1))
        if mode == "death":
            return max(-0.1, 0.25 - 0.03 * (num - 1))
        if mode == "soft":
            return max(-0.2, 0.28 - 0.05 * (num - 1))
        if mode == "wide":
            return max(0.0, -0.12 + 0.07 * (num - 4))
    if text.startswith("SR"):
        try:
            num = int(text[2:])
        except ValueError:
            num = 3
        if mode == "lead":
            return -0.3 - 0.04 * (num - 1)
        if mode == "death":
            return -0.15 - 0.03 * (num - 1)
        if mode == "soft":
            return -0.12 - 0.02 * (num - 1)
        if mode == "wide":
            return 0.12 + 0.05 * (num - 1)
    return 0.0


def _parse_bmr_secs(bmr: object) -> float | None:
    """Convert a BMR string like 'TR1:57.1MS' or '1:57.1MS' to total seconds.

    Strips any leading track-record prefix (letters before the digit) and any
    trailing start-type suffix (letters after the final digit).
    Returns None if the value is absent or cannot be parsed.
    """
    if not bmr:
        return None
    text = str(bmr).strip()
    match = re.search(r"(\d):(\d{2})\.(\d)", text)
    if not match:
        return None
    minutes = int(match.group(1))
    seconds = int(match.group(2))
    tenths = int(match.group(3))
    return round(minutes * 60 + seconds + tenths / 10, 1)


def _days_since_last_run(recent_lines: list[dict[str, object]], meeting_date: object) -> int | None:
    """Return days between the most recent valid recent-line run_date and meeting_date.

    Both dates are expected in 'D Mon YYYY' format (e.g. '29 Mar 2026').
    Returns None if either date cannot be parsed or no valid lines exist.
    """
    MONTHS = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }
    import datetime

    def _parse(text: object):
        parts = str(text or "").split()
        if len(parts) != 3:
            return None
        try:
            return datetime.date(int(parts[2]), MONTHS.get(parts[1], 0), int(parts[0]))
        except (ValueError, KeyError):
            return None

    race_date = _parse(meeting_date)
    if race_date is None:
        return None

    latest = None
    for line in recent_lines:
        if _truthy(line.get("null_run")):
            continue
        d = _parse(line.get("run_date"))
        if d is None:
            continue
        if latest is None or d > latest:
            latest = d

    if latest is None:
        return None
    return (race_date - latest).days


def _cap_outlier_stakes(stakes: list[float]) -> list[float]:
    """Cap a single outlier stake at 30% of the second-highest value.

    If the highest stake is more than 1.5x the second-highest (e.g. a horse
    won one big feature race but normally races at a much lower level), that
    result is replaced with second_highest * 0.30 so it does not inflate the
    horse's apparent class level.
    """
    if len(stakes) < 2:
        return stakes
    sorted_desc = sorted(stakes, reverse=True)
    highest = sorted_desc[0]
    second = sorted_desc[1]
    if second <= 0 or highest <= 1.5 * second:
        return stakes
    cap = second * 0.30
    replaced = False
    result = []
    for s in stakes:
        if not replaced and s == highest:
            result.append(cap)
            replaced = True
        else:
            result.append(s)
    return result


def _parse_race_nr_ceiling(class_name: object) -> float | None:
    """Extract the NR ceiling from a class_name string like 'NR up to 52. ...'

    Returns the ceiling as a float, or None for MAIDEN races and unrecognised formats.
    """
    if not class_name:
        return None
    text = str(class_name)
    match = re.search(r"NR\s+up\s+to\s+(\d+)", text, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None


def _sort_run_date(date_text: object) -> int:
    if not date_text:
        return 0
    months = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }
    try:
        day, mon, year = str(date_text).split()
        return int(year) * 10000 + months[mon.upper()] * 100 + int(day)
    except Exception:
        return 0
