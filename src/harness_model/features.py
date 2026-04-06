from __future__ import annotations

import csv
import json
import math
import re
import sqlite3
import statistics
from datetime import date
from pathlib import Path

from .track_pars import _nr_to_grade_band, lookup_race_par

# Metres of margin adjustment per NR point of grade difference.
# A horse that ran in NR43 dropping to NR40 gets each margin reduced by 3 * factor.
# Calibration starting point — adjust against results once ≥30 grade-drop winners
# have been observed.
_NR_MARGIN_FACTOR = 0.5


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
            COALESCE(rr.form_nr, hp.nr_rating) AS nr_rating,
            COALESCE(rr.form_career_summary, hp.career_summary) AS career_summary,
            COALESCE(rr.form_this_season_summary, hp.this_season_summary) AS this_season_summary,
            COALESCE(rr.form_last_season_summary, hp.last_season_summary) AS last_season_summary,
            rr.form_bmr,
            rr.form_bmr_dist_rge,
            rr.form_dist_rge_summary,
            rr.trainer_change_manual,
            rr.race_purse
        FROM race_runners rr
        LEFT JOIN meetings m ON m.meeting_code = rr.meeting_code
        LEFT JOIN horse_profiles hp ON hp.horse_id = rr.horse_id
        WHERE COALESCE(rr.scratched, 0) = 0
        ORDER BY rr.meeting_code, rr.race_number, rr.runner_number
        """
    ).fetchall()

    rows: list[dict[str, object]] = []
    for runner in runners:
        all_last_runs = conn.execute(
            """
            SELECT *
            FROM horse_runs
            WHERE horse_id = ?
              AND COALESCE(race_type, 'RACE') <> 'TRIAL'
              AND COALESCE(null_run, 0) = 0
            ORDER BY _sort_run_date(run_date) DESC
            LIMIT 20
            """,
            (runner["horse_id"],),
        ).fetchall()
        last_runs = _runs_before_meeting(
            [dict(row) for row in all_last_runs],
            runner["meeting_date"],
        )[:10]
        recent_lines = conn.execute(
            """
            SELECT *
            FROM runner_recent_lines
            WHERE meeting_code = ?
              AND race_number = ?
              AND horse_id = ?
            ORDER BY _sort_run_date(run_date) DESC
            """,
            (runner["meeting_code"], runner["race_number"], runner["horse_id"]),
        ).fetchall()

        rows.append(
            _build_feature_row(
                conn,
                dict(runner),
                last_runs,
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
    # Cap at 50m — values above this are almost always a parser mis-parse
    # (track distance matching the fallback regex). Legitimate harness margins
    # rarely exceed 40m; any higher value corrupts averages and floors scores.
    adjusted = [run["adjusted_margin"] for run in last_runs if run["adjusted_margin"] is not None and run["adjusted_margin"] <= 50.0]
    prices = [run["start_price"] for run in last_runs if run["start_price"] is not None]
    wins = [run for run in last_runs if run["finish_position"] == 1]
    same_driver_runs = [
        run for run in last_runs
        if run["driver_name"] and runner["nominated_driver"]
        and _normalize_name(run["driver_name"]) == _normalize_name(str(runner["nominated_driver"]))
    ]

    driver_stats = _rolling_person_stats(conn, "driver_name", runner["nominated_driver"])
    trainer_stats = _rolling_person_stats(conn, "trainer_name", runner["nominated_trainer"])
    race_par = lookup_race_par(
        track_pars, runner["track_name"], runner["race_distance"],
        nr_ceiling=_parse_race_nr_ceiling(runner.get("class_name")),
    )
    sectional_deltas = _sectional_deltas_vs_par(recent_lines, track_pars)
    valid_recent_lines = [line for line in recent_lines if not _truthy(line.get("null_run"))]
    line_comment_adjustments = [float(line["comment_adjustment"]) for line in valid_recent_lines if line.get("comment_adjustment") not in (None, "")]
    line_tempo_adjustments = [float(line["tempo_adjustment"]) for line in valid_recent_lines if line.get("tempo_adjustment") not in (None, "")]
    tempo_flag_count = sum(1 for line in recent_lines if _to_float_local(line.get("tempo_adjustment")) not in (None, 0.0))
    null_line_count = sum(1 for line in recent_lines if _truthy(line.get("null_run")))
    raw_recent_margins = [float(line["raw_margin"]) for line in valid_recent_lines if line.get("raw_margin") not in (None, "")]
    adj_recent_margins = [float(line["adjusted_margin"]) for line in valid_recent_lines if line.get("adjusted_margin") not in (None, "") and float(line["adjusted_margin"]) <= 50.0]
    primary_adj_margins = adjusted if adjusted else adj_recent_margins
    primary_prices = prices
    primary_win_rate_source = wins if last_runs else [line for line in valid_recent_lines if line.get("finish_position") == 1]
    primary_source = last_runs if last_runs else valid_recent_lines
    primary_source_5 = primary_source[:5]
    top3_in_5 = [run for run in primary_source_5 if run.get("finish_position") in (1, 2, 3)]
    competitive_in_5 = [
        run for run in primary_source_5
        if run.get("adjusted_margin") is not None and float(run["adjusted_margin"]) <= 3.0
    ]
    last_5_top3_rate = round(len(top3_in_5) / len(primary_source_5), 4) if primary_source_5 else None
    last_5_competitive_rate = round(len(competitive_in_5) / len(primary_source_5), 4) if primary_source_5 else None
    map_signals = _map_signals(valid_recent_lines, runner.get("barrier"))
    bmr_secs = _parse_bmr_secs(runner.get("form_bmr"))
    # Avg of best 3 runs at today's distance from profile data — more reliable than
    # the single form-page best.  Falls back to the form-page value when no profile
    # data exists at this distance (new horse or different distance range).
    bmr_dist_rge_secs = (
        _compute_bmr_avg_top3(last_runs, runner.get("race_distance"))
        or _parse_bmr_secs(runner.get("form_bmr_dist_rge"))
    )
    days_since_last_run = _days_since_last_run(recent_lines, runner.get("meeting_date"))
    second_up_improvement = _second_up_improvement(days_since_last_run, recent_lines)
    race_nr_ceiling = _parse_race_nr_ceiling(runner.get("class_name"))
    race_nr_floor = _parse_race_nr_floor(runner.get("class_name"))
    nr_rating = runner["nr_rating"]
    nr_headroom = round(race_nr_ceiling - float(nr_rating), 1) if (race_nr_ceiling is not None and nr_rating is not None) else None
    # Use all recent lines (including null runs) — line_nr_ceiling is a property of
    # the race entered, not the horse's performance, so null runs are still valid.
    recent_line_nr_ceilings = [
        line["line_nr_ceiling"] for line in recent_lines[:5]
        if line.get("line_nr_ceiling") is not None
    ]
    avg_recent_nr_ceiling = _avg(recent_line_nr_ceilings) if len(recent_line_nr_ceilings) >= 2 else None
    # Negative = dropping in grade (today's ceiling lower than recent), positive = rising.
    nr_grade_delta = round(race_nr_ceiling - avg_recent_nr_ceiling, 1) if (race_nr_ceiling is not None and avg_recent_nr_ceiling is not None) else None
    # Class-adjusted margins — each recent-line margin is shifted by the NR grade
    # difference between that run and today's race.  A horse that ran 15m back in NR43
    # competing today in NR40 gets 1.5m reduction (3 pts × 0.5 factor); a horse that
    # ran 15m back in NR37 now going up to NR40 gets +1.5m added.
    # Lines WITHOUT line_nr_ceiling are included unadjusted so the full 5-run window
    # is preserved — excluding them causes selection bias toward the runs where we
    # happen to have NR data (often the worst-performing ones).
    # The metric is only output when ≥1 line was actually NR-adjusted (otherwise it
    # would be identical to recent_line_avg_adj_margin and add no value).
    # For avg: floor at 0.0 so a single standout run doesn't dominate the average.
    # For ceiling (best): uncapped — a win in a tougher grade should produce a
    # negative class-adj margin (better than par), which _neg_scale rewards positively.
    class_adj_recent_margins: list[float] = []       # capped at 0.0 — used for avg
    class_adj_recent_margins_raw: list[float] = []   # uncapped — used for ceiling
    _class_adj_nr_count = 0
    if race_nr_ceiling is not None:
        for line in valid_recent_lines[:5]:
            if line.get("adjusted_margin") in (None, ""):
                continue
            margin = float(line["adjusted_margin"])
            if margin > 50.0:
                continue
            if line.get("line_nr_ceiling") is not None:
                adj = margin - (float(line["line_nr_ceiling"]) - race_nr_ceiling) * _NR_MARGIN_FACTOR
                class_adj_recent_margins.append(max(0.0, adj))
                class_adj_recent_margins_raw.append(adj)
                _class_adj_nr_count += 1
            else:
                class_adj_recent_margins.append(margin)
                class_adj_recent_margins_raw.append(margin)
    # Ceiling support rate and best-run index — mirror the priority logic in odds.py
    # so both columns align with whichever margin list ceiling_adj is drawn from.
    _ceiling_margins: list[float] = (
        class_adj_recent_margins_raw if (_class_adj_nr_count >= 1 and class_adj_recent_margins_raw)
        else (primary_adj_margins[:5] if primary_adj_margins else adj_recent_margins)
    )
    if _ceiling_margins:
        _ceiling_val = min(_ceiling_margins)
        ceiling_support_rate: float | None = round(
            sum(1 for m in _ceiling_margins if m <= _ceiling_val + 6.0) / len(_ceiling_margins), 4
        )
        ceiling_best_run_index: int | None = _ceiling_margins.index(_ceiling_val)
    else:
        ceiling_support_rate = None
        ceiling_best_run_index = None

    raw_run_purses = [line["run_purse"] for line in recent_lines if line.get("run_purse") is not None]
    capped_run_purses = _cap_outlier_stakes([float(p) for p in raw_run_purses])
    avg_recent_run_purse = _avg(capped_run_purses[:5])
    race_purse = runner.get("race_purse")
    class_delta = round(float(race_purse) - avg_recent_run_purse, 0) if (race_purse is not None and avg_recent_run_purse is not None) else None
    career_starts = _summary_part(runner["career_summary"], 0)
    career_wins = _summary_part(runner["career_summary"], 1)
    # Require ≥5 career starts before trusting the win rate — fewer starts give
    # too noisy a signal (1/2 = 50% and 1/30 = 3% should not be treated equally).
    career_win_rate = round(career_wins / career_starts, 4) if (career_starts is not None and career_starts >= 5 and career_wins is not None) else None

    sp_class_values: list[float] = []
    sp_values: list[float] = []
    for line in valid_recent_lines[:5]:
        sp = _to_float_local(line.get("run_sp"))
        purse = _to_float_local(line.get("run_purse"))
        if sp and sp > 0:
            sp_values.append(sp)
            if purse and purse > 0:
                sp_class_values.append(-math.log(sp) * (purse / 8000.0))
    recent_line_last_sp = sp_values[0] if sp_values else None
    sp_class_score = _avg(sp_class_values) if sp_class_values else None
    # Compute SP trend in implied-probability space so that longshot-to-longshot
    # moves ($81→$31) produce a tiny signal while genuine shortening into
    # competitive prices ($31→$5) produces a meaningful one.
    # Positive = shortening (market gaining confidence), negative = drifting.
    if len(sp_values) >= 2:
        last_prob = 1.0 / sp_values[0]
        prior_avg_prob = _avg([1.0 / s for s in sp_values[1:]])
        sp_trend = (last_prob - prior_avg_prob) if prior_avg_prob else None
    else:
        sp_trend = None

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
        "career_starts": career_starts,
        "career_wins": career_wins,
        "career_win_rate": career_win_rate,
        "season_starts": _summary_part(runner["this_season_summary"], 0),
        "season_wins": _summary_part(runner["this_season_summary"], 1),
        "last_5_avg_adj_margin": _avg(primary_adj_margins[:5]),
        "last_10_avg_adj_margin": _avg(primary_adj_margins[:10]),
        "last_5_best_adj_margin": min(primary_adj_margins[:5]) if primary_adj_margins[:5] else None,
        "last_5_avg_sp": _avg(primary_prices[:5]),
        "last_5_win_rate": round(len(primary_win_rate_source[:5]) / min(len((last_runs or valid_recent_lines)[:5]), 5), 4) if (last_runs or valid_recent_lines)[:5] else None,
        "last_5_top3_rate": last_5_top3_rate,
        "last_5_competitive_rate": last_5_competitive_rate,
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
        "trainer_page_season_win_rate": _trainer_page_win_rate(conn, runner["nominated_trainer"]),
        "trainer_change_manual": int(runner["trainer_change_manual"]) if runner.get("trainer_change_manual") is not None else 0,
        "driver_page_season_win_rate": _driver_page_win_rate(conn, runner["nominated_driver"]),
        "form_bmr_secs": bmr_secs,
        "form_bmr_dist_rge_secs": bmr_dist_rge_secs,
        "dist_strike_rate_ratio": _dist_strike_rate_ratio(
            runner.get("form_dist_rge_summary"),
            runner.get("career_summary"),
        ),
        "dist_rge_starts": _summary_part(runner.get("form_dist_rge_summary"), 0),
        "days_since_last_run": days_since_last_run,
        "second_up_improvement": second_up_improvement,
        "race_nr_ceiling": race_nr_ceiling,
        "race_nr_floor": race_nr_floor,
        "nr_headroom": nr_headroom,
        "avg_recent_nr_ceiling": avg_recent_nr_ceiling,
        "nr_grade_delta": nr_grade_delta,
        "recent_line_avg_class_adj_margin": _avg(class_adj_recent_margins) if (len(class_adj_recent_margins) >= 2 and _class_adj_nr_count >= 1) else None,
        "recent_line_best_class_adj_margin": min(class_adj_recent_margins_raw) if (_class_adj_nr_count >= 1 and class_adj_recent_margins_raw) else None,
        "ceiling_support_rate": ceiling_support_rate,
        "ceiling_best_run_index": ceiling_best_run_index,
        "race_purse": race_purse,
        "avg_recent_run_purse": avg_recent_run_purse,
        "class_delta": class_delta,
        "recent_line_last_sp": recent_line_last_sp,
        "recent_line_sp_class_score": sp_class_score,
        "recent_line_sp_trend": sp_trend,
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
    prior_trainer = _latest_known_person(last_runs, "trainer_name")
    if not nominated_trainer or not prior_trainer:
        return None
    return int(not _person_names_match(prior_trainer, str(nominated_trainer)))


def _trainer_change_recent_flag(last_runs: list[dict[str, object]], nominated_trainer: object) -> int | None:
    if not nominated_trainer:
        return None

    nominated = str(nominated_trainer)
    known_trainers = [
        str(run["trainer_name"])
        for run in last_runs
        if run.get("trainer_name")
    ]
    if not known_trainers:
        return None

    if not _person_names_match(known_trainers[0], nominated):
        return 1

    streak = 0
    for trainer in known_trainers:
        if _person_names_match(trainer, nominated):
            streak += 1
        else:
            break

    if streak <= 2 and len(known_trainers) > streak:
        return 1
    return 0


def _driver_change_flag(last_runs: list[dict[str, object]], nominated_driver: object) -> int | None:
    prior_driver = _latest_known_person(last_runs, "driver_name")
    if not nominated_driver or not prior_driver:
        return None
    return int(not _person_names_match(prior_driver, str(nominated_driver)))


def _driver_page_win_rate(conn: sqlite3.Connection, driver_name: object) -> float | None:
    if not driver_name:
        return None
    slug = str(driver_name).lower().strip().replace(" ", "-")
    row = conn.execute(
        "SELECT season_win_rate FROM driver_stats WHERE driver_slug = ?",
        (slug,),
    ).fetchone()
    return float(row["season_win_rate"]) if row and row["season_win_rate"] is not None else None


def _trainer_page_win_rate(conn: sqlite3.Connection, trainer_name: object) -> float | None:
    if not trainer_name:
        return None
    slug = str(trainer_name).lower().strip().replace(" ", "-")
    row = conn.execute(
        "SELECT season_win_rate FROM trainer_stats WHERE trainer_slug = ?",
        (slug,),
    ).fetchone()
    return float(row["season_win_rate"]) if row and row["season_win_rate"] is not None else None


def _dist_strike_rate_ratio(
    dist_rge_summary: object,
    career_summary: object,
) -> float | None:
    """Ratio of distance win rate to career win rate.

    Returns None (neutral) when:
    - dist_starts < 2 (insufficient distance sample)
    - career_starts == 0 (no career data)
    - career win rate == 0 (avoid division by zero; treat as neutral)
    """
    dist_starts = _summary_part(dist_rge_summary, 0)
    dist_wins = _summary_part(dist_rge_summary, 1)
    career_starts = _summary_part(career_summary, 0)
    career_wins = _summary_part(career_summary, 1)

    if dist_starts is None or dist_starts < 2:
        return None
    if not career_starts or not career_wins:
        return None

    career_win_rate = career_wins / career_starts
    if career_win_rate == 0.0:
        return None

    dist_win_rate = dist_wins / dist_starts
    return round(dist_win_rate / career_win_rate, 4)


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


def _latest_known_person(last_runs: list[dict[str, object]], field_name: str) -> str | None:
    for run in last_runs:
        value = run.get(field_name)
        if value:
            return str(value)
    return None


def _person_names_match(left: str, right: str) -> bool:
    left_tokens = _name_tokens(left)
    right_tokens = _name_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    if left_tokens[-1] != right_tokens[-1]:
        return False

    left_initials = "".join(token[0] for token in left_tokens[:-1] if token)
    right_initials = "".join(token[0] for token in right_tokens[:-1] if token)
    if not left_initials or not right_initials:
        return False
    shorter = left_initials if len(left_initials) <= len(right_initials) else right_initials
    longer = right_initials if len(right_initials) > len(left_initials) else left_initials
    return longer.startswith(shorter)


def _name_tokens(name: str) -> list[str]:
    cleaned = re.sub(r"[^A-Z ]+", " ", _normalize_name(name))
    return [token for token in cleaned.split() if token]


def _runs_before_meeting(last_runs: list[dict[str, object]], meeting_date: object) -> list[dict[str, object]]:
    meeting_key = _sort_run_date(meeting_date)
    if meeting_key <= 0:
        return last_runs
    return [run for run in last_runs if _sort_run_date(run.get("run_date")) < meeting_key]


def _sectional_deltas_vs_par(recent_lines: list[dict[str, object]], track_pars: dict | None) -> list[float]:
    deltas: list[float] = []
    for line in recent_lines:
        if _truthy(line.get("null_run")):
            continue
        if line.get("last_half") is None:
            continue
        par = lookup_race_par(
            track_pars, line.get("track_name"), line.get("distance"),
            str(line.get("condition") or "Good"),
            nr_ceiling=line.get("line_nr_ceiling"),
        )
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
        # Interaction term: inside draw × lead tendency compound bonus.
        # A Fr1 horse that leads 70% is worth more than Fr1 + 70% summed separately —
        # the draw makes the tendency more likely to be realised, and realising it is
        # disproportionately valuable.  Only fires when real form-line rates are available.
        # Lead rate is discounted by barrier via _barrier_commitment_scale: a 60% leader
        # earns that rate from inside draws and is unlikely to lead from Fr6; a 90%+ horse
        # gets minimal discount as it will find the front from almost anywhere, but even
        # fully committed front-runners face a marginal penalty from wide draws.
        "map_lead_score": round(
            (lead_rate * _barrier_commitment_scale(_parse_barrier_num(barrier) or 1, lead_rate) * 1.2)
            + (forward_rate * 0.6)
            + _barrier_map_bonus(barrier, "lead")
            + _barrier_lead_interaction(barrier, lead_rate, forward_rate),
            4,
        ),
        "map_death_score": round((death_rate * 1.1) + (forward_rate * 0.4) + _barrier_map_bonus(barrier, "death"), 4),
        "map_soft_trip_score": round((lead_rate * 0.6) - (restrained_rate * 0.15) + _barrier_map_bonus(barrier, "soft"), 4),
        "map_wide_risk_score": round((wide_rate * 1.1) + (death_rate * 0.35) + _barrier_map_bonus(barrier, "wide"), 4),
    }


def _parse_barrier_num(barrier: object) -> int | None:
    text = str(barrier or "").upper().strip()
    if text.startswith("FR"):
        try:
            return int(text[2:])
        except ValueError:
            return None
    return None


def _barrier_commitment_scale(barrier_num: int, lead_rate: float) -> float:
    """Effective scale for the lead_rate contribution to map_lead_score.

    Blends between a non-committed scale (full barrier penalty) and a committed
    front-runner scale (minimal penalty) based on how consistently the horse leads.
    Even fully committed horses face a marginal discount from wide draws —
    they need to do more work to cross from barrier 8 than barrier 1.
    """
    commitment = max(0.0, (lead_rate - 0.30) / 0.70)  # 0.0 at ≤30%, 1.0 at 100%
    _base = {1: 1.00, 2: 0.90, 3: 0.78, 4: 0.65, 5: 0.52, 6: 0.38, 7: 0.25, 8: 0.15}
    _max  = {1: 1.00, 2: 0.98, 3: 0.96, 4: 0.93, 5: 0.90, 6: 0.87, 7: 0.84, 8: 0.82}
    base_scale = _base.get(barrier_num, 0.15)
    max_scale  = _max.get(barrier_num, 0.80)
    return base_scale + commitment * (max_scale - base_scale)


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


def _barrier_lead_interaction(barrier: object, lead_rate: float, forward_rate: float) -> float:
    """Extra bonus when inside draw AND lead tendency compound each other.

    An Fr1 horse that leads 70% is more valuable than Fr1 + 70% added separately:
    the draw makes the tendency more likely to be realised, and the lead is
    disproportionately valuable in harness racing.

    Scale: Fr1 × high lead tendency → +0.30–0.35 bonus.
           Fr4+ → 0.0 (draw no longer amplifies lead ability).
           Lead tendency ≤ 30% → 0.0 (not a genuine speed horse).
    """
    text = str(barrier or "").upper().strip()
    if not text.startswith("FR"):
        return 0.0
    try:
        num = int(text[2:])
    except ValueError:
        return 0.0

    # draw_factor: Fr1=1.5, Fr2=1.0, Fr3=0.5, Fr4+=0.0
    draw_factor = max(0.0, (5 - num) * 0.5 - 0.5)
    if draw_factor == 0.0:
        return 0.0

    # Combined speed tendency; forward runs count half (press early but not lead)
    speed_tendency = lead_rate + forward_rate * 0.5
    if speed_tendency <= 0.30:
        return 0.0

    return round((speed_tendency - 0.30) * draw_factor * 0.40, 4)


def _compute_bmr_avg_top3(
    last_runs: list[dict[str, object]],
    race_distance: object,
    tolerance_m: int = 200,
) -> float | None:
    """Average of the 3 fastest mile rates from horse_runs at a similar distance.

    Using the average of 3 performances rather than the single best handles
    the case where a horse posted one exceptional time but is typically slower —
    a genuine 1:57.0 horse should be averaging 1:57s, not posting it once.

    Returns None when fewer than 1 qualifying run exists (caller falls back to
    the form-page BMR in that case).
    """
    if not last_runs or race_distance is None:
        return None
    try:
        target = int(float(str(race_distance)))
    except (TypeError, ValueError):
        return None

    times: list[float] = []
    for run in last_runs:
        dist = run.get("distance")
        mile_rate = run.get("mile_rate")
        if dist is None or mile_rate is None:
            continue
        try:
            d = int(dist)
        except (TypeError, ValueError):
            continue
        if abs(d - target) > tolerance_m:
            continue
        secs = _parse_bmr_secs(mile_rate)
        if secs is not None:
            times.append(secs)

    if not times:
        return None
    times.sort()  # ascending = fastest first
    best3 = times[:3]
    return round(sum(best3) / len(best3), 1)


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


def _parse_run_date(text: object) -> "date | None":
    _MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
               "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
    parts = str(text or "").split()
    if len(parts) != 3:
        return None
    try:
        return date(int(parts[2]), _MONTHS[parts[1].title()], int(parts[0]))
    except (ValueError, KeyError):
        return None


def _second_up_improvement(days_since_last_run: int | None, recent_lines: list[dict]) -> float | None:
    """Positive metres of improvement when today is second-up after a first-up spell run.

    Fires when:
      - days_since_last_run <= 28 (ran within the last month)
      - gap between recent_lines[0] and recent_lines[1] >= 43d (first run back was after a spell)
      - recent_lines[0] adj_margin was better than average of prior 1-3 lines

    Returns the improvement in metres, or None if conditions aren't met.
    """
    if days_since_last_run is None or days_since_last_run > 28:
        return None
    if len(recent_lines) < 2:
        return None
    d0 = _parse_run_date(recent_lines[0].get("run_date"))
    d1 = _parse_run_date(recent_lines[1].get("run_date"))
    if d0 is None or d1 is None:
        return None
    if abs((d0 - d1).days) < 43:
        return None
    # Most recent run was first-up — compare its margin to prior form
    first_up_adj = float(recent_lines[0].get("adjusted_margin") or 0.0)
    prior_margins = [
        float(line["adjusted_margin"])
        for line in recent_lines[1:4]
        if line.get("adjusted_margin") not in (None, "")
    ]
    if not prior_margins:
        return None
    improvement = round(sum(prior_margins) / len(prior_margins) - first_up_adj, 2)
    return improvement if improvement > 0 else None


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
    """Extract the NR ceiling from a class_name string.

    Handles three formats:
      - 'NR up to 52. ...'       → 52  (standard ceiling race)
      - 'NR 45 to 55. ...'       → 55  (banded race — ceiling is upper bound)
      - 'Also eligible NR.47. ..' → 47  (LTW/win-based race with NR also-eligible clause)

    Returns None for MAIDEN, win-based, Listed, and unrecognised formats.
    """
    if not class_name:
        return None
    text = str(class_name)
    match = re.search(r"NR\s+up\s+to\s+(\d+)", text, re.IGNORECASE)
    if match:
        return float(match.group(1))
    match = re.search(r"NR\s+(\d+)\s+to\s+(\d+)", text, re.IGNORECASE)
    if match:
        return float(match.group(2))
    match = re.search(r"NR\.(\d+)", text, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None


def _parse_race_nr_floor(class_name: object) -> float | None:
    """Extract the NR floor from a banded class_name like 'NR 45 to 55. ...'

    Returns None for ceiling-only races (NR up to XX) and unrecognised formats.
    Only banded races have a meaningful floor — all other race types allow any NR
    from 0 up to the ceiling.
    """
    if not class_name:
        return None
    text = str(class_name)
    match = re.search(r"NR\s+(\d+)\s+to\s+(\d+)", text, re.IGNORECASE)
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


def generate_track_pars_from_db(conn: sqlite3.Connection) -> dict:
    """Build a track par database from last_half sectionals in runner_recent_lines.

    Uses the median of trimmed values (top/bottom 5% removed) per
    track_name / distance / condition combination.  Only includes cells with
    n >= 10 samples to avoid spurious pars from thin data.

    Returns a dict in the same structure as track_pars.json so it can be
    passed directly to lookup_race_par().
    """
    # Sanity bounds: harness last-half times are almost always 52–68 seconds.
    rows = conn.execute(
        """
        SELECT track_name, distance, condition, last_half, line_nr_ceiling
        FROM runner_recent_lines
        WHERE last_half IS NOT NULL
          AND last_half BETWEEN 52.0 AND 68.0
          AND distance IS NOT NULL
          AND track_name IS NOT NULL
          AND track_name != ''
        """
    ).fetchall()

    # Group into {track_name: {distance: {condition: [times]}}}
    # and grade bands: {track_name: {distance: {condition: {band: [times]}}}}
    grouped: dict[str, dict[int, dict[str, list[float]]]] = {}
    grade_grouped: dict[str, dict[int, dict[str, dict[str, list[float]]]]] = {}
    for row in rows:
        track = str(row["track_name"])
        dist = int(row["distance"])
        cond = str(row["condition"] or "Good")
        lh = float(row["last_half"])
        grouped.setdefault(track, {}).setdefault(dist, {}).setdefault(cond, []).append(lh)
        band = _nr_to_grade_band(row["line_nr_ceiling"])
        if band:
            grade_grouped.setdefault(track, {}).setdefault(dist, {}).setdefault(cond, {}).setdefault(band, []).append(lh)

    def _trimmed_par(times: list[float]) -> dict[str, object]:
        sorted_times = sorted(times)
        trim = max(1, len(sorted_times) // 20)
        trimmed = sorted_times[trim:-trim] if len(sorted_times) > 20 else sorted_times
        return {
            "par": round(statistics.median(trimmed), 2),
            "std": round(statistics.stdev(trimmed), 2) if len(trimmed) > 1 else 0.0,
            "n": len(times),
        }

    pars: dict[str, dict[str, dict[str, dict[str, object]]]] = {}
    total_cells = 0
    for track, dist_dict in grouped.items():
        pars[track] = {}
        for dist, cond_dict in dist_dict.items():
            pars[track][str(dist)] = {}
            for cond, times in cond_dict.items():
                if len(times) < 10:
                    continue
                sorted_times = sorted(times)
                trim = max(1, len(sorted_times) // 20)
                trimmed = sorted_times[trim:-trim] if len(sorted_times) > 20 else sorted_times
                par = round(statistics.median(trimmed), 2)
                std = round(statistics.stdev(trimmed), 2) if len(trimmed) > 1 else 0.0
                cell: dict[str, object] = {
                    "par": par,
                    "std": std,
                    "n": len(times),
                    "min": round(min(trimmed), 2),
                    "max": round(max(trimmed), 2),
                }
                # Grade-banded pars — stored for all bands regardless of n;
                # the lookup function applies _MIN_GRADE_N at query time.
                band_data = grade_grouped.get(track, {}).get(dist, {}).get(cond, {})
                if band_data:
                    cell["grades"] = {
                        band_key: _trimmed_par(band_times)
                        for band_key, band_times in sorted(band_data.items())
                    }
                pars[track][str(dist)][cond] = cell
                total_cells += 1

    return {
        "generated": date.today().isoformat(),
        "source": "runner_recent_lines",
        "total_runs": len(rows),
        "total_cells": total_cells,
        "pars": pars,
    }


def calibrate_nr_margin_factor(conn: sqlite3.Connection, min_grade_spread: int = 5) -> dict:
    """Estimate the margin-per-NR-point effect from within-horse grade comparisons.

    For each horse with runs at ≥2 NR ceiling values spanning ≥min_grade_spread
    points, computes pairwise slopes (Δmargin / ΔNR) and returns the median
    and mean across all contributing horses.

    Uses within-horse comparisons to avoid selection bias (better horses tend
    to run in higher grades, masking the true grade effect in pooled data).
    """
    rows = conn.execute(
        """
        SELECT horse_id, line_nr_ceiling, AVG(adjusted_margin) AS avg_margin, COUNT(*) AS n
        FROM runner_recent_lines
        WHERE line_nr_ceiling IS NOT NULL
          AND adjusted_margin IS NOT NULL
          AND null_run = 0
          AND adjusted_margin <= 50
        GROUP BY horse_id, line_nr_ceiling
        """
    ).fetchall()

    # Group into {horse_id: {nr_ceiling: avg_margin}}
    by_horse: dict[int, dict[int, float]] = {}
    for row in rows:
        by_horse.setdefault(row["horse_id"], {})[int(row["line_nr_ceiling"])] = float(row["avg_margin"])

    slopes: list[float] = []
    horses_used = 0
    for horse_id, grade_map in by_horse.items():
        nrs = sorted(grade_map.keys())
        if max(nrs) - min(nrs) < min_grade_spread:
            continue
        # All pairwise slopes where the NR spread meets the minimum
        horse_slopes: list[float] = []
        for i, nr_lo in enumerate(nrs):
            for nr_hi in nrs[i + 1:]:
                if nr_hi - nr_lo < min_grade_spread:
                    continue
                delta_margin = grade_map[nr_hi] - grade_map[nr_lo]
                delta_nr = nr_hi - nr_lo
                horse_slopes.append(delta_margin / delta_nr)
        if horse_slopes:
            slopes.append(sum(horse_slopes) / len(horse_slopes))
            horses_used += 1

    if not slopes:
        return {"horses": 0, "median": None, "mean": None, "suggested": _NR_MARGIN_FACTOR}

    slopes_sorted = sorted(slopes)
    n = len(slopes_sorted)
    median = (
        slopes_sorted[n // 2] if n % 2
        else (slopes_sorted[n // 2 - 1] + slopes_sorted[n // 2]) / 2
    )
    mean = sum(slopes_sorted) / n
    # Round to nearest 0.25 for interpretability
    suggested = round(round(median / 0.25) * 0.25, 2)

    return {
        "horses": horses_used,
        "median": round(median, 3),
        "mean": round(mean, 3),
        "suggested": suggested,
        "current": _NR_MARGIN_FACTOR,
        "min_grade_spread": min_grade_spread,
    }


def write_track_pars(track_pars: dict, output_path: str | Path) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(track_pars, indent=2), encoding="utf-8")
    return out
