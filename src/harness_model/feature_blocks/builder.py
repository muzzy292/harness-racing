from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path

from ..track_pars import lookup_race_par

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
            rr.form_bmr_dist_rge,
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
    return _apply_race_map_context(rows)


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
    bmr_dist_rge_secs = _parse_bmr_secs(runner.get("form_bmr_dist_rge"))
    days_since_last_run = _days_since_last_run(recent_lines, runner.get("meeting_date"))
    race_nr_ceiling = _parse_race_nr_ceiling(runner.get("class_name"))
    nr_rating = runner["nr_rating"]
    nr_headroom = round(race_nr_ceiling - float(nr_rating), 1) if (race_nr_ceiling is not None and nr_rating is not None) else None
    raw_stakes = [run["stake"] for run in last_runs if run.get("stake") is not None]
    capped_stakes = _cap_outlier_stakes([float(s) for s in raw_stakes])
    last_5_avg_stake = _avg(capped_stakes[:5])
    raw_run_purses = [line["run_purse"] for line in recent_lines if line.get("run_purse") is not None]
    capped_run_purses = _cap_outlier_stakes([float(p) for p in raw_run_purses])
    avg_recent_run_purse = _avg(capped_run_purses[:5])
    race_purse = runner.get("race_purse")
    class_delta = round(float(race_purse) - avg_recent_run_purse, 0) if (race_purse is not None and avg_recent_run_purse is not None) else None

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
        "map_leader_pos_score": map_signals["map_leader_pos_score"],
        "map_outside_leader_pos_score": map_signals["map_outside_leader_pos_score"],
        "map_behind_leader_pos_score": map_signals["map_behind_leader_pos_score"],
        "map_one_one_pos_score": map_signals["map_one_one_pos_score"],
        "map_three_back_pegs_pos_score": map_signals["map_three_back_pegs_pos_score"],
        "map_one_out_two_back_pos_score": map_signals["map_one_out_two_back_pos_score"],
        "map_back_pegs_pos_score": map_signals["map_back_pegs_pos_score"],
        "map_one_out_back_pos_score": map_signals["map_one_out_back_pos_score"],
        "predicted_map_position": map_signals["predicted_map_position"],
        "predicted_map_bucket": map_signals["predicted_map_bucket"],
        "predicted_map_confidence": map_signals["predicted_map_confidence"],
        "lead_probability": map_signals["lead_probability"],
        "leaders_back_probability": map_signals["leaders_back_probability"],
        "parked_probability": map_signals["parked_probability"],
        "one_one_probability": map_signals["one_one_probability"],
        "three_back_pegs_probability": map_signals["three_back_pegs_probability"],
        "one_out_two_back_probability": map_signals["one_out_two_back_probability"],
        "back_pegs_probability": map_signals["back_pegs_probability"],
        "one_out_back_probability": map_signals["one_out_back_probability"],
        "three_wide_risk_probability": map_signals["three_wide_risk_probability"],
        "forward_intent_score": map_signals["forward_intent_score"],
        "map_lead_score": map_signals["map_lead_score"],
        "map_death_score": map_signals["map_death_score"],
        "map_soft_pegs_score": map_signals["map_soft_pegs_score"],
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
        "trainer_change_flag": _trainer_change_flag(last_runs, runner["nominated_trainer"]),
        "driver_change_flag": _driver_change_flag(last_runs, runner["nominated_driver"]),
        "form_bmr_secs": bmr_secs,
        "form_bmr_dist_rge_secs": bmr_dist_rge_secs,
        "days_since_last_run": days_since_last_run,
        "race_nr_ceiling": race_nr_ceiling,
        "nr_headroom": nr_headroom,
        "last_5_avg_stake": last_5_avg_stake,
        "race_purse": race_purse,
        "avg_recent_run_purse": avg_recent_run_purse,
        "class_delta": class_delta,
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
    base_position_scores = _base_map_position_scores(barrier)
    if not recent_lines:
        position_probabilities = _position_probabilities(base_position_scores)
        forward_intent_score = _forward_intent_score(
            position_probabilities["lead_probability"],
            position_probabilities["parked_probability"],
            None,
            barrier,
        )
        position_bucket, position_confidence = _map_position_summary(base_position_scores)
        return {
            "style_lead_rate": None,
            "style_forward_rate": None,
            "style_restrained_rate": None,
            "style_death_rate": None,
            "style_wide_rate": None,
            "map_leader_pos_score": base_position_scores["leader"],
            "map_outside_leader_pos_score": base_position_scores["outside_leader"],
            "map_behind_leader_pos_score": base_position_scores["behind_leader"],
            "map_one_one_pos_score": base_position_scores["one_one"],
            "map_three_back_pegs_pos_score": base_position_scores["three_back_pegs"],
            "map_one_out_two_back_pos_score": base_position_scores["one_out_two_back"],
            "map_back_pegs_pos_score": base_position_scores["back_pegs"],
            "map_one_out_back_pos_score": base_position_scores["one_out_back"],
            "predicted_map_position": position_bucket,
            "predicted_map_bucket": _predicted_map_bucket(position_bucket),
            "predicted_map_confidence": position_confidence,
            "lead_probability": position_probabilities["lead_probability"],
            "leaders_back_probability": position_probabilities["leaders_back_probability"],
            "parked_probability": position_probabilities["parked_probability"],
            "one_one_probability": position_probabilities["one_one_probability"],
            "three_back_pegs_probability": position_probabilities["three_back_pegs_probability"],
            "one_out_two_back_probability": position_probabilities["one_out_two_back_probability"],
            "back_pegs_probability": position_probabilities["back_pegs_probability"],
            "one_out_back_probability": position_probabilities["one_out_back_probability"],
            "three_wide_risk_probability": position_probabilities["three_wide_risk_probability"],
            "forward_intent_score": forward_intent_score,
            "map_lead_score": base_position_scores["leader"],
            "map_death_score": base_position_scores["outside_leader"],
            "map_soft_pegs_score": round(base_position_scores["behind_leader"] + base_position_scores["three_back_pegs"], 4),
            "map_soft_trip_score": round(base_position_scores["one_one"], 4),
            "map_wide_risk_score": round(base_position_scores["one_out_back"] + base_position_scores["back_pegs"], 4),
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

    observed_positions = [_recent_comment_position(c) for c in comments]
    evidence_scale = min(1.0, max(0.35, total / 3.0))
    position_counts = {
        "leader": sum(1 for p in observed_positions if p == "leader"),
        "outside_leader": sum(1 for p in observed_positions if p == "outside_leader"),
        "behind_leader": sum(1 for p in observed_positions if p == "behind_leader"),
        "one_one": sum(1 for p in observed_positions if p == "1_out_1_back"),
        "three_back_pegs": sum(1 for p in observed_positions if p == "3_back_pegs"),
        "one_out_two_back": sum(1 for p in observed_positions if p == "1_out_2_back"),
        "back_pegs": sum(1 for p in {"4_back_pegs", "5_back_pegs", "6_back_pegs"} for _ in []),
        "one_out_back": sum(1 for p in {"1_out_3_back", "1_out_4_back", "1_out_5_back"} for _ in []),
    }
    position_counts["back_pegs"] = sum(1 for p in observed_positions if p in {"4_back_pegs", "5_back_pegs", "6_back_pegs", "7_back_pegs", "8_back_pegs"})
    position_counts["one_out_back"] = sum(1 for p in observed_positions if p in {"1_out_3_back", "1_out_4_back", "1_out_5_back", "1_out_6_back", "1_out_7_back", "tailed_off"})

    position_scores = {
        "leader": round(((position_counts["leader"] / total) * 1.2 * evidence_scale) + base_position_scores["leader"] + (forward_rate * 0.15), 4),
        "outside_leader": round(((position_counts["outside_leader"] / total) * 1.15 * evidence_scale) + base_position_scores["outside_leader"] + (forward_rate * 0.2), 4),
        "behind_leader": round(((position_counts["behind_leader"] / total) * 1.0 * evidence_scale) + base_position_scores["behind_leader"], 4),
        "one_one": round(((position_counts["one_one"] / total) * 1.0 * evidence_scale) + base_position_scores["one_one"], 4),
        "three_back_pegs": round(((position_counts["three_back_pegs"] / total) * 0.9 * evidence_scale) + base_position_scores["three_back_pegs"] + (restrained_rate * 0.05), 4),
        "one_out_two_back": round(((position_counts["one_out_two_back"] / total) * 0.9 * evidence_scale) + base_position_scores["one_out_two_back"] + (restrained_rate * 0.04), 4),
        "back_pegs": round(((position_counts["back_pegs"] / total) * 0.85 * evidence_scale) + base_position_scores["back_pegs"] + (restrained_rate * 0.08), 4),
        "one_out_back": round(((position_counts["one_out_back"] / total) * 0.85 * evidence_scale) + base_position_scores["one_out_back"] + (wide_rate * 0.15), 4),
    }
    position_probabilities = _position_probabilities(position_scores)
    forward_intent_score = _forward_intent_score(
        position_probabilities["lead_probability"],
        position_probabilities["parked_probability"],
        forward_rate,
        barrier,
    )
    position_bucket, position_confidence = _map_position_summary(position_scores)

    return {
        "style_lead_rate": lead_rate,
        "style_forward_rate": forward_rate,
        "style_restrained_rate": restrained_rate,
        "style_death_rate": death_rate,
        "style_wide_rate": wide_rate,
        "map_leader_pos_score": position_scores["leader"],
        "map_outside_leader_pos_score": position_scores["outside_leader"],
        "map_behind_leader_pos_score": position_scores["behind_leader"],
        "map_one_one_pos_score": position_scores["one_one"],
        "map_three_back_pegs_pos_score": position_scores["three_back_pegs"],
        "map_one_out_two_back_pos_score": position_scores["one_out_two_back"],
        "map_back_pegs_pos_score": position_scores["back_pegs"],
        "map_one_out_back_pos_score": position_scores["one_out_back"],
        "predicted_map_position": position_bucket,
        "predicted_map_bucket": _predicted_map_bucket(position_bucket),
        "predicted_map_confidence": position_confidence,
        "lead_probability": position_probabilities["lead_probability"],
        "leaders_back_probability": position_probabilities["leaders_back_probability"],
        "parked_probability": position_probabilities["parked_probability"],
        "one_one_probability": position_probabilities["one_one_probability"],
        "three_back_pegs_probability": position_probabilities["three_back_pegs_probability"],
        "one_out_two_back_probability": position_probabilities["one_out_two_back_probability"],
        "back_pegs_probability": position_probabilities["back_pegs_probability"],
        "one_out_back_probability": position_probabilities["one_out_back_probability"],
        "three_wide_risk_probability": position_probabilities["three_wide_risk_probability"],
        "forward_intent_score": forward_intent_score,
        "map_lead_score": position_scores["leader"],
        "map_death_score": position_scores["outside_leader"],
        "map_soft_pegs_score": round(position_scores["behind_leader"] + position_scores["three_back_pegs"], 4),
        "map_soft_trip_score": round(position_scores["one_one"], 4),
        "map_wide_risk_score": round(position_scores["one_out_back"] + position_scores["back_pegs"], 4),
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


def _base_map_position_scores(barrier: object) -> dict[str, float]:
    scores = {
        "leader": round(_barrier_map_bonus(barrier, "lead"), 4),
        "outside_leader": round(_barrier_map_bonus(barrier, "death"), 4),
        "behind_leader": round(_barrier_map_bonus(barrier, "soft") + 0.06, 4),
        "one_one": round(_barrier_map_bonus(barrier, "soft") + 0.02, 4),
        "three_back_pegs": round(max(-0.2, _barrier_map_bonus(barrier, "soft") - 0.06), 4),
        "one_out_two_back": round(max(-0.25, _barrier_map_bonus(barrier, "soft") - 0.02), 4),
        "back_pegs": round(max(-0.35, _barrier_map_bonus(barrier, "wide") + 0.02), 4),
        "one_out_back": round(max(-0.35, _barrier_map_bonus(barrier, "wide") + 0.08), 4),
    }
    text = str(barrier or "").upper().strip()
    if text == "FR1":
        scores["leader"] = round(scores["leader"] + 0.22, 4)
        scores["behind_leader"] = round(scores["behind_leader"] + 0.26, 4)
        scores["three_back_pegs"] = round(scores["three_back_pegs"] + 0.14, 4)
        scores["one_one"] = round(scores["one_one"] - 0.06, 4)
        scores["one_out_two_back"] = round(scores["one_out_two_back"] - 0.14, 4)
        scores["one_out_back"] = round(scores["one_out_back"] - 0.22, 4)
    elif text == "SR1":
        scores["behind_leader"] = round(scores["behind_leader"] + 0.18, 4)
        scores["three_back_pegs"] = round(scores["three_back_pegs"] + 0.24, 4)
        scores["back_pegs"] = round(scores["back_pegs"] + 0.18, 4)
        scores["one_one"] = round(scores["one_one"] - 0.08, 4)
        scores["one_out_two_back"] = round(scores["one_out_two_back"] - 0.12, 4)
        scores["one_out_back"] = round(scores["one_out_back"] - 0.20, 4)
    return scores


def _recent_comment_position(comment: str) -> str | None:
    text = comment.lower()
    if "outside leader" in text or "death seat" in text:
        return "outside_leader"
    if "behind leader" in text:
        return "behind_leader"
    if "1 out 1 back" in text:
        return "1_out_1_back"
    if "1 out 2 back" in text:
        return "1_out_2_back"
    if "1 out 3 back" in text:
        return "1_out_3_back"
    if "1 out 4 back" in text:
        return "1_out_4_back"
    if "1 out 5 back" in text:
        return "1_out_5_back"
    if "3 back on pegs" in text or "3 back pegs" in text:
        return "3_back_pegs"
    if "4 back on the pegs" in text or "4 back pegs" in text:
        return "4_back_pegs"
    if "5 back on the pegs" in text or "5 back pegs" in text:
        return "5_back_pegs"
    if "6 back on the pegs" in text or "6 back pegs" in text:
        return "6_back_pegs"
    if "7 back on the pegs" in text or "7 back pegs" in text:
        return "7_back_pegs"
    if "8 back on the pegs" in text or "8 back pegs" in text:
        return "8_back_pegs"
    if "tailed off" in text:
        return "tailed_off"
    if " led" in f" {text}" or "leader" in text:
        return "leader"
    return None


def _map_position_summary(position_scores: dict[str, float]) -> tuple[str, float]:
    ranked = sorted(position_scores.items(), key=lambda item: item[1], reverse=True)
    top_position, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else top_score
    positive_total = sum(max(score, 0.0) for score in position_scores.values())
    dominance = (top_score / positive_total) if positive_total > 0 else 0.0
    gap = max(0.0, top_score - second_score)
    confidence = round(min(0.8, max(0.15, 0.35 + (dominance * 0.35) + (gap * 0.2))), 4)
    return top_position, confidence


def _position_probabilities(position_scores: dict[str, float]) -> dict[str, float]:
    weights = {name: max(0.02, 1.0 + score) for name, score in position_scores.items()}
    total = sum(weights.values()) or 1.0
    probs = {name: round(value / total, 4) for name, value in weights.items()}
    return {
        "lead_probability": probs.get("leader", 0.0),
        "leaders_back_probability": probs.get("behind_leader", 0.0),
        "parked_probability": probs.get("outside_leader", 0.0),
        "one_one_probability": probs.get("one_one", 0.0),
        "three_back_pegs_probability": probs.get("three_back_pegs", 0.0),
        "one_out_two_back_probability": probs.get("one_out_two_back", 0.0),
        "back_pegs_probability": probs.get("back_pegs", 0.0),
        "one_out_back_probability": probs.get("one_out_back", 0.0),
        "three_wide_risk_probability": round(
            min(
                1.0,
                (probs.get("outside_leader", 0.0) * 0.45)
                + (probs.get("one_out_back", 0.0) * 0.75)
                + (probs.get("back_pegs", 0.0) * 0.15),
            ),
            4,
        ),
    }


def _forward_intent_score(
    lead_probability: float,
    parked_probability: float,
    forward_rate: float | None,
    barrier: object,
) -> float:
    barrier_bonus = 0.0
    text = str(barrier or "").upper().strip()
    if text.startswith("FR"):
        try:
            barrier_num = int(text[2:])
        except ValueError:
            barrier_num = 6
        barrier_bonus = max(0.0, 0.18 - 0.03 * (barrier_num - 1))
    elif text.startswith("SR"):
        barrier_bonus = 0.03 if text == "SR1" else 0.0

    return round(
        min(
            1.0,
            (lead_probability * 0.65)
            + (parked_probability * 0.35)
            + ((forward_rate or 0.0) * 0.35)
            + barrier_bonus,
        ),
        4,
    )


def _apply_race_map_context(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, int], list[dict[str, object]]] = {}
    for row in rows:
        key = (str(row.get("meeting_code") or ""), int(row.get("race_number") or 0))
        grouped.setdefault(key, []).append(row)

    for group_rows in grouped.values():
        candidate_rows: list[tuple[dict[str, object], float]] = []
        for row in group_rows:
            lead_probability = float(row.get("lead_probability") or 0.0)
            parked_probability = float(row.get("parked_probability") or 0.0)
            forward_intent = float(row.get("forward_intent_score") or 0.0)
            barrier = str(row.get("barrier") or "").upper().strip()
            front_row_factor = 1.0 if barrier.startswith("FR") else 0.45
            candidate_score = round(
                ((lead_probability * 0.7) + (parked_probability * 0.2) + (forward_intent * 0.35)) * front_row_factor,
                4,
            )
            candidate_rows.append((row, candidate_score))

        candidate_rows.sort(key=lambda item: item[1], reverse=True)
        total_candidate_score = sum(score for _, score in candidate_rows[:4])
        pace_pressure_score = round(min(1.0, total_candidate_score / 1.75), 4)
        leader_count = sum(1 for _, score in candidate_rows if score >= 0.24)
        if pace_pressure_score >= 0.72 or leader_count >= 3:
            pressure_band = "high"
        elif pace_pressure_score >= 0.42 or leader_count == 2:
            pressure_band = "medium"
        else:
            pressure_band = "low"

        for rank, (row, candidate_score) in enumerate(candidate_rows, start=1):
            row["leader_candidate_score"] = candidate_score
            row["leader_candidate_rank"] = rank
            row["lead_share"] = round(candidate_score / total_candidate_score, 4) if total_candidate_score > 0 else None
            row["pace_pressure_score"] = pace_pressure_score
            row["pace_pressure_band"] = pressure_band

    return rows


def _predicted_map_bucket(position: str | None) -> str:
    if position == "leader":
        return "lead"
    if position == "outside_leader":
        return "death"
    if position in {"behind_leader", "three_back_pegs"}:
        return "soft_pegs"
    if position in {"one_one"}:
        return "soft"
    if position in {"one_out_two_back", "back_pegs", "one_out_back"}:
        return "back"
    return "unknown"


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
