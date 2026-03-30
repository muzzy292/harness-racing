from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path
from statistics import mean, median


def load_results_for_meeting(conn: sqlite3.Connection, meeting_code: str) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT meeting_code, race_number, horse_id, horse_name, finish_position, margin, starting_price, steward_comment
        FROM race_results
        WHERE meeting_code = ?
        ORDER BY race_number, finish_position
        """,
        (meeting_code,),
    ).fetchall()
    return [dict(row) for row in rows]


def compare_meeting_scores_to_results(
    meeting_code: str,
    meeting_scores: dict[int, list[dict[str, object]]],
    result_rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    results_by_race: dict[int, list[dict[str, object]]] = {}
    for row in result_rows:
        results_by_race.setdefault(int(row["race_number"]), []).append(row)

    comparisons: list[dict[str, object]] = []
    for race_number, scored_rows in sorted(meeting_scores.items()):
        if not scored_rows:
            continue
        race_results = results_by_race.get(race_number, [])
        winner = next((row for row in race_results if row.get("finish_position") == 1), None)
        if not winner:
            continue

        ranked_rows = sorted(
            scored_rows,
            key=lambda row: float(row.get("win_probability") or 0.0),
            reverse=True,
        )
        top_pick = ranked_rows[0]
        winner_row = next(
            (row for row in ranked_rows if str(row.get("horse_name", "")).upper() == str(winner["horse_name"]).upper()),
            None,
        )
        winner_rank = (
            next(
                index
                for index, row in enumerate(ranked_rows, start=1)
                if str(row.get("horse_name", "")).upper() == str(winner["horse_name"]).upper()
            )
            if winner_row
            else None
        )
        comparisons.append(
            {
                "meeting_code": meeting_code,
                "race_number": race_number,
                "runner_number": top_pick.get("runner_number"),
                "top_pick_barrier": top_pick.get("barrier"),
                "top_pick": top_pick.get("horse_name"),
                "top_pick_probability": top_pick.get("win_probability"),
                "top_pick_fair_odds": top_pick.get("fair_odds"),
                "winner_runner_number": winner_row.get("runner_number") if winner_row else None,
                "winner_barrier": winner_row.get("barrier") if winner_row else None,
                "winner": winner.get("horse_name"),
                "winner_starting_price": winner.get("starting_price"),
                "winner_rank": winner_rank,
                "winner_probability": winner_row.get("win_probability") if winner_row else None,
                "winner_fair_odds": winner_row.get("fair_odds") if winner_row else None,
                "top_pick_won": bool(
                    str(top_pick.get("horse_name", "")).upper() == str(winner.get("horse_name", "")).upper()
                ),
            }
        )

    summary = _build_summary(comparisons)
    return comparisons, summary


def compare_meeting_map_to_results(
    meeting_code: str,
    feature_rows: list[dict[str, str]],
    result_rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    feature_lookup = {
        (int(row.get("race_number", 0) or 0), str(row.get("horse_name", "")).upper()): row
        for row in feature_rows
        if row.get("meeting_code") == meeting_code
    }

    comparisons: list[dict[str, object]] = []
    for result in result_rows:
        key = (int(result["race_number"]), str(result["horse_name"]).upper())
        feature = feature_lookup.get(key)
        if not feature:
            continue
        predicted_position = feature.get("predicted_map_position") or _predicted_map_tag(feature)
        predicted_tag = feature.get("predicted_map_bucket") or _predicted_map_bucket(str(predicted_position))
        actual_position = _actual_map_position(result.get("steward_comment"))
        actual_tag = _actual_map_bucket(result.get("steward_comment"), actual_position)
        comparisons.append(
            {
                "meeting_code": meeting_code,
                "race_number": result["race_number"],
                "horse_name": result["horse_name"],
                "finish_position": result.get("finish_position"),
                "predicted_map_position": predicted_position,
                "predicted_map_tag": predicted_tag,
                "actual_map_position": actual_position,
                "actual_map_tag": actual_tag,
                "map_match": predicted_tag == actual_tag if actual_tag != "unknown" else None,
                "predicted_map_confidence": _float_or_none(feature.get("predicted_map_confidence")),
                "map_lead_score": _float_or_none(feature.get("map_lead_score")),
                "map_death_score": _float_or_none(feature.get("map_death_score")),
                "map_soft_pegs_score": _float_or_none(feature.get("map_soft_pegs_score")),
                "map_soft_trip_score": _float_or_none(feature.get("map_soft_trip_score")),
                "map_wide_risk_score": _float_or_none(feature.get("map_wide_risk_score")),
                "barrier": feature.get("barrier"),
                "steward_comment": result.get("steward_comment"),
            }
        )

    ordered = sorted(
        comparisons,
        key=lambda row: (
            int(row.get("race_number") or 0),
            _barrier_sort_key(row.get("barrier")),
            str(row.get("horse_name") or "").upper(),
        ),
    )

    summary = _build_map_summary(ordered)
    return ordered, summary


def write_comparison_csv(rows: list[dict[str, object]], output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output.write_text("", encoding="utf-8")
        return output

    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return output


def render_comparison_report(meeting_code: str, rows: list[dict[str, object]], summary: dict[str, object]) -> str:
    if not rows:
        return f"No comparable result rows found for {meeting_code}."

    lines = [
        f"Meeting {meeting_code} Results Comparison",
        "",
        "Race  Top Pick             Bar  TopP    Fair   Winner               Bar  SP    WRank  WinP    WinFair",
        "----  -------------------  ---  ------  -----  -------------------  ---  ----  -----  ------  -------",
    ]
    for row in rows:
        top_prob = row["top_pick_probability"]
        winner_prob = row["winner_probability"]
        lines.append(
            f"{int(row['race_number']):>4}  "
            f"{str(row['top_pick'])[:19]:<19}  "
            f"{str(row.get('top_pick_barrier') or '')[:3]:<3}  "
            f"{'' if top_prob is None else f'{float(top_prob)*100:>5.1f}%':>6}  "
            f"{'' if row['top_pick_fair_odds'] is None else f'{float(row['top_pick_fair_odds']):>5.2f}':>5}  "
            f"{str(row['winner'])[:19]:<19}  "
            f"{str(row.get('winner_barrier') or '')[:3]:<3}  "
            f"{'' if row['winner_starting_price'] is None else f'{float(row['winner_starting_price']):>4.2f}':>4}  "
            f"{'' if row['winner_rank'] is None else str(row['winner_rank']).rjust(5)}  "
            f"{'' if winner_prob is None else f'{float(winner_prob)*100:>5.1f}%':>6}  "
            f"{'' if row['winner_fair_odds'] is None else f'{float(row['winner_fair_odds']):>7.2f}':>7}"
        )

    lines.extend(
        [
            "",
            f"Top-pick winners: {summary['top_pick_winners']}/{summary['races_compared']}",
            f"Average winner rank: {summary['avg_winner_rank']}",
            f"Median winner rank: {summary['median_winner_rank']}",
        ]
    )
    return "\n".join(lines)


def render_map_comparison_report(meeting_code: str, rows: list[dict[str, object]], summary: dict[str, object]) -> str:
    if not rows:
        return f"No map comparison rows found for {meeting_code}."

    lines = [
        f"Meeting {meeting_code} Map Comparison",
        "",
        "Race  Bar  Horse                Fin  Pred Pos         Pred Bkt   Conf  Actual Pos       Act      Match",
        "----  ---  -------------------  ---  ---------------  ---------  ----  ---------------  -------  -----",
    ]
    for row in rows:
        lines.append(
            f"{int(row['race_number']):>4}  "
            f"{str(row.get('barrier') or '')[:3]:<3}  "
            f"{str(row['horse_name'])[:19]:<19}  "
            f"{'' if row['finish_position'] is None else str(row['finish_position']).rjust(3)}  "
            f"{str(row.get('predicted_map_position') or '')[:15]:<15}  "
            f"{str(row['predicted_map_tag'])[:9]:<9}  "
            f"{'' if row.get('predicted_map_confidence') is None else f'{float(row['predicted_map_confidence']):.2f}':>4}  "
            f"{str(row.get('actual_map_position') or '')[:15]:<15}  "
            f"{str(row['actual_map_tag'])[:7]:<7}  "
            f"{'' if row['map_match'] is None else ('yes' if row['map_match'] else 'no'):>5}"
        )
    lines.extend(
        [
            "",
            f"Runners compared: {summary['runners_compared']}",
            f"Runners with actual tag: {summary['runners_with_actual_tag']}",
            f"Map matches: {summary['map_matches']}",
            f"Map match rate: {summary['map_match_rate']}",
        ]
    )
    return "\n".join(lines)


def _build_summary(rows: list[dict[str, object]]) -> dict[str, object]:
    winner_ranks = [int(row["winner_rank"]) for row in rows if row.get("winner_rank") is not None]
    return {
        "races_compared": len(rows),
        "top_pick_winners": sum(1 for row in rows if row.get("top_pick_won")),
        "avg_winner_rank": round(mean(winner_ranks), 2) if winner_ranks else None,
        "median_winner_rank": median(winner_ranks) if winner_ranks else None,
    }


def _predicted_map_tag(feature_row: dict[str, str]) -> str:
    scores = {
        "lead": _float_or_none(feature_row.get("map_lead_score")) or 0.0,
        "death": _float_or_none(feature_row.get("map_death_score")) or 0.0,
        "soft": _float_or_none(feature_row.get("map_soft_trip_score")) or 0.0,
        "wide": _float_or_none(feature_row.get("map_wide_risk_score")) or 0.0,
    }
    tag, value = max(scores.items(), key=lambda item: item[1])
    return tag if value > 0 else "neutral"


def _predicted_map_bucket(position: str | None) -> str:
    text = str(position or "").lower()
    if text == "leader":
        return "lead"
    if text == "outside_leader":
        return "death"
    if text in {"behind_leader", "three_back_pegs", "3_back_pegs"}:
        return "soft_pegs"
    if text in {"one_one", "1_out_1_back"}:
        return "soft"
    if text in {
        "one_out_two_back",
        "back_pegs",
        "one_out_back",
        "1_out_2_back",
        "4_back_pegs",
        "5_back_pegs",
        "6_back_pegs",
        "7_back_pegs",
        "8_back_pegs",
        "1_out_3_back",
        "1_out_4_back",
        "1_out_5_back",
        "1_out_6_back",
        "1_out_7_back",
        "tailed_off",
    }:
        return "back"
    return "unknown"


def _actual_map_position(comment: object) -> str | None:
    text = str(comment or "").lower()
    if not text:
        return None
    if "outside leader" in text:
        return "outside leader"
    if "behind leader" in text:
        return "behind leader"
    if "1 out 1 back" in text:
        return "1 out 1 back"
    if "1 out 2 back" in text:
        return "1 out 2 back"
    if "1 out 3 back" in text:
        return "1 out 3 back"
    if "1 out 4 back" in text:
        return "1 out 4 back"
    if "3 back on pegs" in text or "3 back pegs" in text:
        return "3 back pegs"
    if "4 back on the pegs" in text or "4 back pegs" in text:
        return "4 back pegs"
    if "5 back on the pegs" in text or "5 back pegs" in text:
        return "5 back pegs"
    if "led" in text or "leader" in text:
        return "leader"

    bell_lap = _bell_lap_position(text)
    if bell_lap is not None:
        return bell_lap
    return None


def _actual_map_bucket(comment: object, position: str | None) -> str:
    text = str(comment or "").lower()
    if not text and not position:
        return "unknown"
    if "outside leader" in text:
        return "death"
    if "caught wide" in text or "3 wide" in text:
        return "wide"
    if position == "tailed off":
        return "tailed"
    if position == "leader":
        return "lead"
    if position == "outside leader":
        return "death"
    if position in {"behind leader", "3 back pegs"}:
        return "soft_pegs"
    if position in {"1 out 1 back"}:
        return "soft"
    if position in {"1 out 2 back", "4 back pegs", "1 out 3 back", "5 back pegs", "1 out 4 back"}:
        return "back"
    if "worked forward" in text:
        return "wide"
    return "unknown"


def _build_map_summary(rows: list[dict[str, object]]) -> dict[str, object]:
    comparable = [row for row in rows if row.get("map_match") is not None]
    matches = [row for row in comparable if row.get("map_match")]
    return {
        "runners_compared": len(rows),
        "runners_with_actual_tag": len(comparable),
        "map_matches": len(matches),
        "map_match_rate": round(len(matches) / len(comparable), 4) if comparable else None,
    }


def _bell_lap_position(text: str) -> str | None:
    match = re.search(r"\bbl\s+(\d+)\b", text, re.IGNORECASE)
    if not match:
        return None
    number = int(match.group(1))
    if number == 17:
        return "tailed off"
    if number == 1:
        return "leader"
    if number == 2:
        return "outside leader"
    if number == 3:
        return "behind leader"
    if number == 4:
        return "1 out 1 back"
    if number >= 5 and number % 2 == 1:
        return f"{(number + 1) // 2} back pegs"
    if number >= 6 and number % 2 == 0:
        return f"1 out {number // 2 - 1} back"
    return None


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _barrier_sort_key(barrier: object) -> tuple[int, int]:
    text = str(barrier or "").strip().upper()
    if not text:
        return (9, 999)
    if text.startswith("FR"):
        try:
            return (0, int(text[2:]))
        except ValueError:
            return (0, 999)
    if text.startswith("SR"):
        try:
            return (1, int(text[2:]))
        except ValueError:
            return (1, 999)
    return (8, 999)
