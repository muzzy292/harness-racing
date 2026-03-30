from __future__ import annotations

import csv
import math
from pathlib import Path


def load_feature_rows(csv_path: str | Path) -> list[dict[str, str]]:
    with Path(csv_path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_market_rows(csv_path: str | Path) -> list[dict[str, str]]:
    with Path(csv_path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_scored_rows_csv(rows: list[dict[str, object]], output_path: str | Path) -> Path:
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


def flatten_meeting_scores(meeting_code: str, meeting_scores: dict[int, list[dict[str, object]]]) -> list[dict[str, object]]:
    flattened: list[dict[str, object]] = []
    for race_number, rows in meeting_scores.items():
        for row in rows:
            flat_row = {"meeting_code": meeting_code, "race_number": race_number}
            flat_row.update(row)
            flattened.append(flat_row)
    return flattened


def score_race_rows(
    rows: list[dict[str, str]],
    meeting_code: str,
    race_number: int,
    min_probability: float = 0.0,
    max_probability: float = 1.0,
    market_rows: list[dict[str, str]] | None = None,
    model_weight: float = 0.45,
    market_weight: float = 0.55,
) -> list[dict[str, object]]:
    race_rows = [
        row for row in rows
        if row.get("meeting_code") == meeting_code and int(row.get("race_number", 0) or 0) == race_number
    ]
    if not race_rows:
        return []

    enriched = []
    for row in race_rows:
        components = _score_components(row)
        enriched.append(
            {
                "horse_name": row.get("horse_name"),
                "runner_number": _to_int(row.get("runner_number")),
                "barrier": row.get("barrier"),
                "nominated_driver": row.get("nominated_driver"),
                "nominated_trainer": row.get("nominated_trainer"),
                "score": round(sum(components.values()), 4),
                "components": components,
            }
        )

    scores = [item["score"] for item in enriched]
    probs = _softmax(scores, temperature=2.75)
    probs = _apply_probability_guardrails(probs, min_probability=min_probability, max_probability=max_probability)
    fair_market_probs = _fair_market_probs(race_rows, market_rows, meeting_code, race_number)
    for item, prob in zip(enriched, probs):
        item["win_probability"] = round(prob, 4)
        item["fair_odds"] = round(1.0 / prob, 2) if prob > 0 else None
        market_prob = fair_market_probs.get(_market_key(item["horse_name"], item["runner_number"]))
        item["fair_market_probability"] = round(market_prob, 4) if market_prob is not None else None
        if market_prob is not None:
            adjusted_prob = (model_weight * prob) + (market_weight * market_prob)
            item["adjusted_probability"] = round(adjusted_prob, 4)
            item["adjusted_fair_odds"] = round(1.0 / adjusted_prob, 2) if adjusted_prob > 0 else None
        else:
            item["adjusted_probability"] = item["win_probability"]
            item["adjusted_fair_odds"] = item["fair_odds"]

    enriched.sort(key=lambda item: item["win_probability"], reverse=True)
    return enriched


def score_meeting_rows(
    rows: list[dict[str, str]],
    meeting_code: str,
    min_probability: float = 0.0,
    max_probability: float = 1.0,
    market_rows: list[dict[str, str]] | None = None,
    model_weight: float = 0.45,
    market_weight: float = 0.55,
) -> dict[int, list[dict[str, object]]]:
    race_numbers = sorted(
        {
            int(row.get("race_number", 0) or 0)
            for row in rows
            if row.get("meeting_code") == meeting_code and row.get("race_number")
        }
    )
    return {
        race_number: score_race_rows(
            rows,
            meeting_code,
            race_number,
            min_probability=min_probability,
            max_probability=max_probability,
            market_rows=market_rows,
            model_weight=model_weight,
            market_weight=market_weight,
        )
        for race_number in race_numbers
    }


def render_race_odds_table(scored_rows: list[dict[str, object]]) -> str:
    if not scored_rows:
        return "No rows found for that race."

    display_rows = sorted(
        scored_rows,
        key=lambda row: (
            row["runner_number"] is None,
            row["runner_number"] if row["runner_number"] is not None else 999,
        ),
    )
    lines = []
    has_market = any(row.get("fair_market_probability") is not None for row in display_rows)
    if has_market:
        lines.append("No.  Horse                 Barrier  ModelP  AdjP    Fair Odds  Adj Odds  Score")
        lines.append("---  --------------------  -------  ------  ------  ---------  --------  ------")
    else:
        lines.append("No.  Horse                 Barrier  Prob    Fair Odds  Score")
        lines.append("---  --------------------  -------  ------  ---------  ------")
    for row in display_rows:
        if has_market:
            lines.append(
                f"{str(row['runner_number'] or ''):<3}  "
                f"{str(row['horse_name'])[:20]:<20}  "
                f"{str(row['barrier'] or ''):<7}  "
                f"{row['win_probability']:.4f}  "
                f"{float(row['adjusted_probability']):.4f}  "
                f"{row['fair_odds']:<9}  "
                f"{row['adjusted_fair_odds']:<8}  "
                f"{row['score']:.4f}"
            )
        else:
            lines.append(
                f"{str(row['runner_number'] or ''):<3}  "
                f"{str(row['horse_name'])[:20]:<20}  "
                f"{str(row['barrier'] or ''):<7}  "
                f"{row['win_probability']:.4f}  "
                f"{row['fair_odds']:<9}  "
                f"{row['score']:.4f}"
            )
    return "\n".join(lines)


def render_meeting_odds(meeting_scores: dict[int, list[dict[str, object]]], meeting_code: str) -> str:
    sections: list[str] = [f"Meeting {meeting_code}"]
    for race_number, scored_rows in meeting_scores.items():
        if not scored_rows:
            continue
        sections.append("")
        sections.append(f"Race {race_number}")
        sections.append(render_race_odds_table(scored_rows))
    if len(sections) == 1:
        sections.append("No races found for that meeting.")
    return "\n".join(sections)


def _score_components(row: dict[str, str]) -> dict[str, float]:
    last5_adj = _to_float(row.get("last_5_avg_adj_margin"))
    last10_adj = _to_float(row.get("last_10_avg_adj_margin"))
    best_adj = _to_float(row.get("last_5_best_adj_margin"))
    recent_line_adj = _to_float(row.get("recent_line_avg_adj_margin"))
    recent_line_best = _to_float(row.get("recent_line_best_adj_margin"))
    avg_sp = _to_float(row.get("last_5_avg_sp"))
    win_rate = _to_float(row.get("last_5_win_rate"))
    sec3 = _to_float(row.get("last_3_avg_sectional_delta"))
    sec5 = _to_float(row.get("last_5_avg_sectional_delta"))
    comment_adj = _to_float(row.get("recent_line_avg_comment_adj"))
    tempo_adj = _to_float(row.get("recent_line_avg_tempo_adj"))
    tempo_flags = _to_float(row.get("recent_line_tempo_flags"))
    null_flags = _to_float(row.get("recent_line_null_flags"))
    map_lead = _to_float(row.get("map_lead_score"))
    map_soft = _to_float(row.get("map_soft_trip_score"))
    map_wide = _to_float(row.get("map_wide_risk_score"))
    map_death = _to_float(row.get("map_death_score"))
    nr = _to_float(row.get("nr_rating"))
    barrier = row.get("barrier") or ""
    bmr_dist_rge = _to_float(row.get("form_bmr_dist_rge_secs"))
    days_since_last_run = _to_float(row.get("days_since_last_run"))
    nr_headroom = _to_float(row.get("nr_headroom"))
    avg_stake = _to_float(row.get("last_5_avg_stake"))

    # When horse-page run data is available use it exclusively for the ability
    # bucket.  Form-page recent lines are a fallback for horses with no history
    # in the DB — they should not stack on top of horse-page data.
    has_horse_data = last5_adj is not None
    consistency_adj = last5_adj if has_horse_data else recent_line_adj
    ceiling_adj     = best_adj  if has_horse_data else recent_line_best

    components = {
        # ── Ability bucket (one metric per concept) ──────────────────────────
        "consistency": _neg_scale(consistency_adj, divisor=12.0, floor=-4.0, missing=0.0) * 1.8,
        "ceiling":     _neg_scale(ceiling_adj,     divisor=10.0, floor=-3.5, missing=0.0) * 1.2,
        "late_speed":  _neg_scale(sec3,            divisor=1.2,  floor=-2.5, missing=0.0) * 1.4,
        "comment_adj": _pos_scale(comment_adj, center=0.0, divisor=6.0, missing=0.0) * 0.5,
        "tempo_adj": _pos_scale(tempo_adj, center=0.0, divisor=1.2, missing=0.0) * 0.45,
        "tempo_flags": -(tempo_flags or 0.0) * 0.08,
        "null_flags": -(null_flags or 0.0) * 0.25,
        "map_lead": (map_lead or 0.0) * 0.7,
        "map_soft": (map_soft or 0.0) * 0.45,
        "map_wide": -(map_wide or 0.0) * 0.5,
        "map_death": -(map_death or 0.0) * 0.35,
        "market": _neg_log_scale(avg_sp, missing=0.0) * 0.6,
        "win_rate": (win_rate or 0.0) * 1.2,
        "nr": _pos_scale(nr, center=45.0, divisor=8.0, missing=0.0) * 0.25,
        # Class position — lower headroom (NR near ceiling) = horse is near top of grade.
        # Large headroom can indicate declining form (NR has dropped) → small penalty.
        # Weight is intentionally small to avoid double-counting with nr component.
        "class_pos": _neg_scale(nr_headroom, divisor=8.0, floor=-2.0, missing=0.0) * 0.15,
        # Stake class — average earnings from recent runs (outlier-capped).
        # Centred at $4500; horses earning above that have been racing at a higher level.
        # Only populated when horse-page data has been ingested.
        "stake_class": _pos_scale(avg_stake, center=4500.0, divisor=2500.0, missing=0.0) * 0.2,
        "barrier": _barrier_score(barrier),
        # BMR at race distance range — faster (lower seconds) = better.
        # Centre around 117s (1:57.0), 1s difference ≈ 0.5 point swing.
        "bmr_dist_rge": _pos_scale(bmr_dist_rge, center=117.0, divisor=-2.0, missing=0.0) * 0.6,
        # Fitness penalty — last run > 14 days ago scores negatively.
        "fitness": _fitness_score(days_since_last_run),
    }
    return components


def _softmax(scores: list[float], temperature: float = 1.0) -> list[float]:
    max_score = max(scores)
    scale = max(temperature, 0.001)
    exps = [math.exp((score - max_score) / scale) for score in scores]
    total = sum(exps)
    return [value / total for value in exps]


def _to_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _neg_scale(value: float | None, divisor: float, floor: float, missing: float) -> float:
    if value is None:
        return missing
    return max(floor, -value / divisor)


def _pos_scale(value: float | None, center: float, divisor: float, missing: float) -> float:
    if value is None:
        return missing
    return (value - center) / divisor


def _neg_log_scale(value: float | None, missing: float) -> float:
    if value is None or value <= 0:
        return missing
    return -math.log(value)


def _barrier_score(barrier: str) -> float:
    text = barrier.upper().strip()
    if not text:
        return 0.0
    if text.startswith("FR"):
        try:
            num = int(text[2:])
        except ValueError:
            return 0.0
        return max(-0.45, 0.45 - 0.09 * (num - 1))
    if text.startswith("SR"):
        try:
            num = int(text[2:])
        except ValueError:
            return -0.2
        return max(-0.55, -0.18 - 0.08 * (num - 1))
    return 0.0


def _apply_probability_guardrails(
    probs: list[float],
    min_probability: float,
    max_probability: float,
    iterations: int = 8,
) -> list[float]:
    if not probs:
        return probs

    clamped = probs[:]
    for _ in range(iterations):
        clamped = [min(max(prob, min_probability), max_probability) for prob in clamped]
        total = sum(clamped)
        if total <= 0:
            return probs
        clamped = [prob / total for prob in clamped]

    return clamped


def _fair_market_probs(
    race_rows: list[dict[str, str]],
    market_rows: list[dict[str, str]] | None,
    meeting_code: str,
    race_number: int,
) -> dict[tuple[str, int | None], float]:
    if not market_rows:
        return {}

    relevant = []
    for row in market_rows:
        if row.get("meeting_code") != meeting_code:
            continue
        try:
            if int(row.get("race_number", 0) or 0) != race_number:
                continue
        except ValueError:
            continue
        price = _to_float(row.get("market_odds"))
        if price and price > 0:
            relevant.append(row)

    if not relevant:
        return {}

    implied = []
    for row in relevant:
        price = float(row["market_odds"])
        implied.append((_market_key(row.get("horse_name"), _to_int(row.get("runner_number"))), 1.0 / price))

    total = sum(prob for _, prob in implied)
    if total <= 0:
        return {}

    return {key: prob / total for key, prob in implied}


def _fitness_score(days: float | None) -> float:
    """Return a score penalty when a horse has been off the track for more than 14 days.

    Mirrors Claude's model: last race > 14 days = meaningful penalty.
    > 14 days  → -0.35  (roughly equivalent to ×1.25 fair-odds lengthening)
    > 28 days  → -0.55  (extended break, steeper penalty)
    None / ≤ 14 days → 0.0
    """
    if days is None:
        return 0.0
    if days > 28:
        return -0.55
    if days > 14:
        return -0.35
    return 0.0


def _market_key(horse_name: object, runner_number: object) -> tuple[str, int | None]:
    name = str(horse_name or "").strip().upper()
    number = None
    try:
        number = int(runner_number) if runner_number not in (None, "") else None
    except (TypeError, ValueError):
        number = None
    return name, number
