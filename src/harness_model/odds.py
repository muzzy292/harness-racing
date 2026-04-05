from __future__ import annotations

import copy
import csv
import json
import math
from pathlib import Path

_DEFAULT_WEIGHTS: dict = {
    "stage1": {
        "consistency": 1.8,
        "ceiling": 1.2,
        "late_speed": 1.4,
        "tempo_adj": 0.45,
        "tempo_flags": 0.08,
        "null_flags": 0.25,
        "market_min": 0.3,
        "market_max": 0.6,
        "win_rate": 0.7,
        "career_win_rate": 0.6,
        "top3_rate": 0.6,
        "competitive_rate": 0.5,
        "nr": 0.25,
        "class_pos": 0.15,
        "stake_class": 0.2,
        "class_delta": 0.3,
    },
    "stage2": {
        "map_lead": 2.0,
        "map_soft": 0.45,
        "map_soft_context": 0.3,
        "pace_backmarker": 0.6,
        "pace_backmarker_threshold": 0.4,
        "map_wide": 0.5,
        "map_death": 1.2,
        "dist_strike_rate": 0.9,
        "nr_grade_delta": 0.4,
        "driver_form": 0.3,
        "trainer_form_page": 0.24,
        "trainer_form_30d": 0.12,
        "trainer_form_90d": 0.08,
    },
    "fitness": {
        "tier_15_28": -0.35,
        "tier_29_42": -0.60,
        "tier_43_84": -0.85,
        "tier_85_99": -1.10,
        "tier_100_119": -1.45,
        "tier_120_149": -1.70,
        "tier_150_plus": -2.00,
    },
    "softmax": {
        "temperature": 2.0,
        "model_weight": 0.45,
        "market_weight": 0.55,
    },
}


def load_weights(path: str | Path) -> dict:
    """Load weights from a JSON file, merging with defaults so partial files work."""
    override = json.loads(Path(path).read_text(encoding="utf-8"))
    merged = copy.deepcopy(_DEFAULT_WEIGHTS)
    for section, values in override.items():
        if section in merged and isinstance(values, dict):
            merged[section].update(values)
        else:
            merged[section] = values
    return merged


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
    model_weight: float | None = None,
    market_weight: float | None = None,
    temperature: float | None = None,
    weights: dict | None = None,
) -> list[dict[str, object]]:
    w = weights if weights is not None else _DEFAULT_WEIGHTS
    sw = w.get("softmax", _DEFAULT_WEIGHTS["softmax"])
    _temperature = temperature if temperature is not None else sw.get("temperature", 2.0)
    _model_weight = model_weight if model_weight is not None else sw.get("model_weight", 0.45)
    _market_weight = market_weight if market_weight is not None else sw.get("market_weight", 0.55)

    race_rows = [
        row for row in rows
        if row.get("meeting_code") == meeting_code and int(row.get("race_number", 0) or 0) == race_number
    ]
    if not race_rows:
        return []

    # Field-normalise map lead and death scores.
    # A horse that leads 70% of the time should get less credit when 3 other speed
    # horses are in the same race — only one horse can actually lead.
    # Softmax converts each horse's raw tendency into a field-relative probability
    # (all horses sum to 1.0) so contested pace automatically reduces the lead bonus.
    _lead_raw = [_to_float(row.get("map_lead_score")) or 0.0 for row in race_rows]
    _death_raw = [_to_float(row.get("map_death_score")) or 0.0 for row in race_rows]
    field_lead_probs = _softmax(_lead_raw, temperature=1.0)
    field_death_probs = _softmax(_death_raw, temperature=1.0)

    # pace_pressure measures how contested the pace is.
    # When one horse dominates (high max lead prob) pressure is low → uncontested leader.
    # When several horses compete (low max lead prob) pressure is high → contested pace.
    pace_pressure = 1.0 - max(field_lead_probs)

    enriched = []
    for i, row in enumerate(race_rows):
        s1 = _stage1_components(row, w)
        s2 = _stage2_components(row, w, field_lead_prob=field_lead_probs[i], field_death_prob=field_death_probs[i], pace_pressure=pace_pressure)
        stage1_score = round(sum(s1.values()), 4)
        stage2_score = round(sum(s2.values()), 4)
        enriched.append(
            {
                "horse_name": row.get("horse_name"),
                "runner_number": _to_int(row.get("runner_number")),
                "barrier": row.get("barrier"),
                "nominated_driver": row.get("nominated_driver"),
                "nominated_trainer": row.get("nominated_trainer"),
                "stage1_score": stage1_score,
                "stage2_score": stage2_score,
                "score": round(stage1_score + stage2_score, 4),
                "components": {**s1, **s2},
            }
        )

    scores = [item["score"] for item in enriched]
    field_mean = sum(scores) / len(scores) if scores else 0.0
    for item in enriched:
        item["relative_score"] = round(item["score"] - field_mean, 4)
    relative_scores = [item["relative_score"] for item in enriched]
    probs = _softmax(relative_scores, temperature=_temperature)
    probs = _apply_probability_guardrails(probs, min_probability=min_probability, max_probability=max_probability)
    fair_market_probs = _fair_market_probs(race_rows, market_rows, meeting_code, race_number)
    for item, prob in zip(enriched, probs):
        item["win_probability"] = round(prob, 4)
        item["fair_odds"] = round(1.0 / prob, 2) if prob > 0 else None
        market_prob = fair_market_probs.get(_market_key(item["horse_name"], item["runner_number"]))
        item["fair_market_probability"] = round(market_prob, 4) if market_prob is not None else None
        if market_prob is not None:
            adjusted_prob = (_model_weight * prob) + (_market_weight * market_prob)
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
    model_weight: float | None = None,
    market_weight: float | None = None,
    temperature: float | None = None,
    weights: dict | None = None,
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
            temperature=temperature,
            weights=weights,
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
        lines.append("No.  Horse                 Barrier  ModelP  AdjP    Fair Odds  Adj Odds  S1      S2      Score    Rel")
        lines.append("---  --------------------  -------  ------  ------  ---------  --------  ------  ------  ------  ------")
    else:
        lines.append("No.  Horse                 Barrier  Prob    Fair Odds  S1      S2      Score    Rel")
        lines.append("---  --------------------  -------  ------  ---------  ------  ------  ------  ------")
    for row in display_rows:
        s1 = f"{row.get('stage1_score', 0.0):>6.3f}"
        s2 = f"{row.get('stage2_score', 0.0):>6.3f}"
        rel = row.get("relative_score")
        rel_str = f"{rel:>+7.3f}" if rel is not None else f"{'':>7}"
        if has_market:
            lines.append(
                f"{str(row['runner_number'] or ''):<3}  "
                f"{str(row['horse_name'])[:20]:<20}  "
                f"{str(row['barrier'] or ''):<7}  "
                f"{row['win_probability']:.4f}  "
                f"{float(row['adjusted_probability']):.4f}  "
                f"{row['fair_odds']:<9}  "
                f"{row['adjusted_fair_odds']:<8}  "
                f"{s1}  {s2}  "
                f"{row['score']:.4f}  {rel_str}"
            )
        else:
            lines.append(
                f"{str(row['runner_number'] or ''):<3}  "
                f"{str(row['horse_name'])[:20]:<20}  "
                f"{str(row['barrier'] or ''):<7}  "
                f"{row['win_probability']:.4f}  "
                f"{row['fair_odds']:<9}  "
                f"{s1}  {s2}  "
                f"{row['score']:.4f}  {rel_str}"
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


def _stage1_components(row: dict[str, str], weights: dict | None = None) -> dict[str, float]:
    """Stage 1 — Horse performance rating.

    Built from historical form: adjusted margins, sectionals, consistency,
    ceiling run, win rate, NR, and class-normalised form signals.
    These components are independent of today's specific race conditions.
    """
    w = (weights or _DEFAULT_WEIGHTS).get("stage1", _DEFAULT_WEIGHTS["stage1"])
    last5_adj = _to_float(row.get("last_5_avg_adj_margin"))
    best_adj = _to_float(row.get("last_5_best_adj_margin"))
    recent_line_adj = _to_float(row.get("recent_line_avg_adj_margin"))
    recent_line_best = _to_float(row.get("recent_line_best_adj_margin"))
    class_adj_margin = _to_float(row.get("recent_line_avg_class_adj_margin"))
    class_adj_best = _to_float(row.get("recent_line_best_class_adj_margin"))
    avg_sp = _to_float(row.get("last_5_avg_sp"))
    win_rate = _to_float(row.get("last_5_win_rate"))
    top3_rate = _to_float(row.get("last_5_top3_rate"))
    competitive_rate = _to_float(row.get("last_5_competitive_rate"))
    career_starts = _to_int(row.get("career_starts"))
    sec3 = _to_float(row.get("last_3_avg_sectional_delta"))
    comment_adj = _to_float(row.get("recent_line_avg_comment_adj"))
    tempo_adj = _to_float(row.get("recent_line_avg_tempo_adj"))
    tempo_flags = _to_float(row.get("recent_line_tempo_flags"))
    null_flags = _to_float(row.get("recent_line_null_flags"))
    nr = _to_float(row.get("nr_rating"))
    nr_headroom = _to_float(row.get("nr_headroom"))
    avg_run_purse = _to_float(row.get("avg_recent_run_purse"))
    class_delta = _to_float(row.get("class_delta"))
    career_win_rate = _to_float(row.get("career_win_rate"))
    sp_class_score = _to_float(row.get("recent_line_sp_class_score"))
    sp_trend = _to_float(row.get("recent_line_sp_trend"))

    # Priority for consistency/ceiling:
    # 1. Class-adjusted recent-line margins — same data as recent_line_adj but each
    #    margin is shifted by the NR grade difference (run grade vs today's race grade).
    #    Contains the most information when available (≥2 lines with NR ceiling data).
    # 2. Horse-page run data — more runs, more reliable, but no per-run grade context.
    # 3. Form-page recent lines — fallback when no profile has been fetched.
    has_horse_data = last5_adj is not None
    has_class_data = class_adj_margin is not None
    consistency_adj = class_adj_margin if has_class_data else (last5_adj if has_horse_data else recent_line_adj)
    ceiling_adj     = class_adj_best   if has_class_data else (best_adj  if has_horse_data else recent_line_best)

    # Market weight weakens as the horse accumulates starts — for exposed horses
    # the model's own data is more reliable than market history (which risks
    # just learning public opinion). For lightly raced horses, trust market more.
    market_min = w.get("market_min", 0.3)
    market_max = w.get("market_max", 0.6)
    if career_starts is None or career_starts < 5:
        market_wt = market_max
    elif career_starts >= 15:
        market_wt = market_min
    else:
        step = (market_max - market_min) / 10.0
        market_wt = round(market_max - step * (career_starts - 5), 2)

    return {
        "consistency": _neg_scale(consistency_adj, divisor=12.0, floor=-4.0, missing=0.0) * w.get("consistency", 1.8),
        "ceiling":     _neg_scale(ceiling_adj,     divisor=10.0, floor=-3.5, missing=0.0) * w.get("ceiling", 1.2),
        "late_speed":  _neg_scale(sec3,            divisor=1.2,  floor=-2.5, missing=0.0) * w.get("late_speed", 1.4),
        # comment_adj removed — the margin adjustments in adjusted_margin already
        # capture positional/trouble credit. Using it as a separate feature
        # double-penalised horses that ran in tough conditions (negative adj)
        # while rewarding soft-trip horses, which is the wrong direction.
        # Data is preserved in recent_line_avg_comment_adj (CSV) and
        # comment_adjustment (DB) for future revisiting.
        # "comment_adj": _pos_scale(comment_adj, center=0.0, divisor=6.0, missing=0.0) * 0.5,
        "tempo_adj":   _pos_scale(tempo_adj, center=0.0, divisor=1.2, missing=0.0) * w.get("tempo_adj", 0.45),
        "tempo_flags": -(tempo_flags or 0.0) * w.get("tempo_flags", 0.08),
        "null_flags":  -(null_flags or 0.0) * w.get("null_flags", 0.25),
        # Market weakens for exposed horses; stronger prior for lightly raced.
        "market":      _neg_log_scale(avg_sp, missing=0.0) * market_wt,
        # win_rate reduced; supplemented by top3_rate and competitive_rate which
        # are less distorted by field quality and bad luck runs.
        "win_rate":      (win_rate or 0.0) * w.get("win_rate", 0.7),
        # Career win rate — a horse that has rarely won over a full career (e.g. 1/32)
        # should carry a meaningful S1 penalty regardless of recent sectional speed.
        # Centred at 12% (typical NSW win rate), capped ±1.5 before applying weight
        # so no single extreme case dominates. Requires ≥5 career starts (None → 0).
        "career_win_rate": max(-1.5, min(1.5, _pos_scale(career_win_rate, center=0.12, divisor=0.08, missing=0.0))) * w.get("career_win_rate", 0.6),
        "top3_rate":     (top3_rate or 0.0) * w.get("top3_rate", 0.6),
        "competitive_rate": (competitive_rate or 0.0) * w.get("competitive_rate", 0.5),
        "nr":          _pos_scale(nr, center=45.0, divisor=8.0, missing=0.0) * w.get("nr", 0.25),
        # Class signals — lower NR headroom = near top of grade; stake class and
        # class delta capture recent competition level vs today's race.
        "class_pos":   _neg_scale(nr_headroom, divisor=8.0, floor=-2.0, missing=0.0) * w.get("class_pos", 0.15),
        # Race purse of recent runs — measures the class of races competed in,
        # not prize money won (which is position-dependent and unreliable as a
        # class proxy). Centred at $8,000 (between the two most common NSW purses
        # of $6,936 and $9,792). Divisor $3,000 gives meaningful spread:
        # country ($6,936) ≈ −0.35, metro ($20k+) capped at +1.2.
        "stake_class": _pos_scale(avg_run_purse, center=8000.0, divisor=3000.0, missing=0.0) * w.get("stake_class", 0.2),
        "class_delta": _pos_scale(class_delta, center=0.0, divisor=-2000.0, missing=0.0) * w.get("class_delta", 0.3),
        # SP relative to class — was the horse well-backed in quality races?
        # sp_class_score = avg(-log(SP) × purse/8000); negative scores = outsider,
        # near-zero = neutral. Capped ±1.5 so a single extreme run doesn't dominate.
        "sp_class": max(-1.5, min(1.5, sp_class_score or 0.0)) * w.get("sp_class", 0.4),
        # SP trend — shortening (negative) = market gaining confidence → boost.
        # Drifting (positive) = market losing confidence → penalty.
        # divisor=-5.0: $5 of shortening → +1.0 in _pos_scale before weight.
        "sp_trend": _pos_scale(sp_trend, center=0.0, divisor=-5.0, missing=0.0) * w.get("sp_trend", 0.3),
    }


def _stage2_components(
    row: dict[str, str],
    weights: dict | None = None,
    field_lead_prob: float | None = None,
    field_death_prob: float | None = None,
    pace_pressure: float = 0.0,
) -> dict[str, float]:
    """Stage 2 — Today's race adjustment.

    Built from race-day factors: barrier draw, projected map position,
    distance suitability (BMR), and fitness (days since last run).
    These are independent of the horse's historical ability rating.

    field_lead_prob / field_death_prob are softmax-normalised across the race
    field so that only one horse can realistically lead.  Weights are scaled up
    (×2.0 / ×1.2) vs the old raw-score weights to preserve a similar contribution
    to the total score despite the probability (0–1) input range.

    pace_pressure = 1.0 - max(field_lead_probs): 0 when one horse clearly leads
    (uncontested), approaches 1.0 when pace is contested across multiple horses.
    Used to boost soft-trip horses and restrained backmarkers in contested fields.
    """
    w = (weights or _DEFAULT_WEIGHTS).get("stage2", _DEFAULT_WEIGHTS["stage2"])
    fw = (weights or _DEFAULT_WEIGHTS).get("fitness", _DEFAULT_WEIGHTS["fitness"])
    map_soft = _to_float(row.get("map_soft_trip_score"))
    map_wide = _to_float(row.get("map_wide_risk_score"))
    style_restrained_rate = _to_float(row.get("style_restrained_rate"))
    barrier = row.get("barrier") or ""
    nr_grade_delta = _to_float(row.get("nr_grade_delta"))
    dist_strike_rate_ratio = _to_float(row.get("dist_strike_rate_ratio"))
    dist_rge_starts = _to_int(row.get("dist_rge_starts")) or 0
    days_since_last_run = _to_float(row.get("days_since_last_run"))
    driver_win_rate = _to_float(row.get("driver_page_season_win_rate"))
    trainer_change_manual = _to_float(row.get("trainer_change_manual"))
    trainer_page_win_rate = _to_float(row.get("trainer_page_season_win_rate"))
    trainer_win_rate_30 = _to_float(row.get("trainer_last_30_win_rate"))
    trainer_win_rate_90 = _to_float(row.get("trainer_last_90_win_rate"))
    class_delta = _to_float(row.get("class_delta"))
    nr_headroom = _to_float(row.get("nr_headroom"))

    return {
        "barrier":      _barrier_score(barrier),
        # Lead and death use field-normalised probabilities (sum to 1.0 across the
        # race) so contested pace automatically reduces the lead bonus.
        # Weight ×2.0 for lead, ×1.2 for death — scaled to match old raw-score
        # contribution for a typical uncontested leader / single death-seat horse.
        "map_lead":     (field_lead_prob or 0.0) * w.get("map_lead", 2.0),
        "map_soft":     (map_soft or 0.0) * w.get("map_soft", 0.45),
        # Soft-trip horses get an extra bonus when pace is genuinely contested —
        # sitting behind a tired, pressured leader is better than an uncontested one.
        # Only fires when pace_pressure > 0; weight 0.3 keeps total soft influence reasonable.
        "map_soft_context": (map_soft or 0.0) * pace_pressure * w.get("map_soft_context", 0.3),
        # Backmarker bonus when pace is contested — restrained-style horses benefit
        # from speed duels (fast early, tired late). Only kicks in at pace_pressure > threshold
        # to avoid rewarding backmarkers in genuinely uncontested fields.
        "pace_backmarker": (style_restrained_rate or 0.0) * max(0.0, pace_pressure - w.get("pace_backmarker_threshold", 0.4)) * w.get("pace_backmarker", 0.6),
        "map_wide":    -(map_wide or 0.0) * w.get("map_wide", 0.5),
        "map_death":   -(field_death_prob or 0.0) * w.get("map_death", 1.2),
        # Distance strike rate — ratio of win% at this distance band vs career win%.
        # ratio > 1 = horse wins more often at this distance than on average → boost.
        # ratio < 1 = horse wins less often → penalty.
        # Neutral (0) when dist_starts < 2 or no career data.
        # Confidence-scaled by sample size: full weight at ≥15 distance starts,
        # scaling linearly to 0 at 0 starts. Prevents a single win in 5 starts
        # from capping the signal at ±1.215 (e.g. 7 starts → 47% confidence).
        "dist_strike_rate": (
            max(-1.35, min(1.35, (dist_strike_rate_ratio - 1.0) / 0.4)) * w.get("dist_strike_rate", 0.9)
            * min(1.0, dist_rge_starts / 15.0)
            if dist_strike_rate_ratio is not None else 0.0
        ),
        # NR grade delta — today's NR ceiling vs avg ceiling of last 5 runs.
        # Negative = dropping in grade → boost. Positive = rising → penalty.
        # ~0.04 per NR point of drop. Requires ≥2 recent lines with NR data (else 0).
        "nr_grade_delta": max(-1.5, min(1.5, _pos_scale(nr_grade_delta, center=0.0, divisor=-10.0, missing=0.0))) * w.get("nr_grade_delta", 0.4),
        # Fitness — graduated penalty by days since last run.
        "fitness":      _fitness_score(days_since_last_run, fw),
        # Driver form — current season win rate from official profile page.
        # Centred at 15% (average NSW win rate). Missing = 0 (no effect).
        "driver_form":  _pos_scale(driver_win_rate, center=0.15, divisor=0.10, missing=0.0) * w.get("driver_form", 0.3),
        # Trainer form and stable-change overlay.
        # We keep this measured: genuine trainer changes matter, but they should
        # not drown out the horse's core profile.
        "trainer_form": _trainer_form_score(trainer_page_win_rate, trainer_win_rate_30, trainer_win_rate_90, w),
        "stable_change": _stable_change_score(
            trainer_change_manual,
            days_since_last_run,
            class_delta,
            nr_headroom,
        ),
    }


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


def _fitness_score(days: float | None, fw: dict | None = None) -> float:
    """Graduated penalty by days since last run.

    ≤ 14 days   →  0.00  (fit, no penalty)
    15–28 days  → -0.35  (short freshening)
    29–42 days  → -0.60  (spell, fitness uncertain)
    43–84 days  → -0.85  (extended spell, likely returning from injury/prep)
    85–99 days  → -1.10  (long absence, significant fitness risk)
    100–119 days → -1.45 (100+ day spell — harshly penalised; horse rarely race-fit first-up)
    120–149 days → -1.70 (extended absence)
    150+ days   → -2.00  (very long layoff)
    None        →  0.00  (no data, treat as fit)
    """
    if days is None:
        return 0.0
    f = fw or _DEFAULT_WEIGHTS["fitness"]
    if days > 149:
        return f.get("tier_150_plus", -2.00)
    if days > 119:
        return f.get("tier_120_149", -1.70)
    if days > 99:
        return f.get("tier_100_119", -1.45)
    if days > 84:
        return f.get("tier_85_99", -1.10)
    if days > 42:
        return f.get("tier_43_84", -0.85)
    if days > 28:
        return f.get("tier_29_42", -0.60)
    if days > 14:
        return f.get("tier_15_28", -0.35)
    return 0.0


def _trainer_form_score(page_win_rate: float | None, win_rate_30: float | None, win_rate_90: float | None, w: dict | None = None) -> float:
    sw = w or _DEFAULT_WEIGHTS["stage2"]
    score_page = _pos_scale(page_win_rate, center=0.12, divisor=0.08, missing=0.0) * sw.get("trainer_form_page", 0.24)
    score_30 = _pos_scale(win_rate_30, center=0.12, divisor=0.08, missing=0.0) * sw.get("trainer_form_30d", 0.12)
    score_90 = _pos_scale(win_rate_90, center=0.12, divisor=0.08, missing=0.0) * sw.get("trainer_form_90d", 0.08)
    return score_page + score_30 + score_90


def _stable_change_score(
    trainer_change_manual: float | None,
    days_since_last_run: float | None,
    class_delta: float | None,
    nr_headroom: float | None,
) -> float:
    """Score a manually flagged trainer change.

    Set via the flag-trainer-change CLI command before scoring.
    Applies a base boost then adjusts for supporting context
    (time off, class rise/drop, NR headroom).
    """
    if trainer_change_manual != 1:
        return 0.0

    score = 0.25
    if days_since_last_run is not None and days_since_last_run >= 45:
        score += 0.20
    elif days_since_last_run is not None and days_since_last_run >= 21:
        score += 0.10

    if class_delta is not None and class_delta >= 500:
        score += 0.15
    elif class_delta is not None and class_delta <= -2000:
        score -= 0.10

    if nr_headroom is not None and nr_headroom < 0:
        score += 0.10
    elif nr_headroom is not None and nr_headroom > 4:
        score -= 0.10

    return score


def _market_key(horse_name: object, runner_number: object) -> tuple[str, int | None]:
    name = str(horse_name or "").strip().upper()
    number = None
    try:
        number = int(runner_number) if runner_number not in (None, "") else None
    except (TypeError, ValueError):
        number = None
    return name, number


def sweep_temperature(
    rows: list[dict[str, str]],
    winners: dict[tuple[str, int], str],
    temperatures: list[float] | None = None,
) -> list[dict[str, object]]:
    """Sweep softmax temperatures and report log loss against known race winners.

    winners maps (meeting_code, race_number) → winning horse name.
    Lower log loss = better calibrated probabilities.
    Returns rows sorted by log_loss ascending so the first entry is the best temperature.

    Requires results from at least ~20 races to be meaningful.  With fewer races
    the optimal temperature will overfit to noise.
    """
    if temperatures is None:
        # 0.5 to 6.0 in 0.25 steps
        temperatures = [round(0.5 + i * 0.25, 2) for i in range(23)]

    race_keys = sorted({
        (row["meeting_code"], int(row["race_number"]))
        for row in rows
        if row.get("meeting_code") and row.get("race_number")
    })

    results: list[dict[str, object]] = []
    for temp in temperatures:
        total_log_loss = 0.0
        race_count = 0
        for meeting_code, race_number in race_keys:
            winner_name = winners.get((meeting_code, race_number))
            if not winner_name:
                continue
            scored = score_race_rows(rows, meeting_code, race_number, temperature=temp)
            if not scored:
                continue
            winner_key = str(winner_name).strip().upper()
            winner_prob = next(
                (h["win_probability"] for h in scored
                 if str(h["horse_name"]).strip().upper() == winner_key),
                None,
            )
            if winner_prob is None or winner_prob <= 0:
                continue
            total_log_loss += -math.log(winner_prob)
            race_count += 1
        if race_count > 0:
            results.append({
                "temperature": temp,
                "log_loss": round(total_log_loss / race_count, 4),
                "races_scored": race_count,
            })

    results.sort(key=lambda r: r["log_loss"])
    return results
