"""Gradient-boosted tree second model.

Trains on raw feature columns from runner_features.csv, produces within-race
win probabilities, and blends them with the rule-based model's output.

HistGradientBoostingClassifier is used throughout — it handles NaN natively
(no imputer required) and is fast enough on this data volume.
"""
from __future__ import annotations

import math
import pickle
from collections import defaultdict
from pathlib import Path

# Raw CSV columns used as tree inputs.
# Excludes metadata (meeting_code, horse_name, etc.) and already-blended outputs.
_FEATURE_COLS: list[str] = [
    # Recent form — margins
    "recent_line_avg_class_adj_margin",
    "recent_line_best_class_adj_margin",
    "recent_line_avg_raw_margin",
    "recent_line_avg_adj_margin",
    "recent_line_best_adj_margin",
    "ceiling_support_rate",
    "ceiling_best_run_index",
    # Sectionals / speed
    "last_3_avg_sectional_delta",
    "last_5_avg_sectional_delta",
    "best_recent_sectional_delta",
    # Tempo / data quality flags
    "recent_line_avg_tempo_adj",
    "recent_line_tempo_flags",
    "recent_line_null_flags",
    # Win / place rates
    "last_5_win_rate",
    "last_10_win_rate",
    "last_5_top3_rate",
    "last_5_competitive_rate",
    "last_5_avg_adj_margin",
    "last_5_best_adj_margin",
    # Career
    "career_win_rate",
    "career_starts",
    # Season
    "season_starts",
    "season_wins",
    # NR / class
    "nr_rating",
    "nr_headroom",
    "race_nr_ceiling",
    "nr_grade_delta",
    "avg_recent_nr_ceiling",
    "class_delta",
    # Race map / style
    "style_lead_rate",
    "style_forward_rate",
    "style_restrained_rate",
    "style_death_rate",
    "style_wide_rate",
    "map_lead_score",
    "map_death_score",
    "map_soft_trip_score",
    "map_wide_risk_score",
    "barrier_relief_score",
    # Fitness / freshness
    "days_since_last_run",
    "second_up_improvement",
    "developmental_return",
    # Distance record
    "dist_strike_rate_ratio",
    "dist_rge_starts",
    # Driver / trainer stats
    "driver_last_30_win_rate",
    "driver_last_30_starts",
    "driver_last_90_win_rate",
    "trainer_last_30_win_rate",
    "trainer_last_30_starts",
    "trainer_last_90_win_rate",
    # SP signals
    "sp_avg_prob_last5",
    "sp_best_prob_last5",
    "sp_best_prob_at_class",
    "sp_reliability_rate",
    "last_5_avg_sp",
    # Field context
    "race_field_size",
    "race_field_avg_nr",
    # Last win
    "last_win_run_index",
    "last_win_class_adj_margin",
]


def _to_float(v: object) -> float:
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float("nan")


def _feature_row(row: dict[str, str], cols: list[str]) -> list[float]:
    return [_to_float(row.get(c)) for c in cols]


def _softmax(values: list[float]) -> list[float]:
    safe = [v if not math.isnan(v) else 0.0 for v in values]
    m = max(safe)
    exps = [math.exp(v - m) for v in safe]
    total = sum(exps)
    return [e / total for e in exps]


def train_tree(
    rows: list[dict[str, str]],
    winners: dict[tuple[str, int], str],
    feature_cols: list[str] | None = None,
    test_fraction: float = 0.25,
) -> dict:
    """Train a HistGradientBoostingClassifier on raw CSV features.

    Uses a time-based train/holdout split (sorted by meeting code) to avoid
    data leakage.  Returns a result dict with keys:
        model, feature_cols, train_races, test_races,
        feature_importances (list of (name, importance) sorted desc)
    """
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
    except ImportError:
        raise RuntimeError("scikit-learn required — pip install scikit-learn")

    cols = feature_cols or _FEATURE_COLS

    # Group runners by race
    races: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        mc = row.get("meeting_code", "")
        rn = int(row.get("race_number") or 0)
        if mc and rn:
            races[(mc, rn)].append(row)

    # Sort chronologically (meeting code encodes DDMMYY in positions 2-8)
    race_keys = sorted(races.keys())

    n_test = max(1, int(len(race_keys) * test_fraction))
    train_keys = set(race_keys[:-n_test])
    test_keys  = set(race_keys[-n_test:])

    X_train: list[list[float]] = []
    y_train: list[int]         = []
    X_test:  list[list[float]] = []
    y_test:  list[int]         = []
    # Keep test race membership for ROI evaluation later
    test_race_idx: list[tuple] = []
    test_horse_names: list[str] = []

    for key in race_keys:
        race_rows = races[key]
        if key not in winners:
            continue
        winner_key = winners[key].strip().upper()
        for row in race_rows:
            feat = _feature_row(row, cols)
            won  = 1 if str(row.get("horse_name", "")).strip().upper() == winner_key else 0
            if key in train_keys:
                X_train.append(feat)
                y_train.append(won)
            else:
                X_test.append(feat)
                y_test.append(won)
                test_race_idx.append(key)
                test_horse_names.append(str(row.get("horse_name", "")))

    if not X_train:
        raise RuntimeError("No training data — check race_results are populated.")

    model = HistGradientBoostingClassifier(
        max_iter=300,
        max_depth=4,
        learning_rate=0.05,
        min_samples_leaf=15,
        random_state=42,
    )
    model.fit(X_train, y_train)

    # Feature importances via permutation on training set
    try:
        from sklearn.inspection import permutation_importance
        pi = permutation_importance(model, X_train, y_train, n_repeats=5, random_state=42, n_jobs=-1)
        importances = list(zip(cols, pi.importances_mean))
    except Exception:
        # Fallback: uniform importances if permutation fails
        importances = [(c, 0.0) for c in cols]
    importances.sort(key=lambda x: x[1], reverse=True)

    return {
        "model":               model,
        "feature_cols":        cols,
        "train_races":         len(train_keys & winners.keys()),
        "test_races":          len(test_keys & winners.keys()),
        "train_samples":       len(X_train),
        "test_samples":        len(X_test),
        "feature_importances": importances,
        # Raw test set for external ROI evaluation
        "_X_test":             X_test,
        "_y_test":             y_test,
        "_test_race_idx":      test_race_idx,
        "_test_horse_names":   test_horse_names,
        "_test_keys":          test_keys,
    }


def tree_race_probs(
    race_rows: list[dict[str, str]],
    model,
    feature_cols: list[str],
) -> dict[str, float]:
    """Return within-race win probabilities from the tree for one race.

    Probabilities are normalised across the field so they sum to 1.
    Key is horse_name.upper().
    """
    if not race_rows:
        return {}
    X = [_feature_row(row, feature_cols) for row in race_rows]
    raw_probs = [p[1] for p in model.predict_proba(X)]
    normed = _softmax(raw_probs)
    return {
        str(row.get("horse_name", "")).strip().upper(): prob
        for row, prob in zip(race_rows, normed)
    }


def save_tree(result: dict, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "model":        result["model"],
        "feature_cols": result["feature_cols"],
    }
    with open(path, "wb") as f:
        pickle.dump(payload, f)


def load_tree(path: str | Path) -> tuple:
    """Returns (model, feature_cols)."""
    with open(path, "rb") as f:
        payload = pickle.load(f)
    return payload["model"], payload["feature_cols"]
