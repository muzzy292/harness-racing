"""
Weight sweep script for QLD ROI optimisation.
Round 1: individual sweeps on key weights.
Round 2: combined best values.
DO NOT modify weights.json.
"""
import copy
import sys
import os
from datetime import datetime

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.harness_model.web import _compute_bet_records
from src.harness_model.storage import connect, init_db
from src.harness_model.odds import load_weights

DB_PATH = "data/harness.db"
CSV_PATH = "data/features/runner_features.csv"
WEIGHTS_PATH = "weights.json"
FROM_DATE = datetime(2026, 5, 1)
STATE = "QLD"
BANKROLL = 1000.0
EDGE_35 = 35.0
EDGE_50 = 50.0


def calc_roi_at_threshold(records, threshold):
    filtered = [r for r in records if r["edge_pct"] >= threshold]
    total_staked = sum(r["stake"] for r in filtered)
    total_profit = sum(r["profit"] for r in filtered)
    n = len(filtered)
    roi = total_profit / total_staked * 100 if total_staked else 0.0
    return round(roi, 2), n


def run_config(conn, weights_override: dict, label: str, base_weights: dict):
    """Run _compute_bet_records with a modified copy of base_weights, filtered to QLD from May 1."""
    w = copy.deepcopy(base_weights)
    for section, vals in weights_override.items():
        if isinstance(vals, dict):
            for k, v in vals.items():
                w[section][k] = v
        else:
            w[section] = vals

    records, summary = _compute_bet_records(
        conn, CSV_PATH, w, BANKROLL, state=STATE, from_date=FROM_DATE
    )
    roi_35, n_35 = calc_roi_at_threshold(records, EDGE_35)
    roi_50, n_50 = calc_roi_at_threshold(records, EDGE_50)

    print(
        f"  {label:<55} | bets={summary['total_bets']:3d} wins={summary['winners']:3d} "
        f"wr={summary['win_rate']:5.1f}% roi={summary['roi']:+7.2f}% "
        f"| >=35%: n={n_35:3d} roi={roi_35:+7.2f}% "
        f"| >=50%: n={n_50:3d} roi={roi_50:+7.2f}%"
    )
    sys.stdout.flush()
    return summary, roi_35, roi_50, records


def main():
    conn = connect(DB_PATH)
    init_db(conn)
    base_weights = load_weights(WEIGHTS_PATH)

    print("=" * 130)
    print("BASELINE (current weights.json) — QLD only, from 2026-05-01")
    print("=" * 130)
    _, baseline_roi35, baseline_roi50, _ = run_config(conn, {}, "BASELINE", base_weights)

    print()
    print("=" * 130)
    print("ROUND 1: Individual sweeps (one weight at a time)")
    print("=" * 130)

    best = {}  # param_name -> (best_value, best_roi35)

    def sweep(section, key, values):
        param = f"{section}.{key}"
        print(f"\n--- {param} (baseline={base_weights[section][key]}) ---")
        best_v, best_r = None, -9999.0
        for v in values:
            _, r35, r50, _ = run_config(conn, {section: {key: v}}, f"{key}={v}", base_weights)
            if r35 > best_r:
                best_r, best_v = r35, v
        print(f"  >> Best: {key}={best_v}  roi35={best_r:+.2f}%  (vs baseline {baseline_roi35:+.2f}%)")
        best[param] = (best_v, best_r)
        return best_v, best_r

    # 1. nr
    sweep("stage1", "nr", [0.3, 0.5, 0.75, 1.0, 1.5, 2.0])

    # 2. consistency
    sweep("stage1", "consistency", [0.5, 1.0, 1.5, 2.0, 2.5, 3.0])

    # 3. ceiling
    sweep("stage1", "ceiling", [0.5, 1.0, 1.5, 2.0, 2.5])

    # 4. win_rate
    sweep("stage1", "win_rate", [0.5, 1.0, 1.5, 2.0, 2.5])

    # 5. softmax temperature
    sweep("softmax", "temperature", [1.2, 1.4, 1.6, 1.8, 2.0, 2.4, 3.0])

    # 6. nr_grade_delta (S2)
    sweep("stage2", "nr_grade_delta", [0.5, 1.0, 1.5, 2.0, 2.5, 3.0])

    print()
    print("=" * 130)
    print("ROUND 1 SUMMARY")
    print("=" * 130)
    for param, (val, r35) in best.items():
        baseline_val = base_weights[param.split(".")[0]][param.split(".")[1]]
        print(f"  {param:<30} baseline={baseline_val:<6} -> best={val:<6}  roi35={r35:+.2f}%")

    print()
    print("=" * 130)
    print("ROUND 2: Combined sweeps")
    print("=" * 130)

    # Extract best individual values
    nr_best        = best["stage1.nr"][0]
    cons_best      = best["stage1.consistency"][0]
    ceil_best      = best["stage1.ceiling"][0]
    wr_best        = best["stage1.win_rate"][0]
    temp_best      = best["softmax.temperature"][0]
    nrgd_best      = best["stage2.nr_grade_delta"][0]

    print("\n--- Combo A: all best individual values ---")
    run_config(conn, {
        "stage1": {"nr": nr_best, "consistency": cons_best, "ceiling": ceil_best, "win_rate": wr_best},
        "stage2": {"nr_grade_delta": nrgd_best},
        "softmax": {"temperature": temp_best},
    }, "Combo A: all best", base_weights)

    print("\n--- Combo B: best S1 only (keep S2 + temp at baseline) ---")
    run_config(conn, {
        "stage1": {"nr": nr_best, "consistency": cons_best, "ceiling": ceil_best, "win_rate": wr_best},
    }, "Combo B: best S1 only", base_weights)

    print("\n--- Combo C: best temp + nr_grade_delta (keep S1 at baseline) ---")
    run_config(conn, {
        "stage2": {"nr_grade_delta": nrgd_best},
        "softmax": {"temperature": temp_best},
    }, "Combo C: best temp+nrgd", base_weights)

    print("\n--- Combo D: temperature grid alone ---")
    for t in [2.0, 2.4, 3.0]:
        run_config(conn, {"softmax": {"temperature": t}}, f"Combo D: temp={t}", base_weights)

    print("\n--- Combo E: nr x temperature grid ---")
    for nr_v in [0.5, 0.75, 1.0]:
        for temp_v in [2.0, 2.4, 3.0]:
            run_config(conn,
                {"stage1": {"nr": nr_v}, "softmax": {"temperature": temp_v}},
                f"Combo E: nr={nr_v} temp={temp_v}", base_weights)

    print("\n--- Combo F: all best S1 + temperature variations ---")
    for temp_v in [2.0, 2.4, 3.0]:
        run_config(conn, {
            "stage1": {"nr": nr_best, "consistency": cons_best, "ceiling": ceil_best, "win_rate": wr_best},
            "stage2": {"nr_grade_delta": nrgd_best},
            "softmax": {"temperature": temp_v},
        }, f"Combo F: all best + temp={temp_v}", base_weights)

    # Extra: market blend variations (reduce model confidence)
    print("\n--- Combo G: market blend variations ---")
    for mw, mkw in [(0.3, 0.7), (0.4, 0.6), (0.45, 0.55), (0.6, 0.4), (0.7, 0.3)]:
        run_config(conn, {
            "softmax": {"model_weight": mw, "market_weight": mkw},
        }, f"Combo G: model_w={mw} market_w={mkw}", base_weights)

    conn.close()
    print()
    print("Sweep complete.")


if __name__ == "__main__":
    main()
