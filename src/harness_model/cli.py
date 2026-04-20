from __future__ import annotations

import argparse
from pathlib import Path

from .storage import connect, scratch_horse as db_scratch_horse, set_trainer_change_manual as db_set_trainer_change, set_trainer_form_manual as db_set_trainer_form
from .pipeline import (
    build_feature_dataset,
    build_track_par_database,
    calibrate_nr_factor,
    calibrate_temperature,
    fetch_driver_stats_for_meeting,
    fetch_horse_pages_from_meeting_html,
    fetch_meeting,
    fetch_results_history,
    fetch_results,
    fetch_trainer_stats_for_meeting,
    ingest_horse_dir,
    ingest_horse_html,
    ingest_meeting_html,
    ingest_results_html,
    ingest_results_dir,
    snapshot_meeting,
    sync_upcoming_meetings,
    sync_recent_results,
)
from .odds import (
    flatten_meeting_scores,
    load_feature_rows,
    load_market_rows,
    load_weights,
    render_meeting_odds,
    render_race_odds_table,
    score_meeting_rows,
    score_race_rows,
    write_scored_rows_csv,
)
from .web import build_betting_site, build_diagnose_site, build_meeting_site, build_stats_site, publish_scored_meeting, republish_all_meetings, serve_site


_DEFAULT_WEIGHTS_PATH = Path("weights.json")


def _resolve_weights(explicit_path: str | None) -> dict | None:
    """Load weights from an explicit path, or from data/weights.json if it exists.

    Returns None if no weights file is found — callers fall back to hardcoded defaults.
    """
    path = Path(explicit_path) if explicit_path else _DEFAULT_WEIGHTS_PATH
    if path.exists():
        return load_weights(path)
    if explicit_path:
        raise FileNotFoundError(f"Weights file not found: {explicit_path}")
    return None


def _print_backtest_report(records: list[dict], min_edge: float = 0.0, model_only: bool = False) -> None:
    odds_key = "model_odds" if model_only else "blended_odds"
    prob_key = "model_prob" if model_only else "blended_prob"
    odds_label = "Model-only" if model_only else "Blended (model*0.45 + market*0.55)"

    valid = [r for r in records if r[odds_key] is not None and r["sp"] is not None and r["sp"] > 1.0]
    total_horses = len(valid)
    total_races = len({(r["meeting"], r["race"]) for r in valid})
    total_winners = sum(1 for r in valid if r["won"])

    print(f"\nBacktest Report - {total_horses} runners across {total_races} races  ({odds_label})")
    print(f"Races with at least one result: {total_races}  |  Winners recorded: {total_winners}\n")

    thresholds = [(0.0, "Any edge"), (0.10, ">=10%"), (0.25, ">=25%"), (0.50, ">=50%"), (1.00, ">=100%")]

    def _roi_row(bets: list[dict]) -> tuple:
        n = len(bets)
        if n == 0:
            return (0, 0, 0.0, 0.0, 0.0)
        winners = [r for r in bets if r["won"]]
        w = len(winners)
        avg_sp = sum(r["sp"] for r in bets) / n
        roi = (sum(r["sp"] for r in winners) - n) / n * 100
        return (n, w, w / n * 100, avg_sp, roi)

    def _print_roi_table(odds_k: str) -> None:
        print(f"  {'Edge threshold':<16}  {'Bets':>6}  {'Winners':>8}  {'Strike%':>8}  {'Avg SP':>8}  {'ROI%':>8}")
        print("  " + "-" * 62)
        for threshold, label in thresholds:
            bets = [
                r for r in valid
                if r[odds_k] < r["sp"] and (r["sp"] / r[odds_k] - 1) >= threshold
            ]
            n, w, strike, avg_sp, roi = _roi_row(bets)
            if n == 0:
                continue
            print(f"  {label:<16}  {n:>6}  {w:>8}  {strike:>7.1f}%  ${avg_sp:>6.2f}  {roi:>+7.1f}%")

    print(f"--- 1. Value Bet ROI - {odds_label} ---")
    _print_roi_table(odds_key)

    # Note: blended < SP iff model < SP (mathematical identity), so the same horses
    # qualify for both. We show the model-only table only when --model-only is set.

    # Calibration buckets
    buckets = [(0.0, 0.05, "0-5%"), (0.05, 0.10, "5-10%"), (0.10, 0.20, "10-20%"),
               (0.20, 0.35, "20-35%"), (0.35, 1.01, "35%+")]
    print(f"\n--- 2. Calibration (model prob vs actual win rate) ---")
    print(f"  {'Prob bucket':<12}  {'Horses':>7}  {'Actual W':>9}  {'Actual%':>8}  {'Model avg%':>11}  {'Diff':>7}")
    print("  " + "-" * 60)
    for lo, hi, label in buckets:
        bucket = [r for r in valid if lo <= r[prob_key] < hi]
        if not bucket:
            continue
        n = len(bucket)
        w = sum(1 for r in bucket if r["won"])
        actual_pct = w / n * 100
        model_avg_pct = sum(r[prob_key] for r in bucket) / n * 100
        diff = actual_pct - model_avg_pct
        print(f"  {label:<12}  {n:>7}  {w:>9}  {actual_pct:>7.1f}%  {model_avg_pct:>10.1f}%  {diff:>+6.1f}%")

    # Top value bets (model shortest vs SP)
    value_records = sorted(
        [r for r in valid if r[odds_key] < r["sp"]],
        key=lambda r: r[odds_key] / r["sp"],
    )
    sec = "3"
    print(f"\n--- {sec}. Top 20 value bets (model shortest vs SP) ---")
    print(f"  {'Meeting':<10}  {'R':>2}  {'Horse':<26}  {'Model $':>8}  {'SP $':>7}  {'Edge':>7}  Won?")
    print("  " + "-" * 74)
    for r in value_records[:20]:
        edge = r["sp"] / r[odds_key] - 1
        won = "YES" if r["won"] else "no"
        print(f"  {r['meeting']:<10}  {r['race']:>2}  {r['horse']:<26}  ${r[odds_key]:>6.2f}  ${r['sp']:>5.2f}  {edge:>+6.0%}  {won}")

    # Top model misses (model much longer than SP)
    miss_records = sorted(valid, key=lambda r: r[odds_key] / r["sp"], reverse=True)
    print(f"\n--- 4. Top 20 model misses (model far above SP) ---")
    print(f"  {'Meeting':<10}  {'R':>2}  {'Horse':<26}  {'Model $':>8}  {'SP $':>7}  {'Ratio':>7}  Won?")
    print("  " + "-" * 74)
    for r in miss_records[:20]:
        ratio = r[odds_key] / r["sp"]
        won = "YES" if r["won"] else "no"
        print(f"  {r['meeting']:<10}  {r['race']:>2}  {r['horse']:<26}  ${r[odds_key]:>6.2f}  ${r['sp']:>5.2f}  {ratio:>6.1f}x  {won}")


def _print_diagnose_report(race_groups: dict, out_path: str | None = None) -> None:
    """Per-race truth set: hit rates, failure quadrants, S1/S2 analysis, biggest misses.

    race_groups: (meeting_code, race_number) -> list of horse dicts with ranks assigned.
    """
    all_keys = sorted(race_groups.keys())
    total_races = len(all_keys)
    if total_races == 0:
        print("No races with results found.")
        return

    def _avg(lst: list) -> float:
        return sum(lst) / len(lst) if lst else 0.0

    # Build one summary dict per race
    summaries = []
    for key in all_keys:
        horses = race_groups[key]
        meeting, race = key
        winner    = next((h for h in horses if h["finish_pos"] == 1), None)
        model_top = next((h for h in horses if h["model_rank"] == 1), None)
        sp_top    = next((h for h in horses if h["sp_rank"] == 1), None)
        s1_top    = next((h for h in horses if h["s1_rank"] == 1), None)
        model_top3 = [h for h in horses if h["model_rank"] <= 3]
        best_map   = max(horses, key=lambda h: (h.get("map_lead_score") or -99))
        summaries.append({
            "meeting": meeting, "race": race,
            "winner": winner, "model_top": model_top, "sp_top": sp_top,
            "s1_top": s1_top, "model_top3": model_top3, "best_map": best_map,
            "horses": horses,
        })

    def _won(r: dict, pick_key: str) -> bool:
        return bool(r["winner"] and r[pick_key] and r["winner"]["horse"] == r[pick_key]["horse"])

    # ---- 1. Hit Rate Summary ----
    model_correct = sum(1 for r in summaries if _won(r, "model_top"))
    sp_correct    = sum(1 for r in summaries if _won(r, "sp_top"))
    top3_hit      = sum(1 for r in summaries if r["winner"] and any(h["horse"] == r["winner"]["horse"] for h in r["model_top3"]))
    rank2         = sum(1 for r in summaries if r["winner"] and r["winner"].get("model_rank") == 2)
    rank3         = sum(1 for r in summaries if r["winner"] and r["winner"].get("model_rank") == 3)
    rank4plus     = sum(1 for r in summaries if r["winner"] and (r["winner"].get("model_rank") or 0) >= 4)

    print(f"\nDiagnose Report  --  {total_races} races with results\n")
    print("--- 1. Hit Rate Summary ---")
    print(f"  {'':32}  Model       SP (market)")
    print(f"  {'Top pick won':32}  {model_correct}/{total_races} ({model_correct/total_races*100:.1f}%)   {sp_correct}/{total_races} ({sp_correct/total_races*100:.1f}%)")
    print(f"  {'Top 3 contained winner':32}  {top3_hit}/{total_races} ({top3_hit/total_races*100:.1f}%)")
    print(f"  {'Winner was model 2nd':32}  {rank2}/{total_races}")
    print(f"  {'Winner was model 3rd':32}  {rank3}/{total_races}")
    print(f"  {'Winner was model 4th+':32}  {rank4plus}/{total_races}")

    # ---- 2. Failure Pattern Quadrants ----
    both_correct = sum(1 for r in summaries if _won(r, "model_top") and _won(r, "sp_top"))
    model_edge   = sum(1 for r in summaries if _won(r, "model_top") and not _won(r, "sp_top"))
    sp_failure   = sum(1 for r in summaries if not _won(r, "model_top") and _won(r, "sp_top"))
    both_wrong   = sum(1 for r in summaries if not _won(r, "model_top") and not _won(r, "sp_top"))

    print(f"\n--- 2. Failure Pattern Breakdown ---")
    print(f"  {'Outcome':<34}  {'Races':>6}  {'%':>6}")
    print("  " + "-" * 52)
    print(f"  {'Model correct, SP correct':<34}  {both_correct:>6}  {both_correct/total_races*100:>5.1f}%  <- both right")
    print(f"  {'Model correct, SP wrong':<34}  {model_edge:>6}  {model_edge/total_races*100:>5.1f}%  <- model edge")
    print(f"  {'SP correct, model wrong':<34}  {sp_failure:>6}  {sp_failure/total_races*100:>5.1f}%  <- model failure")
    print(f"  {'Both wrong':<34}  {both_wrong:>6}  {both_wrong/total_races*100:>5.1f}%  <- hard race")

    # ---- 3. S1 vs S2 on Model Failures ----
    failures = [r for r in summaries if r["winner"] and r["model_top"] and r["winner"]["horse"] != r["model_top"]["horse"]]
    if failures:
        top_s1  = [r["model_top"]["stage1"] for r in failures if r["model_top"]["stage1"] is not None]
        top_s2  = [r["model_top"]["stage2"] for r in failures if r["model_top"]["stage2"] is not None]
        top_tot = [r["model_top"]["total_score"] for r in failures if r["model_top"]["total_score"] is not None]
        win_s1  = [r["winner"]["stage1"] for r in failures if r["winner"]["stage1"] is not None]
        win_s2  = [r["winner"]["stage2"] for r in failures if r["winner"]["stage2"] is not None]
        win_tot = [r["winner"]["total_score"] for r in failures if r["winner"]["total_score"] is not None]
        avg_wr  = _avg([r["winner"].get("model_rank", 0) for r in failures])
        s2_swung = sum(1 for r in failures if r["s1_top"] and r["model_top"] and r["s1_top"]["horse"] != r["model_top"]["horse"])

        print(f"\n--- 3. S1 vs S2 Contribution on Model Failures (n={len(failures)}) ---")
        print(f"  {'':30}  Model top pick   Actual winner")
        print(f"  {'Avg S1 score':30}  {_avg(top_s1):>13.3f}   {_avg(win_s1):>12.3f}")
        print(f"  {'Avg S2 score':30}  {_avg(top_s2):>13.3f}   {_avg(win_s2):>12.3f}")
        print(f"  {'Avg total score':30}  {_avg(top_tot):>13.3f}   {_avg(win_tot):>12.3f}")
        print(f"  {'Avg winner model rank':30}  {avg_wr:>13.1f}")
        print(f"  {'S2 changed top pick':30}  {s2_swung:>13} races (S1 rank 1 != final rank 1)")

    # ---- 4. Per-Race Detail Table ----
    print(f"\n--- 4. Per-Race Detail Table ---")
    print(f"  {'Meeting':<10}  {'R':>2}  {'Winner':<22}  {'SP$':>6}  {'Mdl#':>4}  {'SP#':>4}  {'Model top 3':<38}  {'W.S1':>6}  {'W.S2':>6}")
    print("  " + "-" * 112)
    for r in summaries:
        w = r["winner"]
        if not w:
            continue
        top3_str = ", ".join(h["horse"][:12] for h in sorted(r["model_top3"], key=lambda h: h["model_rank"]))
        sp_str   = f"${w['sp']:.2f}" if w.get("sp") else "  n/a"
        s1_str   = f"{w['stage1']:+.2f}" if w.get("stage1") is not None else "  n/a"
        s2_str   = f"{w['stage2']:+.2f}" if w.get("stage2") is not None else "  n/a"
        print(f"  {r['meeting']:<10}  {r['race']:>2}  {w['horse']:<22}  {sp_str:>6}  {w.get('model_rank','?'):>4}  {w.get('sp_rank','?'):>4}  {top3_str:<38}  {s1_str:>6}  {s2_str:>6}")

    # ---- 5. SP Knew, Model Didn't ----
    sp_knew = [
        r for r in summaries
        if r["winner"] and r["sp_top"] and r["model_top"]
        and r["winner"]["horse"] == r["sp_top"]["horse"]
        and (r["winner"].get("model_rank") or 0) >= 4
    ]
    sp_knew.sort(key=lambda r: r["winner"].get("model_rank") or 0, reverse=True)
    print(f"\n--- 5. SP Knew, Model Didn't (SP fav won, model rank 4+) --- n={len(sp_knew)} ({len(sp_knew)/total_races*100:.1f}% of races)")
    if sp_knew:
        print(f"  {'Meeting':<10}  {'R':>2}  {'Winner':<22}  {'Mdl#':>4}  {'SP#':>4}  {'Model$':>8}  {'SP$':>6}  {'nr_grade_delta':>15}  {'sp_class_ceil':>14}")
        print("  " + "-" * 104)
        for r in sp_knew:
            w = r["winner"]
            ngd = w.get("nr_grade_delta")
            spc = w.get("sp_class_ceiling")
            ngd_str  = f"{ngd:+.1f}" if ngd is not None else "    n/a"
            spc_str  = f"{spc:+.3f}" if spc is not None else "      n/a"
            odds_str = f"${w['model_odds']:.2f}" if w.get("model_odds") else "  n/a"
            sp_str   = f"${w['sp']:.2f}" if w.get("sp") else " n/a"
            print(f"  {r['meeting']:<10}  {r['race']:>2}  {w['horse']:<22}  {w.get('model_rank','?'):>4}  {w.get('sp_rank','?'):>4}  {odds_str:>8}  {sp_str:>6}  {ngd_str:>15}  {spc_str:>14}")

    # ---- 6. Map Override Opportunities ----
    map_overrides = []
    for r in summaries:
        bm = r["best_map"]
        mt = r["model_top"]
        if not (bm and mt):
            continue
        if bm["horse"] == mt["horse"]:
            continue  # best-map horse IS the top pick — no override opportunity
        bm_finish = bm.get("finish_pos")
        if bm_finish and bm_finish <= 3:
            map_overrides.append({
                "meeting": r["meeting"], "race": r["race"],
                "bm_horse": bm["horse"],
                "bm_model_rank": bm.get("model_rank"),
                "bm_sp_rank": bm.get("sp_rank"),
                "bm_finish": bm_finish,
                "bm_map": bm.get("map_lead_score"),
                "winner": r["winner"]["horse"] if r["winner"] else "?",
                "winner_map": r["winner"].get("map_lead_score") if r["winner"] else None,
            })
    print(f"\n--- 6. Map Override Opportunities (best-map horse not top pick, placed top 3) --- n={len(map_overrides)} ({len(map_overrides)/total_races*100:.1f}% of races)")
    if map_overrides:
        print(f"  {'Meeting':<10}  {'R':>2}  {'Best-map horse':<22}  {'Mdl#':>4}  {'SP#':>4}  {'Finish':>6}  {'MapLd':>6}  {'Winner':<22}  {'WinMap':>6}")
        print("  " + "-" * 108)
        for mo in map_overrides[:20]:
            bm_map = f"{mo['bm_map']:.2f}" if mo.get("bm_map") is not None else "  n/a"
            w_map  = f"{mo['winner_map']:.2f}" if mo.get("winner_map") is not None else "  n/a"
            print(f"  {mo['meeting']:<10}  {mo['race']:>2}  {mo['bm_horse']:<22}  {mo['bm_model_rank']:>4}  {mo['bm_sp_rank']:>4}  {mo['bm_finish']:>6}  {bm_map:>6}  {mo['winner']:<22}  {w_map:>6}")

    # ---- 7. Biggest Model Misses ----
    misses = sorted(
        [r for r in summaries if r["winner"] and (r["winner"].get("model_rank") or 0) >= 5],
        key=lambda r: r["winner"].get("model_rank") or 0, reverse=True,
    )
    print(f"\n--- 7. Biggest Model Misses (winner ranked 5th+ by model) --- n={len(misses)}")
    if misses:
        print(f"  {'Meeting':<10}  {'R':>2}  {'Winner':<22}  {'Mdl#':>4}  {'SP#':>4}  {'Model$':>8}  {'SP$':>6}  {'S1':>7}  {'S2':>7}  {'nr_grade_d':>11}  {'NR ceil':>8}")
        print("  " + "-" * 116)
        for r in misses[:20]:
            w = r["winner"]
            ngd = w.get("nr_grade_delta")
            nrc = w.get("race_nr_ceiling")
            s1_str   = f"{w['stage1']:+.3f}" if w.get("stage1") is not None else "    n/a"
            s2_str   = f"{w['stage2']:+.3f}" if w.get("stage2") is not None else "    n/a"
            ngd_str  = f"{ngd:+.1f}" if ngd is not None else "      n/a"
            nrc_str  = f"{nrc:.0f}" if nrc is not None else "    n/a"
            odds_str = f"${w['model_odds']:.2f}" if w.get("model_odds") else "  n/a"
            sp_str   = f"${w['sp']:.2f}" if w.get("sp") else " n/a"
            print(f"  {r['meeting']:<10}  {r['race']:>2}  {w['horse']:<22}  {w.get('model_rank','?'):>4}  {w.get('sp_rank','?'):>4}  {odds_str:>8}  {sp_str:>6}  {s1_str:>7}  {s2_str:>7}  {ngd_str:>11}  {nrc_str:>8}")

    # ---- Optional CSV output ----
    if out_path:
        import csv as _csv
        csv_fields = [
            "meeting", "race", "horse", "horse_id", "runner_number",
            "finish_pos", "model_rank", "sp_rank", "s1_rank",
            "model_prob", "model_odds", "sp",
            "stage1", "stage2", "total_score",
            "race_nr_ceiling", "nr_grade_delta",
            "sp_avg_market", "sp_class_ceiling", "sp_best_prob_at_class",
            "map_lead_score", "map_death_score", "map_soft_trip_score",
        ]
        all_rows = [h for horses in race_groups.values() for h in horses]
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\nWrote per-horse diagnostic CSV to {out_path}  ({len(all_rows)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Harness racing odds model pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_meeting_parser = subparsers.add_parser("fetch-meeting", help="Fetch a meeting page")
    fetch_meeting_parser.add_argument("--meeting-code", required=True)
    fetch_meeting_parser.add_argument("--out", default="data/raw")

    ingest_meeting_parser = subparsers.add_parser("ingest-meeting", help="Parse and store a meeting HTML file")
    ingest_meeting_parser.add_argument("--html", required=True)
    ingest_meeting_parser.add_argument("--db", default="data/harness.db")

    fetch_results_parser = subparsers.add_parser("fetch-results", help="Fetch a post-race results page for a meeting")
    fetch_results_parser.add_argument("--meeting-code", required=True)
    fetch_results_parser.add_argument("--out", default="data/raw")

    fetch_results_history_parser = subparsers.add_parser("fetch-results-history", help="Fetch recent NSW results pages from the HRNSW results index")
    fetch_results_history_parser.add_argument("--out", default="data/raw")
    fetch_results_history_parser.add_argument("--tracks", help="Comma-separated track names to include")
    fetch_results_history_parser.add_argument("--limit", type=int)
    fetch_results_history_parser.add_argument("--force-refresh", action="store_true")

    ingest_results_parser = subparsers.add_parser("ingest-results", help="Parse and store a results HTML file")
    ingest_results_parser.add_argument("--html", required=True)
    ingest_results_parser.add_argument("--db", default="data/harness.db")

    ingest_results_dir_parser = subparsers.add_parser("ingest-results-dir", help="Parse and store all results HTML files in a folder")
    ingest_results_dir_parser.add_argument("--results-dir", required=True)
    ingest_results_dir_parser.add_argument("--db", default="data/harness.db")

    fetch_horses_parser = subparsers.add_parser("fetch-horses", help="Fetch horse pages referenced by a meeting HTML file")
    fetch_horses_parser.add_argument("--meeting-html", required=True)
    fetch_horses_parser.add_argument("--out", default="data/raw/horses")
    fetch_horses_parser.add_argument("--race-number", type=int)
    fetch_horses_parser.add_argument("--horse-library")
    fetch_horses_parser.add_argument("--db", default="data/harness.db")
    fetch_horses_parser.add_argument("--force-refresh", action="store_true")

    ingest_horse_parser = subparsers.add_parser("ingest-horse", help="Parse and store one horse HTML file")
    ingest_horse_parser.add_argument("--html", required=True)
    ingest_horse_parser.add_argument("--db", default="data/harness.db")

    ingest_horses_parser = subparsers.add_parser("ingest-horses", help="Parse and store all horse HTML files in a folder")
    ingest_horses_parser.add_argument("--horse-dir", required=True)
    ingest_horses_parser.add_argument("--db", default="data/harness.db")

    track_pars_parser = subparsers.add_parser("build-track-pars", help="Generate track par database from runner_recent_lines in SQLite")
    track_pars_parser.add_argument("--db", default="data/harness.db")
    track_pars_parser.add_argument("--output", default="data/track_pars.json")

    features_parser = subparsers.add_parser("build-features", help="Build runner-level feature CSV from SQLite")
    features_parser.add_argument("--db", default="data/harness.db")
    features_parser.add_argument("--csv", default="data/features/runner_features.csv")
    features_parser.add_argument("--track-pars", default="data/track_pars.json", help="Path to track_pars.json (default: data/track_pars.json)")

    score_parser = subparsers.add_parser("score-race", help="Score one race and print fair odds")
    score_parser.add_argument("--csv", default="data/features/runner_features.csv")
    score_parser.add_argument("--meeting-code", required=True)
    score_parser.add_argument("--race-number", type=int, required=True)
    score_parser.add_argument("--min-prob", type=float, default=0.0)
    score_parser.add_argument("--max-prob", type=float, default=1.0)
    score_parser.add_argument("--market-csv")
    score_parser.add_argument("--model-weight", type=float, default=None)
    score_parser.add_argument("--market-weight", type=float, default=None)
    score_parser.add_argument("--temperature", type=float, default=None, help="Softmax temperature (overrides weights file)")
    score_parser.add_argument("--weights", default=None, help="Path to weights JSON file (default: data/weights.json if it exists)")
    score_parser.add_argument("--out-csv")

    score_meeting_parser = subparsers.add_parser("score-meeting", help="Score all races in a meeting and print fair odds tables")
    score_meeting_parser.add_argument("--csv", default="data/features/runner_features.csv")
    score_meeting_parser.add_argument("--meeting-code", required=True)
    score_meeting_parser.add_argument("--min-prob", type=float, default=0.0)
    score_meeting_parser.add_argument("--max-prob", type=float, default=1.0)
    score_meeting_parser.add_argument("--market-csv")
    score_meeting_parser.add_argument("--model-weight", type=float, default=None)
    score_meeting_parser.add_argument("--market-weight", type=float, default=None)
    score_meeting_parser.add_argument("--temperature", type=float, default=None, help="Softmax temperature (overrides weights file)")
    score_meeting_parser.add_argument("--weights", default=None, help="Path to weights JSON file (default: data/weights.json if it exists)")
    score_meeting_parser.add_argument("--out-csv")
    score_meeting_parser.add_argument("--publish", action="store_true", help="Write HTML to docs/ and push to GitHub Pages")
    score_meeting_parser.add_argument("--db", default="data/harness.db", help="Database path (used with --publish for meeting metadata)")

    calibrate_nr_parser = subparsers.add_parser("calibrate-nr-factor", help="Estimate the margin-per-NR-point factor from within-horse grade comparisons")
    calibrate_nr_parser.add_argument("--db", default="data/harness.db")
    calibrate_nr_parser.add_argument("--min-grade-spread", type=int, default=5)

    calibrate_parser = subparsers.add_parser("calibrate-temperature", help="Sweep softmax temperatures and report log loss against stored results")
    calibrate_parser.add_argument("--csv", default="data/features/runner_features.csv")
    calibrate_parser.add_argument("--db", default="data/harness.db")
    calibrate_parser.add_argument("--meetings", help="Comma-separated meeting codes to include (default: all)")
    calibrate_parser.add_argument("--temperatures", help="Comma-separated temperatures to test (default: 0.5–6.0 step 0.25)")

    scratch_parser = subparsers.add_parser("scratch-horse", help="Mark a horse as scratched in the DB (for late scratchings not yet on the form page)")
    scratch_parser.add_argument("--meeting-code", required=True)
    scratch_parser.add_argument("--horse-name", required=True, help="Horse name (case-insensitive partial match)")
    scratch_parser.add_argument("--race-number", type=int, help="Limit to a specific race number (optional)")
    scratch_parser.add_argument("--db", default="data/harness.db")

    set_trainer_form_parser = subparsers.add_parser("set-trainer-form", help="Manually set trainer form for a horse (+1 good / 0 neutral / -1 poor)")
    set_trainer_form_parser.add_argument("--meeting-code", required=True)
    set_trainer_form_parser.add_argument("--horse-name", required=True, help="Horse name (case-insensitive partial match)")
    set_trainer_form_parser.add_argument("--value", type=int, choices=[-1, 0, 1], default=1, help="+1 good form, 0 neutral/clear, -1 poor form")
    set_trainer_form_parser.add_argument("--race-number", type=int, help="Limit to a specific race number (optional)")
    set_trainer_form_parser.add_argument("--db", default="data/harness.db")

    flag_trainer_parser = subparsers.add_parser("flag-trainer-change", help="Manually flag a trainer change for a horse before scoring")
    flag_trainer_parser.add_argument("--meeting-code", required=True)
    flag_trainer_parser.add_argument("--horse-name", required=True, help="Horse name (case-insensitive partial match)")
    flag_trainer_parser.add_argument("--race-number", type=int, help="Limit to a specific race number (optional)")
    flag_trainer_parser.add_argument("--clear", action="store_true", help="Clear the flag instead of setting it")
    flag_trainer_parser.add_argument("--db", default="data/harness.db")

    fetch_driver_stats_parser = subparsers.add_parser("fetch-driver-stats", help="Fetch driver profile pages and store season win rates")
    fetch_driver_stats_parser.add_argument("--meeting-code", required=True)
    fetch_driver_stats_parser.add_argument("--db", default="data/harness.db")
    fetch_driver_stats_parser.add_argument("--force-refresh", action="store_true")
    fetch_driver_stats_parser.add_argument("--max-age-days", type=int, default=7)
    fetch_driver_stats_parser.add_argument("--driver-library", default="data/driver_library")

    fetch_trainer_stats_parser = subparsers.add_parser("fetch-trainer-stats", help="Fetch trainer profile pages and store season win rates")
    fetch_trainer_stats_parser.add_argument("--meeting-code", required=True)
    fetch_trainer_stats_parser.add_argument("--db", default="data/harness.db")
    fetch_trainer_stats_parser.add_argument("--force-refresh", action="store_true")
    fetch_trainer_stats_parser.add_argument("--max-age-days", type=int, default=7)
    fetch_trainer_stats_parser.add_argument("--trainer-library", default="data/trainer_library")

    snapshot_parser = subparsers.add_parser("snapshot-meeting", help="Archive a pre-race meeting or one race into a timestamped snapshot folder")
    snapshot_parser.add_argument("--meeting-code", required=True)
    snapshot_parser.add_argument("--snapshots-root", default="data/snapshots")
    snapshot_parser.add_argument("--race-number", type=int)
    snapshot_parser.add_argument("--horse-library", default="data/horse_library/nsw")

    sync_upcoming_parser = subparsers.add_parser("sync-upcoming", help="Fetch and ingest all upcoming NSW meetings not already in the DB")
    sync_upcoming_parser.add_argument("--db", default="data/harness.db")
    sync_upcoming_parser.add_argument("--out", default="data/raw")
    sync_upcoming_parser.add_argument("--delay", type=float, default=2.0, help="Seconds between fetches (default 2)")

    sync_results_parser = subparsers.add_parser("sync-results", help="Fetch and ingest results for recently run NSW meetings with no stored results")
    sync_results_parser.add_argument("--db", default="data/harness.db")
    sync_results_parser.add_argument("--out", default="data/raw")
    sync_results_parser.add_argument("--delay", type=float, default=2.0, help="Seconds between fetches (default 2)")

    build_site_parser = subparsers.add_parser("build-meeting-site", help="Build a light static website page for one scored meeting")
    build_site_parser.add_argument("--meeting-code", required=True)
    build_site_parser.add_argument("--csv", default="data/features/runner_features.csv")
    build_site_parser.add_argument("--db", default="data/harness.db")
    build_site_parser.add_argument("--out", default="data/site")
    build_site_parser.add_argument("--market-csv")
    build_site_parser.add_argument("--min-prob", type=float, default=0.0)
    build_site_parser.add_argument("--max-prob", type=float, default=1.0)
    build_site_parser.add_argument("--model-weight", type=float, default=None)
    build_site_parser.add_argument("--market-weight", type=float, default=None)
    build_site_parser.add_argument("--temperature", type=float, default=None, help="Softmax temperature (overrides weights file)")
    build_site_parser.add_argument("--weights", default=None, help="Path to weights JSON file (default: data/weights.json if it exists)")

    serve_site_parser = subparsers.add_parser("serve-site", help="Serve the generated site folder locally")
    serve_site_parser.add_argument("--site-dir", default="data/site")
    serve_site_parser.add_argument("--host", default="127.0.0.1")
    serve_site_parser.add_argument("--port", type=int, default=8000)

    build_stats_parser = subparsers.add_parser("build-stats-site", help="Build driver and trainer stats page from ingested results")
    build_stats_parser.add_argument("--db", default="data/harness.db")
    build_stats_parser.add_argument("--out", default="docs", help="Output directory for stats.html (default: docs)")

    republish_all_parser = subparsers.add_parser("republish-all", help="Re-score and re-publish every meeting in docs/meetings.json in one batch")
    republish_all_parser.add_argument("--csv", default="data/features/runner_features.csv")
    republish_all_parser.add_argument("--db", default="data/harness.db")
    republish_all_parser.add_argument("--out", default="docs", help="docs/ output directory (default: docs)")
    republish_all_parser.add_argument("--weights", default=None, help="Path to weights JSON file (default: data/weights.json if it exists)")

    backtest_parser = subparsers.add_parser("backtest", help="Compare model fair odds vs SP across all meetings with stored results")
    backtest_parser.add_argument("--db", default="data/harness.db")
    backtest_parser.add_argument("--csv", default="data/features/runner_features.csv")
    backtest_parser.add_argument("--min-edge", type=float, default=0.0, help="Minimum edge threshold for value bets (default: 0.0 = all)")
    backtest_parser.add_argument("--model-only", action="store_true", help="Use model-only fair odds instead of blended")
    backtest_parser.add_argument("--weights", default=None, help="Path to weights JSON file (default: weights.json if it exists)")

    diagnose_parser = subparsers.add_parser("diagnose", help="Per-race truth set: hit rates, failure quadrants, S1/S2 analysis, map overrides, biggest misses")
    diagnose_parser.add_argument("--db", default="data/harness.db")
    diagnose_parser.add_argument("--csv", default="data/features/runner_features.csv")
    diagnose_parser.add_argument("--meeting-code", default=None, help="Limit to a single meeting (default: all meetings with results)")
    diagnose_parser.add_argument("--out", default=None, help="Write per-horse diagnostic CSV to this path")
    diagnose_parser.add_argument("--weights", default=None, help="Path to weights JSON file (default: weights.json if it exists)")

    build_diagnose_parser = subparsers.add_parser("build-diagnose-site", help="Build the per-race diagnostic truth-set page (diagnose.html)")
    build_diagnose_parser.add_argument("--db", default="data/harness.db")
    build_diagnose_parser.add_argument("--csv", default="data/features/runner_features.csv")
    build_diagnose_parser.add_argument("--out", default="docs", help="Output directory (default: docs)")
    build_diagnose_parser.add_argument("--weights", default=None, help="Path to weights JSON file")

    build_betting_parser = subparsers.add_parser("build-betting-site", help="Build fractional-Kelly bankroll backtest page from scored meetings vs stored results")
    build_betting_parser.add_argument("--db", default="data/harness.db")
    build_betting_parser.add_argument("--csv", default="data/features/runner_features.csv")
    build_betting_parser.add_argument("--out", default="docs", help="Output directory for betting.html (default: docs)")
    build_betting_parser.add_argument("--starting-bankroll", type=float, default=1000.0)

    args = parser.parse_args()

    if args.command == "fetch-meeting":
        print(f"Saved meeting HTML to {fetch_meeting(args.meeting_code, args.out)}")
    elif args.command == "ingest-meeting":
        meetings, runners = ingest_meeting_html(args.db, args.html)
        print(f"Stored {meetings} meeting and {runners} runners in {Path(args.db)}")
    elif args.command == "fetch-results":
        print(f"Saved results HTML to {fetch_results(args.meeting_code, args.out)}")
    elif args.command == "fetch-results-history":
        tracks = [part.strip() for part in (args.tracks or "").split(",") if part.strip()]
        paths = fetch_results_history(
            args.out,
            tracks=tracks or None,
            limit=args.limit,
            force_refresh=args.force_refresh,
        )
        print(f"Prepared {len(paths)} results history files in {Path(args.out)}")
    elif args.command == "ingest-results":
        count = ingest_results_html(args.db, args.html)
        print(f"Stored {count} result rows in {Path(args.db)}")
    elif args.command == "ingest-results-dir":
        count = ingest_results_dir(args.db, args.results_dir)
        print(f"Stored {count} result rows from {Path(args.results_dir)} in {Path(args.db)}")
    elif args.command == "fetch-horses":
        paths = fetch_horse_pages_from_meeting_html(
            args.meeting_html,
            args.out,
            race_number=args.race_number,
            horse_library_dir=args.horse_library,
            db_path=args.db,
            force_refresh=args.force_refresh,
        )
        print(f"Fetched {len(paths)} horse pages into {Path(args.out)}")
    elif args.command == "ingest-horse":
        horse_id = ingest_horse_html(args.db, args.html)
        print(f"Stored horse profile {horse_id} in {Path(args.db)}")
    elif args.command == "ingest-horses":
        count = ingest_horse_dir(args.db, args.horse_dir)
        print(f"Stored {count} horse profiles in {Path(args.db)}")
    elif args.command == "build-track-pars":
        build_track_par_database(args.db, args.output)
    elif args.command == "build-features":
        track_pars_path = args.track_pars if Path(args.track_pars).exists() else None
        if args.track_pars and not track_pars_path:
            print(f"  Warning: --track-pars file not found ({args.track_pars}) — sectional deltas will be empty. Run build-track-pars first.")
        print(f"Wrote feature dataset to {build_feature_dataset(args.db, args.csv, track_pars_path=track_pars_path)}")
    elif args.command == "score-race":
        rows = load_feature_rows(args.csv)
        market_rows = load_market_rows(args.market_csv) if args.market_csv else None
        weights = _resolve_weights(getattr(args, "weights", None))
        scored = score_race_rows(
            rows,
            args.meeting_code,
            args.race_number,
            min_probability=args.min_prob,
            max_probability=args.max_prob,
            market_rows=market_rows,
            model_weight=args.model_weight,
            market_weight=args.market_weight,
            temperature=args.temperature,
            weights=weights,
        )
        if args.out_csv:
            out_path = write_scored_rows_csv(
                [
                    {"meeting_code": args.meeting_code, "race_number": args.race_number, **row}
                    for row in scored
                ],
                args.out_csv,
            )
            print(f"Saved race odds CSV to {out_path}")
        print(render_race_odds_table(scored))
    elif args.command == "score-meeting":
        rows = load_feature_rows(args.csv)
        market_rows = load_market_rows(args.market_csv) if args.market_csv else None
        weights = _resolve_weights(getattr(args, "weights", None))
        scored = score_meeting_rows(
            rows,
            args.meeting_code,
            min_probability=args.min_prob,
            max_probability=args.max_prob,
            market_rows=market_rows,
            model_weight=args.model_weight,
            market_weight=args.market_weight,
            temperature=args.temperature,
            weights=weights,
        )
        if args.out_csv:
            out_path = write_scored_rows_csv(
                flatten_meeting_scores(args.meeting_code, scored),
                args.out_csv,
            )
            print(f"Saved meeting odds CSV to {out_path}")
        print(render_meeting_odds(scored, args.meeting_code))
        if getattr(args, "publish", False):
            publish_scored_meeting(
                args.meeting_code,
                scored,
                db_path=args.db,
            )
    elif args.command == "scratch-horse":
        conn = connect(args.db)
        scratched = db_scratch_horse(conn, args.meeting_code, args.horse_name, race_number=args.race_number)
        conn.close()
        if scratched:
            for name, race in scratched:
                print(f"Scratched: {name}  (Race {race})")
        else:
            print(f"No matching horse found for '{args.horse_name}' in meeting {args.meeting_code}")
    elif args.command == "set-trainer-form":
        conn = connect(args.db)
        updated = db_set_trainer_form(conn, args.meeting_code, args.horse_name, value=args.value, race_number=args.race_number)
        conn.close()
        label = {1: "Good form (+1)", 0: "Neutral (0)", -1: "Poor form (-1)"}.get(args.value, str(args.value))
        if updated:
            for name, race in updated:
                print(f"Set trainer form [{label}]: {name}  (Race {race})")
        else:
            print(f"No matching horse found for '{args.horse_name}' in meeting {args.meeting_code}")
    elif args.command == "flag-trainer-change":
        conn = connect(args.db)
        value = 0 if args.clear else 1
        updated = db_set_trainer_change(conn, args.meeting_code, args.horse_name, value=value, race_number=args.race_number)
        conn.close()
        action = "Cleared" if args.clear else "Flagged"
        if updated:
            for name, race in updated:
                print(f"{action} trainer change: {name}  (Race {race})")
        else:
            print(f"No matching horse found for '{args.horse_name}' in meeting {args.meeting_code}")
    elif args.command == "fetch-driver-stats":
        count = fetch_driver_stats_for_meeting(
            args.db,
            args.meeting_code,
            force_refresh=args.force_refresh,
            max_age_days=args.max_age_days,
            driver_library_dir=args.driver_library,
        )
        print(f"Stored driver stats for {count} drivers in {Path(args.db)}")
    elif args.command == "fetch-trainer-stats":
        count = fetch_trainer_stats_for_meeting(
            args.db,
            args.meeting_code,
            force_refresh=args.force_refresh,
            max_age_days=args.max_age_days,
            trainer_library_dir=args.trainer_library,
        )
        print(f"Stored trainer stats for {count} trainers in {Path(args.db)}")
    elif args.command == "snapshot-meeting":
        result = snapshot_meeting(
            args.meeting_code,
            args.snapshots_root,
            race_number=args.race_number,
            horse_library_dir=args.horse_library,
        )
        print(f"Snapshot saved to {result['snapshot_dir']}")
        print(f"Meeting HTML: {result['meeting_path']}")
        print(f"Horse pages saved: {result['horse_count']}")
    elif args.command == "sync-upcoming":
        fetched, skipped = sync_upcoming_meetings(args.db, args.out, delay_s=args.delay)
        print(f"Done: {fetched} fetched, {skipped} skipped")
    elif args.command == "sync-results":
        fetched, skipped = sync_recent_results(args.db, args.out, delay_s=args.delay)
        print(f"Done: {fetched} fetched, {skipped} skipped")
    elif args.command == "build-meeting-site":
        weights = _resolve_weights(getattr(args, "weights", None))
        page_path = build_meeting_site(
            args.meeting_code,
            csv_path=args.csv,
            db_path=args.db,
            out_dir=args.out,
            market_csv=args.market_csv,
            min_probability=args.min_prob,
            max_probability=args.max_prob,
            model_weight=args.model_weight,
            market_weight=args.market_weight,
            temperature=args.temperature,
            weights=weights,
        )
        print(f"Built meeting site page at {page_path}")
    elif args.command == "serve-site":
        serve_site(args.site_dir, host=args.host, port=args.port)
    elif args.command == "republish-all":
        weights = _resolve_weights(getattr(args, "weights", None))
        republish_all_meetings(args.csv, args.db, args.out, weights=weights)
    elif args.command == "build-stats-site":
        build_stats_site(args.db, args.out)
    elif args.command == "backtest":
        import sqlite3 as _sqlite3
        rows = load_feature_rows(args.csv)
        weights = _resolve_weights(getattr(args, "weights", None))
        conn = _sqlite3.connect(args.db)
        conn.row_factory = _sqlite3.Row
        meetings = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT meeting_code FROM race_results ORDER BY meeting_code"
            ).fetchall()
        ]
        results_lookup: dict[tuple, dict] = {}
        for r in conn.execute(
            "SELECT meeting_code, race_number, horse_name, finish_position, starting_price "
            "FROM race_results WHERE starting_price IS NOT NULL"
        ).fetchall():
            key = (r["meeting_code"], r["race_number"], r["horse_name"].upper())
            results_lookup[key] = {"pos": r["finish_position"], "sp": r["starting_price"]}
        conn.close()
        records: list[dict] = []
        for meeting_code in meetings:
            try:
                scored = score_meeting_rows(rows, meeting_code, weights=weights)
            except Exception:
                continue
            for race_number, horse_rows in scored.items():
                for h in horse_rows:
                    key = (meeting_code, race_number, h["horse_name"].upper())
                    if key not in results_lookup:
                        continue
                    res = results_lookup[key]
                    records.append({
                        "meeting": meeting_code,
                        "race": race_number,
                        "horse": h["horse_name"],
                        "model_odds": h.get("fair_odds"),
                        "blended_odds": h.get("adjusted_fair_odds"),
                        "model_prob": h.get("win_probability"),
                        "blended_prob": h.get("adjusted_probability"),
                        "sp": res["sp"],
                        "won": res["pos"] == 1,
                    })
        _print_backtest_report(records, min_edge=args.min_edge, model_only=args.model_only)
    elif args.command == "diagnose":
        import sqlite3 as _sqlite3
        rows = load_feature_rows(args.csv)
        weights = _resolve_weights(getattr(args, "weights", None))
        conn = _sqlite3.connect(args.db)
        conn.row_factory = _sqlite3.Row
        if args.meeting_code:
            meetings = [args.meeting_code]
        else:
            meetings = [r[0] for r in conn.execute(
                "SELECT DISTINCT meeting_code FROM race_results ORDER BY meeting_code"
            ).fetchall()]
        # Load all finish positions (not just non-null SP — we want full finish order)
        results_lookup: dict[tuple, dict] = {}
        for r in conn.execute(
            "SELECT meeting_code, race_number, horse_name, finish_position, starting_price "
            "FROM race_results"
        ).fetchall():
            key = (r["meeting_code"], r["race_number"], r["horse_name"].upper())
            results_lookup[key] = {"pos": r["finish_position"], "sp": r["starting_price"]}
        conn.close()
        def _fv(row: dict, key: str) -> float | None:
            """Read a CSV passthrough field as float; return None if absent or non-numeric."""
            val = row.get(key)
            if val is None:
                return None
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        race_groups: dict[tuple, list[dict]] = {}
        for meeting_code in meetings:
            try:
                scored = score_meeting_rows(rows, meeting_code, weights=weights)
            except Exception:
                continue
            for race_number, horse_rows in scored.items():
                group = []
                for h in horse_rows:
                    key = (meeting_code, race_number, h["horse_name"].upper())
                    if key not in results_lookup:
                        continue
                    res = results_lookup[key]
                    group.append({
                        "meeting": meeting_code,
                        "race": race_number,
                        "horse": h["horse_name"],
                        "horse_id": h.get("horse_id"),
                        "runner_number": h.get("runner_number"),
                        "finish_pos": res["pos"],
                        "sp": res["sp"],
                        "stage1": h.get("stage1_score"),
                        "stage2": h.get("stage2_score"),
                        "total_score": h.get("score"),
                        "model_prob": h.get("adjusted_probability"),
                        "model_odds": h.get("adjusted_fair_odds"),
                        # Grade context (CSV passthrough — convert to float)
                        "race_nr_ceiling": _fv(h, "race_nr_ceiling"),
                        "nr_grade_delta": _fv(h, "nr_grade_delta"),
                        # SP subsystem
                        "sp_avg_market": _fv(h, "sp_avg_prob_last3"),
                        "sp_class_ceiling": _fv(h, "sp_best_prob_similar_grade"),
                        "sp_best_prob_at_class": _fv(h, "sp_best_prob_at_class"),
                        # Map
                        "map_lead_score": _fv(h, "map_lead_score"),
                        "map_death_score": _fv(h, "map_death_score"),
                        "map_soft_trip_score": _fv(h, "map_soft_trip_score"),
                    })
                if len(group) < 2:
                    continue
                # Model rank: highest adjusted_probability = rank 1
                group.sort(key=lambda x: -(x["model_prob"] or 0))
                for i, h in enumerate(group):
                    h["model_rank"] = i + 1
                # SP rank: lowest SP = rank 1 (favourite); None SP gets pushed to the back
                group_sp = sorted(group, key=lambda x: x["sp"] if x["sp"] else 9999)
                for i, h in enumerate(group_sp):
                    h["sp_rank"] = i + 1
                # S1 rank: highest stage1_score = rank 1
                group.sort(key=lambda x: -(x["stage1"] or 0))
                for i, h in enumerate(group):
                    h["s1_rank"] = i + 1
                race_groups[(meeting_code, race_number)] = group
        _print_diagnose_report(race_groups, out_path=args.out)
    elif args.command == "build-betting-site":
        build_betting_site(args.db, args.csv, args.out, starting_bankroll=args.starting_bankroll)
    elif args.command == "build-diagnose-site":
        weights = _resolve_weights(getattr(args, "weights", None))
        page_path = build_diagnose_site(args.csv, args.db, args.out, weights=weights)
        print(f"Built diagnostic page at {page_path}")
    elif args.command == "calibrate-nr-factor":
        from .features import _NR_MARGIN_FACTOR
        r = calibrate_nr_factor(args.db)
        print("\nNR margin factor calibration")
        print(f"  Horses contributing (grade spread >={args.min_grade_spread}): {r['horses']}")
        print(f"  Median slope: {r['median']:.2f} m/NR point")
        print(f"  Mean slope:   {r['mean']:.2f} m/NR point")
        print(f"  Current _NR_MARGIN_FACTOR: {r['current']}")
        print(f"  Suggested value: {r['suggested']:.2f} (rounded to nearest 0.25)")
    elif args.command == "calibrate-temperature":
        meeting_codes = [m.strip() for m in (args.meetings or "").split(",") if m.strip()] or None
        temperatures = [float(t.strip()) for t in (args.temperatures or "").split(",") if t.strip()] or None
        results = calibrate_temperature(args.csv, args.db, meeting_codes=meeting_codes, temperatures=temperatures)
        if results:
            print(f"\n{'Temperature':>12}  {'Log Loss':>10}  {'Races':>6}")
            print("-" * 34)
            for r in results:
                marker = " <-- best" if r == results[0] else ""
                print(f"{r['temperature']:>12.2f}  {r['log_loss']:>10.4f}  {r['races_scored']:>6}{marker}")


if __name__ == "__main__":
    main()
