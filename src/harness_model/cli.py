from __future__ import annotations

import argparse
from pathlib import Path

from .storage import connect, scratch_horse as db_scratch_horse
from .pipeline import (
    build_feature_dataset,
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
)
from .odds import (
    flatten_meeting_scores,
    load_feature_rows,
    load_market_rows,
    render_meeting_odds,
    render_race_odds_table,
    score_meeting_rows,
    score_race_rows,
    write_scored_rows_csv,
)


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

    features_parser = subparsers.add_parser("build-features", help="Build runner-level feature CSV from SQLite")
    features_parser.add_argument("--db", default="data/harness.db")
    features_parser.add_argument("--csv", default="data/features/runner_features.csv")
    features_parser.add_argument("--track-pars")

    score_parser = subparsers.add_parser("score-race", help="Score one race and print fair odds")
    score_parser.add_argument("--csv", default="data/features/runner_features.csv")
    score_parser.add_argument("--meeting-code", required=True)
    score_parser.add_argument("--race-number", type=int, required=True)
    score_parser.add_argument("--min-prob", type=float, default=0.0)
    score_parser.add_argument("--max-prob", type=float, default=1.0)
    score_parser.add_argument("--market-csv")
    score_parser.add_argument("--model-weight", type=float, default=0.45)
    score_parser.add_argument("--market-weight", type=float, default=0.55)
    score_parser.add_argument("--out-csv")

    score_meeting_parser = subparsers.add_parser("score-meeting", help="Score all races in a meeting and print fair odds tables")
    score_meeting_parser.add_argument("--csv", default="data/features/runner_features.csv")
    score_meeting_parser.add_argument("--meeting-code", required=True)
    score_meeting_parser.add_argument("--min-prob", type=float, default=0.0)
    score_meeting_parser.add_argument("--max-prob", type=float, default=1.0)
    score_meeting_parser.add_argument("--market-csv")
    score_meeting_parser.add_argument("--model-weight", type=float, default=0.45)
    score_meeting_parser.add_argument("--market-weight", type=float, default=0.55)
    score_meeting_parser.add_argument("--out-csv")

    scratch_parser = subparsers.add_parser("scratch-horse", help="Mark a horse as scratched in the DB (for late scratchings not yet on the form page)")
    scratch_parser.add_argument("--meeting-code", required=True)
    scratch_parser.add_argument("--horse-name", required=True, help="Horse name (case-insensitive partial match)")
    scratch_parser.add_argument("--race-number", type=int, help="Limit to a specific race number (optional)")
    scratch_parser.add_argument("--db", default="data/harness.db")

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
    elif args.command == "build-features":
        print(f"Wrote feature dataset to {build_feature_dataset(args.db, args.csv, track_pars_path=args.track_pars)}")
    elif args.command == "score-race":
        rows = load_feature_rows(args.csv)
        market_rows = load_market_rows(args.market_csv) if args.market_csv else None
        scored = score_race_rows(
            rows,
            args.meeting_code,
            args.race_number,
            min_probability=args.min_prob,
            max_probability=args.max_prob,
            market_rows=market_rows,
            model_weight=args.model_weight,
            market_weight=args.market_weight,
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
        scored = score_meeting_rows(
            rows,
            args.meeting_code,
            min_probability=args.min_prob,
            max_probability=args.max_prob,
            market_rows=market_rows,
            model_weight=args.model_weight,
            market_weight=args.market_weight,
        )
        if args.out_csv:
            out_path = write_scored_rows_csv(
                flatten_meeting_scores(args.meeting_code, scored),
                args.out_csv,
            )
            print(f"Saved meeting odds CSV to {out_path}")
        print(render_meeting_odds(scored, args.meeting_code))
    elif args.command == "scratch-horse":
        conn = connect(args.db)
        scratched = db_scratch_horse(conn, args.meeting_code, args.horse_name, race_number=args.race_number)
        conn.close()
        if scratched:
            for name, race in scratched:
                print(f"Scratched: {name}  (Race {race})")
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


if __name__ == "__main__":
    main()
