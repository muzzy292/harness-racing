# Harness Racing Odds Model

This project builds a structured data pipeline for a harness racing odds model
targeted at NSW pacing races.

Current scope:

- Tracks: Menangle, Penrith, Goulburn, Bathurst
- Race types: pacing races only
- Exclusions: trotters, 2YO races
- Timing: night-before pricing, rerun after scratchings
- Inputs: race fields/results pages and horse profile pages from `harness.org.au`

Track par logic is intentionally excluded from this first build.

## What this project does

1. Fetches rendered meeting and horse pages from `harness.org.au`
2. Parses them into structured race, runner, and horse-history records
3. Stores the data in SQLite
4. Builds runner-level features for later probability modeling

The pipeline is now form-first:

- `form.cfm` is the primary source for runner profiles, recent form, comments, margins, and sectionals
- horse pages are optional enrichment and local-library backfill, not the default requirement for scoring

Horse-page fetching now includes:

- live per-horse progress output
- automatic retry/backoff on rate limiting
- validation that skips saving tiny invalid error pages
- reuse of previously saved horse pages from a local library

## Setup

```bash
pip install playwright
python -m playwright install chromium
pip install -e .
```

## Example usage

```bash
harness-model fetch-meeting --meeting-code PC290326 --out data/raw
harness-model ingest-meeting --html data/raw/meeting_PC290326.html --db data/harness.db
harness-model fetch-horses --meeting-html data/raw/meeting_PC290326.html --out data/raw/horses
harness-model ingest-horses --horse-dir data/raw/horses --db data/harness.db
harness-model build-features --db data/harness.db --csv data/features/runner_features.csv
harness-model score-meeting --csv data/features/runner_features.csv --meeting-code PC290326
harness-model score-meeting --csv data/features/runner_features.csv --meeting-code PC290326 --out-csv data/odds/PC290326_odds.csv
```

To blend model probabilities with fair market probabilities, provide a market CSV with columns:

- `meeting_code`
- `race_number`
- `runner_number`
- `horse_name`
- `market_odds`

Example:

```bash
harness-model score-meeting --csv data/features/runner_features.csv --meeting-code LM300326 --market-csv data/markets/LM300326_market.csv --model-weight 0.45 --market-weight 0.55
```

To fetch one race at a time:

```bash
harness-model fetch-horses --meeting-html data/raw/meeting_LM300326.html --out data/raw/horses --race-number 3
```

Race filtering still excludes trotters and 2YO races automatically.

To create a reusable pre-race snapshot and NSW horse library:

```bash
harness-model snapshot-meeting --meeting-code LM300326 --race-number 3 --snapshots-root data/snapshots --horse-library data/horse_library/nsw
```

This creates a timestamped snapshot folder containing:

- the saved meeting form page
- the horse pages used for that race

and also stores each successfully fetched horse page in a reusable local library by horse ID.

To fetch and ingest post-race results for the meeting:

```bash
harness-model fetch-results --meeting-code LM300326 --out data/raw
harness-model ingest-results --html data/raw/results_LM300326.html --db data/harness.db
```

Results ingestion is a first-pass scaffold and may need one saved real results page
to fine-tune the parser against the exact completed-meeting layout.
