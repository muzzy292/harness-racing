# Harness Racing Odds Model Handoff

## Project goal

Build a harness racing odds model focused on NSW pacing races using `harness.org.au` as the main source data.

Current track focus:

- Goulburn
- Menangle
- Penrith
- Bathurst

Current race exclusions:

- Trotters
- 2YO races

## Current architecture

This project has been refactored into a Python package under:

- `src/harness_model`

Main modules:

- `scraper.py`
- `parsers.py`
- `storage.py`
- `features.py`
- `odds.py`
- `pipeline.py`
- `cli.py`

## Core design decisions

### 1. Form-first model

We decided that the model should rely primarily on the `form.cfm` race page rather than full horse-page scraping.

Reason:

- horse-page scraping is too slow
- horse-page scraping hits rate limits
- `form.cfm` already contains:
  - runners
  - barriers
  - drivers
  - trainers
  - race metadata
  - recent visible form lines
  - recent sectionals
  - recent comments
  - basic visible profile stats like Career / This Season / Last Season

Horse pages are now optional enrichment and local-library backfill, not the default required source.

### 2. Snapshot workflow

The `form.cfm` page disappears after racing, so calibration requires pre-race snapshots.

Workflow:

1. Save pre-race meeting/race snapshot
2. Save horse pages if needed
3. After race, fetch and ingest results
4. Join pre-race features to post-race outcomes

### 3. Local NSW horse library

The project supports a reusable local horse profile library so previously collected horse pages can be reused instead of fetched again.

Library folder example:

- `data/horse_library/nsw`

The pipeline will:

- reuse matching valid horse files by horse ID
- skip already saved valid files
- replace invalid / tiny / rate-limited files

## Implemented features

### Data collection

- fetch meeting page from `form.cfm`
- fetch results page scaffolding
- fetch horse pages with:
  - progress output
  - retry / backoff
  - timeout handling
  - library reuse

### Parsing

From `form.cfm` meeting page:

- meeting metadata
- runners
- race name
- race distance
- race conditions/class text
- barrier
- nominated driver
- nominated trainer
- form-page Career / TS / LS summaries
- recent visible form lines

From recent visible form lines:

- track
- date
- distance
- condition
- last half
- quarter splits
- raw comment text
- raw margin
- adjusted margin
- tempo adjustment
- null-run flag

From horse pages:

- horse profile data
- recent horse-run history
- adjusted horse-run margins

### Storage

SQLite database currently stores:

- meetings
- race_runners
- runner_recent_lines
- horse_profiles
- horse_runs
- race_results

### Feature engineering

Current feature set includes:

- form-page summaries:
  - career starts / wins
  - season starts / wins
- horse-history adjusted margin features
- recent visible-form adjusted margin features
- recent sectional vs par features
- comment/tempo-derived visible-form features
- basic map-style features inferred from comments + barrier
- nominated driver/trainer fields

### Scoring

Current scorer:

- creates race probabilities from heuristic scores
- outputs fair odds as `1 / probability`
- probabilities already normalize to 100% within race

There is also optional support for:

- market blending via:
  - `Adjusted Prob = 0.45 * Model Prob + 0.55 * Fair Market Prob`

But the user currently does **not** expect to have market CSVs available, so normal workflow should assume pure model odds.

## Document logic from Fixing-the-data-2021

Implemented or partly implemented:

- held up / no clear run
- outside leader / death seat
- checked / inconvenienced
- three wide early / middle
- one out four back / deeper
- behind lead at bell
- flat tyre rule scaffold
- broke / checked and broke / locked wheels nulling scaffold
- first-half vs last-half tempo adjustment scaffold

Still incomplete:

- prizemoney adjustment rule
- fully robust flat-tyre handling
- broader raw comment phrase coverage
- stronger use of these adjustments in a calibrated model

## Important modeling direction

The model should rely heavily on:

- cleaned margins
- visible form comments
- sectionals vs par
- map / likely run

It should not rely primarily on:

- raw finishing positions
- raw margins without cleaning
- horse-page scraping for every race

## Commands currently available

### Setup

```bash
pip install playwright
python -m playwright install chromium
pip install -e .
```

### Fetch and ingest meeting

```bash
python -m harness_model.cli fetch-meeting --meeting-code LM300326 --out data/raw
python -m harness_model.cli ingest-meeting --html data/raw/meeting_LM300326.html --db data/harness.db
```

### Fetch horses

One race:

```bash
python -m harness_model.cli fetch-horses --meeting-html data/raw/meeting_LM300326.html --out data/raw/horses --race-number 3 --horse-library data/horse_library/nsw
```

Full meeting:

```bash
python -m harness_model.cli fetch-horses --meeting-html data/raw/meeting_LM300326.html --out data/raw/horses --horse-library data/horse_library/nsw
```

### Snapshot meeting

One race:

```bash
python -m harness_model.cli snapshot-meeting --meeting-code LM300326 --race-number 3 --snapshots-root data/snapshots --horse-library data/horse_library/nsw
```

Full meeting:

```bash
python -m harness_model.cli snapshot-meeting --meeting-code LM300326 --snapshots-root data/snapshots --horse-library data/horse_library/nsw
```

### Ingest horses and build features

```bash
python -m harness_model.cli ingest-horses --horse-dir data/raw/horses --db data/harness.db
python -m harness_model.cli build-features --db data/harness.db --csv data/features/runner_features.csv --track-pars "C:\Users\Paul Mustica\Desktop\track_pars.json"
```

### Score one race

```bash
python -m harness_model.cli score-race --csv data/features/runner_features.csv --meeting-code LM300326 --race-number 3
```

### Score full meeting

```bash
python -m harness_model.cli score-meeting --csv data/features/runner_features.csv --meeting-code LM300326
```

### Export odds to CSV

One race:

```bash
python -m harness_model.cli score-race --csv data/features/runner_features.csv --meeting-code LM300326 --race-number 3 --out-csv data/odds/LM300326_R3_odds.csv
```

Meeting:

```bash
python -m harness_model.cli score-meeting --csv data/features/runner_features.csv --meeting-code LM300326 --out-csv data/odds/LM300326_odds.csv
```

### Results workflow

```bash
python -m harness_model.cli fetch-results --meeting-code LM300326 --out data/raw
python -m harness_model.cli ingest-results --html data/raw/results_LM300326.html --db data/harness.db
```

Note:

- results ingestion is scaffolded
- it likely needs refinement once a saved real post-race results HTML page is available

## Current important caveats

### 1. Results parser still needs a real saved results page

The results ingestion path exists, but the parser has not yet been tuned against a confirmed real completed-meeting results HTML page.

### 2. Map is only first-pass

Map currently comes from:

- barrier
- inferred style from recent visible comments

It is not yet a full map model.

### 3. Some barrier strings are still messy

Examples seen:

- `Fr-`

These likely need cleanup in parsing.

### 4. Horse pages are still supported, but should not be the main dependency

The strategic direction is:

- form page first
- horse pages second

## What should happen next

The highest-value next steps are:

1. Tighten the map layer
2. Tune / validate results ingestion with a real post-race page
3. Build calibration workflow from archived snapshots + results
4. Improve comment parsing coverage further
5. Consider a local button-based interface later, but results ingestion and calibration matter more first

## Key user preferences and intent

- Use `form.cfm` as primary source
- Horse scraping should be minimized
- Model should focus on cleaned margins and race comments
- Sectionals matter, especially last-half context vs pars
- Odds should be produced across the whole meeting
- Market CSV input is optional and currently not expected in day-to-day use
- The user may later want a button-driven interface, but that is not the immediate priority

