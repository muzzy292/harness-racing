# Harness Racing Form Analysis — CLAUDE.md

## Project Overview

Python package (`src/harness_model`) for harness racing form analysis and fair odds generation.
Fetches race data from harness.org.au, applies margin adjustments from stewards codes,
and generates fair odds using a multi-factor scoring model.

## Rules of Engagement

These rules apply to ALL AI assistants working on this project:

### Code Changes
- **One logical change per commit** with a clear reason in the message
- **Never gut a working file and replace it wholesale** — refactor incrementally
- **Never restructure without testing before and after** — run the pipeline on LM300326 minimum
- **New scoring components must be validated** against a real meeting before committing
- **Weight changes require before/after output comparison** — show the diff in scores
- **Always use functions** — no giant scripts or inline logic blocks
- **Docstrings only where logic isn't self-evident** — don't document `_avg()` or `_clean_spaces()`

### Data Handling
- **Validate required columns at system boundaries** (CSV load, DB read) — not internally
- **Never silently drop rows** — warn on unexpected missing data
- **Expected None values are normal** (e.g. no horse page = no stake data) — don't warn on these
- **No pandas** — use dicts, `csv.DictReader`, `sqlite3.Row` (data volumes are small)

### Architecture
- **Keep scraping separate from modelling** — `parsers.py` vs `odds.py`
- **Keep feature engineering separate from scoring** — `features.py` vs `odds.py`
- **Do not hardcode race-specific assumptions** — pass as parameters
- **Return dicts or JSON** — not custom objects for data exchange

### Stewards Comments
- Stewards comments (`comment_adj`) are **reliable signals** from official race observers
- Weight should remain at 0.5 — do not reduce without explicit user approval

## Package Structure

```
src/harness_model/
  cli.py          — CLI commands (argparse)
  models.py       — Dataclasses (RunnerInfo, HorseRun, HorseProfile, etc.)
  parsers.py      — HTML parsing (form pages, horse pages, results)
  storage.py      — SQLite schema, upsert, migration (_ensure_columns)
  features.py     — Feature engineering (SQL queries, computed columns)
  odds.py         — Scoring model (Stage 1 + Stage 2), softmax, rendering
  pipeline.py     — High-level pipeline orchestration
  track_pars.py   — Track par lookup
  scraper.py      — Playwright-based fetching
```

## Scoring Model (3-Stage Architecture)

### Stage 1: Horse Performance Rating
Historical form — independent of today's race conditions.

| Component | Weight | Source |
|---|---|---|
| consistency | 1.8 | class-adj avg margin (recent lines), fallback: last5_adj or form-sync avg |
| ceiling | 1.2 | class-adj best margin (uncapped, negative = above-grade win), fallback: best_adj |
| late_speed | 1.4 | last_3_avg_sectional_delta vs track par |
| tempo_adj | 0.45 | tempo adjustment average |
| tempo_flags | -0.08 | count of tempo-adjusted runs |
| null_flags | -0.25 | count of null (excluded) runs |
| market | 0.3-0.6 | avg SP (dynamic by career starts) |
| win_rate | 0.7 | last 5 win rate |
| top3_rate | 0.6 | last 5 top-3 finish rate |
| competitive_rate | 0.5 | last 5 runs within 3m of winner |
| career_win_rate | 0.6 | career win rate vs 12% centre |
| nr | 0.25 | NR rating vs centre of 45 |
| class_pos | 0.15 | NR headroom from race ceiling |
| stake_class | 0.2 | avg recent stake (outlier-capped) |
| class_delta | 0.3 | race purse vs avg recent run purse |

### Stage 2: Today's Race Adjustment
Race-day factors — barrier, map, distance suitability, fitness.

| Component | Weight | Source |
|---|---|---|
| barrier | varies | FR/SR position scoring |
| map_lead | 0.7 | lead rate + barrier bonus |
| map_soft | 0.45 | soft trip score |
| map_soft_context | 0.3 | soft trip × pace pressure |
| map_wide | -0.5 | wide risk penalty |
| map_death | -0.35 | death seat penalty |
| pace_backmarker | 0.6 | restrained rate × (pace_pressure − 0.4) |
| fitness | graduated | 15-28d: -0.35, 29-42d: -0.60, 43-84d: -0.85, 85-99d: -1.10, 100-119d: -1.45, 120-149d: -1.70, 150+d: -2.00 |
| dist_strike_rate | 0.9 | win rate at distance vs career rate (confidence-scaled, full weight ≥15 starts) |
| driver_form | 0.6 | season win rate from driver profile page |
| nr_grade_delta | 0.4 | today's NR ceiling vs avg of last 5 runs (negative = dropping in grade) |

### Stage 3: Market Calibration
- Softmax (temperature 2.75) converts scores to probabilities
- Optional market blend: 45% model + 55% market
- Probability guardrails prevent extreme outputs

## CLI Commands

```bash
python -m harness_model.cli fetch-form --url URL --output PATH
python -m harness_model.cli ingest-meeting --html PATH --db PATH
python -m harness_model.cli fetch-driver-stats --meeting-code MC --db PATH [--force-refresh] [--max-age-days N]
python -m harness_model.cli build-features --db PATH --csv PATH --track-pars PATH
python -m harness_model.cli score-race --csv PATH --meeting-code MC --race-number N
python -m harness_model.cli score-meeting --csv PATH --meeting-code MC
python -m harness_model.cli scratch-horse --meeting-code MC --horse-name NAME --db PATH
```

### Web pipeline (meeting code → scored race cards)

```
ingest-meeting → fetch-driver-stats → build-features → score-meeting
```

`fetch-horses` is **excluded from the web pipeline** — too slow for on-demand use.
Driver stats are cached for 7 days; a typical meeting fetches 6–10 driver pages.
`driver_form` component is replaced by a manual +/−/0 override per horse in the web UI.

## Database (SQLite)

Tables: `meetings`, `race_runners`, `runner_recent_lines`, `horse_profiles`, `horse_runs`, `race_results`, `driver_stats`

Auto-migration via `_ensure_columns()` — new columns added non-destructively on connect.

## Model Improvement Backlog

Known gaps and future work. Do not implement without discussing with the user first.

### Race Map (field-awareness)
- **Pace pressure calibration** — `map_soft_context` and `pace_backmarker` are live but unvalidated. Weights (0.3, 0.6) are starting points. Calibrate against results once 20+ races with clear pace scenarios are available.

### Scoring / Weights
- **`comment_adj` — removed, revisit later** — removed after PK040426 R6 deep dive: double-penalised tough-trip horses and rewarded soft-trip horses on top of their already-adjusted margins. Options: (A) keep removed; (B) flip sign — reward tough trips; (C) use `abs(comment_adj)` as volatility penalty. Data in `recent_line_avg_comment_adj` (CSV) and `comment_adjustment` (DB).
- **`_NR_MARGIN_FACTOR` calibration** — class-adjusted margins use 0.5m per NR point as a starting point. Calibrate against grade-drop winners once 20+ examples observed. Constant is at the top of `features.py`.
- **Field strength z-score normalisation** — extend `relative_score` (score − field_mean) to z-score (÷ field_std_dev) for consistent softmax temperature across fields of varying spread. Low priority until temperature is calibrated from results data.
- **Weight optimisation** — weights are currently hand-tuned. Once 30+ meetings of results are stored in `race_results`, fit weights against actual win outcomes (simple logistic regression on scored probabilities vs finish position).
- **`competitive_rate` redundancy** — overlaps heavily with `consistency` (avg adj margin). Consider removing or halving its weight (currently 0.5) after calibration review.
- **`class_pos` (nr_headroom) redundancy** — derived from the same NR value as `nr`. Low marginal value at weight 0.15. Candidate for removal.

### BMR
- **BMR removed from scoring** — `bmr_dist_rge` removed (hardcoded 117.0s centre was track-blind). Feature column `form_bmr_dist_rge_secs` preserved in CSV. Reinstate as a track-par delta once `par_mile_rate` is added per track/distance to `track_pars.json`.

### Track Pars
- **Track condition collapsed — Fast ≠ Good** — `_normalize_track_condition()` in `parsers.py` maps FAST → "Good" before storing in `runner_recent_lines`. In harness racing, Fast (firm/hard) tracks produce faster times than Good. Collapsing them means a horse running 60.1s on a Fast track is compared to the same par as one running 60.1s on a Good track — understating the penalty. Fix: preserve "Fast" as its own condition bucket in the normaliser and build separate pars. Requires n ≥ 10 Fast samples per track/distance cell before a par is usable. Current data is too thin at most country tracks. Revisit once more meetings are ingested.

### Data Quality
- **Trainer rolling stats are thin** — `trainer_last_30_win_rate` and `trainer_last_90_win_rate` are calculated from `horse_runs`, which only covers horses we have profiles for. Until historical results are bulk-ingested these numbers are unreliable. `trainer_form` score should be treated with caution until `fetch-results-history` has been run.
- **`trainer_change_recent_flag` misfires on FORM:xxx runs** — FORM-synced runs have no `trainer_name`, so the streak calculation can't build a reliable history. May produce false positives for horses that haven't had profiles fetched.

## Key Validation Meeting

**LM300326** (Goulburn, 30 March 2026) — use this for pipeline validation.
Race 4 should show 8 runners with BAM BAM BROOK at Fr4 and TONYS DREAM scratched.
