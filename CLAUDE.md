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
| consistency | 1.8 | last5_adj or recent_line_adj |
| ceiling | 1.2 | best_adj or recent_line_best |
| late_speed | 1.4 | last_3_avg_sectional_delta |
| comment_adj | 0.5 | steward comment adjustments |
| tempo_adj | 0.45 | tempo adjustment average |
| tempo_flags | -0.08 | count of tempo-adjusted runs |
| null_flags | -0.25 | count of null (excluded) runs |
| market | 0.3-0.6 | avg SP (dynamic by career starts) |
| win_rate | 0.7 | last 5 win rate |
| top3_rate | 0.6 | last 5 top-3 finish rate |
| competitive_rate | 0.5 | last 5 runs within 3m of winner |
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
| map_wide | -0.5 | wide risk penalty |
| map_death | -0.35 | death seat penalty |
| bmr_dist_rge | 0.6 | BMR at distance (capped ±1.2) |
| fitness | graduated | 15-28d: -0.35, 29-42d: -0.60, 43-84d: -0.85, 85+d: -1.10 |

### Stage 3: Market Calibration
- Softmax (temperature 2.75) converts scores to probabilities
- Optional market blend: 45% model + 55% market
- Probability guardrails prevent extreme outputs

## CLI Commands

```bash
python -m harness_model.cli fetch-form --url URL --output PATH
python -m harness_model.cli fetch-horses --form-html PATH --output-dir PATH
python -m harness_model.cli ingest-meeting --html PATH --db PATH
python -m harness_model.cli build-features --db PATH --csv PATH --track-pars PATH
python -m harness_model.cli score-race --csv PATH --meeting-code MC --race-number N
python -m harness_model.cli score-meeting --csv PATH --meeting-code MC
python -m harness_model.cli scratch-horse --meeting-code MC --horse-name NAME --db PATH
```

## Database (SQLite)

Tables: `meetings`, `race_runners`, `runner_recent_lines`, `horse_profiles`, `horse_runs`, `race_results`

Auto-migration via `_ensure_columns()` — new columns added non-destructively on connect.

## Model Improvement Backlog

Known gaps and future work. Do not implement without discussing with the user first.

### Race Map (field-awareness)
- **`map_soft` pace context** — sitting behind a contested leader is better than an uncontested one (contested leader tires). `map_soft_trip_score` should be boosted when the field has multiple speed horses competing for the lead.
- **Pace pressure bonus for backmarkers** — horses with restrained/back style benefit when the field is speed-heavy (fast early, tired late). Detect contested pace (≥2 horses with high lead probability) and apply a small bonus to restrained-style horses.
- **Early speed pressure metric** — `pace_pressure = sum of lead_scores for top 3 lead horses in the field`. High pressure signals a speed duel: penalise all front-runners and bonus backmarkers. Compute `pace_pressure` in `score_race_rows()` alongside `field_lead_probs`, pass into `_stage2_components()`, and adjust `map_lead`/`map_soft` weights accordingly. Requires threshold calibration against results before committing weights.

### Scoring / Weights
- **`comment_adj` — removed, revisit later** — was using `recent_line_avg_comment_adj` as a standalone Stage 1 signal (weight 0.5). Removed in commit after PK040426 R6 deep dive because it double-penalised horses that ran in tough conditions (negative adjustments already improve adjusted_margin via `consistency`; comment_adj then penalised the same horse again). Also rewarded soft-trip horses (positive adj) on top of their already-worsened margins. Three options to consider when revisiting: (A) keep removed — let adjusted margins do the work; (B) flip sign — reward tough-trip horses; (C) use `abs(comment_adj)` as a chaos/volatility penalty regardless of direction. Data preserved in `recent_line_avg_comment_adj` (CSV) and `comment_adjustment` (DB).
- **Field strength z-score normalisation** — `relative_score` (score − field_mean) is already computed and displayed. Extending to z-score (÷ field_std_dev) would make the softmax temperature consistent across fields of varying spread. Low priority until temperature is calibrated from results data.
- **Weight optimisation** — weights are currently hand-tuned. Once 30+ meetings of results are stored in `race_results`, fit weights against actual win outcomes (simple logistic regression on scored probabilities vs finish position).
- **`competitive_rate` redundancy** — overlaps heavily with `consistency` (avg adj margin). Consider removing or halving its weight (currently 0.5) after calibration review.
- **`class_pos` (nr_headroom) redundancy** — derived from the same NR value as `nr`. Low marginal value at weight 0.15. Candidate for removal.
- **Class-blind consistency scores** — `consistency` (avg adjusted margin) doesn't know what grade the runs were in. A horse averaging 18m in NR43 races is penalised identically to one averaging 18m in NR40 races, despite the former being a stronger performance. `nr_grade_delta` partially corrects this at the field level but doesn't recontextualise the margin data itself. Observed in NA050426 R4: SEVEN RIPPIN ACES had 18m avg from NR43 races, dropping to NR40 today — model gave 6.3% ($15.87), actual winner at $5.50. Fix options: (A) weight `consistency` by `nr_grade_delta` — soften the margin penalty when the horse is dropping in class; (B) store the NR ceiling of each historical run (now available via `line_nr_ceiling`) and compute class-adjusted margins directly; (C) increase `nr_grade_delta` weight as a proxy correction. Requires calibration across multiple grade-drop winners before committing weights.

### BMR
- **Track-speed adjustment for BMR** — `bmr_dist_rge_secs` is currently compared to a fixed 117.0s (1:57.0) centre. Different tracks run faster/slower (Menangle fast, country tracks slow). Requires adding a `par_mile_rate` field per track/distance to `track_pars.json`. Once added, express BMR as delta from track par (same approach as sectionals) and remove the hardcoded 117.0 centre from `odds.py`.
- **Tempo adjustment for BMR** — full mile rate is affected by the pace of the first half, which is harder to normalise than last-half sectionals. Consider using pace-adjusted mile rate if tempo data becomes available at the run level.

### Track Pars
- **Track condition collapsed — Fast ≠ Good** — `_normalize_track_condition()` in `parsers.py` maps FAST → "Good" before storing in `runner_recent_lines`. In harness racing, Fast (firm/hard) tracks produce faster times than Good. Collapsing them means a horse running 60.1s on a Fast track is compared to the same par as one running 60.1s on a Good track — understating the penalty. Fix: preserve "Fast" as its own condition bucket in the normaliser and build separate pars. Requires n ≥ 10 Fast samples per track/distance cell before a par is usable. Current data is too thin at most country tracks. Revisit once more meetings are ingested.

### Data Quality
- **Trainer rolling stats are thin** — `trainer_last_30_win_rate` and `trainer_last_90_win_rate` are calculated from `horse_runs`, which only covers horses we have profiles for. Until historical results are bulk-ingested these numbers are unreliable. `trainer_form` score should be treated with caution until `fetch-results-history` has been run.
- **`trainer_change_recent_flag` misfires on FORM:xxx runs** — FORM-synced runs have no `trainer_name`, so the streak calculation can't build a reliable history. May produce false positives for horses that haven't had profiles fetched.

## Key Validation Meeting

**LM300326** (Goulburn, 30 March 2026) — use this for pipeline validation.
Race 4 should show 8 runners with BAM BAM BROOK at Fr4 and TONYS DREAM scratched.
