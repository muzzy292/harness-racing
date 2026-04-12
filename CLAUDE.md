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
| barrier | varies | FR/SR position scoring (style-split Sr at lead_rate ≥ 0.25) |
| barrier_relief | 0.4 | today barrier score vs avg of last 5 starts (field-size adjusted, clip at 0) |
| map_lead | 0.7 | lead rate + barrier bonus |
| map_soft | 0.45 | soft trip score |
| map_soft_context | 0.3 | soft trip × pace pressure |
| map_wide | -0.5 | wide risk penalty |
| map_death | -0.35 | death seat penalty |
| pace_backmarker | 0.6 | restrained rate × (pace_pressure − 0.4) |
| fitness | graduated | 15-28d: -0.35, 29-42d: -0.60, 43-84d: -0.85, 85-99d: -1.10, 100-119d: -1.45, 120-149d: -1.70, 150+d: -2.00 |
| dist_strike_rate | 0.9 | penalty-only: win rate at distance vs career avg — penalises poor distance record, no boost for good (confidence-scaled, full weight ≥15 starts) |
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
fetch-meeting → ingest-meeting → build-features → score-meeting
```

`fetch-horses` and `fetch-driver-stats` are **excluded from the web pipeline**.
`driver_form` is replaced by a manual +/−/0 button per horse in the web UI — applied at score time, no scraping required.

## Database (SQLite)

Tables: `meetings`, `race_runners`, `runner_recent_lines`, `horse_profiles`, `horse_runs`, `race_results`, `driver_stats`

Auto-migration via `_ensure_columns()` — new columns added non-destructively on connect.

## Model Improvement Backlog

Known gaps and future work. Do not implement without discussing with the user first.

### Race Map (field-awareness)
- **Pace pressure calibration** — `map_soft_context` and `pace_backmarker` are live but unvalidated. Weights (0.3, 0.6) are starting points. Calibrate against results once 20+ races with clear pace scenarios are available.
- **Barrier relief signal — implemented** — `barrier_relief_score` in S2 at weight 0.4. Historical barriers sourced from `race_runners` (last 5 prior meetings), field-size-adjusted using `race_field_size` (COUNT per race). Requires ≥3 historical starts; degrades gracefully to None for thin history. `_barrier_score()` also rebalanced (new discrete Fr scale, style-split Sr at lead_rate ≥ 0.25). Note: `runner_recent_lines.mile_rate` stores pace times ("1:57.3"), NOT barrier strings — historical barriers must come from `race_runners`.

### Scoring / Weights
- **`comment_adj` — removed, revisit later** — removed after PK040426 R6 deep dive: double-penalised tough-trip horses and rewarded soft-trip horses on top of their already-adjusted margins. Options: (A) keep removed; (B) flip sign — reward tough trips; (C) use `abs(comment_adj)` as volatility penalty. Data in `recent_line_avg_comment_adj` (CSV) and `comment_adjustment` (DB).
- **`_NR_MARGIN_FACTOR` calibration** — class-adjusted margins use 0.5m per NR point as a starting point. Calibrate against grade-drop winners once 20+ examples observed. Constant is at the top of `features.py`.
- **Field strength z-score normalisation** — extend `relative_score` (score − field_mean) to z-score (÷ field_std_dev) for consistent softmax temperature across fields of varying spread. Low priority until temperature is calibrated from results data.
- **Weight optimisation** — weights are currently hand-tuned. Once 30+ meetings of results are stored in `race_results`, fit weights against actual win outcomes (simple logistic regression on scored probabilities vs finish position).
- **`competitive_rate` redundancy** — overlaps heavily with `consistency` (avg adj margin). Consider removing or halving its weight (currently 0.5) after calibration review.
- **Form trajectory — model has no concept of declining horses** — VAN BASTEN (PC110426 R1): model $9.67 vs SP $81. NR86, career win rate 23.4% (15/64), but 0 wins from 7 starts this season. `career_win_rate +0.858` and `nr +1.28` dominate S1 based on historical quality that no longer reflects current ability. Four options discussed (do not implement without discussion):
  - **(A) Season win rate component** — `season_win_rate = season_wins / season_starts`, require ≥5 starts, centre 12%, weight ~0.4–0.5. Adds a current-season signal alongside `last_5_win_rate`. `season_starts` and `season_wins` already in features CSV.
  - **(B) Winless season penalty** — when `season_starts >= 5 AND season_wins == 0`, apply `-(season_starts - 4) * 0.15` capped at -1.5. Surgical, only fires on this specific pattern.
  - **(C) Reduce `career_win_rate` weight** — drop from 0.6 to 0.2 or 0.0, lean on `last_5_win_rate`. Risk: loses signal for genuinely good horses. `last_5_win_rate` already covers recent form.
  - **(D) Blended career/season rate** — `(career_wins + season_wins × k) / (career_starts + season_starts × k)` at k=3–5. Only modest correction for VAN BASTEN (23.4% → ~17%). Insufficient alone.
  - Preferred: Option A + B combined. Collect more examples before implementing.
- **`class_delta` and `nr_grade_delta` misfire for floor-NR horses in wide-grade races** — OTIS RISING (NA120426 R5): NR 45 entering at the floor of a NR 45–55 race, market $1.95, model $23.09. Two compounding problems: (1) `class_delta = +$7,181` (horse has been running in ~$8k races, today's race is $15.3k) is treated as a class step-up penalty in S1, but the horse is at the *bottom* of the grade — it has a class *advantage* over higher-NR runners in the field. The purse went up because the ceiling is wider, not because the horse is facing stiffer competition. (2) `nr_grade_delta = +5.4` (race ceiling 55 vs avg recent ceiling 49.6) also fires as a penalty, same misfired logic. (3) `recent_line_avg_class_adj_margin: 10.48m` — the NR_MARGIN_FACTOR upward correction inflates margins for a horse that's been running at the bottom of its eligible grade. Pattern: model penalises low-NR horses entering wide-grade races at the floor, market correctly identifies the class edge. Proposed fixes: (A) suppress or dampen `class_delta` penalty when `nr_headroom` is large (horse is well below ceiling — purse increase reflects wider field, not harder opposition); (B) treat `nr_grade_delta` symmetrically with the grade-drop case — positive delta at wide `nr_headroom` should not be penalised. Needs more examples before changing.
- **`class_pos` (nr_headroom) redundancy** — derived from the same NR value as `nr`. Low marginal value at weight 0.15. Candidate for removal.
- **`sp_class` over-penalises grade-drop horses and maiden runners** — Two confirmed patterns:
  - **(1) Grade-drop (STUDLEIGH MELISE — BH080426 R2)**: NR68 horse dropping to NR58 field won at $3.40; model had $16.12. Being a $21 outsider in NR63 races is expected for a horse of that class, not a signal of poor ability relative to today's weaker field. `sp_class` drag (-0.600) combined with `market` (-0.913) and `sp_trend` (-0.810) = -3.51 total, swamping grade-drop signals (`nr` + `class_pos` + `nr_grade_delta` = +1.12). Proposed fix: suppress/dampen `sp_class` when `nr_headroom < -5` OR increase `nr_grade_delta` signal strength (currently divisor=-10.0, weight=0.4 = only +0.04 per NR point).
  - **(2) Maiden races (CAPTAINS DELIGHT — PE090426 R8)**: model $10.02 vs SP $2.15. `sp_class_score = -3.3026` (≈ -1.32 S1 contribution at weight 0.4) from horse being $6.50 in previous maiden starts. NR proxy assigns ~NR48 to the $11,832 maiden purse, but in maiden racing $6.50 is an entirely normal SP — there is no reliable ability benchmark. Additional context: best class-adj run (-7.32m) was the most recent start (ceiling_best_run_index = 0, improving trajectory); model can't see the market reassessment from $6.50 → $2.15 today. Proposed fix: suppress `sp_class` when `race_nr_ceiling` is empty (i.e. maiden/no-NR races) or when horse has fewer than 10 career starts.
  Needs more examples before changing weights.

- **`late_speed` inflated by shared race-level sectionals (COLLECT A DIME — BH080426 R3)** — every horse in a race is stored with the same `last_half`/`first_half` (the race-level time). A horse 32m behind the winner gets the winner's fast time credited to it. COLLECT A DIME (BH080426 R3): model $5.73 vs $41 SP. Sectionals driving `late_speed = +2.45` came from pos 6 (32.5m behind, shared last_half 54.1s → actual ≈ 56.1s) and pos 10 (33.8m behind, shared 55.5s → actual ≈ 57.7s). Both runs were in fast-paced races the horse couldn't stay with. Also affects TASBMIKI 25 Mar run (8.3m behind, shared 55.7 → actual ≈ 56.2). Two fix options: (A) **margin correction** — `corrected_last_half = last_half + margin × (last_half / (distance / 2))` to estimate individual time from shared race time; (B) **discard threshold** — skip runs where `adjusted_margin > N` (e.g. 8m) from sectional calculation. Fix is in `_sectional_deltas_vs_par` in `features.py`.

### BMR
- **BMR removed from scoring** — `bmr_dist_rge` removed (hardcoded 117.0s centre was track-blind). Feature column `form_bmr_dist_rge_secs` preserved in CSV. Reinstate as a track-par delta once `par_mile_rate` is added per track/distance to `track_pars.json`.

### Track Pars
- **Tempo adjustment is a blunt instrument** — `tempo_adjustment = -1.5` fires whenever `abs(first_half - last_half) <= 2.0s` (even-split race = slow tempo). The threshold is binary (no adjustment vs -1.5m), doesn't scale with how slow the tempo actually was, and the 1.5m magnitude is arbitrary. NR45-47 races at Menangle consistently trigger it. Options: (A) make adjustment proportional to the even-split degree — e.g. `-(2.0 - abs(diff)) * scale`; (B) compare first_half to a track/grade par for the first half rather than just checking the split differential. Revisit once first-half par data is available.
- **Track condition collapsed — Fast ≠ Good** — `_normalize_track_condition()` in `parsers.py` maps FAST → "Good" before storing in `runner_recent_lines`. In harness racing, Fast (firm/hard) tracks produce faster times than Good. Collapsing them means a horse running 60.1s on a Fast track is compared to the same par as one running 60.1s on a Good track — understating the penalty. Fix: preserve "Fast" as its own condition bucket in the normaliser and build separate pars. Requires n ≥ 10 Fast samples per track/distance cell before a par is usable. Current data is too thin at most country tracks. Revisit once more meetings are ingested.

### Data Quality
- **Duplicate rows in `runner_recent_lines`** — a horse appearing in multiple meetings (e.g. BH010426 and BH080426) gets its form lines written on each ingestion, bloating the table. The `meeting_code/race_number` filter in `build_runner_feature_rows` means only 6 unique rows are used per race (no scoring impact currently), but re-ingesting a meeting likely creates further duplicates over time. Fix: add a unique constraint or dedup on upsert in `storage.py`.

## Website To-Do

- **Trainer +/0/− button per horse** — mirrors the existing driver form button. Feeds `trainer_form_manual` which is already wired into S1 at weight 0.6 (`trainer_form_manual * w`). The automated rolling stats (`trainer_last_30/90_win_rate`, `trainer_page_season_win_rate`, `_trainer_form_score()`) are computed in features but intentionally unused in scoring — trainer form is manual-only until the button exists. Note: `trainer_change_recent_flag` logic is also unreliable on FORM-synced runs (no trainer name) — if trainer change flagging is added to the UI, source it from the manual `trainer_change_manual` field, not the automated flag.

## Key Validation Meeting

**LM300326** (Goulburn, 30 March 2026) — use this for pipeline validation.
Race 4 should show 8 runners with BAM BAM BROOK at Fr4 and TONYS DREAM scratched.
