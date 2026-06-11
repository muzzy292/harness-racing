# Harness Racing Model — Review Brief

Prepared 11 June 2026 for an external model review. This document is self-contained:
it describes the project, its architecture, current configuration, current
performance, known issues, and open questions — without requiring access to the
codebase. Where useful, file paths are given for a reviewer who *does* have repo
access.

## 1. What this is

A Python package (`src/harness_model`) that scrapes Australian harness racing form
data (from harness.org.au), engineers features from horses' historical runs, and
generates "fair odds" (model probabilities) for each runner in a race using a
hand-tuned multi-factor scoring model. Output is rendered as a static website
(GitHub Pages) showing race cards, model vs market odds, a betting backtest, and
diagnostic pages.

The author is not a professional quant — this is a hobby project built up
incrementally over time, with weights hand-tuned based on reviewing individual
race outcomes ("deep dives") rather than formal statistical fitting. **The goal
of this review is to sanity-check the architecture, identify structural risks
(overfitting, leakage, double-counting), and prioritise the improvement backlog
below.**

## 2. Architecture

```
src/harness_model/
  cli.py          — CLI commands (argparse)
  models.py       — Dataclasses (RunnerInfo, HorseRun, HorseProfile, etc.)
  parsers.py      — HTML parsing (form pages, horse pages, results)
  storage.py      — SQLite schema, upsert, migration
  features.py     — Feature engineering (SQL queries, computed columns)
  odds.py         — Scoring model (Stage 1 + Stage 2 + Stage 3), softmax, rendering
  pipeline.py     — High-level pipeline orchestration
  track_pars.py   — Track par lookup (sectional time benchmarks)
  scraper.py      — Playwright-based fetching
  web.py          — Static site generation (race cards, betting backtest, diagnostics)
```

Pipeline: `fetch-meeting → ingest-meeting → build-features → score-meeting → republish-all`

Data store: SQLite (`data/harness.db`). Tables: `meetings`, `race_runners`,
`runner_recent_lines`, `horse_profiles`, `horse_runs`, `race_results`, `driver_stats`.

Feature CSV: `data/features/runner_features.csv` — one row per runner per race,
~100+ computed columns, rebuilt by `build-features` from SQL queries over the DB.

## 3. Scoring model — 3 stages

### Stage 1: Horse Performance Rating (historical form, race-independent)

Each component is computed per-horse, scaled (typically via a centred/clamped
function), then multiplied by a weight and summed.

| Component | Weight (current) | Source |
|---|---|---|
| consistency | 2.5 | class-adjusted avg margin (recent lines) |
| ceiling | 2.5 | class-adjusted best margin (uncapped; negative = above-grade win) |
| late_speed | 0.8 | last-3-run avg sectional delta vs track par |
| tempo_adj | 0.35 | tempo adjustment average |
| tempo_flags | 0.05 | count of tempo-adjusted runs (penalty) |
| null_flags | 0.20 | count of excluded/null runs (penalty) |
| market | 0.0 | avg SP (currently zeroed — `market_min`/`market_max` both 0) |
| win_rate | 2.0 | last-5 win rate vs ~12% baseline |
| career_win_rate | 0.0 | career win rate (zeroed out) |
| top3_rate | 1.0 | last-5 top-3 rate |
| competitive_rate | 0.75 | last-5 runs within 3m of winner |
| nr | 1.5 | NR rating vs centre of 45 |
| class_pos | 0.30 | NR headroom from race ceiling |
| stake_class | 0.0 | avg recent stake (zeroed out) |
| class_delta | 0.1 | race purse vs avg recent run purse |
| sp_class / sp_trend | 0.0 / 0.0 | zeroed |
| sp_market_ceiling | 0.35 | |
| sp_class_ceiling | 0.45 | |
| sp_avg_market | 0.25 | |
| sp_reliability | 0.25 | |
| sp_similar_grade | 0.0 | zeroed |
| form_staleness | 1.0 | penalty for stale/old form |

### Stage 2: Today's Race Adjustment (race-day context)

| Component | Weight (current) | Source |
|---|---|---|
| map_lead | 1.5 | lead rate + barrier bonus |
| map_soft | 0.35 | soft-trip score |
| map_soft_context | 0.25 | soft trip × pace pressure |
| map_wide | 0.25 | wide-running penalty |
| map_death | 0.75 | death-seat penalty |
| pace_backmarker | 0.3 (threshold 0.2) | restrained rate × (pace pressure − threshold) |
| dist_strike_rate | 0.6 | penalty-only: win rate at distance vs career avg |
| nr_grade_delta | 2.5 | today's NR ceiling vs avg of last-5-run ceilings |
| driver_form (manual) | 0.6 | manual +1/0/−1 button in web UI |
| trainer_form_manual | 0.6 | manual +1/0/−1 button in web UI |
| hot_driver / hot_trainer | 0.5 / 0.4 | automated rolling form bonus |
| cold_driver / cold_trainer | 0.5 / 0.4 | automated rolling form penalty |
| second_up | 0.0 | zeroed |
| barrier_relief | 0.0 | zeroed (was 0.4, see backlog) |
| barrier (FR/SR) | varies | barrier position scoring, style-split at lead_rate ≥ 0.25 |

Fitness penalty (graduated by days since last run):
15-28d: -0.35, 29-42d: -0.60, 43-84d: -0.85, 85-99d: -1.10, 100-119d: -1.45,
120-149d: -1.70, 150+d: -2.00

### Stage 3: Market Calibration

- Softmax (temperature **1.6**, recently lowered from 2.75) converts S1+S2 scores
  to win probabilities.
- Optional blend: 45% model / 55% market (used for "fair odds" display, not for
  the backtest below). **Confirmed (June 2026 review):** the Kelly stake uses the
  pure model `win_probability` — by design, since no pre-race market odds are
  available; SP is only used post-race to settle bets.
- Probability guardrails prevent extreme outputs.

Full weight config: `weights.json` (attached / reproduced above).

## 4. Current Results (as of 11 June 2026)

### 4a. Per-race diagnostic (model top-pick vs market top-pick vs actual winner)

Source: `docs/diagnose.html` / `diagnose-nsw.html` / `diagnose-qld.html`,
989 races total with stored results.

| | All (989 races) | NSW (774) | QLD (215) |
|---|---|---|---|
| Model top pick won | 26.2% (259) | 26.9% (208) | 23.7% (51) |
| Market top pick (SP) won | 42.6% (421) | 42.4% (328) | 43.3% (93) |
| Top-3 model picks contained winner | 58.0% (574) | 59.3% (459) | 53.5% (115) |
| Winner was model's 2nd pick | 17.3% (171) | 17.3% (134) | 17.2% (37) |
| Winner was model's 3rd pick | 14.6% (144) | 15.1% (117) | 12.6% (27) |
| Winner ranked 4th+ by model | 42.0% (415) | 40.7% (315) | 46.5% (100) |
| Both model and market correct | 170 | 134 | 36 |
| Model right, market wrong ("model edge") | 89 | 74 | 15 |
| Market right, model wrong ("SP failure") | 251 | 194 | 57 |
| Both wrong | 479 | 372 | 107 |
| Avg winner's model rank | 4.38 | 4.37 | 4.43 |

**Headline gap: the model's top pick wins 26.2% of races vs the market's 42.6%.**
The market is a substantially better single-pick predictor than the model. The
model's edge has to come from finding *value* (probability vs price), not from
out-predicting the market on raw hit rate — but a 16-point gap in top-pick
accuracy is large, and worth the reviewer's attention: is this expected for a
"fair odds vs market" model, or does it suggest the model's probability
distribution is too flat / mis-ranked?

### 4b. Betting backtest (Quarter-Kelly, edge ≥25% with no upper bound, SP ≤ $60,
4% max bet, bank halves if it falls 25%, all bets from 1 May 2026 onwards)

**Correction (June 2026 review):** earlier versions of this document described a
"25%–100% edge window" — the upper bound was never implemented and the
no-upper-bound behaviour is by design. QLD bets are now excluded from the
headline ledger entirely.

Source: `docs/betting.html` / `betting-nsw.html` / `betting-qld.html`.

| | All (1190 bets) | NSW (797 bets) | QLD (393 bets) |
|---|---|---|---|
| Win rate | 11.1% (132) | 12.8% (102) | 7.6% (30) |
| ROI (profit / total staked) | **-10.4%** | **+9.5%** | **-44.8%** |
| Total staked | $9,708.25 | $26,446.79 | $2,235.61 |
| Net profit | -$5.71... | … | -$0.68... |
| Avg edge (model prob vs implied market prob) | 256.6% | 249.0% | 272.1% |
| Bank (from $1,000 start) | $1,005.71 | $3,523.94 | $1,000.68 |

Notes:
- "Avg edge" of ~250-270% is enormous — this likely reflects the model assigning
  probabilities many times higher than the market implies for the bets it takes.
  A reviewer should sanity check whether an edge that large is plausible, or
  whether it signals the softmax/temperature is still producing overconfident
  probabilities for outsiders.
- NSW alone is strongly profitable (+9.5% ROI on $26.4k staked → ~+$2,524
  profit on a $1,000 bank — note the bank figure reflects compounding/staking
  dynamics, not a simple sum).
- QLD is significantly unprofitable (-44.8% ROI) and has a much lower win rate
  (7.6% vs 12.8%). See "QLD calibration" in the backlog — this is believed to be
  a structural data-quality issue (NR ceilings often unparseable in QLD race
  conditions), not just weight miscalibration. A change strong enough to fix QLD
  (`nr_grade_delta` 2.5→0.5) was tested and found to *destroy* NSW performance
  (+24%→+8.8%), suggesting QLD needs separate weights/handling rather than a
  shared-weight fix.
- "Units halved" warning is showing on all three pages — bank fell 25% from peak
  at some point in the backtest window. Worth checking the drawdown shape (the
  underlying SVG sparkline shows a fairly smooth climb for NSW and a rocky climb
  for QLD with an early sharp drawdown around bet #18-50).

## 5. Data quality / pipeline notes

- **No pandas** — everything is dicts / `csv.DictReader` / `sqlite3.Row`. Data
  volumes are small (~1,200 runner-rows per rebuild, 989 races with results).
- **Auto-migration**: `_ensure_columns()` adds new columns non-destructively on
  connect — schema has grown organically over time (~100+ feature columns).
- **Excluded race types**: trotting and 2YO races are excluded from form/results
  scoring entirely (`_EXCLUDED_RACE_RE`). A recent fix backfilled driver/trainer
  results from these excluded races into `horse_runs` so rolling driver/trainer
  stats include all race types (1,197 rows added: 729 TROT + 468 2YO). This had
  negligible direct effect on ROI.
- **Known duplicate-row issue**: `runner_recent_lines` accumulates duplicate rows
  for horses appearing across multiple meetings (no unique constraint on upsert).
  Currently no scoring impact (per-race filter limits to 6 unique rows) but will
  bloat over time.
- **Track condition collapse**: `_normalize_track_condition()` maps "Fast" →
  "Good" before storing — this understates expected times on Fast tracks (Fast is
  faster than Good in harness racing). Not yet fixed; needs ≥10 Fast samples per
  track/distance cell before a separate par is usable.

## 6. Known issues / improvement backlog (not yet implemented)

These are documented in `CLAUDE.md` and represent the author's own running list
of suspected model weaknesses, mostly identified via individual race "deep
dives" where the model badly mispriced a horse. **A reviewer's prioritisation of
this list (which are real structural problems vs noise from small samples) would
be valuable.**

1. **Track-specific map multipliers** — analysis of 83 meetings (2,125+ runners)
   shows the lead/soft/death-seat signals (`map_lead`, `map_soft`, `map_death`)
   vary dramatically by track, and at a few tracks (Albury, Leeton, Redcliffe)
   the signal runs *backwards* (frontrunners underperform). Proposed: a
   `map_multiplier` dict per track in `track_pars.json`, applied as a scalar in
   `_score_stage2()` before weighting. Not implemented.

2. **Pace pressure calibration** — `map_soft_context` (0.25) and
   `pace_backmarker` (0.3) are live but their weights are unvalidated starting
   points.

3. **`comment_adj` (stewards comments) — removed** — was double-penalising
   tough-trip horses (their adjusted margins already account for trouble) while
   rewarding soft-trip horses on top of their already-adjusted margins. Currently
   fully removed from scoring. Per project rules, stewards comments are
   considered a *reliable* signal and any reinstatement needs explicit approval —
   options discussed: keep removed, flip sign (reward tough trips), or use
   `abs(comment_adj)` as a volatility penalty.

4. **`_NR_MARGIN_FACTOR`** (0.5m per NR point, used in class-adjusted margins) —
   a starting-point constant, not calibrated against actual grade-drop winners.

5. **No season-form-trajectory signal** — the model has no concept of a horse in
   *decline*. Concrete example: VAN BASTEN, NR86 with 23.4% career win rate but 0
   wins from 7 starts this season — model priced at $9.67, SP was $81. The
   `career_win_rate` (now weighted 0) and `nr` (1.5) components are based on
   historical quality that no longer reflects current ability. Four options on
   the table (none implemented): (A) season win-rate component, (B) winless
   season penalty, (C) reduce career_win_rate weight (already done — now 0), (D)
   blended career/season rate. Author's preference: A+B combined, pending more
   examples.

6. **`class_delta` / `nr_grade_delta` misfire for floor-NR horses entering
   wide-grade races** — concrete example: OTIS RISING, NR45 in a NR45-55 race
   (bottom of grade), market $1.95, model $23.09. The model penalises the horse
   for a purse increase that reflects a *wider field ceiling*, not *harder
   opposition* — but the horse is actually advantaged at the bottom of a wide
   grade. Needs more examples before fixing.

7. **`class_pos` (nr_headroom) redundancy** — derived from the same NR value as
   `nr` (1.5). Currently weighted 0.30. Candidate for removal/merging.

8. **`sp_class` over-penalises grade-drop horses and maiden runners** — two
   confirmed examples (STUDLEIGH MELISE grade-drop winner at $3.40 priced $16.12
   by model; CAPTAINS DELIGHT maiden-race winner at $2.15 priced $10.02 by
   model). **Resolved:** `sp_class` was zeroed deliberately as the fix for
   these cases (per code comments in `odds.py` it is retained for potential
   reinstatement once more data exists).

9. **`late_speed` inflated by shared race-level sectionals** — every horse in a
   race is stored with the *same* `last_half`/`first_half` time (the race-level
   time, not per-horse), so a horse who finished 30+ metres behind gets credited
   with the winner's fast sectional. Concrete example: COLLECT A DIME, model
   $5.73 vs $41 SP, driven by `late_speed = +2.45` from two runs where the horse
   was 32-34m behind in fast-paced races. **Resolved:** fix (B) is implemented —
   `_sectional_deltas_vs_par` in `features.py` discards sectionals from runs
   with `adjusted_margin > 8.0` (`_SECTIONAL_MARGIN_THRESHOLD`).

10. **BMR removed** — `bmr_dist_rge` was removed (hardcoded 117.0s centre was
    track-blind). Column preserved in CSV but unused. Reinstatement needs
    per-track/distance par data.

11. **Tempo adjustment is binary/blunt** — `tempo_adjustment = -1.5` fires
    whenever `|first_half - last_half| <= 2.0s`, regardless of how slow the race
    actually was. NR45-47 Menangle races consistently trigger it. Proposed:
    proportional adjustment or comparison to a track/grade first-half par.

12. **Field-strength z-score normalisation** — `relative_score` (score − field
    mean) could be extended to z-score (÷ field std-dev) for consistent softmax
    temperature across fields of varying spread. Low priority until temperature
    is calibrated from results.

13. **Weight optimisation** — all weights are hand-tuned via deep-dive review of
    individual races, not fit against outcomes. Author's stated plan: once 30+
    meetings of results are stored, fit weights via logistic regression on scored
    probabilities vs finish position. **989 races / ~108 meetings are now
    available — this threshold may already be reached.** This is probably the
    single highest-leverage structural recommendation a reviewer could weigh in
    on: should hand-tuning continue, or is there now enough data for a proper fit
    (and if so, what guardrails — e.g. regularisation, train/test split by
    meeting date — would prevent overfitting on ~1,000 races / ~100 features)?

14. **`competitive_rate` redundancy** — overlaps with `consistency` (avg
    adjusted margin). Currently weighted 0.75. Candidate for removal/halving.

15. **QLD calibration** — see section 4b above. 44% of QLD races lack a
    parseable NR ceiling (vs 24% NSW) due to QLD's win-restriction-based class
    structure vs NSW's NR-band structure. The `nr` component (weight 1.5) and
    `nr_grade_delta` (weight 2.5) fire on raw NR values without grade context in
    these races, producing extreme overconfidence (one example: a horse scored
    98.1% win probability and finished 2nd). Recommended: defer until 25+ QLD
    meetings available, or implement state-specific weight sets.

## 7. Architectural / process constraints (for context, not necessarily to be
   reviewed, but useful background)

- One logical change per commit, tested on a known validation meeting (LM300326)
  before/after.
- New scoring components must be validated against a real meeting before
  committing; weight changes require before/after score diffs.
- No silent dropping of rows; expected-None values (e.g. no horse page → no
  stake data) are normal and shouldn't warn.
- Scraping (`parsers.py`) is kept separate from modelling (`odds.py`); feature
  engineering (`features.py`) is kept separate from scoring.
- Stewards comments (`comment_adj`) are considered a reliable signal and its
  weight (when reinstated) should not be reduced below 0.5 without explicit
  approval — currently fully removed from scoring (see backlog item 3).

## 8. Specific questions for the reviewer

1. Is a 26.2% top-pick hit rate vs the market's 42.6% a red flag for this kind of
   model, or expected/fine if the model is finding *value* rather than competing
   on raw accuracy?
2. Is an average backtested "edge" of 250-270% plausible, or does it suggest
   probability miscalibration (overconfidence on long-shots)?
3. Given ~1,000 races of results are now available, should the project move from
   hand-tuned weights to a fitted model (logistic regression / similar)? What
   would a sound train/validation split and regularisation approach look like
   given ~100 candidate features and ~1,000 observations?
4. Of the 15 backlog items above, which 2-3 represent the highest-value /
   lowest-risk next changes?
5. The QLD vs NSW performance gap (-44.8% vs +9.5% ROI) — is state-specific
   weighting the right fix, or is there a simpler feature-engineering fix for the
   NR-ceiling parsing gap that would close most of the gap?
6. Any structural overfitting risk from the iterative "find a bad race, add/tune
   a weight to fix it" process used so far (15 backlog items, several already
   "fixed" by zeroing weights to 0)?
