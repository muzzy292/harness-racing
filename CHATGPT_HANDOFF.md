# Claude Handoff

Date: 2026-04-06
Workspace: `C:\Users\Paul Mustica\Documents\Harness Racing Scripting`

## Current Goal

Continue improving the scoring model accuracy, particularly for:
- Horses with No NR form (Breeders Challenge, age-restricted series)
- One-hit-wonder overrating (single dominant run inflating sp_trend)
- Bulk results harvesting and trainer aggregate metrics (longer term)

## Important Working Rules

- Keep changes incremental — one logical change per commit
- No pandas
- Do not change `comment_adj = 0.5` unless discussed
- Prompt the user at meaningful commit points
- Prefer preserving current CLI and Streamlit flows

## Recent Commits (this session)

All on `main`, not yet pushed to origin:

- `0226aae` Age-based purse proxy NR for No NR class adjustment
- `b6c3119` Fix fitness window and sp_trend grade contamination
- `fb475bf` Null out checked/inconvenienced runs with margin > 20m
- `02226ac` Weight SP trend priors by grade context (NR-class-adjusted SP trend)

## High-Level Changes Made This Session

### 1. NR-Class-Adjusted SP Trend (`features.py`)

Prior SPs in `sp_trend` are now weighted by grade context:
- `weight = max(0.5, min(2.0, 1.0 + (line_nr_ceiling - race_nr_ceiling) / 20.0))`
- Tougher prior grade → weight > 1.0; easier grade → weight < 1.0
- Only prior average is weighted; most recent SP stays at face value
- When `race_nr_ceiling` is set but all prior SPs are from No NR runs → `sp_trend = None`

### 2. Null Out Checked/Inconvenienced Runs >20m (`parsers.py`)

If a horse is checked or inconvenienced **and** finishes more than 20m back, `null_run = True`. Under 20m still gets the -10m comment adjustment. Mirrors existing "contacted sulky" logic.

Example: DANCING WITH MY EX's 16 Mar run ("inconvenienced after start", 39.6m raw) was dragging S1 to -4.1 → fair odds $18.50. After fix: $7.89 (actual SP $5).

### 3. Fitness Window Uses Physical Run Date (`features.py`)

`_days_since_last_run()` previously skipped `null_run=True` lines when finding the most recent date. A horse that broke or was checked still physically raced — fitness window now uses ALL run dates.

Example: ONE MORE REASON ran 23 Mar 2026 (broke, null_run). Previous calculation: 163 days. Correct: **14 days**.

### 4. SP Trend Excludes No NR Grades When Today Is NR-Graded (`features.py`)

Breeders Challenge / Gold Bracelet / elite restricted series runs have `line_nr_ceiling = None`. When today's race has an NR ceiling, these runs are excluded from sp_trend to avoid false drift signals (e.g. BC heat $1.50 → G1 final $18 reading as catastrophic drift).

### 5. Age-Based Purse Proxy NR for No NR Class Adjustment (4 files)

**New DB column**: `line_race_age TEXT` in `runner_recent_lines`. Extracted from form line HTML via `re.search(r'(\d)yo', line_html)`. Captures "2yo", "3yo", "4yo" from race names.

**New helper** `_no_nr_proxy(purse, age)` in `features.py` returns `(proxy_nr_ceiling, reliability)`:

| Age | Purse | Proxy NR | Reliability |
|-----|-------|----------|-------------|
| 2yo | $100k+ | 78 | 0.55 |
| 2yo | $25k+ | 70 | 0.60 |
| 2yo | $10k+ | 63 | 0.65 |
| 2yo | $5k+ | 54 | 0.70 |
| 2yo | <$5k | 46 | 0.75 |
| 3yo | $50k+ | 80 | 0.65 |
| 3yo | $20k+ | 72 | 0.70 |
| 3yo | $10k+ | 65 | 0.72 |
| 3yo | $5k+ | 55 | 0.75 |
| 3yo | <$5k | 47 | 0.78 |
| 4yo/open | $30k+ | 76 | 0.75 |
| 4yo/open | $10k+ | 66 | 0.78 |
| 4yo/open | $5k+ | 56 | 0.80 |
| 4yo/open | <$5k | 46 | 0.82 |

**Class reference fallback**: When today's race has no NR ceiling (NMT1W, age-restricted No NR), the horse's own NR rating is used as `_class_ref_nr` for the class-adjustment block. This enables grade calibration even for fully juvenile fields.

### 6. Net Effect — ONE MORE REASON (TM060426 R6, winner $1.90)

| Fix applied | Fair odds |
|-------------|-----------|
| Start of session | $16.81 |
| Fitness: physical run date | $5.65 |
| sp_trend: exclude No NR grades | $5.57 |
| Age-based purse proxy NR | **$4.96** |
| Actual SP | $1.90 |

Remaining gap = trial form + massive class drop (G1 $150k → $11k NMT1W) + self-trainer confidence. These are unmodelable without trial scraping.

## Outstanding / Next Steps

### Priority 1 — First thing tomorrow

```powershell
git push
```

Then re-ingest active meetings to populate the new `line_race_age` column:

```powershell
python -m harness_model.cli ingest-meeting --html data/raw/meeting_NR060426.html --db data/harness.db
python -m harness_model.cli ingest-meeting --html data/raw/meeting_TM060426.html --db data/harness.db
# Repeat for any others in data/raw/ you care about
python -m harness_model.cli build-features --db data/harness.db --csv data/features/runner_features.csv --track-pars data/track_pars.json
```

### Priority 2 — SEASIDE SID one-hit-wonder problem

TM060426 R6: model $2.18 vs $6 SP, finished 7th by 33.9m.

- Horse won last start at $2.10 → huge sp_trend shortening signal
- avg_adj_margin = 8.12m (poor consistency overall)
- `ceiling_support_rate = 0.2` (only 1 of 5 runs within 6m of ceiling)
- Issue: a single dominant win + shortening market can dominate the model score
- Possible fix: dampening sp_trend when shortening is concentrated in just 1 run vs sustained trend across 2–3 starts

### Priority 3 — ONE MORE REASON Oct 11 null_run

The Semi run (11 Oct 2025, $25k, 6th by 24.8m, "sulky contacted") shows `null_run=0` in DB. The raw_comment may have been truncated at ingest time and the "contacted sulky" keyword was cut off. Check:

```sql
SELECT raw_comment FROM runner_recent_lines
WHERE run_date LIKE '%Oct 2025%'
  AND horse_id IN (SELECT horse_id FROM horse_profiles WHERE UPPER(horse_name) LIKE '%ONE MORE%');
```

If truncated, re-ingest the meeting HTML for that meeting (MENANGL 11Oct25 — which meeting code?).

### Priority 4 — Historical results harvest + trainer aggregates

From the previous handoff — still pending:
- Run `fetch-results-history` with VPN if needed
- Build trainer aggregate metrics once enough results are stored
- Target fields: `trainer_market_outperformance_season`, `trainer_win_sr_season`, `trainer_driver_combo_sr`, etc.

## Useful Commands

```powershell
# Rebuild features and score
python -m harness_model.cli build-features --db data/harness.db --csv data/features/runner_features.csv --track-pars data/track_pars.json
python -m harness_model.cli score-meeting --csv data/features/runner_features.csv --meeting-code NR060426

# Ingest a meeting (re-ingest to pick up parser changes)
python -m harness_model.cli ingest-meeting --html data/raw/meeting_NR060426.html --db data/harness.db

# Re-ingest a horse profile (picks up comment rule changes)
python -m harness_model.cli ingest-horse --html data/horse_library/nsw/816265_DANCING_WITH_MY_EX.html --db data/harness.db

# Score a single race for deep-dive
python -m harness_model.cli score-race --csv data/features/runner_features.csv --meeting-code TM060426 --race-number 6
```

## Key Validation Meetings

- **LM300326** — Goulburn 30 Mar 2026. Race 4 should show BAM BAM BROOK at Fr4 with TONYS DREAM scratched.
- **NR060426** — Newcastle 6 Apr 2026. Used extensively this session. DANCING WITH MY EX R4 ($7.89 fair, $5 SP); DOUBLE LINES R5 ($4.35 fair, $26 SP).
- **TM060426** — Temora 6 Apr 2026. ONE MORE REASON R6 ($4.96 fair, $1.90 SP winner); SEASIDE SID R6 ($2.18 fair, $6 SP, 7th).

## Notes

- `data/` is gitignored — fetched HTML and generated CSVs will not stage by default
- `line_race_age` column is new — existing rows have NULL until meetings are re-ingested
- The user explicitly wants markdown handoff files and commit prompts at important checkpoints
