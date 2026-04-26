# Handover — 9 Apr 2026

## Commit this session

**b5f8650** — `Only null No NR sp_trend when drift, not shortening`

- Previous rule: nulled sp_trend whenever `race_nr_ceiling is None AND trend_nrs[0] is None`
- Problem: also suppressed genuine shortening (ENOLA — $1.25 last-start winner in No NR series was invisible)
- Fix: compute sp_trend first, then null only when `sp_trend < 0` in that context
- ENOLA: $70.21 → $7.89 (actual $3.60, fitness penalty -1.65 S2 explains gap — 125 days off)
- ONE MORE REASON drift suppression still works (sp_trend was negative there)

---

## STUDLEIGH MELISE — BH080426 R2 (deep-dive, no code change yet)

**Result:** Won at $3.40. Model: $16.12. TASBMIKI was model fave at $2.28, finished 4th.

**Root cause — SP signals penalising a legitimate grade-drop horse:**

| Component | Value | Problem |
|---|---|---|
| `market` | -0.913 | avg SP $21 — was consistent outsider |
| `sp_trend` | -0.810 | slight drift |
| `sp_class` | -0.600 | outsider in quality-purse races |
| `consistency` | -1.191 | class-adj avg margin 7.94m |
| **Total drag** | **-3.51** | |

**Grade-drop signals (all combined):**
- `nr` (+0.719) + `class_pos` (+0.188) + `nr_grade_delta` (+0.216) = **+1.12**
- Not enough to overcome SP drag

**Key facts:**
- NR68 horse in NR58 capped field (10 NR points above ceiling)
- avg_recent_nr_ceiling = 63.4 — has been racing NR63 company
- 1 Apr 2026 run: 55.1s last half vs NR68 par 57.2s = **delta -2.1** — exceptional sectional
- late_speed = +1.089 — correctly captured, all NSW tracks
- `sp_class` is systematically wrong for grade-drop horses: being a $21 outsider in NR63 races is *expected*, not a signal of poor ability relative to today's NR58 field

**Proposed fix (not yet implemented, needs more examples):**
- Suppress/dampen `sp_class` when `nr_headroom < -5` (horse significantly above field ceiling)
- OR increase `nr_grade_delta` signal strength (currently divisor=-10.0, weight=0.4 = only +0.04 per NR point)
- Add to CLAUDE.md backlog

---

## Two data quality bugs found (no fix yet)

### 1. Duplicate rows in `runner_recent_lines`

STUDLEIGH MELISE (horse_id 818773) has 12 rows but only 7 unique run/date/track/dist combos.
Cause: horse appears in multiple meetings (BH010426 and BH080426), each ingestion writes its form lines.
The `meeting_code/race_number` filter in `build_runner_feature_rows` means only 6 unique rows are used per race (correct), but the raw table is bloated. No scoring impact currently, but worth investigating whether re-ingesting a meeting creates additional duplicates over time.

### 2. Track name mismatch: `'Bathurst'` vs `'Bthurst'` in track_pars

- Meetings table stores `track_name = 'Bathurst'` (full name from HTML title)
- `track_pars.json` key is `'Bthurst'` (abbreviated, from how sectionals were parsed)
- Result: `race_par_last_half` is **None/empty** for all BH (Bathurst) meetings in features CSV
- `late_speed` scoring is **unaffected** — uses per-run pars from `runner_recent_lines.track_name` which is 'Bthurst' (correct)
- `race_par` is only used for display/context in the CSV, not in `_stage1_components`
- Fix: normalise track name in `parsers.py` or add 'Bathurst' alias in `track_pars.json`

---

## Current model state

Scoring pipeline is healthy. Active backlog items (in CLAUDE.md):
1. sp_trend one-hit-wonder dampening (SEASIDE SID)
2. `comment_adj` — removed, options documented
3. STUDLEIGH MELISE — sp_class over-penalising grade-drop horses (new, from this session)
4. Track name normalisation (Bathurst/Bthurst)
5. Duplicate runner_recent_lines rows

## Validation reference

`python -m harness_model.cli score-race --csv data/features/runner_features.csv --meeting-code BH080426 --race-number 2`

Expected: TASBMIKI fave, STUDLEIGH MELISE at ~$16 (known miss, documented above).
