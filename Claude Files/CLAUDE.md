# Harness Racing Form Analysis Tool — Claude Code Handover

## What This Project Is

A Python-based harness racing form analysis and fair odds generation tool.
It fetches race data from harness.org.au, applies margin adjustments based on
stewards comment codes, and generates fair odds using a multi-factor scoring model.

The user is John, based in Australia. He is relatively new to CLI workflows but
comfortable running Python scripts. He is a Ferrari/F1 fan and owns a 2024 BYD Atto 3.

---

## Project Structure (suggested, to be set up by Claude Code)

```
~/harness/
├── CLAUDE.md                  ← this file
├── scripts/
│   ├── fetch_race_v2.py       ← fetches any harness.org.au page via Playwright
│   ├── fetch_horses.py        ← batch fetches horse profile pages for a race
│   ├── fetch_drivers.py       ← batch fetches driver profile pages for a race
│   ├── score_horses.py        ← main scoring engine (all factors)
│   └── build_track_pars.py    ← builds last-half sectional par database
├── data/
│   ├── track_pars.json        ← sectional par database (build with build_track_pars.py)
│   └── track_pars_log.json    ← resumable checkpoint for par builder
└── races/
    └── [meeting_code]/
        ├── race_[mc]_[ts].html
        ├── horses_[mc]_[ts]/
        └── drivers_[mc]_[ts]/
```

---

## Current Scripts — What Each Does

### fetch_race_v2.py
- Prompts for a URL, fetches with Playwright headless browser
- Saves HTML to Desktop with auto-named file
- Handles: race forms, horse profiles, driver profiles
- Timeouts: 45s page load, 6s render wait, extra 12s for horse/driver profile tables
- Filename format: `race_PC210326_20260329.html`, `horse_826028_ts.html`, `driver_seaton-grima_ts.html`

### fetch_horses.py
- Reads a race form HTML, extracts all horse IDs via regex
- Fetches each horse profile page via Playwright
- Saves to Desktop folder: `horses_[MC]_[timestamp]/HORSENAME_ID.html`

### fetch_drivers.py
- Reads a race form HTML, extracts driver names from `/racing/driverlink/` anchors
- Constructs driver URLs: `harness.org.au/racing/drivers/[firstname-lastname]/`
- Saves to Desktop folder: `drivers_[MC]_[timestamp]/driver_[slug].html`
- Only works with full names (not abbreviated like "J Watson") — abbreviated names skipped

### score_horses.py
- Main analysis engine
- Prompts for: horse folder, driver folder (optional), race date
- Outputs ranked form scores to terminal AND saves `form_scores_[ts].txt` to Desktop
- Upload `form_scores_*.txt` here for odds generation widget

### build_track_pars.py
- Fetches historical results pages: `harness.org.au/racing/fields/race-fields/?mc=[CODE]`
- Extracts Q1/Q2/Q3/Q4 sectionals from completed races
- Filters: excludes trotting races, trials ($0 prize money), non-target tracks
- Target tracks: Menangle (PC), Bathurst (BH), Goulburn (LM)
- Saves `track_pars.json` to Desktop
- Has diagnostic mode (option 2) to test a single URL before full run
- Resumable — delete `track_pars_log.json` to start fresh

---

## URL Structures

```
Race form:      https://www.harness.org.au/form.cfm?mc=PC210326
Results/Fields: https://www.harness.org.au/racing/fields/race-fields/?mc=PC210326
Horse profile:  https://www.harness.org.au/racing/horse-search/?horseId=826028
Driver profile: https://www.harness.org.au/racing/drivers/seaton-grima/
Driver redirect:https://www.harness.org.au/racing/driverlink/[HASH]  → redirects to above
```

### Meeting Code Format
`[TRACK_CODE][DD][MM][YY]` e.g. `PC210326` = Menangle 21 Mar 2026

### Track Codes (confirmed)
| Code | Track |
|------|-------|
| PC   | Menangle (Tabcorp Park) |
| BH   | Bathurst |
| LM   | Goulburn |
| LN   | Launceston |

---

## Scoring Model — Current Variables

### 1. Margin-Adjusted Form Score (55% weight)
- Average adjusted margin from last 5 valid non-trial runs
- Lower = better (winner = 0m, beaten by 5m = 5m, etc.)
- Stewards code adjustments applied per `Fixing-the-data-2021.docx` rules
- Key codes:
  - `13` = 1-out-3-back = −10m
  - `14` = 1-out-4-back = −7.5m
  - `10` = 10-back = −7.5m
  - `HI` = hung in = −5m
  - `INC` = inconvenienced = −10m
  - `RWE` = raced wide early = −5m
  - `BSU`, `LW`, `TF` = NULL (run excluded)
- FT (stand start) runs shift driver/trainer field positions by +2

### 2. Race Map / Positional Score (25% weight)
- Barrier draw (front row vs second row)
- Gate speed from stewards codes
- Sprint lane bonus/penalty — key flag: "sprint lane NOT in operation"
- Cover positions favoured when no sprint lane

### 3. Pricing Signal (20% weight)
- Average recent market price (last 4 starts)
- Price trend: shortening vs drifting
- BMR gap flag: career best mile rate vs this season (>4s/mile = stable transfer)
- NR vs price mismatch detection

### 4. Fitness Penalty (multiplicative, applied after composite)
- If last race > 14 days ago: fair odds × 1.25
- Race date entered at runtime for accurate calculation

### 5. Trainer Form (from horse profile data)
- Aggregated across all horse profiles in the field
- **HOT** flag: ≥5 starts last 30 days, ≥20% win rate
- **COLD** flag: ≥5 starts last 30 days, 0% win rate
- Stats: last 30 days W/S and win%, full prep W/S and win%

### 6. Driver Momentum (from driver profile pages)
- Compares last 7 days win% vs prior 7 days win%
- Requires minimum 3 drives in each window (otherwise INSUFFICIENT)
- **HOT**: last7% > prior7% AND last7% ≥ 20% → upscale = 1 + (gap/100), capped 1.35
- **COLD**: last7% = 0% with 4+ drives AND prior7% > last7% → upscale = 0.85
- **NEUTRAL**: no adjustment
- Upscale applied to win probability (shortens or lengthens fair odds)

### Not Yet In Model (planned)
- Last half sectional speed rating vs track par (pending `track_pars.json`)
- Track bias
- Class rise/drop
- Gear changes
- Second-up/third-up patterns

---

## Key Data Structures

### Horse Profile Parse Output (from `parse_horse_profile`)
```python
{
    'name': 'MAGIC JOE',
    'nr': 76,
    'career_bmr': '1:57.5',
    'this_bmr': '1:57.7',
    'bmr_gap': 0.2,           # seconds/mile, >4 = stable transfer flag
    'this_season': (9, 2, 3, 1),  # starts, wins, places, ...
    'runs': [
        {
            'date': '22 Mar 2026',
            'track': 'LNCSTN',
            'pos': 3,
            'margin': 6.0,
            'mile_rate': '1:57.6',
            'driver': 'J C Duggan',
            'trainer': 'A C Duggan',
            'comment_codes': 'RWE RTR 4 INC',
            'adj': -22.5,
            'null_run': False,
            'adj_margin': -16.5,
            'race_type': 'RACE',
            'dist': 2200,
        }
    ],
    'recent_prices': [15.0, 9.0, 3.6, 6.5],
}
```

### Driver Profile Parse Output (from `parse_driver_profile`)
```python
{
    'name': 'Seaton Grima',
    'season_win_pct': 18.0,
    'season_starts': 122,
    'l7_starts': 7, 'l7_wins': 1, 'l7_win_pct': 14.3,
    'p7_starts': 3, 'p7_wins': 0, 'p7_win_pct': 0.0,
    'momentum': 'HOT',
    'upscale': 1.14,
    'drives': [
        {'date': date(2026,3,28), 'track': 'Newcastle', 'pos': 9, 'win': False}
    ]
}
```

### Track Pars JSON (from `build_track_pars.py`)
```json
{
    "generated": "2026-03-30",
    "pars": {
        "Menangle": {
            "1609": {
                "Good": {"par": 55.2, "n": 312, "std": 0.9, "min": 52.1, "max": 58.3},
                "Slow": {"par": 57.1, "n": 41,  "std": 1.1}
            }
        }
    }
}
```

---

## Immediate Next Steps (Priority Order)

### 1. Reorganise project folder
Move all scripts from Desktop into `~/harness/scripts/`. Claude Code can do this.

### 2. Complete build_track_pars.py run
- Delete old `track_pars_log.json` from Desktop first
- Run `build_track_pars.py` option 1
- Upload `track_pars.json` here once done

### 3. Integrate last-half speed rating into score_horses.py
Once `track_pars.json` exists:
- Parse Q3+Q4 from each horse's form runs (already stored in `mile_rate` field but sectionals not yet extracted from horse profile pages)
- Compare each run's last half vs track par
- Add as weighted input alongside adj margin avg

### 4. Validate model on NSW races
- Run full workflow on a Menangle or Goulburn race
- Compare fair odds vs TAB market
- Track results

### 5. Web app (deferred until model validated)
- Small team use
- Core challenge: harness.org.au blocks server requests → Playwright must run locally
- Preferred approach: local scraper feeds data to hosted web app
- Features: paste URL → odds, bet tracking, model accuracy over time

---

## Methodology Reference

### Margin Adjustment Rules (from Fixing-the-data-2021.docx)
```
Comment                         Adjustment
Behind lead at bell             +7.5m  (easier run)
Held up / no clear run          -7.5m
Outside leader / death seat     -10m
3 wide no cover                 -10m
Checked / inconvenienced        -10m
Sulky contact                   -10m
3 wide early or middle          -5m
1 out 4 back or deeper          -7.5m
Locked wheels / Broke           NULL (run excluded)
```

### Results Page Sectionals Format
```
Track Rating:    FAST
First Quarter:   27.5
Second Quarter:  29.1
Third Quarter:   28.2
Fourth Quarter:  27.2
```
Last half = Q3 + Q4 = 28.2 + 27.2 = 55.4s

### Horse Profile Sectionals Format (in-form guide)
```
(28.7, 30.2, 28.7, 28.8) gate speed, led, surrendered lead early
```
Same Q1,Q2,Q3,Q4 tuple format — already parsed in form guide pages.

---

## Important Gotchas

1. **FT runs**: Stand start races insert 2 extra fields in the run block, shifting driver/trainer from fields [6,7] to [8,9]. Fixed in `_parse_run_block` with `ft_offset = 2`.

2. **Trainer name from horse profile**: Field 7+ft_offset. Validated with regex — if it starts with a digit or `$` it's wrong.

3. **harness.org.au blocks servers**: Playwright with a real Chrome user-agent works. Pure urllib/requests gets blocked. All fetching must run on a local machine.

4. **Results page vs form guide**: 
   - `form.cfm?mc=` = form guide (horse past run histories, NO race result sectionals)
   - `racing/fields/race-fields/?mc=` = fields page that becomes results after races run (has Q1-Q4 per race)

5. **Driver URL construction**: 
   - Driverlink hashes (`/racing/driverlink/HASH`) redirect to `/racing/drivers/firstname-lastname/`
   - Can construct URL directly from full name: "Seaton Grima" → "seaton-grima"
   - Abbreviated names (e.g. "J Watson") cannot be reliably converted — these drivers get skipped

6. **build_track_pars.py note**: The script generates meeting codes for every calendar day going back 12 months. Most will 404 — this is expected. Only ~1-2 days per week will be real meetings per track.

---

## Workflow Summary

```
1. fetch_race_v2.py          # save race form HTML
         ↓
2. fetch_horses.py           # batch fetch all horse profiles → horses_MC_TS/ folder
         ↓
3. fetch_drivers.py          # batch fetch all driver profiles → drivers_MC_TS/ folder
         ↓
4. score_horses.py           # score all horses → form_scores_TS.txt
         ↓
5. Upload form_scores_*.txt  # to Claude.ai web for odds widget generation
```

---

## Dependencies

```bash
pip install playwright
python -m playwright install chromium
```

No other external dependencies. All parsing uses Python stdlib only.
Python 3.8+ required (uses walrus operator and f-strings).
