# Claude Update 2026-04-04

Workspace: `C:\Users\Paul Mustica\Documents\Harness Racing Scripting`

## Current Repo State

Recent commits:

- `f2512b3` Handle CTS null runs and ignore RR in comment parsing
- `9aef498` Set default temperature to 2.0 and refresh NR020426 data
- `d58e26e` Fix swallowed runners in form parser and resync NR020426
- `623ceab` Add early speed pressure metric and z-score normalisation to backlog
- `a13d2ce` Add relative_score (horse score minus field mean) to race output
- `690ac03` Add softmax temperature parameter and calibrate-temperature command
- `9233027` Replace single best BMR with avg of top 3 at race distance

Current uncommitted changes:

- `.claude/settings.local.json`
- `src/harness_model/models.py`
- `src/harness_model/storage.py`
- `CHATGPT_HANDOFF.md` is still untracked

## User Preferences

- Keep changes incremental
- No pandas
- Do not change `comment_adj = 0.5` unless discussed
- Prompt at meaningful commit points
- Preserve current CLI / Streamlit flows where possible

## Key Changes Since The Older Handoff

### 1. NR020426 parser bug fixed

The original issue was not just scratchings.

- `ARTISTIC SCOTT` was present in the raw meeting HTML and in official results
- but missing from `race_runners`, features, and scoring

Root cause:

- the old `_parse_form_guide_races()` regex in `src/harness_model/parsers.py` was too greedy
- a scratched horse block could swallow the next horse block
- in `NR020426` Race 3, `YANKEE CAPTAIN` consumed `ARTISTIC SCOTT`

Fix:

- parse horses by splitting the race body into per-horse blocks first
- then parse each block independently

Result:

- `ARTISTIC SCOTT` now appears correctly in:
  - `race_runners`
  - `runner_features.csv`
  - Race 3 scoring output

### 2. NR020426 was hard-resynced

Actions taken:

- re-downloaded live fields page for `NR020426`
- re-ingested `data/raw/meeting_NR020426.html`
- re-ingested `data/raw/results_NR020426.html`
- rebuilt `data/features/runner_features.csv`

Race 3 official results now align in DB:

- `ARTISTIC SCOTT` won from `Fr1`
- placings / SPs match the official results page

### 3. Default temperature changed to 2.0

Updated defaults in:

- `src/harness_model/cli.py`
- `src/harness_model/odds.py`

Verified:

- `score-race --help` shows `Softmax temperature (default 2.0)`
- `score-meeting --help` shows `Softmax temperature (default 2.0)`

### 4. CTS / RR rule change added

User requested:

- if `CTS` and margin greater than `20m` => null run
- ignore `RR`
- if `CTS` and margin `<= 20m` => margin adjustment of `7.5`

What was implemented in `src/harness_model/parsers.py`:

- code-string path:
  - `_apply_comment_codes(comment_codes, raw_margin=...)`
  - `CTS`:
    - margin `> 20` => `null_run = True`
    - otherwise => `-7.5` adjustment
  - `RR` ignored
- text-comment path:
  - `"contacted sulky"` now follows same CTS-style logic
  - margin `> 20` => null run
  - otherwise => `-7.5`

Also changed:

- removed `BL` from `NULL_RUN_CODES`
- `BL` was acting like a null-run trigger even though it is a common positional trip note

### 5. SINBINNED ugly run now nulls out

Relevant race:

- `NR020426` Race 4

The ugly run:

- date: `5 Mar 2026`
- stored comment: `contacted sulky, raced roughly, bell lap, 1 out 2 back, last`
- raw margin: `44.6`

After the rule change and re-ingest:

- `null_run = 1`
- `adjusted_margin = None`

So that bad `SINBINNED` run is now discarded.

## Current Analytical Findings

### Meeting comparison for NR020426

Only races `1-6` currently have official results stored for direct comparison.

Across those 6 races:

- top-pick winners: `2/6`
- winner in model top 3: `4/6`

### Race 5

Race 5 was noisy and likely not a good tuning anchor.

- winner: `CAPTAIN GROOVE`
- model rank: `10th`
- looked like a genuine chaos / upset race
- user said there was interference and was happy to move on

### Race 4

Race 4 is a cleaner near-miss and probably a better calibration target.

Before the CTS rule change:

- top pick: `ULTIMATE TROUBLE`
- actual winner: `SINBINNED`
- `SINBINNED` was model rank `2`

After nulling the ugly `SINBINNED` run:

- `SINBINNED` improved
- but `ULTIMATE TROUBLE` still remains top pick

Current Race 4 top section after rebuild:

- `ULTIMATE TROUBLE` `0.2879`
- `BRING YOUR BEST` `0.1588`
- `SINBINNED` `0.1559`

So the CTS rule helped, but it did not fully flip the race.

## Useful Files

- New dated handoff:
  - `CLAUDE_UPDATE_2026-04-04.md`
- Older handoff:
  - `CHATGPT_HANDOFF.md`

Key code files touched recently:

- `src/harness_model/parsers.py`
- `src/harness_model/cli.py`
- `src/harness_model/odds.py`

Relevant raw/data files for NR020426:

- `data/raw/meeting_NR020426.html`
- `data/raw/results_NR020426.html`
- `data/features/runner_features.csv`

## Suggested Next Steps

### 1. Review Race 4 weighting

Race 4 now looks like a legitimate modeling tradeoff rather than bad data.

Worth checking:

- whether front-line draw / map advantage should matter slightly more
- whether some recent-form penalties are still too harsh relative to map and barrier

### 2. Audit CTS-like phrases beyond exact match

Current text rule looks for `contacted sulky`.

Potentially add coverage for variants if seen in raw data, for example:

- `contact with sulky`
- `sulky contacted`
- similar phrase variants

### 3. Revisit remaining uncommitted user changes carefully

Current local modified files not made by this workflow:

- `.claude/settings.local.json`
- `src/harness_model/models.py`
- `src/harness_model/storage.py`

Do not overwrite those without review.

## Good Commit Point

If the user wants to commit the recent parser / temperature / CTS work:

```powershell
git add src/harness_model/parsers.py src/harness_model/cli.py src/harness_model/odds.py
git commit -m "Fix NR020426 parsing, set default temperature, and refine CTS handling"
```
