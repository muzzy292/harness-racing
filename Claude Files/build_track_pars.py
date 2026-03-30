"""
Harness Track Par Database Builder
====================================
Fetches historical NSW race form pages, extracts sectional times
(Q3+Q4 = last half), and builds a par database per track/distance/condition.

This par database is used by the scoring engine to compare each horse's
last half time against the par for that track — giving a standardised
speed rating that accounts for track differences.

TARGET TRACKS (NSW):
  Menangle   = PC
  Bathurst   = BH
  Goulburn   = LM

MEETING CODE FORMAT: [TRACK][DD][MM][YY]
  e.g. PC280326  = Menangle 28 Mar 2026
       LM300326  = Goulburn 30 Mar 2026
       PE210326  = Penrith  21 Mar 2026

HOW TO USE:
1. Run this script: python build_track_pars.py
2. It will fetch ~50 meetings per track (about 6 months back)
3. Results saved to track_pars.json on your Desktop
4. Upload track_pars.json here — Claude will integrate it into score_horses.py

FIRST-TIME SETUP:
  pip install playwright
  py -m playwright install chromium   (Windows)
  python3 -m playwright install chromium  (Mac)
"""

import os
import re
import sys
import json
import time
from datetime import date, timedelta
from html.parser import HTMLParser
from collections import defaultdict


# ─── TRACK CONFIG ─────────────────────────────────────────────────────────────

TRACKS = {
    'PC': {'name': 'Menangle', 'state': 'NSW', 'distances': [1609, 2300]},
    'BH': {'name': 'Bathurst', 'state': 'NSW', 'distances': [1730, 2260]},
    'LM': {'name': 'Goulburn', 'state': 'NSW', 'distances': [1710, 2090]},
}

# Target: 50+ meetings per track = ~6 months of history
# NSW harness runs weekly at most tracks so 6 months ~ 26 meetings
# Go back 12 months to get 50+
MONTHS_BACK = 12

# Condition groupings for par calculation
# 'Good' includes Good, Fast
# 'Slow' includes Slow, Heavy, Rain Affected
CONDITION_MAP = {
    'good': 'Good', 'fast': 'Good', 'firm': 'Good',
    'slow': 'Slow', 'heavy': 'Slow', 'rain affected': 'Slow',
    'wet': 'Slow', 'soft': 'Slow',
}


# ─── HTML EXTRACTOR ───────────────────────────────────────────────────────────

class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style', 'nav', 'header', 'footer'):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ('script', 'style', 'nav', 'header', 'footer'):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            s = data.strip()
            if s:
                self.text.append(s)


def extract_text(html):
    p = TextExtractor()
    p.feed(html)
    return '\n'.join(p.text)


# ─── MEETING CODE GENERATOR ───────────────────────────────────────────────────

def generate_meeting_codes(track_code, months_back=12):
    """
    Generate candidate meeting codes for a track going back N months.
    Uses yesterday as the end date — today's meeting won't have results yet.
    Returns list of (date, code) tuples, newest first so we hit recent
    meetings before older ones.
    """
    codes = []
    yesterday = date.today() - timedelta(days=1)
    start = yesterday - timedelta(days=months_back * 30)

    d = start
    while d <= yesterday:
        dd = f"{d.day:02d}"
        mm = f"{d.month:02d}"
        yy = str(d.year)[2:]
        codes.append((d, f"{track_code}{dd}{mm}{yy}"))
        d += timedelta(days=1)

    # Return newest first — more likely to get recent valid meetings quickly
    return list(reversed(codes))


# ─── SECTIONAL PARSER ─────────────────────────────────────────────────────────

# Track name mappings — form guide uses abbreviated venue names
TRACK_NAME_MAP = {
    'MENANGL': 'Menangle', 'MENANGLE': 'Menangle',
    'BATHURS': 'Bathurst', 'BATHURST': 'Bathurst',
    'GOULBUR': 'Goulburn', 'GOULBURN': 'Goulburn',
    'PENRITH': 'Penrith',
    'NEWCAST': 'Newcastle', 'NEWCASTLE': 'Newcastle',
    'CARRICK': 'Carrick',
    'TAMWRTH': 'Tamworth', 'TAMWORTH': 'Tamworth',
    'WAGGA':   'Wagga',
    'DUBBO':   'Dubbo',
    'YOUNG':   'Young',
    'LNCSTN': 'Launceston', 'LAUNCESTON': 'Launceston',
    'HOBART':  'Hobart',
    'BURNIE':  'Burnie',
    'ALBPK':   'Albion Park', 'ALBPARK': 'Albion Park',
    'IPSWICH': 'Ipswich',
    'REDCLIF': 'Redcliffe',
    'BNKSTW':  'Bankstown', 'BNKSTW': 'Bankstown',
    'SCOTSD':  'Scottsdale',
    'MARYBROO': 'Maryborough', 'MARYBRO': 'Maryborough',
    'KILMORE': 'Kilmore',
    'MELTON':  'Melton',
}

# Track name mappings — results page uses full venue names
TRACK_NAME_MAP = {
    'MENANGL': 'Menangle', 'MENANGLE': 'Menangle',
    'NSWHRC AT TABCORP PK MENANGLE': 'Menangle',
    'TABCORP PK MENANGLE': 'Menangle',
    'BATHURS': 'Bathurst', 'BATHURST': 'Bathurst',
    'GOULBUR': 'Goulburn', 'GOULBURN': 'Goulburn',
    'PENRITH': 'Penrith',
    'NEWCAST': 'Newcastle', 'NEWCASTLE': 'Newcastle',
    'TAMWRTH': 'Tamworth', 'TAMWORTH': 'Tamworth',
}

TARGET_TRACKS = set(TRACKS[k]['name'] for k in TRACKS)

# Keywords that identify a trotting race
TROT_KEYWORDS = ['trot', 'trotter', 'trotters', 'trotting']

def is_trot_race(race_name):
    name_lower = race_name.lower()
    return any(kw in name_lower for kw in TROT_KEYWORDS)


def parse_sectionals(text):
    """
    Extract sectional data from race results pages.

    Results pages use labelled lines per race:
        Track Rating:    FAST
        First Quarter:   27.5
        Second Quarter:  29.1
        Third Quarter:   28.2
        Fourth Quarter:  27.2

    Meeting track is detected from the page header line, e.g.:
        "Nswhrc at Tabcorp Pk Menangle (Night) - Saturday, 21 March 2026"

    Trotting races are excluded. Only TARGET_TRACKS are kept.
    """
    results      = []
    skipped_trot  = 0
    skipped_trial = 0

    lines = text.split('\n')
    n     = len(lines)

    # ── Detect meeting track from page header ────────────────────────────────
    meeting_track = None
    for line in lines[:40]:
        for key, val in TRACK_NAME_MAP.items():
            if key.lower() in line.lower():
                meeting_track = val
                break
        if meeting_track:
            break

    # Current race context
    current_dist  = None
    current_race  = ''
    current_trot  = False
    current_trial = False   # True if prize money is $0
    current_cond  = 'Good'

    i = 0
    while i < n:
        line = lines[i].strip()

        # ── Race name: ALL-CAPS line with race-type keywords ─────────────────
        if (re.match(r'^[A-Z0-9][A-Z0-9\s\-&\'\.]{10,}$', line)
                and not re.match(r'^\d', line)
                and not line.endswith('.')
                and len(line) > 12
                and any(w in line for w in ('PACE', 'TROT', 'STAKES',
                                            'MOBILE', 'HANDICAP', 'FREE',
                                            'HEAT', 'FINAL', 'CLASSIC',
                                            'CUP', 'SPRINT', 'MILE'))):
            current_race  = line
            current_trot  = is_trot_race(line)
            current_trial = False   # reset trial flag on new race

        # ── Prize money: "$20,400" or "$0" — immediately after distance ──────
        prize_match = re.match(r'^\$([\d,]+)$', line)
        if prize_match:
            amount = int(prize_match.group(1).replace(',', ''))
            if amount == 0:
                current_trial = True

        # ── Distance: e.g. "1609M" or "1609" on its own line ─────────────────
        dist_match = re.match(
            r'^(1609|1720|1710|2090|2300|2200|2150|1680|1660|'
            r'2280|2650|2700|2400|1740|1730|1770|1670|1890|2500|2260)M?$',
            line
        )
        if dist_match:
            current_dist = int(dist_match.group(1))

        # ── Track condition: standalone word after "Track Rating:" ────────────
        if line in ('FAST', 'GOOD', 'SLOW', 'HEAVY', 'WET', 'RAIN AFFECTED'):
            cond_map = {
                'FAST': 'Good', 'GOOD': 'Good',
                'SLOW': 'Slow', 'HEAVY': 'Slow',
                'WET':  'Slow', 'RAIN AFFECTED': 'Slow',
            }
            current_cond = cond_map.get(line, 'Good')

        # ── Sectional block starting with "First Quarter:" ───────────────────
        if line == 'First Quarter:':
            try:
                q1 = float(lines[i + 1].strip())   # value line
                # lines[i+2] = "Second Quarter:"
                q2 = float(lines[i + 3].strip())
                # lines[i+4] = "Third Quarter:"
                q3 = float(lines[i + 5].strip())
                # lines[i+6] = "Fourth Quarter:"
                q4 = float(lines[i + 7].strip())
            except (IndexError, ValueError):
                i += 1
                continue

            last_half  = round(q3 + q4, 2)
            first_half = round(q1 + q2, 2)

            if current_trot:
                skipped_trot += 1
                i += 8
                continue

            if current_trial:
                skipped_trial += 1
                i += 8
                continue

            if meeting_track not in TARGET_TRACKS:
                i += 8
                continue

            # Sanity check — realistic pace last half
            if 52 <= last_half <= 68 and current_dist:
                results.append({
                    'q1':         q1, 'q2': q2, 'q3': q3, 'q4': q4,
                    'last_half':  last_half,
                    'first_half': first_half,
                    'distance':   current_dist,
                    'condition':  current_cond,
                    'track':      meeting_track,
                    'race_name':  current_race,
                })

            i += 8
            continue

        i += 1

    if skipped_trot or skipped_trial:
        print(f'       (skipped: {skipped_trot} trot, '
              f'{skipped_trial} trials)', flush=True)

    return results


# ─── PAR CALCULATOR ───────────────────────────────────────────────────────────

def calculate_pars(all_data):
    """
    Calculate par last half for each track/distance/condition combination.
    Uses median (more robust than mean against outliers).

    Returns dict:
    {
        'Menangle': {
            1609: {
                'Good': {'par': 57.2, 'n': 156, 'std': 1.1},
                'Slow': {'par': 58.8, 'n': 34,  'std': 1.3},
            }
        }
    }
    """
    import statistics

    pars = {}

    for track_name, dist_dict in all_data.items():
        pars[track_name] = {}
        for dist, cond_dict in dist_dict.items():
            pars[track_name][dist] = {}
            for cond, times in cond_dict.items():
                if len(times) >= 10:  # minimum sample
                    times_sorted = sorted(times)
                    # Remove top/bottom 5% as outliers
                    trim = max(1, len(times_sorted) // 20)
                    trimmed = times_sorted[trim:-trim] if len(times_sorted) > 20 else times_sorted
                    par = round(statistics.median(trimmed), 2)
                    std = round(statistics.stdev(trimmed), 2) if len(trimmed) > 1 else 0
                    pars[track_name][dist][cond] = {
                        'par': par,
                        'n': len(times),
                        'std': std,
                        'min': round(min(trimmed), 2),
                        'max': round(max(trimmed), 2),
                    }

    return pars


# ─── MAIN FETCHER ─────────────────────────────────────────────────────────────

def fetch_page(playwright_page, url):
    """Fetch a URL using Playwright headless browser."""
    try:
        playwright_page.goto(url, timeout=20000, wait_until='domcontentloaded')
        playwright_page.wait_for_timeout(3000)
        return playwright_page.content()
    except Exception:
        return None


def is_real_meeting(html):
    """
    Check if the fetched page is a real completed meeting with results.
    The race-fields page becomes a results page once races are run.
    """
    if not html or len(html) < 5000:
        return False
    has_races    = 'Final Results' in html or 'First Quarter' in html
    has_quarters = 'First Quarter' in html and 'Fourth Quarter' in html
    return has_races and has_quarters


def build_url(mc):
    """Return the race-fields/results URL for a given meeting code."""
    return f'https://www.harness.org.au/racing/fields/race-fields/?mc={mc}'


def progress_bar(done, total, width=30):
    """Return a simple ASCII progress bar string."""
    filled = int(width * done / max(total, 1))
    bar = '█' * filled + '░' * (width - filled)
    pct = int(100 * done / max(total, 1))
    return f'[{bar}] {pct:3d}%  {done}/{total}'


def print_live_summary(all_data):
    """Print a compact summary of data collected so far."""
    print('\n  ┌─ Data collected so far ──────────────────────────────┐')
    any_data = False
    for track_name, dist_dict in all_data.items():
        for dist in sorted(dist_dict.keys()):
            for cond, times in dist_dict[dist].items():
                n = len(times)
                if n > 0:
                    avg = round(sum(times) / n, 2)
                    status = '✓ enough' if n >= 50 else f'  need {50-n} more'
                    print(f'  │  {track_name:10} {dist}m {cond:5}  '
                          f'n={n:4}  avg last half={avg}s  {status}')
                    any_data = True
    if not any_data:
        print('  │  (no sectionals collected yet)')
    print('  └──────────────────────────────────────────────────────┘\n')


def run_diagnostic(desktop):
    """
    Fetch one known-good URL and save the raw HTML to the Desktop.
    This tells us exactly what the site is returning so we can fix
    the is_real_meeting() detection if needed.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print('Playwright not installed.')
        return

    # Use a known completed Menangle meeting
    test_mc  = 'PC210326'
    test_url = build_url(test_mc)

    print(f'\n  DIAGNOSTIC MODE')
    print(f'  Fetching: {test_url}')
    print('  Please wait...\n')

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        try:
            page.goto(test_url, timeout=25000, wait_until='domcontentloaded')
            page.wait_for_timeout(4000)
            html = page.content()
        except Exception as e:
            print(f'  Error loading page: {e}')
            browser.close()
            return
        browser.close()

    # Save raw HTML
    diag_path = os.path.join(desktop, 'diagnostic_results.html')
    with open(diag_path, 'w', encoding='utf-8') as f:
        f.write(html)

    # Report what we see
    print(f'  Page size:        {len(html):,} bytes')
    print(f'  Has "Race 1":     {"Race 1" in html}')
    print(f'  Has sectionals:   {bool(re.search(r"\\(\\d+\\.\\d+,\\s*\\d+\\.\\d+", html))}')
    print(f'  Has results:      {"Result" in html or "result" in html}')
    print(f'  Has "Redirect":   {"Redirect" in html or "redirect" in html}')
    print(f'  Is real meeting:  {is_real_meeting(html)}')
    print()

    # Show first 500 chars of text content
    p2 = TextExtractor()
    p2.feed(html)
    text = '\n'.join(p2.text)
    print('  First 500 chars of extracted text:')
    print('  ' + '-' * 50)
    print(text[:500])
    print('  ' + '-' * 50)
    print(f'\n  Full page saved to: {diag_path}')
    print('  Upload diagnostic_page.html to Claude if you need help diagnosing.')


def main():
    print('=' * 60)
    print('  Harness Track Par Database Builder')
    print('  Target: Menangle, Bathurst, Goulburn')
    print('=' * 60)
    print()
    print('  Options:')
    print('    1. Run normally (fetch all meetings)')
    print('    2. Diagnostic mode (fetch one page to check connection)')
    print()
    mode = input('  Enter 1 or 2: ').strip()

    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')

    if mode == '2':
        run_diagnostic(desktop)
        input('\n  Press Enter to close...')
        sys.exit()

    # Import Playwright
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print('\n  Playwright not installed.')
        print('  Run:  pip install playwright')
        print('  Then: py -m playwright install chromium')
        input('\n  Press Enter to close...')
        sys.exit()

    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_path = os.path.join(desktop, 'track_pars.json')
    existing_log = os.path.join(desktop, 'track_pars_log.json')

    # Load existing data if resuming
    all_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    meeting_log = set()

    if os.path.exists(existing_log):
        print('\n  ► Resuming from previous run...')
        with open(existing_log, 'r') as f:
            saved = json.load(f)
            meeting_log = set(saved.get('fetched', []))
            raw = saved.get('raw', {})
            for track, dist_dict in raw.items():
                for dist, cond_dict in dist_dict.items():
                    for cond, times in cond_dict.items():
                        all_data[track][int(dist)][cond].extend(times)
        print(f'  Already checked: {len(meeting_log)} meeting codes')
        print_live_summary(all_data)
    else:
        print(f'\n  Starting fresh — going back {MONTHS_BACK} months.')
        print('  Progress saves automatically every 10 meetings.')
        print('  You can stop (Ctrl+C) and resume any time.\n')

    total_sectionals = sum(
        len(times)
        for dist_dict in all_data.values()
        for cond_dict in dist_dict.values()
        for times in cond_dict.values()
    )

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()

            for track_code, track_info in TRACKS.items():
                track_name = track_info['name']
                codes = generate_meeting_codes(track_code, MONTHS_BACK)
                remaining = [c for c in codes if c[1] not in meeting_log]
                total_codes = len(codes)
                already_done = total_codes - len(remaining)

                print(f'\n  ══ {track_name} ({track_code}) '
                      f'══════════════════════════════')
                print(f'  Dates to check: {len(remaining)} '
                      f'(skipping {already_done} already done)')

                meetings_found = 0
                track_sectionals = 0
                checked = 0
                save_counter = 0

                for meeting_date, mc in remaining:
                    checked += 1
                    save_counter += 1

                    # Live progress line (overwrites itself)
                    bar = progress_bar(checked, len(remaining))
                    print(f'\r  {bar}  checking {mc} ({meeting_date})...     ',
                          end='', flush=True)

                    url  = build_url(mc)
                    html = fetch_page(page, url)
                    meeting_log.add(mc)

                    if not is_real_meeting(html):
                        time.sleep(0.3)
                        # Auto-save every 10 checks
                        if save_counter >= 10:
                            _save_progress(desktop, existing_log, meeting_log,
                                           all_data)
                            save_counter = 0
                        continue

                    # Real meeting found — parse sectionals
                    text = extract_text(html)
                    sectionals = parse_sectionals(text)

                    if sectionals:
                        for s in sectionals:
                            # Attribute to the track the run was actually at
                            trk  = s['track']
                            dist = s['distance']
                            cond = s['condition']
                            all_data[trk][dist][cond].append(s['last_half'])
                        meetings_found += 1
                        track_sectionals += len(sectionals)
                        total_sectionals += len(sectionals)

                        # Print result on new line so it persists
                        print(f'\r  ✓ {mc} {meeting_date}  '
                              f'{len(sectionals):3d} pace sectionals saved          ')
                    else:
                        # Met a real page but no sectionals (e.g. fields only)
                        print(f'\r  ~ {mc} {meeting_date}  '
                              f'page found but no sectionals parsed              ')

                    time.sleep(1.2)  # polite delay

                    # Auto-save every 10 meetings
                    if save_counter >= 10:
                        _save_progress(desktop, existing_log, meeting_log,
                                       all_data)
                        save_counter = 0

                # Final newline after progress bar
                print()

                print(f'\n  ── {track_name} complete ──')
                print(f'     Meetings found:   {meetings_found}')
                print(f'     Sectionals saved: {track_sectionals}')

                # Show what we have for this track
                if track_name in all_data:
                    for dist in sorted(all_data[track_name].keys()):
                        for cond, times in all_data[track_name][dist].items():
                            n = len(times)
                            avg = round(sum(times) / n, 2) if n else 0
                            flag = ' ✓' if n >= 50 else f' (need {max(0,50-n)} more)'
                            print(f'     {dist}m {cond:5}  '
                                  f'n={n}  avg last half={avg}s{flag}')

            browser.close()

    except KeyboardInterrupt:
        print('\n\n  Stopped by user — saving progress...')

    # Final save
    _save_progress(desktop, existing_log, meeting_log, all_data)
    print(f'  Progress saved to: {existing_log}')

    # Calculate and save final par database
    print('\n  Calculating par times from all collected data...')
    pars = calculate_pars(all_data)

    output = {
        'generated':        date.today().isoformat(),
        'tracks_fetched':   list(TRACKS.keys()),
        'total_sectionals': total_sectionals,
        'pars':             pars,
    }

    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    # Final summary
    print('\n' + '=' * 60)
    print('  FINAL PAR SUMMARY')
    print('=' * 60)
    for track_name, dist_dict in pars.items():
        print(f'\n  {track_name}:')
        for dist in sorted(dist_dict.keys()):
            for cond, stats in dist_dict[dist].items():
                print(f'    {dist}m {cond:5}  par last half: {stats["par"]}s  '
                      f'n={stats["n"]}  std={stats["std"]}  '
                      f'range {stats["min"]}–{stats["max"]}')

    print(f'\n  ✓ Par database saved to: {output_path}')
    print('  Upload track_pars.json to Claude to integrate into scoring.')
    print()
    input('  Press Enter to close...')


def _save_progress(desktop, log_path, meeting_log, all_data):
    """Save progress so the script can be safely interrupted and resumed."""
    raw_serialisable = {}
    for track, dist_dict in all_data.items():
        raw_serialisable[track] = {}
        for dist, cond_dict in dist_dict.items():
            raw_serialisable[track][str(dist)] = {}
            for cond, times in cond_dict.items():
                raw_serialisable[track][str(dist)][cond] = times

    with open(log_path, 'w') as f:
        json.dump({'fetched': list(meeting_log), 'raw': raw_serialisable}, f)


if __name__ == '__main__':
    main()
