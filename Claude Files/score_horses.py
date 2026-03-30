"""
Harness Form Scoring Engine
============================
Reads horse profile HTML files (fetched by fetch_horses.py) and a race
form HTML file, then produces a margin-adjusted form score for each horse.

Margin adjustment rules from Fixing-the-data-2021.docx:
  Behind lead at bell        +7.5m  (had easier run than margin suggests)
  Held up / no clear run     -7.5m  (excused)
  Outside leader/death seat  -10m   (excused)
  3 wide no cover            -10m   (excused)
  Checked / inconvenienced   -10m   (excused)
  Sulky contact              -10m   (excused)
  3 wide early or middle     -5m    (excused)
  1 out 4 back or deeper     -7.5m  (excused)
  Locked wheels / Broke      NULL   (run excluded entirely)

Abbreviated stewards codes from harness.org.au horse profile pages are
decoded into the same adjustment values.

HOW TO USE:
1. Fetch race form:   python fetch_race_v2.py
2. Fetch horse pages: python fetch_horses.py
3. Run this script:   python score_horses.py
4. Enter paths when prompted (or drag files/folders onto window)
5. Copy output into Claude for full analysis

OUTPUT:
  Ranked table of horses with:
  - Adjusted margin average (last 5 valid runs)
  - Raw runs showing each adjustment applied
  - Pricing signal (recent market prices vs NR)
  - BMR gap flag (stable transfer detector)
"""

import os
import re
import sys
from html.parser import HTMLParser
from datetime import datetime, date, timedelta


# ─── HTML TEXT EXTRACTOR ──────────────────────────────────────────────────────

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


def extract_text(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        html = f.read()
    p = TextExtractor()
    p.feed(html)
    return '\n'.join(p.text)


# ─── STEWARDS CODE DECODER ────────────────────────────────────────────────────
# Maps abbreviated stewards codes to margin adjustments
# Format: CODE: (description, adjustment_metres, is_null_run)

CODES = {
    # Positive/neutral position codes
    'GS':    ('gate_speed',               0,     False),
    'L':     ('led',                      0,     False),
    'WF':    ('worked_forward',           0,     False),
    'RTR':   ('restrained_rear',          0,     False),
    'RAS':   ('restrained_after_start',   0,     False),
    'OPS':   ('out_of_position_start',    0,     False),
    'FBS':   ('fractious_before_start',   0,     False),
    'GO':    ('gave_ground',              0,     False),
    'PRBT':  ('pre_race_blood_test',      0,     False),
    'SWAB':  ('swabbed',                  0,     False),
    'L1W':   ('last_1_win_condition',     0,     False),
    'W1':    ('won_1',                    0,     False),
    'W2':    ('won_2',                    0,     False),
    'WF':    ('whip_free',                0,     False),
    'WFS':   ('whip_free_sprint',         0,     False),
    'WFE':   ('whip_free_eligible',       0,     False),
    'WFM':   ('whip_free_mandatory',      0,     False),
    'SWAB':  ('swabbed',                  0,     False),
    'D/F':   ('driver_cautioned',         0,     False),
    'D/S':   ('driver_suspended',         0,     False),
    'D/RWA': ('driver_reprimanded_whip',  0,     False),
    'D/SCD': ('driver_suspended_careless',0,     False),
    'D/FWA': ('driver_fined_whip',        0,     False),
    'D/CT':  ('driver_cautioned',         0,     False),
    'D/C':   ('driver_cautioned',         0,     False),

    # EXCUSED positions — horse performed better than margin shows
    'OL':    ('outside_leader',          -10,    False),  # death seat
    'OLM':   ('outside_leader_mid',      -10,    False),
    'OLT':   ('outside_leader',          -10,    False),
    'OTE':   ('outside_throughout_early',-10,    False),
    'OTM':   ('outside_throughout_mid',  -10,    False),
    'OT':    ('outside_throughout',      -10,    False),
    'SCT':   ('sulky_contact',           -10,    False),
    'DINC':  ('directly_inconvenienced', -10,    False),
    'INC':   ('inconvenienced',          -10,    False),
    'CI':    ('checked_inconvenienced',  -10,    False),
    'IAS':   ('interference_at_start',    -5,    False),
    'HU':    ('held_up',                 -7.5,   False),
    'HUE':   ('held_up_early',           -7.5,   False),
    'ODM':   ('outside_draw_mobile',     -7.5,   False),
    'OV':    ('overraced',                -5,    False),
    'OVR':   ('overraced',                -5,    False),
    'HI':    ('hung_in',                  -5,    False),
    'HO':    ('hung_out',                 -5,    False),
    'CWE':   ('caught_wide_early',        -5,    False),
    'WE':    ('wide_early',               -5,    False),
    'WM':    ('wide_middle',              -5,    False),
    'WET':   ('wide_throughout',          -7.5,  False),
    'RWE':   ('raced_wide_early',         -5,    False),
    'RWM':   ('raced_wide_middle',        -5,    False),
    '3WL':   ('3_wide_late',              -5,    False),
    '3WM':   ('3_wide_middle',            -5,    False),
    '3WET':  ('3_wide_early_throughout',  -5,    False),
    'WLT':   ('wide_late',                -3,    False),
    'WL':    ('wide_late',                -3,    False),
    'SHI':   ('shifted_in',               -3,    False),
    'SHO':   ('shifted_out',              -3,    False),

    # Beneficial — horse had easier run than margin shows
    'OIR':   ('obtained_inside_run',      +5,    False),
    'ODS':   ('obtained_outside_run',     +3,    False),
    'SLE':   ('sprint_lane_entered',      +5,    False),
    'USL':   ('used_sprint_lane',         +3,    False),
    'SOUP':  ('swooped',                  +5,    False),
    'SIUP':  ('swept_inside',             +5,    False),

    # NULL runs — broken, fell, gear failure, etc.
    'BSU':   ('broke_score_up',           None,  True),
    'BL':    ('broke_late',               None,  True),
    'BCE':   ('broke_checked_early',      None,  True),
    'SLM':   ('sprint_lane_miss',         None,  True),
}


def decode_positional(code):
    """
    Decode positional shorthand codes:
      '13' = 1 out, 3 back = outside leader/death seat = -10
      '14' = 1 out, 4 back = deep back = -7.5
      '15' = 1 out, 5 back = very deep = -7.5
      '4'–'9' = X back on pegs = deep = -7.5
      '10'+ back on pegs = deep = -7.5
    """
    if re.match(r'^1[2-9]$', code):
        back = int(code[1])
        if back >= 4:
            return -7.5   # 1 out, 4+ back = deep
        elif back >= 2:
            return -10    # 1 out, 2–3 back = outside leader
    elif re.match(r'^[4-9]$', code):
        return -7.5       # 4–9 back on pegs
    elif re.match(r'^[1-9][0-9]$', code) and int(code) >= 10:
        return -7.5       # 10+ back on pegs
    return 0


def apply_codes(code_str):
    """
    Apply margin adjustment rules to a stewards comment code string.
    Returns (total_adjustment, is_null_run, list_of_reasons).
    """
    adj = 0
    null_run = False
    reasons = []
    parts = code_str.upper().split()

    for part in parts:
        if part in CODES:
            name, adjustment, is_null = CODES[part]
            if is_null:
                null_run = True
                reasons.append(f"NULL({part})")
            elif adjustment and adjustment != 0:
                adj += adjustment
                reasons.append(f"{adjustment:+.1f}({part}={name})")
        else:
            pos_adj = decode_positional(part)
            if pos_adj != 0:
                adj += pos_adj
                reasons.append(f"{pos_adj:+.1f}(pos:{part})")

    return adj, null_run, reasons


# ─── HORSE PROFILE PARSER ─────────────────────────────────────────────────────

def parse_horse_profile(filepath):
    """
    Parse a horse profile HTML file into structured data.
    Returns dict with name, NR, career stats, this_season, last_season,
    bmr values, runs list.
    """
    text = extract_text(filepath)
    fname = os.path.basename(filepath)

    # Horse name from filename
    name = re.sub(r'_\d+\.html$', '', fname).replace('_', ' ')

    # NR
    nr_match = re.search(r'Class\s*\n(NR\d+)', text)
    nr = int(nr_match.group(1)[2:]) if nr_match else 0

    # Career summary
    career = re.search(r'Lifetime\nSummary:\n([\d\-]+)', text)
    this_season = re.search(r'This Season\nSummary:\n([\d\-]+)', text)
    last_season = re.search(r'Last Season\nSummary:\n([\d\-]+)', text)

    def parse_summary(m):
        if not m: return (0,0,0,0)
        parts = m.group(1).split('-')
        return tuple(int(x) for x in parts) if len(parts) == 4 else (0,0,0,0)

    # BMRs — career, this season, last season
    bmrs = re.findall(r'Best Winning Mile Rate:\n([\d:\.]+)', text)
    career_bmr = bmrs[0] if bmrs else None
    this_bmr   = bmrs[1] if len(bmrs) > 1 else None
    last_bmr   = bmrs[2] if len(bmrs) > 2 else None

    # BMR gap calculation
    def mr_secs(mr):
        if not mr or ':' not in mr: return None
        try:
            parts = mr.split(':')
            return int(parts[0]) * 60 + float(parts[1])
        except:
            return None

    career_s = mr_secs(career_bmr)
    this_s   = mr_secs(this_bmr)
    bmr_gap  = round(this_s - career_s, 1) if (career_s and this_s) else None

    # Parse runs
    pr_idx = text.find('Performance Records')
    if pr_idx == -1:
        return {'name': name, 'nr': nr, 'runs': [], 'bmr_gap': bmr_gap,
                'career_bmr': career_bmr, 'this_bmr': this_bmr,
                'this_season': parse_summary(this_season),
                'career': parse_summary(career)}

    form = text[pr_idx:pr_idx + 20000]
    lines = form.split('\n')

    run_pat = re.compile(r'^\d{2} \w+ \d{4}$')
    runs = []
    i = 0
    while i < len(lines):
        if run_pat.match(lines[i].strip()):
            block = lines[i:i+20]
            run = _parse_run_block(block)
            if run:
                runs.append(run)
        i += 1

    # Recent prices (last 8 real races, not trials)
    prices = []
    for r in runs:
        if r['price'] and r['price'] > 0 and r['race_type'] != 'TRIAL':
            prices.append(r['price'])
        if len(prices) >= 8:
            break

    return {
        'name': name,
        'nr': nr,
        'career_bmr': career_bmr,
        'this_bmr': this_bmr,
        'last_bmr': last_bmr,
        'bmr_gap': bmr_gap,
        'career': parse_summary(career),
        'this_season': parse_summary(this_season),
        'last_season': parse_summary(last_season),
        'runs': runs,
        'recent_prices': prices,
    }


def _parse_run_block(block):
    """
    Parse a single run block into structured fields.

    The horse profile page has a fixed column order per run:
      0  date         e.g. '22 Mar 2026'
      1  track        e.g. 'LNCSTN'
      2  pos          e.g. '2'
      3  barrier      e.g. 'Sr2'
      4  margin       e.g. '5.9m' or 'HD'
      5  mile_rate    e.g. '1:57.6'
      6  driver       e.g. 'J C Duggan'
      7  trainer      e.g. 'A C Duggan'
      8  stake        e.g. '$1,649'   ← comma-formatted, thousands
      9  distance     e.g. '2200MS'
      10 race_name    e.g. 'THE BOTTLE-O HADSPEN PACE'
      11 price        e.g. '$9.00'    ← decimal, no comma
      12 comment      e.g. '13 HI D/RWA'  (may be absent)
    """
    if len(block) < 8:
        return None

    date    = block[0].strip()
    track   = block[1].strip() if len(block) > 1 else ''
    pos_str = block[2].strip() if len(block) > 2 else ''
    pos     = int(pos_str) if re.match(r'^\d+$', pos_str) else None

    # Margin: field 4 — patterns like '5.9m', 'HD', '0.1m'
    margin = None
    raw_mgn = block[4].strip() if len(block) > 4 else ''
    if raw_mgn == 'HD' or raw_mgn == 'HFHD' or raw_mgn == 'SH':
        margin = 0.1
    elif raw_mgn == 'SHFHD':
        margin = 0.05
    else:
        m = re.match(r'^([\d\.]+)m$', raw_mgn)
        if m:
            margin = float(m.group(1))

    # Mile rate: field 5
    mr = None
    raw_mr = block[5].strip() if len(block) > 5 else ''
    if re.match(r'^\d:\d{2}\.\d$', raw_mr):
        mr = raw_mr

    # FT (stand start) runs have two extra fields inserted after barrier
    # shifting driver/trainer from fields 6/7 to fields 8/9
    ft_offset = 2 if (len(block) > 3 and block[3].strip() == 'FT') else 0

    trainer = block[7 + ft_offset].strip() if len(block) > 7 + ft_offset else ''
    driver  = block[6 + ft_offset].strip() if len(block) > 6 + ft_offset else ''

    # Validate — trainer/driver should look like a name (letters + spaces)
    if trainer and re.match(r'^[\d:\.\$]', trainer):
        trainer = ''
    if driver and re.match(r'^[\d:\.\$]', driver):
        driver = ''
    # Price: field 11 — decimal e.g. '$9.00'
    # Distinguish: price has a decimal point; stake uses comma thousands
    price = None
    raw_price = block[11].strip() if len(block) > 11 else ''
    m = re.match(r'^\$([\d]+\.[\d]{2})$', raw_price)
    if m:
        price = float(m.group(1))

    # Comment codes: field 12 (may be absent or next run starts)
    comment_codes = ''
    if len(block) > 12:
        candidate = block[12].strip()
        # Not a new run date, not empty, not a distance code alone
        if (candidate
                and not re.match(r'^\d{2} \w+ \d{4}$', candidate)
                and not re.match(r'^\d{4}(MS|SS)$', candidate)):
            comment_codes = candidate

    # Race type
    race_name = block[10].strip() if len(block) > 10 else ''
    race_type = 'TRIAL' if 'TRIAL' in race_name.upper() else 'RACE'

    # Distance
    dist_raw = block[9].strip() if len(block) > 9 else ''
    dist_m = re.match(r'^(\d{4})', dist_raw)
    dist = int(dist_m.group(1)) if dist_m else None

    adj, null_run, reasons = apply_codes(comment_codes)

    return {
        'date':          date,
        'track':         track,
        'pos':           pos,
        'margin':        margin,
        'mile_rate':     mr,
        'price':         price,
        'trainer':       trainer,
        'driver':        driver,
        'comment_codes': comment_codes,
        'adj':           adj,
        'null_run':      null_run,
        'reasons':       reasons,
        'race_type':     race_type,
        'dist':          dist,
        'adj_margin':    None if (null_run or margin is None) else round(margin + adj, 1),
    }


# ─── FORM SCORE CALCULATOR ────────────────────────────────────────────────────

def calculate_form_score(horse_data, n_runs=5):
    """
    Calculate adjusted margin average from last N valid (non-null, non-trial) runs.
    Lower score = better performer.
    """
    valid = []
    nulls = 0
    for run in horse_data['runs']:
        if run['race_type'] == 'TRIAL':
            continue
        if run['null_run']:
            nulls += 1
            continue
        if run['adj_margin'] is not None:
            valid.append(run)
        if len(valid) >= n_runs:
            break

    if not valid:
        return None, [], nulls

    margins = [r['adj_margin'] for r in valid]
    avg = round(sum(margins) / len(margins), 1)
    return avg, valid, nulls


# ─── PRICING SIGNAL ───────────────────────────────────────────────────────────

def pricing_signal(horse_data):
    """
    Analyse recent market prices vs NR to detect anomalies.
    Returns (signal_type, avg_last4, trend_description)
    """
    prices = horse_data['recent_prices']
    nr = horse_data['nr']

    if len(prices) < 4:
        return 'insufficient_data', None, 'Insufficient price data'

    recent4 = prices[:4]
    older4  = prices[4:8] if len(prices) >= 8 else prices[2:]
    avg_r4  = round(sum(recent4) / len(recent4), 1)
    avg_o4  = round(sum(older4) / len(older4), 1) if older4 else avg_r4

    # BMR gap flag
    bmr_gap = horse_data.get('bmr_gap')
    transfer_flag = bmr_gap is not None and bmr_gap > 4.0

    # NR vs price mismatch
    expected_max_price = max(4.0, 100 - nr)  # rough heuristic: NR100 → $4, NR75 → $25
    nr_price_mismatch = avg_r4 > expected_max_price * 2

    # Trend
    if avg_r4 < avg_o4 * 0.65:
        trend = 'SHORTENING_SHARPLY'
        label = '▲ Shortening sharply'
    elif avg_r4 < avg_o4 * 0.85:
        trend = 'SHORTENING'
        label = '▲ Shortening'
    elif avg_r4 > avg_o4 * 1.4:
        trend = 'DRIFTING'
        label = '▼ Drifting out'
    else:
        trend = 'STABLE'
        label = '≈ Stable'

    flags = []
    if transfer_flag:
        flags.append(f'⚑ Stable transfer (BMR gap +{bmr_gap}s/mile)')
    if nr_price_mismatch:
        flags.append(f'⚑ NR{nr} but avg price ${avg_r4} — market rejects NR')

    return trend, avg_r4, label, flags


# ─── FITNESS CHECK ────────────────────────────────────────────────────────────

MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,  'May': 5,  'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}

def days_since_last_run(horse_data, race_date=None):
    """
    Calculate days between the horse's most recent race and today's race date.
    Excludes trials. Returns (days, last_run_date_str, fitness_flag).

    fitness_flag values:
      'FRESH'   — last race > 14 days ago → apply +25% odds penalty
      'FIT'     — last race within 14 days → no adjustment
      'UNKNOWN' — no date data available
    """
    from datetime import datetime, date

    if race_date is None:
        race_date = date.today()
    elif isinstance(race_date, str):
        try:
            day, mon, yr = race_date.split()
            race_date = date(int(yr), MONTHS[mon], int(day))
        except Exception:
            race_date = date.today()

    last_race_date = None
    last_race_str = None

    for run in horse_data.get('runs', []):
        if run.get('race_type') == 'TRIAL':
            continue
        date_str = run.get('date', '')
        try:
            parts = date_str.split()
            if len(parts) == 3:
                d = date(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
                if last_race_date is None or d > last_race_date:
                    last_race_date = d
                    last_race_str = date_str
        except Exception:
            continue

    if last_race_date is None:
        return None, None, 'UNKNOWN'

    days = (race_date - last_race_date).days

    if days > 14:
        flag = 'FRESH'
    else:
        flag = 'FIT'

    return days, last_race_str, flag


# ─── TRAINER STATS ────────────────────────────────────────────────────────────

def build_trainer_stats(all_horses, race_date=None):
    """
    Aggregate trainer stats across all horse profiles in the field.

    For each trainer we calculate:
      - Prep starts / wins / places  (all runs in Performance Records)
      - Last 30 day starts / wins / places
      - Win % and place % for both windows

    Returns dict keyed by trainer name:
    {
        'W J Yole': {
            'prep_starts': 28, 'prep_wins': 4, 'prep_places': 9,
            'prep_win_pct': 14.3, 'prep_place_pct': 46.4,
            'l30_starts': 8,  'l30_wins': 2, 'l30_places': 4,
            'l30_win_pct': 25.0, 'l30_place_pct': 75.0,
        }
    }
    """
    from datetime import datetime, date as date_type

    if race_date is None:
        cutoff = date_type.today()
    elif isinstance(race_date, str):
        try:
            parts = race_date.split()
            cutoff = date_type(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
        except Exception:
            cutoff = date_type.today()
    else:
        cutoff = race_date

    from datetime import timedelta
    l30_start = cutoff - timedelta(days=30)

    stats = {}

    for horse in all_horses:
        for run in horse.get('runs', []):
            if run.get('race_type') == 'TRIAL':
                continue
            trainer = run.get('trainer', '').strip()
            if not trainer:
                continue

            # Parse run date
            date_str = run.get('date', '')
            try:
                parts = date_str.split()
                run_date = date_type(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
            except Exception:
                continue

            # Only count runs up to race day
            if run_date > cutoff:
                continue

            pos = run.get('pos')

            if trainer not in stats:
                stats[trainer] = {
                    'prep_starts': 0, 'prep_wins': 0, 'prep_places': 0,
                    'l30_starts':  0, 'l30_wins':  0, 'l30_places':  0,
                }

            s = stats[trainer]
            s['prep_starts'] += 1
            if pos == 1:
                s['prep_wins'] += 1
            if pos in (1, 2, 3):
                s['prep_places'] += 1

            if run_date >= l30_start:
                s['l30_starts'] += 1
                if pos == 1:
                    s['l30_wins'] += 1
                if pos in (1, 2, 3):
                    s['l30_places'] += 1

    # Calculate percentages
    for trainer, s in stats.items():
        s['prep_win_pct']   = round(100 * s['prep_wins']   / s['prep_starts'],  1) if s['prep_starts']  else 0
        s['prep_place_pct'] = round(100 * s['prep_places'] / s['prep_starts'],  1) if s['prep_starts']  else 0
        s['l30_win_pct']    = round(100 * s['l30_wins']    / s['l30_starts'],   1) if s['l30_starts']   else 0
        s['l30_place_pct']  = round(100 * s['l30_places']  / s['l30_starts'],   1) if s['l30_starts']   else 0

    return stats


# ─── DRIVER STATS ─────────────────────────────────────────────────────────────

def parse_driver_profile(filepath, race_date=None):
    """
    Parse a driver profile HTML page into structured stats.

    Returns dict with:
      name, career_win_pct, season_win_pct,
      l7_starts, l7_wins, l7_win_pct,
      p7_starts, p7_wins, p7_win_pct,
      momentum   ('IMPROVING' | 'DECLINING' | 'STABLE' | 'INSUFFICIENT')
      upscale    (float multiplier — >1.0 means driver is in form)
    """
    from datetime import date as date_type, timedelta

    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        html = f.read()

    p = TextExtractor()
    p.feed(html)
    text = '\n'.join(p.text)
    lines = text.split('\n')

    # Driver name from page title
    name = ''
    for line in lines[:10]:
        if line.strip() and 'Australian Harness' not in line and 'All Races' not in line:
            name = line.strip()
            break

    # Career and season win %
    career_pct = 0
    season_pct = 0
    cw = re.search(r'Career Win %\s*\n(\d+)%', text)
    sw = re.search(r'Season Win %\s*\n(\d+)%', text)
    if cw: career_pct = int(cw.group(1))
    if sw: season_pct = int(sw.group(1))

    # Race date for window calculation
    if race_date is None:
        cutoff = date_type.today()
    elif isinstance(race_date, str):
        try:
            parts = race_date.split()
            cutoff = date_type(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
        except Exception:
            cutoff = date_type.today()
    else:
        cutoff = race_date

    l7_end    = cutoff - timedelta(days=1)
    l7_start  = cutoff - timedelta(days=7)
    p7_end    = cutoff - timedelta(days=8)
    p7_start  = cutoff - timedelta(days=14)

    # Parse Latest Drives
    ld_idx  = next((i for i, l in enumerate(lines) if 'Latest Drives' in l), None)
    end_idx = next((i for i, l in enumerate(lines) if 'All Tracks - All Seasons' in l
                    or 'Season Stats' in l), None)

    drives = []
    if ld_idx and end_idx:
        block    = lines[ld_idx:end_idx]
        date_pat = re.compile(r'^\d{2}-\d{2}-\d{4}$')
        i = 0
        while i < len(block):
            if date_pat.match(block[i].strip()):
                try:
                    d_str = block[i].strip()
                    pos   = block[i + 4].strip()
                    day, mon, yr = d_str.split('-')
                    d_date = date_type(int(yr), int(mon), int(day))
                    drives.append({
                        'date': d_date,
                        'pos':  int(pos) if pos.isdigit() else 99,
                    })
                    i += 7
                    continue
                except Exception:
                    pass
            i += 1

    def window_stats(start, end):
        ds = [d for d in drives if start <= d['date'] <= end]
        if not ds:
            return 0, 0, 0.0
        wins = sum(1 for d in ds if d['pos'] == 1)
        return len(ds), wins, round(100 * wins / len(ds), 1)

    l7s, l7w, l7pct = window_stats(l7_start,  l7_end)
    p7s, p7w, p7pct = window_stats(p7_start,  p7_end)

    # Momentum and upscale multiplier
    # Upscale applies when last 7 days win% is meaningfully higher than previous 7
    # Multiplier compresses/expands the horse's composite score (not the odds directly —
    # score_horses reports the multiplier and Claude applies it during odds generation)
    MIN_DRIVES = 3   # need at least 3 drives in window to be meaningful

    if l7s < MIN_DRIVES or p7s < MIN_DRIVES:
        momentum = 'INSUFFICIENT'
        upscale  = 1.0
    elif l7pct > p7pct + 15:
        momentum = 'IMPROVING'
        # Scale proportionally: +15pp improvement = 1.10, +30pp = 1.20, capped at 1.30
        boost    = min(0.30, (l7pct - p7pct) / 100)
        upscale  = round(1.0 + boost, 2)
    elif l7pct < p7pct - 15:
        momentum = 'DECLINING'
        drag     = min(0.20, (p7pct - l7pct) / 150)
        upscale  = round(1.0 - drag, 2)
    else:
        momentum = 'STABLE'
        upscale  = 1.0

    return {
        'name':         name,
        'career_pct':   career_pct,
        'season_pct':   season_pct,
        'l7_starts':    l7s,
        'l7_wins':      l7w,
        'l7_win_pct':   l7pct,
        'p7_starts':    p7s,
        'p7_wins':      p7w,
        'p7_win_pct':   p7pct,
        'momentum':     momentum,
        'upscale':      upscale,
    }


def load_driver_stats(driver_folder, race_date=None):
    """
    Load all driver profile HTML files from a folder.
    Returns dict keyed by slug (filename without driver_ prefix and .html suffix).
    """
    if not driver_folder or not os.path.isdir(driver_folder):
        return {}

    stats = {}
    for fname in os.listdir(driver_folder):
        if fname.startswith('driver_') and fname.endswith('.html'):
            slug = fname[7:-5]  # strip 'driver_' and '.html'
            fpath = os.path.join(driver_folder, fname)
            try:
                ds = parse_driver_profile(fpath, race_date)
                stats[slug] = ds
            except Exception:
                pass
    return stats


def match_driver_to_stats(driver_name, driver_stats):
    """
    Match a driver name (e.g. 'Jack Watson' or 'J Watson') to driver stats dict.
    Tries slug match first, then partial name match.
    """
    if not driver_name or not driver_stats:
        return None

    # Try full name slug
    slug = re.sub(r"[^a-z0-9\-]", '', driver_name.lower().replace(' ', '-'))
    slug = re.sub(r'-+', '-', slug).strip('-')
    if slug in driver_stats:
        return driver_stats[slug]

    # Try matching by last name
    parts = driver_name.split()
    last  = parts[-1].lower() if parts else ''
    for s, ds in driver_stats.items():
        if last in s:
            return ds

    return None


# ─── DRIVER STATS ─────────────────────────────────────────────────────────────

def parse_driver_profile(filepath, race_date=None):
    """
    Parse a driver profile HTML page.

    Extracts Latest Drives (last 10) with:
      date, track, race#, horse, placing, margin, odds

    And season stats summary.

    Returns dict with:
      name, season_starts, season_wins, season_win_pct,
      drives: [{date, track, placing, win}]
      l7_starts, l7_wins, l7_win_pct
      l7_prev_starts, l7_prev_wins, l7_prev_win_pct
      momentum: 'HOT' | 'COLD' | 'NEUTRAL' | 'INSUFFICIENT'
      upscale: float  (multiplier to apply to odds — <1 shortens, >1 lengthens)
    """
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        html = f.read()

    p = TextExtractor()
    p.feed(html)
    text = '\n'.join(p.text)
    lines = text.split('\n')

    # Driver name
    name_match = re.search(r'^([A-Z][a-z]+ [A-Z][a-z]+)', text)
    name = name_match.group(1) if name_match else os.path.basename(filepath)

    # Season stats — "26/26\n122\n22 (18%)\n32 (26%)"
    season_starts = 0
    season_win_pct = 0.0
    season_place_pct = 0.0
    season_match = re.search(
        r'\d{2}/\d{2}\n(\d+)\n(\d+)\s*\((\d+)%\)\n(\d+)\s*\((\d+)%\)',
        text
    )
    if season_match:
        season_starts    = int(season_match.group(1))
        season_win_pct   = float(season_match.group(3))
        season_place_pct = float(season_match.group(5))

    # Latest Drives section
    drives = []
    ld_idx = next((i for i, l in enumerate(lines) if 'Latest Drives' in l), -1)
    if ld_idx != -1:
        # Skip header row: Date / Track / Race / Horse / Placing / Margin / Odds
        i = ld_idx + 1
        while i < len(lines) and lines[i].strip() in ('Date', 'Track', 'Race',
                                                        'Horse', 'Placing',
                                                        'Margin', 'Odds'):
            i += 1

        # Each drive is 7 lines: date, track, race#, horse, placing, margin, odds
        while i + 6 < len(lines):
            date_str  = lines[i].strip()
            track     = lines[i+1].strip()
            # race#   = lines[i+2]
            horse     = lines[i+3].strip()
            placing   = lines[i+4].strip()
            # margin  = lines[i+5]
            # odds    = lines[i+6]

            # Validate date format DD-MM-YYYY
            if not re.match(r'^\d{2}-\d{2}-\d{4}$', date_str):
                break

            try:
                d, m, y = date_str.split('-')
                drive_date = date(int(y), int(m), int(d))
                pos = int(placing) if placing.isdigit() else None
                drives.append({
                    'date':    drive_date,
                    'track':   track,
                    'horse':   horse,
                    'pos':     pos,
                    'win':     pos == 1,
                    'place':   pos in (1, 2, 3) if pos else False,
                })
            except Exception:
                pass

            i += 7

    # Calculate 7-day and prior 7-day windows relative to race date
    if race_date is None:
        cutoff = date.today()
    elif isinstance(race_date, str):
        try:
            parts = race_date.split()
            cutoff = date(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
        except Exception:
            cutoff = date.today()
    else:
        cutoff = race_date

    l7_start = cutoff - timedelta(days=7)
    p7_start = cutoff - timedelta(days=14)

    l7_drives   = [d for d in drives if d['date'] >= l7_start]
    p7_drives   = [d for d in drives if p7_start <= d['date'] < l7_start]

    l7_starts = len(l7_drives)
    l7_wins   = sum(1 for d in l7_drives if d['win'])
    p7_starts = len(p7_drives)
    p7_wins   = sum(1 for d in p7_drives if d['win'])

    l7_win_pct = round(100 * l7_wins / l7_starts, 1) if l7_starts else 0.0
    p7_win_pct = round(100 * p7_wins / p7_starts, 1) if p7_starts else 0.0

    # Momentum signal
    # Need at least 3 drives in each window for a reliable signal
    if l7_starts < 3 or p7_starts < 3:
        momentum = 'INSUFFICIENT'
        upscale  = 1.0
    elif l7_win_pct > p7_win_pct + 10:
        momentum = 'HOT'
        # Upscale = shorten odds proportionally to momentum strength
        # e.g. 50% vs 20% = 30pt gap → multiply fair prob by 1.20 (odds ÷ 1.20)
        gap      = l7_win_pct - p7_win_pct
        upscale  = round(1.0 + min(gap / 100, 0.30), 2)  # cap at 30% boost
    elif p7_win_pct > l7_win_pct + 10:
        momentum = 'COLD'
        # Lengthen odds — driver going cold
        gap      = p7_win_pct - l7_win_pct
        upscale  = round(1.0 / (1.0 + min(gap / 100, 0.20)), 2)  # cap at 20% drag
    else:
        momentum = 'NEUTRAL'
        upscale  = 1.0

    return {
        'name':             name,
        'season_starts':    season_starts,
        'season_win_pct':   season_win_pct,
        'season_place_pct': season_place_pct,
        'drives':           drives,
        'l7_starts':        l7_starts,
        'l7_wins':          l7_wins,
        'l7_win_pct':       l7_win_pct,
        'p7_starts':        p7_starts,
        'p7_wins':          p7_wins,
        'p7_win_pct':       p7_win_pct,
        'momentum':         momentum,
        'upscale':          upscale,
    }


def load_driver_profiles(driver_folder, race_date=None):
    """
    Load all driver HTML files from a folder.
    Returns dict keyed by slug: {slug: driver_stats_dict}
    """
    if not driver_folder or not os.path.isdir(driver_folder):
        return {}

    profiles = {}
    for fname in os.listdir(driver_folder):
        if not fname.endswith('.html') or not fname.startswith('driver_'):
            continue
        slug = fname.replace('driver_', '').replace('.html', '')
        fpath = os.path.join(driver_folder, fname)
        try:
            profiles[slug] = parse_driver_profile(fpath, race_date=race_date)
        except Exception as e:
            print(f'  Warning: could not parse {fname}: {e}')

    return profiles

    return profiles


def match_driver_to_profile(driver_name, profiles):
    """
    Match a driver name (e.g. 'Jack Watson' or 'J Watson') to a loaded profile.
    Returns the profile dict or None.
    """
    if not driver_name or not profiles:
        return None

    parts = driver_name.strip().split()
    if len(parts) < 2:
        return None

    # Try constructing slug from full name
    first = parts[0]
    last  = parts[-1]

    if len(first) > 1:
        slug = re.sub(r'[^a-z0-9\-]', '', f'{first}-{last}'.lower())
        if slug in profiles:
            return profiles[slug]

    # Try matching by last name only across all profiles
    last_lower = last.lower()
    matches = [v for k, v in profiles.items() if last_lower in k]
    if len(matches) == 1:
        return matches[0]

    return None

def analyse(horse_files, race_file=None, race_number=None,
            race_date=None, driver_folder=None):
    """
    Run full analysis on a list of horse profile HTML files.
    race_date:     string like '29 Mar 2026' — used for fitness check.
    driver_folder: path to folder containing driver_*.html profile pages.
    """
    results = []

    # Load driver profiles if provided
    driver_profiles = load_driver_profiles(driver_folder, race_date=race_date) if driver_folder else {}
    if driver_profiles:
        print(f'  Loaded {len(driver_profiles)} driver profiles.')

    for fpath in horse_files:
        print(f"  Parsing {os.path.basename(fpath)}...", flush=True)
        horse = parse_horse_profile(fpath)
        score, valid_runs, nulls = calculate_form_score(horse)
        trend, avg_price, trend_label, flags = pricing_signal(horse)
        days, last_run_str, fitness = days_since_last_run(horse, race_date)

        # Get current driver from most recent real race
        driver_name = None
        for run in horse.get('runs', []):
            if run.get('race_type') != 'TRIAL' and run.get('driver'):
                d = run['driver'].strip()
                if d and not re.match(r'^[\d:\.\$]', d) and len(d) > 3:
                    driver_name = d
                    break

        # Match to driver profile
        driver_stats = match_driver_to_profile(driver_name, driver_profiles)

        horse['form_score']       = score
        horse['valid_runs']       = valid_runs
        horse['nulls']            = nulls
        horse['price_trend']      = trend_label
        horse['avg_recent_price'] = avg_price
        horse['price_flags']      = flags
        horse['days_since_run']   = days
        horse['last_run_date']    = last_run_str
        horse['fitness']          = fitness
        horse['driver_name']      = driver_name
        horse['driver_stats']     = driver_stats
        results.append(horse)

    # Build trainer stats from all horse profiles combined
    trainer_stats = build_trainer_stats(results, race_date)

    # Attach trainer stats and sort
    for horse in results:
        trainer_name = None
        for run in horse.get('runs', []):
            if run.get('race_type') != 'TRIAL' and run.get('trainer'):
                trainer_name = run['trainer'].strip()
                break
        horse['trainer_name']  = trainer_name
        horse['trainer_stats'] = trainer_stats.get(trainer_name, {})

    results.sort(key=lambda h: h['form_score'] if h['form_score'] is not None else 999)
    return results


def print_results(results):
    print()
    print('=' * 70)
    print('  MARGIN-ADJUSTED FORM SCORES — RANKED')
    print('  (Lower average adjusted margin = better performer)')
    print('  Fitness rule: last race > 14 days ago → odds adjusted +25%')
    print('=' * 70)

    for i, h in enumerate(results, 1):
        score = h['form_score']
        score_str = f"{score:.1f}m avg" if score is not None else "N/A"
        margins = [r['adj_margin'] for r in h.get('valid_runs', [])]
        margins_str = ' / '.join(f"{m:.1f}" for m in margins)

        # Fitness
        days = h.get('days_since_run')
        fitness = h.get('fitness', 'UNKNOWN')
        last_run = h.get('last_run_date', 'unknown')
        if fitness == 'FRESH':
            fitness_str = f"⚑ FRESH — last race {days}d ago ({last_run}) → +25% odds penalty"
        elif fitness == 'FIT':
            fitness_str = f"FIT — last race {days}d ago ({last_run})"
        else:
            fitness_str = "UNKNOWN — no race date found"

        print(f"\n  #{i}  {h['name']}  (NR{h['nr']})")
        print(f"       Adj margin avg:  {score_str}")
        print(f"       Runs used:       {margins_str}")
        if h.get('nulls'):
            print(f"       NULLed runs:     {h['nulls']}")
        print(f"       This season:     {h['this_season'][1]}W from {h['this_season'][0]} starts")
        print(f"       Career BMR:      {h['career_bmr']}  |  This season BMR: {h['this_bmr']}", end='')
        if h.get('bmr_gap'):
            gap = h['bmr_gap']
            flag = ' ⚑ LARGE GAP' if gap > 4 else ''
            print(f"  (gap: +{gap}s/mile{flag})", end='')
        print()
        print(f"       Fitness:         {fitness_str}")

        # Driver stats
        dn = h.get('driver_name', 'Unknown')
        ds = h.get('driver_stats')
        if ds:
            l7_str  = f"{ds['l7_wins']}/{ds['l7_starts']} ({ds['l7_win_pct']}%)"
            p7_str  = f"{ds['p7_wins']}/{ds['p7_starts']} ({ds['p7_win_pct']}%)"
            mom     = ds['momentum']
            upscale = ds['upscale']
            if mom == 'HOT':
                mom_flag = f'  ★ HOT — upscale ×{upscale} (shorten odds)'
            elif mom == 'COLD':
                mom_flag = f'  ✗ COLD — upscale ×{upscale} (lengthen odds)'
            else:
                mom_flag = ''
            print(f"       Driver:          {dn}  (season {ds['season_win_pct']}% win)")
            print(f"         Last 7 days:   {l7_str}  |  Prior 7 days: {p7_str}{mom_flag}")
        else:
            print(f"       Driver:          {dn}  (no profile loaded)")
        ts = h.get('trainer_stats', {})
        if ts:
            l30  = f"{ts['l30_wins']}/{ts['l30_starts']} ({ts['l30_win_pct']}% win, {ts['l30_place_pct']}% place)"
            prep = f"{ts['prep_wins']}/{ts['prep_starts']} ({ts['prep_win_pct']}% win)"
            # Flag if trainer is hot (l30 win% > 20) or cold (l30 win% == 0 with 5+ starts)
            if ts['l30_starts'] >= 5 and ts['l30_win_pct'] >= 20:
                flag = '  ★ HOT STABLE'
            elif ts['l30_starts'] >= 5 and ts['l30_win_pct'] == 0:
                flag = '  ✗ COLD STABLE'
            else:
                flag = ''
            print(f"       Trainer:         {tn}")
            print(f"         Last 30 days:  {l30}{flag}")
            print(f"         This prep:     {prep}")
        else:
            print(f"       Trainer:         {tn}  (no stats available)")

        print(f"       Recent prices:   {h['recent_prices'][:6]}")
        print(f"       Price trend:     {h['price_trend']}  (avg last 4: ${h['avg_recent_price']})")
        for flag in h.get('price_flags', []):
            print(f"       ⚑ FLAG:          {flag}")

        # Show detail for each run used
        print(f"       Run detail:")
        for r in h.get('valid_runs', []):
            codes = r['comment_codes'] or '—'
            reasons_str = ', '.join(r['reasons']) if r['reasons'] else 'no adjustment'
            print(f"         {r['date']} {r['track']} Pos:{r['pos']} "
                  f"Raw:{r['margin']:.1f}m → Adj:{r['adj_margin']:.1f}m "
                  f"| {codes} → {reasons_str}")


def get_horse_files():
    """Interactively find horse profile files."""
    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')

    # Look for horse folders on desktop
    folders = [f for f in os.listdir(desktop)
               if f.startswith('horses_') and os.path.isdir(os.path.join(desktop, f))]
    folders.sort(reverse=True)

    html_files = []

    if folders:
        print('\n  Recent horse profile folders found on Desktop:')
        for i, f in enumerate(folders[:5], 1):
            n = len([x for x in os.listdir(os.path.join(desktop, f)) if x.endswith('.html')])
            print(f'    {i}. {f}  ({n} horses)')
        print()
        user_input = input('  Enter folder number, or paste path to folder/file: ').strip().strip('"')

        if user_input.isdigit() and 1 <= int(user_input) <= len(folders):
            folder = os.path.join(desktop, folders[int(user_input) - 1])
            html_files = [os.path.join(folder, f)
                          for f in os.listdir(folder) if f.endswith('.html')]
        elif os.path.isdir(user_input):
            html_files = [os.path.join(user_input, f)
                          for f in os.listdir(user_input) if f.endswith('.html')]
        elif os.path.isfile(user_input):
            html_files = [user_input]
    else:
        print('  No horse profile folders found on Desktop.')
        path = input('  Paste path to horse folder or file: ').strip().strip('"')
        if os.path.isdir(path):
            html_files = [os.path.join(path, f)
                          for f in os.listdir(path) if f.endswith('.html')]
        elif os.path.isfile(path):
            html_files = [path]

    return [f for f in html_files if os.path.isfile(f)]


def main():
    print('=' * 70)
    print('  Harness Form Scoring Engine')
    print('  Margin-adjusted form analysis using Fixing-the-data methodology')
    print('=' * 70)

    horse_files = get_horse_files()

    if not horse_files:
        print('\n  No files found. Exiting.')
        input('\n  Press Enter to close...')
        sys.exit()

    print(f'\n  Found {len(horse_files)} horse profile files.')

    # Ask for driver profiles folder (optional)
    driver_folders = sorted([
        f for f in os.listdir(desktop)
        if f.startswith('drivers_') and os.path.isdir(os.path.join(desktop, f))
    ], reverse=True)

    driver_folder = None
    if driver_folders:
        print(f'\n  Driver profile folders found:')
        for i, f in enumerate(driver_folders[:3], 1):
            n = len([x for x in os.listdir(os.path.join(desktop, f))
                     if x.endswith('.html')])
            print(f'    {i}. {f}  ({n} drivers)')
        print('    0. Skip driver profiles')
        drv_input = input('\n  Select driver folder (or 0 to skip): ').strip()
        if drv_input.isdigit():
            idx = int(drv_input)
            if 1 <= idx <= len(driver_folders):
                driver_folder = os.path.join(desktop, driver_folders[idx - 1])
    else:
        print('\n  No driver folders found on Desktop.')
        print('  Run fetch_drivers.py first to get driver profiles.')
        print('  (Continuing without driver data)')

    # Ask for race date
    print()
    print('  Enter the race date for fitness calculation')
    print('  (format: 29 Mar 2026) or press Enter for today:')
    date_input = input('  Race date: ').strip()
    race_date = date_input if date_input else None

    print('  Parsing and scoring...\n')

    results = analyse(horse_files, race_date=race_date, driver_folder=driver_folder)
    print_results(results)

    # Save to text file on Desktop
    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = os.path.join(desktop, f'form_scores_{ts}.txt')

    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        print_results(results)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(buf.getvalue())

    print(f'\n  Scores saved to: {out_path}')
    print('  Upload that file (or paste its contents) into Claude for odds generation.')
    print()
    input('  Press Enter to close...')


if __name__ == '__main__':
    main()
