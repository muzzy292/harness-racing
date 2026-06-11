import sys, json, copy, sqlite3
sys.path.insert(0, 'src')
from harness_model.web import _compute_bet_records, _meeting_state
from harness_model.features import _sort_run_date

with open('weights.json') as f:
    BASE_WEIGHTS = json.load(f)

CUTOFF = _sort_run_date('1 May 2026')

def get_records(weights):
    conn = sqlite3.connect('data/harness.db')
    conn.row_factory = sqlite3.Row
    recs, _ = _compute_bet_records(conn, 'data/features/runner_features.csv', weights=weights)
    conn.close()
    return [r for r in recs
            if _sort_run_date(r.get('meeting_date','')) >= CUTOFF
            and _meeting_state(r['meeting_code']) == 'NSW']

def roi_at_edge(records, threshold):
    bets = [r for r in records if (r.get('edge_pct') or 0) >= threshold and (r.get('stake') or 0) > 0]
    if not bets:
        return None, 0, 0
    staked = sum(r['stake'] for r in bets)
    returned = sum(r['stake'] * r['market_odds'] for r in bets if r['won'])
    wins = sum(1 for r in bets if r['won'])
    roi = round((returned - staked) / staked * 100, 1)
    return roi, len(bets), wins

print("=== BASELINE ===")
base_recs = get_records(BASE_WEIGHTS)
meetings = set(r['meeting_code'] for r in base_recs)
print(f"NSW meetings from 1 May: {len(meetings)}, total records: {len(base_recs)}")
roi35, b35, w35 = roi_at_edge(base_recs, 35)
roi50, b50, w50 = roi_at_edge(base_recs, 50)
roi100, b100, w100 = roi_at_edge(base_recs, 100)
print(f"ROI>=35%: {roi35}% ({b35} bets, {w35} wins)")
print(f"ROI>=50%: {roi50}% ({b50} bets, {w50} wins)")
print(f"ROI>=100%: {roi100}% ({b100} bets, {w100} wins)")
print()

BASE_ROI35 = roi35 or 0
BASE_ROI50 = roi50 or 0
BASE_ROI100 = roi100 or 0

def sweep(label, key_path, values, summary_rows):
    print(f"=== SWEEP: {label} ===")
    header = "{:>8} | {:>6} {:>6} {:>7} | {:>6} {:>6} {:>7} | {:>7} {:>7} {:>8}".format(
        "value","bets35","wins35","ROI35","bets50","wins50","ROI50","bets100","wins100","ROI100")
    print(header)
    print("-" * len(header))
    best_roi35 = None
    best_row = None
    rows = []
    for v in values:
        w = copy.deepcopy(BASE_WEIGHTS)
        obj = w
        for k in key_path[:-1]:
            obj = obj[k]
        obj[key_path[-1]] = v
        recs = get_records(w)
        r35, b35, wins35 = roi_at_edge(recs, 35)
        r50, b50, wins50 = roi_at_edge(recs, 50)
        r100, b100, wins100 = roi_at_edge(recs, 100)
        rows.append((v, b35, wins35, r35, b50, wins50, r50, b100, wins100, r100))
        if r35 is not None and (best_roi35 is None or r35 > best_roi35):
            best_roi35 = r35
            best_row = v
    for (v, b35, wins35, r35, b50, wins50, r50, b100, wins100, r100) in rows:
        marker = " <-- best" if v == best_row else ""
        r35_str = "{:>7.1f}%".format(r35) if r35 is not None else "    N/A"
        r50_str = "{:>7.1f}%".format(r50) if r50 is not None else "    N/A"
        r100_str = "{:>8.1f}%".format(r100) if r100 is not None else "     N/A"
        print("{:>8} | {:>6} {:>6} {} | {:>6} {:>6} {} | {:>7} {:>7} {}{}".format(
            v, b35, wins35, r35_str, b50, wins50, r50_str, b100, wins100, r100_str, marker))
    print()
    obj = BASE_WEIGHTS
    for k in key_path[:-1]:
        obj = obj[k]
    current_val = obj[key_path[-1]]
    if best_row is not None:
        for (v, b35, wins35, r35, b50, wins50, r50, b100, wins100, r100) in rows:
            if v == best_row:
                summary_rows.append({
                    'weight': label,
                    'current': current_val,
                    'best_val': best_row,
                    'roi35': r35,
                    'roi50': r50,
                    'roi100': r100,
                    'vs_base35': round((r35 or 0) - BASE_ROI35, 1),
                    'direction': 'up' if best_row > current_val else ('down' if best_row < current_val else '='),
                })
                break

def sweep_map_multiplier(summary_rows):
    MAP_KEYS = ['map_lead', 'map_soft', 'map_soft_context', 'map_wide', 'map_death', 'pace_backmarker']
    BASE_MAP = {k: BASE_WEIGHTS['stage2'][k] for k in MAP_KEYS}
    BASE_THRESH = BASE_WEIGHTS['stage2']['pace_backmarker_threshold']
    label = "map_multiplier"
    multipliers = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0]
    print(f"=== SWEEP: {label} ===")
    header = "{:>8} | {:>6} {:>6} {:>7} | {:>6} {:>6} {:>7} | {:>7} {:>7} {:>8}".format(
        "mult","bets35","wins35","ROI35","bets50","wins50","ROI50","bets100","wins100","ROI100")
    print(header)
    print("-" * len(header))
    best_roi35 = None
    best_mult = None
    rows = []
    for m in multipliers:
        w = copy.deepcopy(BASE_WEIGHTS)
        for k in MAP_KEYS:
            w['stage2'][k] = BASE_MAP[k] * m
        w['stage2']['pace_backmarker_threshold'] = BASE_THRESH * m
        recs = get_records(w)
        r35, b35, wins35 = roi_at_edge(recs, 35)
        r50, b50, wins50 = roi_at_edge(recs, 50)
        r100, b100, wins100 = roi_at_edge(recs, 100)
        rows.append((m, b35, wins35, r35, b50, wins50, r50, b100, wins100, r100))
        if r35 is not None and (best_roi35 is None or r35 > best_roi35):
            best_roi35 = r35
            best_mult = m
    for (m, b35, wins35, r35, b50, wins50, r50, b100, wins100, r100) in rows:
        marker = " <-- best" if m == best_mult else ""
        r35_str = "{:>7.1f}%".format(r35) if r35 is not None else "    N/A"
        r50_str = "{:>7.1f}%".format(r50) if r50 is not None else "    N/A"
        r100_str = "{:>8.1f}%".format(r100) if r100 is not None else "     N/A"
        print("{:>8} | {:>6} {:>6} {} | {:>6} {:>6} {} | {:>7} {:>7} {}{}".format(
            m, b35, wins35, r35_str, b50, wins50, r50_str, b100, wins100, r100_str, marker))
    print()
    if best_mult is not None:
        for (m, b35, wins35, r35, b50, wins50, r50, b100, wins100, r100) in rows:
            if m == best_mult:
                summary_rows.append({
                    'weight': label,
                    'current': 1.0,
                    'best_val': best_mult,
                    'roi35': r35,
                    'roi50': r50,
                    'roi100': r100,
                    'vs_base35': round((r35 or 0) - BASE_ROI35, 1),
                    'direction': 'up' if best_mult > 1.0 else ('down' if best_mult < 1.0 else '='),
                })
                break

summary_rows = []
sweep("stage1.ceiling",          ['stage1', 'ceiling'],          [0.0, 0.5, 0.9, 1.2, 1.5, 2.0, 2.5, 3.0], summary_rows)
sweep("stage1.consistency",      ['stage1', 'consistency'],       [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0], summary_rows)
sweep("stage1.late_speed",       ['stage1', 'late_speed'],        [0.0, 0.4, 0.8, 1.2, 1.6, 2.0], summary_rows)
sweep("stage1.win_rate",         ['stage1', 'win_rate'],          [0.0, 0.5, 1.0, 1.5, 2.0, 2.5], summary_rows)
sweep("stage1.top3_rate",        ['stage1', 'top3_rate'],         [0.0, 0.25, 0.5, 0.75, 1.0], summary_rows)
sweep("stage1.competitive_rate", ['stage1', 'competitive_rate'],  [0.0, 0.15, 0.3, 0.5, 0.75], summary_rows)
sweep("stage1.nr",               ['stage1', 'nr'],                [0.0, 0.35, 0.70, 1.05, 1.4, 1.75], summary_rows)
sweep("stage1.class_pos",        ['stage1', 'class_pos'],         [0.0, 0.15, 0.30, 0.5, 0.75], summary_rows)
sweep("stage1.tempo_adj",        ['stage1', 'tempo_adj'],         [0.0, 0.2, 0.35, 0.5, 0.75], summary_rows)
sweep("stage1.form_staleness",   ['stage1', 'form_staleness'],    [0.0, 0.5, 1.0, 1.5, 2.0, 2.5], summary_rows)
sweep("stage1.stake_class",      ['stage1', 'stake_class'],       [0.0, 0.1, 0.2, 0.35, 0.5], summary_rows)
sweep("stage1.class_delta",      ['stage1', 'class_delta'],       [0.0, 0.1, 0.25, 0.4, 0.6], summary_rows)
sweep("stage2.dist_strike_rate", ['stage2', 'dist_strike_rate'],  [0.0, 0.6, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0], summary_rows)
sweep("stage2.nr_grade_delta",   ['stage2', 'nr_grade_delta'],    [0.0, 0.5, 1.0, 1.4, 2.0, 2.5], summary_rows)
sweep("stage2.barrier_relief",   ['stage2', 'barrier_relief'],    [0.0, 0.2, 0.4, 0.6, 0.8], summary_rows)
sweep("softmax.temperature",     ['softmax', 'temperature'],      [0.5, 0.8, 1.0, 1.2, 1.6, 1.8, 2.5, 3.0, 3.5, 5.0], summary_rows)
sweep_map_multiplier(summary_rows)

print("=== FINAL SUMMARY (sorted by vs_baseline_35) ===")
summary_rows.sort(key=lambda x: x['vs_base35'], reverse=True)
hdr = "{:<28} | {:>8} | {:>8} | {:>7} | {:>7} | {:>8} | {:>10} | {:>10}".format(
    "Weight","Current","BestVal","ROI35","ROI50","ROI100","vs_base35","Direction")
print(hdr)
print("-" * len(hdr))
for row in summary_rows:
    r35 = "{:>7.1f}%".format(row['roi35']) if row['roi35'] is not None else "    N/A"
    r50 = "{:>7.1f}%".format(row['roi50']) if row['roi50'] is not None else "    N/A"
    r100 = "{:>8.1f}%".format(row['roi100']) if row['roi100'] is not None else "     N/A"
    vs = "{:>+10.1f}%".format(row['vs_base35'])
    print("{:<28} | {:>8} | {:>8} | {} | {} | {} | {} | {:>10}".format(
        row['weight'], row['current'], row['best_val'], r35, r50, r100, vs, row['direction']))
