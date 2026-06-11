import sys, json, copy
sys.path.insert(0, 'src')
from harness_model.web import _compute_bet_records
from harness_model.storage import connect, init_db

with open('weights.json') as f:
    BASE_WEIGHTS = json.load(f)

DB_PATH = 'data/harness.db'
CSV_PATH = 'data/features/runner_features.csv'

def make_conn():
    conn = connect(DB_PATH)
    init_db(conn)
    return conn

def run_with_weights(w):
    conn = make_conn()
    try:
        records, _summary = _compute_bet_records(conn, CSV_PATH, w)
    finally:
        conn.close()
    return records

def roi_at_edge(records, min_edge_pct):
    bets = [r for r in records if r.get('edge_pct', 0) >= min_edge_pct and r.get('stake', 0) > 0]
    if not bets:
        return None, 0, 0
    staked = sum(r['stake'] for r in bets)
    returned = sum(r['stake'] * r['market_odds'] for r in bets if r.get('won'))
    roi = round((returned - staked) / staked * 100, 1)
    wins = sum(1 for r in bets if r.get('won'))
    return roi, len(bets), wins

def fmt_roi(r):
    if r is None:
        return '    N/A'
    return f'{r:>7.1f}%'

def sweep(label, section, key, values):
    current = BASE_WEIGHTS[section][key]
    print(f'\n=== {label} (current={current}) ===')
    print(f'{"Value":>8} | {"Bets35":>7} | {"W35":>5} | {"ROI35":>8} | {"Bets50":>7} | {"W50":>5} | {"ROI50":>8}')
    print('-'*70)
    best_roi35, best_val = None, None
    results = []
    for v in values:
        w = copy.deepcopy(BASE_WEIGHTS)
        w[section][key] = v
        recs = run_with_weights(w)
        r35, b35, w35_ = roi_at_edge(recs, 35)
        r50, b50, w50_ = roi_at_edge(recs, 50)
        results.append((v, r35, b35, w35_, r50, b50, w50_))
        if r35 is not None and (best_roi35 is None or r35 > best_roi35):
            best_roi35, best_val = r35, v
    for (v, r35, b35, w35_, r50, b50, w50_) in results:
        marker = ' <-- best' if v == best_val else ''
        print(f'{v:>8} | {b35:>7} | {w35_:>5} | {fmt_roi(r35)} | {b50:>7} | {w50_:>5} | {fmt_roi(r50)}{marker}')
    return best_val, best_roi35

# ---- Baseline ----
print('=== BASELINE (unmodified weights) ===')
base_recs = run_with_weights(BASE_WEIGHTS)
base_r35, base_b35, base_w35 = roi_at_edge(base_recs, 35)
base_r50, base_b50, base_w50 = roi_at_edge(base_recs, 50)
print(f'  Edge>=35%: {base_b35} bets, {base_w35} wins, ROI={base_r35}%')
print(f'  Edge>=50%: {base_b50} bets, {base_w50} wins, ROI={base_r50}%')

summary = []

# 1. consistency
bv, br = sweep('1. consistency', 'stage1', 'consistency', [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0])
summary.append(('consistency', 'stage1', BASE_WEIGHTS['stage1']['consistency'], bv, br))

# 2. win_rate
bv, br = sweep('2. win_rate', 'stage1', 'win_rate', [0.0, 0.5, 1.0, 1.5, 2.0, 2.5])
summary.append(('win_rate', 'stage1', BASE_WEIGHTS['stage1']['win_rate'], bv, br))

# 3. dist_strike_rate
bv, br = sweep('3. dist_strike_rate', 'stage2', 'dist_strike_rate', [0.0, 0.3, 0.6, 0.9, 1.2, 1.5])
summary.append(('dist_strike_rate', 'stage2', BASE_WEIGHTS['stage2']['dist_strike_rate'], bv, br))

# 4. temperature
bv, br = sweep('4. temperature', 'softmax', 'temperature', [0.8, 1.2, 1.6, 1.8, 2.2, 2.6, 3.0, 3.5])
summary.append(('temperature', 'softmax', BASE_WEIGHTS['softmax']['temperature'], bv, br))

# 5. ceiling
bv, br = sweep('5. ceiling', 'stage1', 'ceiling', [0.0, 0.5, 0.9, 1.2, 1.5, 2.0, 2.5, 3.0])
summary.append(('ceiling', 'stage1', BASE_WEIGHTS['stage1']['ceiling'], bv, br))

# ---- Summary ----
print('\n\n=== SUMMARY (vs baseline ROI35={base_r35}%) ==='.format(base_r35=base_r35))
print(f'{"Weight":<20} | {"Section":<8} | {"Current":>8} | {"Best val":>9} | {"Best ROI35":>10} | {"vs baseline":>11} | {"Direction"}')
print('-'*90)

def vs_base(roi):
    if roi is None or base_r35 is None:
        return None
    return round(roi - base_r35, 1)

for (label, section, current, best_val, best_roi35) in summary:
    delta = vs_base(best_roi35)
    delta_str = f'{delta:+.1f}%' if delta is not None else '    N/A'
    if best_val == current:
        direction = '(unchanged)'
    elif isinstance(best_val, (int, float)) and isinstance(current, (int, float)):
        direction = 'UP' if best_val > current else 'DOWN'
    else:
        direction = '?'
    roi_str = f'{best_roi35:.1f}%' if best_roi35 is not None else 'N/A'
    print(f'{label:<20} | {section:<8} | {str(current):>8} | {str(best_val):>9} | {roi_str:>10} | {delta_str:>11} | {direction}')
