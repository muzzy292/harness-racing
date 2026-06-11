"""Weight sweep script - finds optimal values for ceiling, dist_strike_rate, and map weights."""
import sys, json, copy, sqlite3
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, 'src')

from harness_model.web import _compute_bet_records

with open('weights.json') as f:
    BASE_WEIGHTS = json.load(f)

DB_PATH = 'data/harness.db'
CSV_PATH = 'data/features/runner_features.csv'

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row


def roi_at_edge(records, min_edge):
    bets = [r for r in records if r.get('edge_pct', 0) >= min_edge and r.get('stake', 0) > 0]
    if not bets:
        return None, 0, 0
    staked = sum(r['stake'] for r in bets)
    returned = sum(r['stake'] * r['market_odds'] for r in bets if r['won'])
    roi = (returned - staked) / staked
    wins = sum(1 for r in bets if r['won'])
    return round(roi * 100, 1), len(bets), wins


def run_sweep(label, param_key, values, key_path):
    """
    key_path: list like ['stage1', 'ceiling'] or ['stage2', 'dist_strike_rate']
    """
    print(f"\nSweep: {label}")
    print(f"{'Value':>8} | {'Bets>=35%':>9} | {'Wins>=35%':>9} | {'ROI>=35%':>9} | {'Bets>=50%':>9} | {'Wins>=50%':>9} | {'ROI>=50%':>9}")
    print("-" * 80)

    best_roi35 = None
    best_val = None
    results = []

    for val in values:
        w = copy.deepcopy(BASE_WEIGHTS)
        # Navigate to the right key and set it
        node = w
        for k in key_path[:-1]:
            node = node[k]
        node[key_path[-1]] = val

        records, _ = _compute_bet_records(conn, CSV_PATH, weights=w)
        roi35, bets35, wins35 = roi_at_edge(records, 35)
        roi50, bets50, wins50 = roi_at_edge(records, 50)
        results.append((val, bets35, wins35, roi35, bets50, wins50, roi50))

        if roi35 is not None and (best_roi35 is None or roi35 > best_roi35):
            best_roi35 = roi35
            best_val = val

    for val, bets35, wins35, roi35, bets50, wins50, roi50 in results:
        roi35_str = f"{roi35}%" if roi35 is not None else "N/A"
        roi50_str = f"{roi50}%" if roi50 is not None else "N/A"
        marker = " <--" if val == best_val else ""
        print(f"{val:>8} | {bets35:>9} | {wins35:>9} | {roi35_str:>9} | {bets50:>9} | {wins50:>9} | {roi50_str:>9}{marker}")


def run_map_sweep(multipliers):
    map_keys = ['map_lead', 'map_soft', 'map_soft_context', 'map_wide', 'map_death',
                'pace_backmarker', 'pace_backmarker_threshold']

    print(f"\nSweep: map weights (multiplier)")
    print(f"{'Mult':>8} | {'Bets>=35%':>9} | {'Wins>=35%':>9} | {'ROI>=35%':>9} | {'Bets>=50%':>9} | {'Wins>=50%':>9} | {'ROI>=50%':>9}")
    print("-" * 80)

    best_roi35 = None
    best_mult = None
    results = []

    for mult in multipliers:
        w = copy.deepcopy(BASE_WEIGHTS)
        for k in map_keys:
            if k in w['stage2']:
                w['stage2'][k] = round(BASE_WEIGHTS['stage2'][k] * mult, 4)

        records, _ = _compute_bet_records(conn, CSV_PATH, weights=w)
        roi35, bets35, wins35 = roi_at_edge(records, 35)
        roi50, bets50, wins50 = roi_at_edge(records, 50)
        results.append((mult, bets35, wins35, roi35, bets50, wins50, roi50))

        if roi35 is not None and (best_roi35 is None or roi35 > best_roi35):
            best_roi35 = roi35
            best_mult = mult

    for mult, bets35, wins35, roi35, bets50, wins50, roi50 in results:
        roi35_str = f"{roi35}%" if roi35 is not None else "N/A"
        roi50_str = f"{roi50}%" if roi50 is not None else "N/A"
        marker = " <--" if mult == best_mult else ""
        print(f"{mult:>8} | {bets35:>9} | {wins35:>9} | {roi35_str:>9} | {bets50:>9} | {wins50:>9} | {roi50_str:>9}{marker}")


# Baseline
print("=== BASELINE ===")
records, summary = _compute_bet_records(conn, CSV_PATH, weights=BASE_WEIGHTS)
roi35, bets35, wins35 = roi_at_edge(records, 35)
roi50, bets50, wins50 = roi_at_edge(records, 50)
print(f"Total bets in dataset: {len(records)}")
print(f"Bets>=35%: {bets35}, Wins: {wins35}, ROI: {roi35}%")
print(f"Bets>=50%: {bets50}, Wins: {wins50}, ROI: {roi50}%")

# Sweep 1: stage1.ceiling
run_sweep(
    "stage1.ceiling",
    "ceiling",
    [0.0, 0.5, 0.9, 1.2, 1.5, 2.0, 2.5, 3.0],
    ['stage1', 'ceiling']
)

# Sweep 2: stage2.dist_strike_rate
run_sweep(
    "stage2.dist_strike_rate",
    "dist_strike_rate",
    [0.0, 0.3, 0.6, 0.9, 1.2, 1.5],
    ['stage2', 'dist_strike_rate']
)

# Sweep 3: map weights
run_map_sweep([0.5, 0.75, 1.0, 1.5, 2.0, 3.0])

conn.close()
print("\nDone.")
