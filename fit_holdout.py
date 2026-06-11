"""Conditional-logit weight fit with a temporal holdout.

Train: NSW races before 7 May 2026 (the last committed weights change).
Test:  NSW races from 7 May 2026 onward (the frozen window) - evaluated once.
Metric: mean log-loss of the winner's probability, against the SP market baseline.

Answers two questions:
  1. Do fitted component weights beat the hand-tuned (frozen) weights out-of-sample?
  2. Does the model add any information beyond the SP? (winner ~ ln(SP prob) + model score)

Writes the fitted candidate to weights-fitted-candidate.json (NOT auto-loaded by
the pipeline - rename to weights.json / weights-nsw.json only after review).
"""
import copy
import json
import math
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "src")
from harness_model.odds import load_feature_rows, load_weights, score_race_rows
from harness_model.web import _meeting_state, _normalise_name

CUTOFF = datetime(2026, 5, 7)
DB = "data/harness.db"
CSV = "data/features/runner_features.csv"
FROZEN_REF = "f3da878"  # last committed weights.json (6 May 2026)
CANDIDATE_OUT = "weights-fitted-candidate.json"

# Manual judgment inputs and placeholders - no historical variance, keep their
# current weights rather than letting the fit zero them.
EXCLUDED_COMPONENTS = {"driver_form", "trainer_form", "stable_change"}

TEMPS = [1.0, 1.2, 1.6, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
L2_GRID = [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0]


def load_frozen_weights() -> dict:
    import subprocess
    raw = subprocess.run(
        ["git", "show", f"{FROZEN_REF}:weights.json"],
        capture_output=True, text=True, check=True,
    ).stdout
    tmp = Path("frozen_weights_tmp.json")
    tmp.write_text(raw, encoding="utf-8")
    try:
        return load_weights(tmp)
    finally:
        tmp.unlink()


def unit_weights(base: dict) -> dict:
    """All S1/S2 weights -> 1.0 so each component column is its raw scaled value.

    pace_backmarker_threshold is a threshold, not a weight - setting it to 1.0
    would zero the pace_backmarker column entirely, so it keeps its real value.
    Fitness tiers are kept as-is (the fit learns a single scale for the column).
    """
    w = copy.deepcopy(base)
    for k in w["stage1"]:
        w["stage1"][k] = 1.0
    for k in w["stage2"]:
        if k != "pace_backmarker_threshold":
            w["stage2"][k] = 1.0
    return w


def load_results():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    ran = {}   # (mc, rn, norm_name) -> sp (float) for horses that actually ran
    winners = {}
    for r in conn.execute(
        "SELECT meeting_code, race_number, horse_name, finish_position, starting_price "
        "FROM race_results WHERE finish_position IS NOT NULL AND starting_price IS NOT NULL"
    ):
        key = (r["meeting_code"], int(r["race_number"]), _normalise_name(str(r["horse_name"])))
        sp = float(r["starting_price"])
        if sp > 1.0:
            ran[key] = sp
        if int(r["finish_position"]) == 1:
            winners[(r["meeting_code"], int(r["race_number"]))] = _normalise_name(str(r["horse_name"]))
    dates = {
        r["meeting_code"]: r["meeting_date"]
        for r in conn.execute("SELECT meeting_code, meeting_date FROM meetings")
    }
    conn.close()
    return ran, winners, dates


def meeting_date(dates: dict, mc: str) -> datetime | None:
    try:
        return datetime.strptime(dates.get(mc, ""), "%d %b %Y")
    except ValueError:
        return None


def build_dataset():
    """One entry per usable race: unit components, frozen relative scores,
    normalised SP probs - all restricted to the same choice set (runners that
    actually ran with a valid SP)."""
    rows = load_feature_rows(CSV)
    ran, winners, dates = load_results()
    frozen = load_frozen_weights()
    unit_w = unit_weights(frozen)

    race_keys = sorted({
        (r["meeting_code"], int(r["race_number"]))
        for r in rows if r.get("meeting_code") and r.get("race_number")
    })

    races = []
    for mc, rn in race_keys:
        if _meeting_state(mc) != "NSW":
            continue
        wname = winners.get((mc, rn))
        d = meeting_date(dates, mc)
        if not wname or d is None:
            continue
        scored_unit = score_race_rows(rows, mc, rn, weights=unit_w)
        if not scored_unit:
            continue
        scored_frozen = score_race_rows(rows, mc, rn, weights=frozen)
        frozen_rel = {
            _normalise_name(str(h["horse_name"])): h["relative_score"] for h in scored_frozen
        }

        names, comps, sps, rel_f = [], [], [], []
        for h in scored_unit:
            nm = _normalise_name(str(h["horse_name"]))
            sp = ran.get((mc, rn, nm))
            if sp is None or nm not in frozen_rel:
                continue
            names.append(nm)
            comps.append(h["components"])
            sps.append(1.0 / sp)
            rel_f.append(frozen_rel[nm])
        if len(names) < 4 or wname not in names:
            continue
        total = sum(sps)
        races.append({
            "key": (mc, rn),
            "bucket": "train" if d < CUTOFF else "test",
            "winner_idx": names.index(wname),
            "names": names,
            "comps": comps,
            "sp_norm": [p / total for p in sps],
            "sp_raw": [1.0 / p for p in sps],
            "rel_frozen": rel_f,
        })
    return races, frozen


def softmax(values: list[float], temp: float = 1.0) -> list[float]:
    m = max(values)
    ex = [math.exp((v - m) / max(temp, 1e-6)) for v in values]
    s = sum(ex)
    return [e / s for e in ex]


def mean_log_loss(races: list[dict], scores_key_fn, temp: float = 1.0) -> float:
    total = 0.0
    for race in races:
        probs = softmax(scores_key_fn(race), temp)
        total += -math.log(max(probs[race["winner_idx"]], 1e-12))
    return total / len(races)


def best_temp(races: list[dict], scores_key_fn) -> tuple[float, float]:
    results = [(mean_log_loss(races, scores_key_fn, t), t) for t in TEMPS]
    ll, t = min(results)
    return t, ll


def fit_conditional_logit(feature_rows_per_race: list[tuple], l2: float) -> list[float]:
    """True conditional logit: P(win) = softmax(X.beta) within each race.

    feature_rows_per_race: list of (X: list[list[float]], winner_idx).
    Features are standardised internally for a fair L2 penalty; returned
    coefficients are on the original feature scale. Analytic gradient + L-BFGS.
    """
    import numpy as np
    from scipy.optimize import minimize

    mats = [(np.asarray(X, dtype=float), wi) for X, wi in feature_rows_per_race]
    pooled = np.vstack([X for X, _ in mats])
    sd = pooled.std(axis=0)
    sd[sd < 1e-9] = 1.0
    mats = [(X / sd, wi) for X, wi in mats]
    n = len(mats)

    def objective(beta):
        nll = 0.0
        grad = np.zeros_like(beta)
        for X, wi in mats:
            s = X @ beta
            s -= s.max()
            e = np.exp(s)
            p = e / e.sum()
            nll += -math.log(max(p[wi], 1e-12))
            grad += X.T @ p - X[wi]
        return nll / n + l2 * float(beta @ beta), grad / n + 2.0 * l2 * beta

    res = minimize(objective, np.zeros(pooled.shape[1]), jac=True,
                   method="L-BFGS-B", options={"maxiter": 1000})
    return list(res.x / sd)  # back to original feature scale


def component_matrix(race: dict, cols: list[str]) -> list[list[float]]:
    return [[c.get(k, 0.0) or 0.0 for k in cols] for c in race["comps"]]


def fit_components(races: list[dict], cols: list[str], l2: float) -> dict:
    beta = fit_conditional_logit(
        [(component_matrix(r, cols), r["winner_idx"]) for r in races], l2,
    )
    return dict(zip(cols, beta))


def linear_scores(race: dict, coefs: dict) -> list[float]:
    return [
        sum(coefs[k] * (c.get(k, 0.0) or 0.0) for k in coefs)
        for c in race["comps"]
    ]


def fit_market_blend(races: list[dict], coefs: dict | None) -> list[float]:
    """winner ~ b1*ln(SP prob) [+ b2*model linear score], true conditional logit."""
    data = []
    for race in races:
        feats = [[math.log(p)] for p in race["sp_norm"]]
        if coefs is not None:
            ms = linear_scores(race, coefs)
            feats = [f + [m] for f, m in zip(feats, ms)]
        data.append((feats, race["winner_idx"]))
    return fit_conditional_logit(data, l2=1e-6)


def blend_scores(race: dict, betas: list[float], coefs: dict | None) -> list[float]:
    ms = linear_scores(race, coefs) if coefs is not None else None
    return [
        betas[0] * math.log(p) + (betas[1] * ms[i] if ms is not None else 0.0)
        for i, p in enumerate(race["sp_norm"])
    ]


def coefs_to_weights(coefs: dict, base: dict, temperature: float) -> dict:
    """Map fitted coefficients back onto the weights.json structure.

    Every component column under unit weights equals (scaled feature) x 1.0,
    so the coefficient IS the weight - except: market (split across the
    market_min/market_max taper, flattened here), debut_adj (unit run used the
    hardcoded default of 2.0), and fitness (a single scale over the tiers).
    Excluded manual components keep their base weights.
    """
    w = copy.deepcopy(base)
    for k in list(w["stage1"]):
        if k in ("market_min", "market_max", "debut_adj"):
            continue
        if k in coefs:
            w["stage1"][k] = round(coefs[k], 4)
    w["stage1"]["market_min"] = w["stage1"]["market_max"] = round(coefs.get("market", 0.0), 4)
    w["stage1"]["debut_adj"] = round(2.0 * coefs.get("debut_adj", 0.0), 4)
    for k in list(w["stage2"]):
        if k == "pace_backmarker_threshold":
            continue
        if k in coefs:
            w["stage2"][k] = round(coefs[k], 4)
    if "barrier" in coefs:
        w["stage2"]["barrier"] = round(coefs["barrier"], 4)
    fscale = coefs.get("fitness", 1.0)
    w["fitness"] = {k: round(v * fscale, 4) for k, v in w["fitness"].items()}
    w["softmax"]["temperature"] = temperature
    return w


def main():
    print("Building dataset (scoring every NSW race with unit + frozen weights)...")
    races, frozen = build_dataset()
    train = [r for r in races if r["bucket"] == "train"]
    test = [r for r in races if r["bucket"] == "test"]
    print(f"  train: {len(train)} races (< 7 May)   test: {len(test)} races (frozen window)")

    # Component columns: everything with variance on train, minus manual inputs
    all_cols = sorted({k for r in train for c in r["comps"] for k in c})
    cols = []
    for k in all_cols:
        if k in EXCLUDED_COMPONENTS:
            continue
        vals = [c.get(k, 0.0) or 0.0 for r in train for c in r["comps"]]
        mean = sum(vals) / len(vals)
        if sum((v - mean) ** 2 for v in vals) / len(vals) > 1e-12:
            cols.append(k)
    print(f"  components fitted: {len(cols)} (excluded: {sorted(EXCLUDED_COMPONENTS)} + zero-variance)")

    # --- Baselines (same races, same choice sets) ---
    mkt_train = mean_log_loss(train, lambda r: [math.log(p) for p in r["sp_norm"]])
    mkt_test = mean_log_loss(test, lambda r: [math.log(p) for p in r["sp_norm"]])
    fz_t, fz_train_ll = best_temp(train, lambda r: r["rel_frozen"])
    fz_test_ll = mean_log_loss(test, lambda r: r["rel_frozen"], fz_t)
    print(f"\nMarket baseline:        train LL {mkt_train:.4f}   test LL {mkt_test:.4f}")
    print(f"Frozen weights (T={fz_t}): train LL {fz_train_ll:.4f}   test LL {fz_test_ll:.4f}")

    # --- L2 selection on an inner temporal split of train ---
    # Conditional-logit MLE makes a softmax temperature redundant (any
    # rescaling of the scores is absorbed into the coefficients), so the
    # fitted model is always evaluated at T=1.
    split = int(len(train) * 0.75)
    inner_train, inner_val = train[:split], train[split:]
    print(f"\nL2 selection (inner split {len(inner_train)}/{len(inner_val)}):")
    best = None
    for l2 in L2_GRID:
        coefs = fit_components(inner_train, cols, l2)
        val_ll = mean_log_loss(inner_val, lambda r: linear_scores(r, coefs))
        print(f"  l2={l2:<7} inner-val LL {val_ll:.4f}")
        if best is None or val_ll < best[0]:
            best = (val_ll, l2)
    l2_star = best[1]

    # --- Final fit on full train, single test evaluation ---
    coefs = fit_components(train, cols, l2_star)
    t_star = 1.0
    fit_train_ll = mean_log_loss(train, lambda r: linear_scores(r, coefs))
    fit_test_ll = mean_log_loss(test, lambda r: linear_scores(r, coefs))
    print(f"\nFitted model (l2={l2_star}): train LL {fit_train_ll:.4f}   TEST LL {fit_test_ll:.4f}")

    top_hits = sum(
        1 for r in test
        if max(range(len(r["comps"])), key=lambda i: linear_scores(r, coefs)[i]) == r["winner_idx"]
    )
    fz_hits = sum(
        1 for r in test
        if max(range(len(r["rel_frozen"])), key=lambda i: r["rel_frozen"][i]) == r["winner_idx"]
    )
    mkt_hits = sum(
        1 for r in test
        if max(range(len(r["sp_norm"])), key=lambda i: r["sp_norm"][i]) == r["winner_idx"]
    )
    print(f"Test top-pick hit rate: fitted {top_hits/len(test)*100:.1f}%  "
          f"frozen {fz_hits/len(test)*100:.1f}%  market {mkt_hits/len(test)*100:.1f}%")

    # --- Does the model add anything beyond SP? ---
    betas_mkt = fit_market_blend(train, None)
    mkt_only_test = mean_log_loss(test, lambda r: blend_scores(r, betas_mkt, None))
    betas_blend = fit_market_blend(train, coefs)
    blend_test = mean_log_loss(test, lambda r: blend_scores(r, betas_blend, coefs))
    print(f"\nBeyond-SP regression (true conditional logit, fit on train, evaluated on test):")
    print(f"  ln(SP) only:            b1={betas_mkt[0]:+.3f}              test LL {mkt_only_test:.4f}")
    print(f"  ln(SP) + fitted score:  b1={betas_blend[0]:+.3f}, b2={betas_blend[1]:+.4f}  test LL {blend_test:.4f}")
    print(f"  raw market (b1=1):                              test LL {mkt_test:.4f}")
    print(f"  -> model adds {'INFORMATION' if blend_test < min(mkt_only_test, mkt_test) - 0.0005 else 'NOTHING'} beyond the SP "
          f"(delta vs best market: {min(mkt_only_test, mkt_test) - blend_test:+.4f})")

    # --- Coefficients ---
    print("\nFitted coefficients (sorted by |coef|):")
    for k, v in sorted(coefs.items(), key=lambda kv: -abs(kv[1])):
        print(f"  {k:<22} {v:+.4f}")

    # --- Candidate weights file + end-to-end mapping verification ---
    # Scoring the test races through score_race_rows with the candidate file
    # must reproduce the direct X.beta log-loss (manual components are zero in
    # historical data; 4dp rounding gives a tiny tolerance).
    candidate = coefs_to_weights(coefs, frozen, t_star)
    Path(CANDIDATE_OUT).write_text(json.dumps(candidate, indent=2) + "\n", encoding="utf-8")

    rows = load_feature_rows(CSV)
    total_ll, n_checked = 0.0, 0
    for race in test:
        mc, rn = race["key"]
        scored = score_race_rows(rows, mc, rn, weights=candidate)
        rel = {_normalise_name(str(h["horse_name"])): h["relative_score"] for h in scored}
        if any(nm not in rel for nm in race["names"]):
            continue
        vals = [rel[nm] for nm in race["names"]]
        probs = softmax(vals, t_star)
        total_ll += -math.log(max(probs[race["winner_idx"]], 1e-12))
        n_checked += 1
    pipeline_ll = total_ll / n_checked if n_checked else float("nan")
    print(f"\nCandidate weights written -> {CANDIDATE_OUT} (temperature {t_star})")
    print(f"Mapping check: pipeline-scored test LL {pipeline_ll:.4f} vs direct {fit_test_ll:.4f} "
          f"({n_checked} races) -> {'OK' if abs(pipeline_ll - fit_test_ll) < 0.01 else 'MISMATCH - investigate'}")
    print("NOT auto-loaded: rename to weights-nsw.json / weights.json only after review.")

    # --- Blend betting simulation (exploratory) ---
    # At bet time the offered price IS visible, so a rule on the blended
    # probability (b1*ln SP + b2*model score, fitted on train) is deployable.
    # Flat stake, SP <= $60. The threshold curve is shown in full rather than
    # picking a winner - choosing the best threshold off this table would be
    # in-sample selection on the test window again.
    print("\nBlend betting simulation on test window (flat stake, SP <= $60):")
    print("  threshold = blended_prob x SP must exceed it (1.0 = breakeven before margin)")
    print("  thresh   bets  wins  win%   flat ROI")
    for thresh in (1.05, 1.10, 1.20, 1.30, 1.50):
        bets = wins_n = 0
        ret = 0.0
        for race in test:
            probs = softmax(blend_scores(race, betas_blend, coefs), 1.0)
            for i, p in enumerate(probs):
                sp = race["sp_raw"][i]
                if sp > 60.0 or p * sp <= thresh:
                    continue
                bets += 1
                if i == race["winner_idx"]:
                    wins_n += 1
                    ret += sp - 1.0
                else:
                    ret -= 1.0
        if bets:
            print(f"  {thresh:<7} {bets:>5} {wins_n:>5}  {wins_n/bets*100:>4.1f}%  {ret/bets*100:>+7.1f}%")
        else:
            print(f"  {thresh:<7}     0     -     -        -")


if __name__ == "__main__":
    main()
