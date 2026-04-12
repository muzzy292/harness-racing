from __future__ import annotations

import json
from pathlib import Path

# Minimum samples required in a grade band before its par is used.
# Below this threshold the lookup falls back to the overall track par.
_MIN_GRADE_N = 15

# Grade bands keyed by (lo, hi inclusive, json_key).
# Bands <=45 cover 30-36, 37-40 and 41-45 — no separate data for lower groups.
_GRADE_BANDS = [
    (0,   45,  "<=45"),
    (46,  49,  "46-49"),
    (50,  55,  "50-55"),
    (56,  63,  "56-63"),
    (64,  70,  "64-70"),
    (71,  80,  "71-80"),
    (81,  90,  "81-90"),
    (91,  95,  "91-95"),
    (96,  999, "96+"),
]


def _nr_to_grade_band(nr_ceiling: int | float | None) -> str | None:
    if nr_ceiling is None:
        return None
    nr = int(nr_ceiling)
    for lo, hi, key in _GRADE_BANDS:
        if lo <= nr <= hi:
            return key
    return None


_TRACK_NAME_ALIASES: dict[str, str] = {
    "Bathurst": "Bthurst",
}


def _normalise_track_name(name: object) -> str:
    s = str(name)
    return _TRACK_NAME_ALIASES.get(s, s)


def load_track_pars(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def lookup_race_par(
    track_pars: dict | None,
    track_name: object,
    distance: object,
    condition: str = "Good",
    nr_ceiling: int | float | None = None,
) -> dict[str, object]:
    empty = {"par_last_half": None, "par_std": None, "par_sample": None, "par_condition": None}
    if not track_pars or not track_name or not distance:
        return empty

    pars = track_pars.get("pars", {})
    track_data = pars.get(_normalise_track_name(track_name))
    if not track_data:
        return empty

    distance_data = track_data.get(str(distance))
    if not distance_data:
        return empty

    cond_data = distance_data.get(condition)
    if not cond_data and condition != "Good":
        cond_data = distance_data.get("Good")
    if not cond_data:
        return empty

    par_condition = condition if condition in distance_data else ("Good" if "Good" in distance_data else None)

    # Grade-banded par — use when nr_ceiling is known and the band has enough data.
    if nr_ceiling is not None:
        band = _nr_to_grade_band(nr_ceiling)
        if band:
            grade = cond_data.get("grades", {}).get(band, {})
            if grade.get("n", 0) >= _MIN_GRADE_N:
                return {
                    "par_last_half": grade["par"],
                    "par_std": grade.get("std"),
                    "par_sample": grade["n"],
                    "par_condition": par_condition,
                }

    # Fallback: overall track par.
    return {
        "par_last_half": cond_data.get("par"),
        "par_std": cond_data.get("std"),
        "par_sample": cond_data.get("n"),
        "par_condition": par_condition,
    }
