from __future__ import annotations

import json
from pathlib import Path


def load_track_pars(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def lookup_race_par(track_pars: dict | None, track_name: object, distance: object, condition: str = "Good") -> dict[str, object]:
    empty = {"par_last_half": None, "par_std": None, "par_sample": None, "par_condition": None}
    if not track_pars or not track_name or not distance:
        return empty

    pars = track_pars.get("pars", {})
    track_data = pars.get(str(track_name))
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

    return {
        "par_last_half": cond_data.get("par"),
        "par_std": cond_data.get("std"),
        "par_sample": cond_data.get("n"),
        "par_condition": condition if condition in distance_data else ("Good" if "Good" in distance_data else None),
    }
