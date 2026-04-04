from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class MeetingInfo:
    meeting_code: str
    meeting_date: str | None
    track_name: str | None
    state: str | None
    raw_title: str | None = None


@dataclass(slots=True)
class RunnerInfo:
    meeting_code: str
    race_number: int
    runner_number: int | None
    horse_id: str
    horse_name: str
    barrier: str | None = None
    driver_name: str | None = None
    driver_link: str | None = None
    trainer_name: str | None = None
    trainer_link: str | None = None
    scratched: bool = False
    race_name: str | None = None
    race_distance: int | None = None
    race_type: str | None = None
    class_name: str | None = None
    raw_price: float | None = None
    form_nr: int | None = None
    form_career_summary: tuple[int, int, int, int] | None = None
    form_this_season_summary: tuple[int, int, int, int] | None = None
    form_last_season_summary: tuple[int, int, int, int] | None = None
    form_dist_rge_summary: tuple[int, int, int, int] | None = None
    form_bmr: str | None = None
    form_bmr_dist_rge: str | None = None
    race_purse: float | None = None
    recent_lines: list["RunnerRecentLine"] = field(default_factory=list)


@dataclass(slots=True)
class RunnerRecentLine:
    meeting_code: str
    race_number: int
    horse_id: str
    line_index: int
    run_date: str | None = None
    track_name: str | None = None
    track_code: str | None = None
    distance: int | None = None
    condition: str | None = None
    last_half: float | None = None
    mile_rate: str | None = None
    first_half: float | None = None
    q1: float | None = None
    q2: float | None = None
    q3: float | None = None
    q4: float | None = None
    raw_comment: str | None = None
    finish_position: int | None = None
    raw_margin: float | None = None
    run_purse: float | None = None
    comment_adjustment: float = 0.0
    tempo_adjustment: float = 0.0
    null_run: bool = False
    adjusted_margin: float | None = None


@dataclass(slots=True)
class HorseRun:
    horse_id: str
    run_date: str | None
    track_code: str | None
    finish_position: int | None
    barrier: str | None
    margin: float | None
    mile_rate: str | None
    driver_name: str | None
    trainer_name: str | None
    stake: float | None
    distance: int | None
    distance_code: str | None
    race_name: str | None
    start_price: float | None
    comment_codes: str | None
    comment_adjustment: float = 0.0
    null_run: bool = False
    adjusted_margin: float | None = None
    race_type: str | None = None


@dataclass(slots=True)
class HorseProfile:
    horse_id: str
    horse_name: str
    nr_rating: int | None = None
    career_summary: tuple[int, int, int, int] | None = None
    this_season_summary: tuple[int, int, int, int] | None = None
    last_season_summary: tuple[int, int, int, int] | None = None
    career_bmr: str | None = None
    this_season_bmr: str | None = None
    last_season_bmr: str | None = None
    runs: list[HorseRun] = field(default_factory=list)


@dataclass(slots=True)
class ResultRunner:
    meeting_code: str
    race_number: int
    horse_name: str
    finish_position: int | None = None
    margin: float | None = None
    starting_price: float | None = None
    horse_id: str | None = None
    barrier: str | None = None
    trainer_name: str | None = None
    driver_name: str | None = None
    stake: float | None = None
    comment_codes: str | None = None
    comment_adjustment: float = 0.0
    null_run: bool = False
    adjusted_margin: float | None = None
    race_name: str | None = None
    distance: int | None = None
