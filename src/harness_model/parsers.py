from __future__ import annotations

import re
import html as html_lib
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from .models import HorseProfile, HorseRun, MeetingInfo, ResultRunner, RunnerInfo, RunnerRecentLine


TRACK_NAME_MAP = {
    "MENANGL": "Menangle",
    "MENANGLE": "Menangle",
    "TABCORP PK MENANGLE": "Menangle",
    "NSWHRC AT TABCORP PK MENANGLE": "Menangle",
    "PENRITH": "Penrith",
    "BATHURS": "Bathurst",
    "BATHURST": "Bathurst",
    "GOULBUR": "Goulburn",
    "GOULBURN": "Goulburn",
    "NEWCAST": "Newcastle",
    "NEWCASTLE": "Newcastle",
    "NARRABRI": "Narrabri",
    "PARKES": "Parkes",
    "WAGGA": "Wagga",
    "TAMWRTH": "Tamworth",
    "TAMWORTH": "Tamworth",
    "TEMORA": "Temora",
    "MARBURG": "Marburg",
    "MARYBRO": "Maryborough",
    "MARYBORO": "Maryborough",
    "MELTON": "Melton",
    "KILMORE": "Kilmore",
    "HORSHAM": "Horsham",
    "BENDIGO": "Bendigo",
}

TRACK_CODE_MAP = {
    "GOULBURN": "Goulburn",
    "GOULBUR": "Goulburn",
    "MENANGL": "Menangle",
    "MENANGLE": "Menangle",
    "BATHURS": "Bathurst",
    "BATHURST": "Bathurst",
    "PENRITH": "Penrith",
    "NEWCAST": "Newcastle",
    "NEWCASTLE": "Newcastle",
    "TAMWORTH": "Tamworth",
    "TAMWRTH": "Tamworth",
    "MARYBRO": "Maryborough",
    "MARYBORO": "Maryborough",
    "MELTON": "Melton",
    "KILMORE": "Kilmore",
    "HORSHAM": "Horsham",
    "BENDIGO": "Bendigo",
    "MENANGLE": "Menangle",
}

STATE_TRACKS = {"Menangle": "NSW", "Penrith": "NSW", "Bathurst": "NSW", "Goulburn": "NSW"}
EXCLUDED_RACE_KEYWORDS = ("TROT", "TROTTERS", "TROTTING", "2YO")

CODES = {
    "OL": -10.0, "OLM": -10.0, "OLT": -10.0, "OTE": -10.0, "OTM": -10.0, "OT": -10.0,
    "SCT": -10.0, "DINC": -10.0, "INC": -10.0, "CI": -10.0, "HU": -7.5, "HUE": -7.5,
    "ODM": -7.5, "WET": -7.5, "IAS": -5.0, "OV": -5.0, "OVR": -5.0, "HI": -5.0,
    "HO": -5.0, "CWE": -5.0, "WE": -5.0, "WM": -5.0, "RWE": -5.0, "RWM": -5.0,
    "3WL": -5.0, "3WM": -5.0, "3WET": -5.0, "WLT": -3.0, "WL": -3.0, "SHI": -3.0,
    "SHO": -3.0, "OIR": 5.0, "ODS": 3.0, "SLE": 5.0, "USL": 3.0, "SOUP": 5.0, "SIUP": 5.0,
}

# These codes indicate a run should be discarded regardless of margin.
# Do not include positional markers like BL ("bell lap"), which are common
# trip notes rather than failure/null-run flags.
NULL_RUN_CODES = {"BSU", "BCE", "SLM"}


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip = False

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {"script", "style", "nav", "header", "footer"}:
            self._skip = True

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "nav", "header", "footer"}:
            self._skip = False

    def handle_data(self, data: str) -> None:
        if self._skip:
            return
        stripped = data.strip()
        if stripped:
            self.parts.append(stripped)


def extract_text(html: str) -> str:
    parser = TextExtractor()
    parser.feed(html)
    return "\n".join(parser.parts)


def parse_meeting_html(html: str, meeting_code: str) -> tuple[MeetingInfo, list[RunnerInfo]]:
    text = extract_text(html)
    lines = text.splitlines()

    title = next((line for line in lines[:40] if len(line) > 12), None)
    track_name = None
    for line in lines[:40]:
        for key, value in TRACK_NAME_MAP.items():
            if key.lower() in line.lower():
                track_name = value
                break
        if track_name:
            break

    meeting_date = None
    for line in lines[:40]:
        match = re.search(r"\b\d{1,2}\s+[A-Za-z]+\s+\d{4}\b", line)
        if match:
            meeting_date = match.group(0)
            break

    meeting = MeetingInfo(
        meeting_code=meeting_code,
        meeting_date=meeting_date,
        track_name=track_name,
        state=STATE_TRACKS.get(track_name),
        raw_title=title,
    )
    return meeting, _parse_runners_from_html(html, meeting_code)


def parse_results_html(html: str, meeting_code: str) -> list[ResultRunner]:
    results: list[ResultRunner] = []

    header_pattern = re.compile(
        r'<tr class="raceHeader">.*?'
        r'<td class="raceNumber[^"]*"[^>]*>\s*(?P<race_number>\d+)\s*</td>.*?'
        r'<td class="raceTitle">(?P<race_title>.*?)</td>.*?'
        r'<td class="distance">(?P<distance>[^<]*)</td>',
        re.IGNORECASE | re.DOTALL,
    )
    table_pattern = re.compile(
        r'<table class="raceFieldTable resultTable">(?P<table>.*?)</table>',
        re.IGNORECASE | re.DOTALL,
    )
    row_pattern = re.compile(
        r"<tr>\s*"
        r'<td class="horse_number">\s*(?P<place>\d+)?\s*</td>.*?'
        r'<a href="[^"]*horseId=(?P<horse_id>\d+)" class="horse_name_link">(?P<horse_name>[^<]+)</a>.*?'
        r'<td class="margin">\s*(?P<margin>[^<]*)</td>.*?'
        r'<td class="starting_price[^"]*">\s*(?P<starting_price>.*?)</td>',
        re.IGNORECASE | re.DOTALL,
    )

    headers = list(header_pattern.finditer(html))
    tables = list(table_pattern.finditer(html))

    for header_match, table_match in zip(headers, tables):
        race_number = int(header_match.group("race_number"))
        race_title = _clean_spaces(re.sub(r"<[^>]+>", " ", header_match.group("race_title")))
        if _is_excluded_race(race_title):
            continue
        distance_text = _clean_spaces(header_match.group("distance") or "")
        distance_m = re.match(r"(\d+)", distance_text)
        race_distance = int(distance_m.group(1)) if distance_m else None

        table_html = table_match.group("table")
        for row_match in row_pattern.finditer(table_html):
            place_text = (row_match.group("place") or "").strip()
            margin_text = _clean_spaces(row_match.group("margin") or "")
            odds_text = _clean_spaces(re.sub(r"<[^>]+>", " ", row_match.group("starting_price") or ""))
            margin = 0.0 if place_text == "1" else (_parse_results_margin(margin_text) if margin_text else None)

            # Sub-extract additional fields from the full row HTML.
            # Start at the row match start; end at the next </tr>.
            row_end = table_html.find("</tr>", row_match.end())
            full_row = table_html[row_match.start(): row_end if row_end != -1 else row_match.end()]

            barrier_m = re.search(r'<td class="barrier[^"]*">\s*([^<]*)\s*</td>', full_row, re.IGNORECASE)
            barrier = barrier_m.group(1).strip() or None if barrier_m else None

            trainer_m = re.search(r'<td class="trainer nowrap">.*?<a[^>]*>([^<]+)</a>', full_row, re.IGNORECASE | re.DOTALL)
            trainer_name = _clean_spaces(trainer_m.group(1)) if trainer_m else None

            driver_m = re.search(r'<td class="driver nowrap">.*?<a[^>]*>([^<]+)</a>', full_row, re.IGNORECASE | re.DOTALL)
            driver_name = _clean_spaces(driver_m.group(1)) if driver_m else None

            prizemoney_m = re.search(r'<td class="prizemoney[^"]*">\s*([^<]*)\s*</td>', full_row, re.IGNORECASE)
            stake = _parse_prizemoney(prizemoney_m.group(1)) if prizemoney_m else None

            # Short codes (e.g. "BL 3 SWAB") stored for reference only —
            # they use a different code system to horse profile pages.
            # Adjustments are computed from the full text in data-original-title.
            comment_codes_m = re.search(
                r'<td class="stewards_comments">.*?<span[^>]*>([^<]+)</span>',
                full_row, re.IGNORECASE | re.DOTALL,
            )
            comment_text_m = re.search(
                r'<td class="stewards_comments">.*?data-original-title="([^"]*)"',
                full_row, re.IGNORECASE | re.DOTALL,
            )
            comment_codes = _clean_spaces(comment_codes_m.group(1)) if comment_codes_m else None
            full_comment_text = comment_text_m.group(1) if comment_text_m else ""
            comment_adj, _, null_run = _apply_form_line_text_rules(full_comment_text, None, None, margin)
            adjusted_margin = None if null_run or margin is None else round(margin + comment_adj, 2)

            results.append(
                ResultRunner(
                    meeting_code=meeting_code,
                    race_number=race_number,
                    horse_name=_clean_spaces(row_match.group("horse_name")).title(),
                    finish_position=int(place_text) if place_text.isdigit() else None,
                    margin=margin,
                    starting_price=_parse_results_price(odds_text),
                    horse_id=row_match.group("horse_id"),
                    barrier=barrier,
                    trainer_name=trainer_name,
                    driver_name=driver_name,
                    stake=stake,
                    comment_codes=comment_codes,
                    comment_adjustment=comment_adj,
                    null_run=null_run,
                    adjusted_margin=adjusted_margin,
                    race_name=race_title,
                    distance=race_distance,
                )
            )

    return _dedupe_results(results)


def parse_hrnsw_results_index(html: str) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    meetings_section = html
    trials_marker = re.search(r'<div[^>]+id="[^"]*pnlTrials"[^>]*>', html, re.IGNORECASE)
    if trials_marker:
        meetings_section = html[:trials_marker.start()]

    row_pattern = re.compile(
        r"<tr\b[^>]*>\s*"
        r'<td\b[^>]*class="[^"]*\btrackname\b[^"]*"[^>]*>(?P<track>.*?)</td>\s*'
        r'<td\b[^>]*class="[^"]*\btimeofday\b[^"]*"[^>]*>(?P<session>.*?)</td>\s*'
        r'<td\b[^>]*class="[^"]*\bdate\b[^"]*"[^>]*>(?P<date>.*?)</td>\s*'
        r"<td>.*?"
        r'<a[^>]+href="(?P<link>[^"]*meeting-results\.cfm\?mc=(?P<code>[A-Z0-9]+)[^"]*)"[^>]*>'
        r"(?P<label>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )

    for match in row_pattern.finditer(meetings_section):
        label = _clean_spaces(re.sub(r"<[^>]+>", " ", match.group("label")))
        if "RESULTS" not in label.upper():
            continue
        absolute_link = urljoin("https://www.hrnsw.com.au", html_lib.unescape(match.group("link")))
        absolute_link = absolute_link.replace("http://www.harness.org.au/", "https://www.harness.org.au/")
        entries.append(
            {
                "track_name": _clean_spaces(re.sub(r"<[^>]+>", " ", match.group("track"))).title(),
                "session": _clean_spaces(re.sub(r"<[^>]+>", " ", match.group("session"))).title(),
                "meeting_date": _clean_spaces(re.sub(r"<[^>]+>", " ", match.group("date"))),
                "meeting_code": match.group("code").upper(),
                "results_url": absolute_link,
            }
        )
    return _dedupe_hrnsw_entries(entries)


def parse_hrnsw_upcoming_meetings(html: str) -> list[dict[str, str]]:
    """Parse upcoming meetings from the HRNSW upcoming meetings page.

    Returns a list of dicts with keys: track_name, session, meeting_date,
    meeting_code, form_url.  Trials section is excluded.
    """
    entries: list[dict[str, str]] = []
    meetings_section = html
    trials_marker = re.search(r'<div[^>]+id="[^"]*pnlTrials"[^>]*>', html, re.IGNORECASE)
    if trials_marker:
        meetings_section = html[:trials_marker.start()]

    row_pattern = re.compile(
        r"<tr\b[^>]*>\s*"
        r'<td\b[^>]*class="[^"]*\btrackname\b[^"]*"[^>]*>(?P<track>.*?)</td>\s*'
        r'<td\b[^>]*class="[^"]*\btimeofday\b[^"]*"[^>]*>(?P<session>.*?)</td>\s*'
        r'<td\b[^>]*class="[^"]*\bdate\b[^"]*"[^>]*>(?P<date>.*?)</td>\s*'
        r"<td>.*?"
        r'<a[^>]+href="(?P<link>[^"]*(?:form|fields)\.cfm\?mc=(?P<code>[A-Z0-9]+)[^"]*)"',
        re.IGNORECASE | re.DOTALL,
    )

    for match in row_pattern.finditer(meetings_section):
        code = match.group("code").upper()
        raw_link = html_lib.unescape(match.group("link"))
        absolute_link = urljoin("https://www.harness.org.au", raw_link)
        absolute_link = absolute_link.replace("http://www.harness.org.au/", "https://www.harness.org.au/")
        entries.append(
            {
                "track_name": _clean_spaces(re.sub(r"<[^>]+>", " ", match.group("track"))).title(),
                "session": _clean_spaces(re.sub(r"<[^>]+>", " ", match.group("session"))).title(),
                "meeting_date": _clean_spaces(re.sub(r"<[^>]+>", " ", match.group("date"))),
                "meeting_code": code,
                "form_url": absolute_link,
            }
        )
    return _dedupe_hrnsw_entries(entries)


def parse_hrnsw_track_options(html: str) -> list[dict[str, str]]:
    select_match = re.search(
        r'<select[^>]+id="ContentPlaceHolderMain_ContentPlaceHolderContent_ddlSearchTrack"[^>]*>(?P<body>.*?)</select>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if not select_match:
        return []
    options: list[dict[str, str]] = []
    option_pattern = re.compile(
        r'<option value="(?P<value>[^"]*)">(?P<label>.*?)</option>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in option_pattern.finditer(select_match.group("body")):
        value = _clean_spaces(match.group("value"))
        label = _clean_spaces(re.sub(r"<[^>]+>", " ", match.group("label")))
        if not value or not label:
            continue
        options.append({"value": value, "label": label.title()})
    return options


def _parse_runners_from_html(html: str, meeting_code: str) -> list[RunnerInfo]:
    runners = _parse_form_guide_races(html, meeting_code)
    if runners:
        return _dedupe_runners(runners)

    runners = _parse_structured_race_tables(html, meeting_code)
    if runners:
        return _dedupe_runners(runners)

    return _dedupe_runners(_parse_fallback_race_blocks(html, meeting_code))


def _parse_form_guide_races(html: str, meeting_code: str) -> list[RunnerInfo]:
    race_pattern = re.compile(
        r'<a name="(?P<anchor>\d+)"></a>.*?'
        r'<table class="raceHeader">(?P<header>.*?)</table>\s*'
        r'<table class="raceMoreInfo">(?P<info>.*?)</table>'
        r'(?P<body>.*?)(?=<div class="formRow"><div class="quickNav">|<a name="\d+"></a>|$)',
        flags=re.IGNORECASE | re.DOTALL,
    )
    runners: list[RunnerInfo] = []

    for race_match in race_pattern.finditer(html):
        header = race_match.group("header")
        info = race_match.group("info")
        body = race_match.group("body")

        race_number_match = re.search(r'<td class="raceNumber">\s*Race\s+(\d+)\s*</td>', header, re.IGNORECASE)
        if not race_number_match:
            continue
        race_number = int(race_number_match.group(1))

        race_name = _extract_form_race_name(header)
        if _is_excluded_race(race_name):
            continue

        race_distance = _extract_form_distance(info)
        race_conditions = _extract_form_conditions(info)
        race_type = _extract_form_start_type(info)
        race_purse = _extract_form_race_purse(info)

        horse_start_pattern = re.compile(r'<td class="horse_name_td">', flags=re.IGNORECASE)
        horse_starts = [match.start() for match in horse_start_pattern.finditer(body)]
        if not horse_starts:
            continue

        for idx, horse_html_start in enumerate(horse_starts):
            next_start = horse_starts[idx + 1] if idx + 1 < len(horse_starts) else len(body)
            horse_block = body[horse_html_start:next_start]

            number_match = re.search(
                r'<span class="horse_number">(?P<number>\d+)</span>',
                horse_block,
                flags=re.IGNORECASE,
            )
            horse_match = re.search(
                r'<span class="horse_name">\s*<a href="[^"]*horseId=(?P<horse_id>\d+)">(?P<horse_name>[^<]+)</a>',
                horse_block,
                flags=re.IGNORECASE | re.DOTALL,
            )
            barrier_match = re.search(
                r'<div class="horse_handicap">(?P<barrier>[^<]+)</div>',
                horse_block,
                flags=re.IGNORECASE,
            )
            class_match = re.search(
                r'<div class="horse_class">(?P<horse_class>[^<]+)</div>',
                horse_block,
                flags=re.IGNORECASE,
            )
            driver_match = re.search(
                r'<div class="driver">Driver:\s*<span class="driver_name">.*?<a href="(?P<driver_link>[^"]+)">(?P<driver>[^<]*)</a>',
                horse_block,
                flags=re.IGNORECASE | re.DOTALL,
            )
            trainer_match = re.search(
                r'<div class="trainer">Trainer:\s*<span class="trainer_name">(?:<span class="bolded">)?(?:<a href="(?P<trainer_link>[^"]+)">)?(?P<trainer>[^<(]+)',
                horse_block,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not number_match or not horse_match:
                continue

            recent_lines = _extract_recent_lines_from_horse_block(
                horse_block=horse_block,
                meeting_code=meeting_code,
                race_number=race_number,
                horse_id=horse_match.group("horse_id"),
            )
            career_summary = _extract_form_stats_summary(horse_block, "Career")
            this_season_summary = _extract_form_stats_summary(horse_block, "TS")
            last_season_summary = _extract_form_stats_summary(horse_block, "LS")
            dist_rge_summary = _extract_form_stats_summary(horse_block, "DistRge")
            form_horse_nr = _extract_form_horse_nr(horse_block)
            form_bmr = _extract_form_bmr(horse_block)
            form_bmr_dist_rge = _extract_form_bmr_dist_rge(horse_block)
            # Only check for scratching markers in the horse's own header area,
            # not in the full block — the horseScratched div for horse N+1 can
            # appear before that horse's horse_name_td and bleed into horse N's block.
            # Limit the search to content before the first form_line row.
            form_line_pos = horse_block.find('class="form_line"')
            header_section = horse_block[:form_line_pos] if form_line_pos != -1 else horse_block[:2000]
            scratched = bool(
                re.search(r'class="horseScratched"', header_section, re.IGNORECASE)
                or re.search(r'<span class="scratched">', header_section, re.IGNORECASE)
            )
            runners.append(
                RunnerInfo(
                    meeting_code=meeting_code,
                    race_number=race_number,
                    runner_number=int(number_match.group("number")),
                    horse_id=horse_match.group("horse_id"),
                    horse_name=_clean_spaces(horse_match.group("horse_name")),
                    barrier=_clean_spaces(barrier_match.group("barrier")) if barrier_match else None,
                    driver_name=_clean_driver_name(_clean_spaces(driver_match.group("driver"))) if driver_match else None,
                    driver_link=driver_match.group("driver_link") if driver_match else None,
                    trainer_name=_clean_spaces(trainer_match.group("trainer")).rstrip(",") if trainer_match else None,
                    trainer_link=trainer_match.group("trainer_link") if trainer_match else None,
                    scratched=scratched,
                    race_name=race_name,
                    race_distance=race_distance,
                    race_type=race_type,
                    class_name=race_conditions or (_clean_spaces(class_match.group("horse_class")) if class_match else None),
                    raw_price=None,
                    form_career_summary=career_summary,
                    form_this_season_summary=this_season_summary,
                    form_last_season_summary=last_season_summary,
                    form_dist_rge_summary=dist_rge_summary,
                    form_nr=form_horse_nr,
                    form_bmr=form_bmr,
                    form_bmr_dist_rge=form_bmr_dist_rge,
                    race_purse=race_purse,
                    recent_lines=recent_lines,
                )
            )

    return runners


def _parse_structured_race_tables(html: str, meeting_code: str) -> list[RunnerInfo]:
    pattern = re.compile(
        r'<table class="raceMoreInfo">(?P<header>.*?)</table>\s*'
        r'<table class="raceFieldTable">(?P<field>.*?)</table>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    runners: list[RunnerInfo] = []

    for race_match in pattern.finditer(html):
        header = race_match.group("header")
        field = race_match.group("field")
        race_number_match = re.search(r'<td class="raceNumber[^"]*"[^>]*>(\d+)</td>', header, re.IGNORECASE)
        if not race_number_match:
            continue

        race_number = int(race_number_match.group(1))
        race_name_match = re.search(r'<td class="raceTitle">([^<]+)</td>', header, re.IGNORECASE)
        race_name = _clean_spaces(race_name_match.group(1)) if race_name_match else None
        if _is_excluded_race(race_name):
            continue

        distance_match = re.search(r'<td class="distance">(\d{4})M</td>', header, re.IGNORECASE)
        race_distance = int(distance_match.group(1)) if distance_match else None

        info_match = re.search(r'<td class="raceInformation">(.*?)</td>', header, re.IGNORECASE | re.DOTALL)
        info_plain = _clean_spaces(re.sub(r"<[^>]+>", " ", info_match.group(1))) if info_match else ""

        row_pattern = re.compile(r"<tr[^>]+data-hid=\"(?P<horse_id>\d+)\"[^>]*>(?P<row>.*?)</tr>", re.IGNORECASE | re.DOTALL)
        for row_match in row_pattern.finditer(field):
            row = row_match.group("row")
            horse_name = _extract_cell_anchor_text(row, "horse_name")
            if not horse_name:
                continue
            runners.append(
                RunnerInfo(
                    meeting_code=meeting_code,
                    race_number=race_number,
                    runner_number=_extract_cell_int(row, "horse_number"),
                    horse_id=row_match.group("horse_id"),
                    horse_name=horse_name,
                    barrier=_extract_cell_text(row, "hcp"),
                    driver_name=_clean_driver_name(_extract_cell_anchor_text(row, "driver")),
                    driver_link=_extract_cell_anchor_href(row, "driver"),
                    trainer_name=_extract_cell_anchor_text(row, "trainer"),
                    trainer_link=_extract_cell_anchor_href(row, "trainer"),
                    scratched=False,
                    race_name=race_name,
                    race_distance=race_distance,
                    race_type="PACE",
                    class_name=_extract_cell_text(row, "horse_class") or _extract_class_name(info_plain),
                    raw_price=_extract_cell_price(row, "market"),
                )
            )

    return runners


def _parse_fallback_race_blocks(html: str, meeting_code: str) -> list[RunnerInfo]:
    race_blocks = list(re.finditer(r"Race\s+(\d+)", html, flags=re.IGNORECASE))
    if not race_blocks:
        return []

    runners: list[RunnerInfo] = []
    for idx, match in enumerate(race_blocks):
        race_number = int(match.group(1))
        start = match.start()
        end = race_blocks[idx + 1].start() if idx + 1 < len(race_blocks) else len(html)
        block = html[start:end]
        race_name = _extract_race_name(block)
        if _is_excluded_race(race_name):
            continue

        race_distance = _extract_race_distance(block)
        class_name = _extract_class_name(block)
        horse_matches = list(re.finditer(
            r'horseId=(?P<horse_id>\d+)[^>]*>(?P<horse_name>[A-Z][A-Z0-9\s\'\-\.\(\)]+)</a>',
            block,
            flags=re.IGNORECASE,
        ))

        for order, horse_match in enumerate(horse_matches, start=1):
            snippet = block[max(0, horse_match.start() - 600): horse_match.end() + 1200]
            runners.append(
                RunnerInfo(
                    meeting_code=meeting_code,
                    race_number=race_number,
                    runner_number=_extract_runner_number(snippet) or order,
                    horse_id=horse_match.group("horse_id"),
                    horse_name=_clean_spaces(horse_match.group("horse_name")),
                    barrier=_extract_barrier(snippet),
                    driver_name=_extract_driver(snippet),
                    driver_link=_extract_driver_link(snippet),
                    trainer_name=_extract_trainer(snippet),
                    trainer_link=_extract_trainer_link(snippet),
                    scratched=_extract_scratched(snippet),
                    race_name=race_name,
                    race_distance=race_distance,
                    race_type="PACE",
                    class_name=class_name,
                    raw_price=_extract_price(snippet),
                )
            )

    return runners


def parse_horse_profile_html(html: str, horse_id: str | None = None, source_name: str | None = None) -> HorseProfile:
    text = extract_text(html)
    lines = text.splitlines()
    bmrs = re.findall(r"Best Winning Mile Rate:\n([\d:\.]+)", text)

    profile = HorseProfile(
        horse_id=horse_id or _extract_horse_id(html) or "unknown",
        horse_name=_extract_horse_name(text, source_name),
        nr_rating=_extract_nr(text),
        career_summary=_extract_summary(text, "Lifetime"),
        this_season_summary=_extract_summary(text, "This Season"),
        last_season_summary=_extract_summary(text, "Last Season"),
        career_bmr=bmrs[0] if len(bmrs) > 0 else None,
        this_season_bmr=bmrs[1] if len(bmrs) > 1 else None,
        last_season_bmr=bmrs[2] if len(bmrs) > 2 else None,
    )
    profile.runs = _extract_horse_runs(lines, profile.horse_id)
    return profile


def _extract_horse_runs(lines: list[str], horse_id: str) -> list[HorseRun]:
    start_idx = next((idx for idx, line in enumerate(lines) if line.strip() == "Performance Records"), None)
    if start_idx is None:
        return []

    runs: list[HorseRun] = []
    run_pat = re.compile(r"^\d{2}\s+\w+\s+\d{4}$")
    idx = start_idx
    while idx < len(lines):
        if run_pat.match(lines[idx].strip()):
            run = _parse_run_block(lines[idx:idx + 20], horse_id)
            if run:
                runs.append(run)
        idx += 1
    return runs


def _parse_run_block(block: list[str], horse_id: str) -> HorseRun | None:
    if len(block) < 12:
        return None

    margin = _parse_margin(block[4].strip() if len(block) > 4 else "")

    comment_codes = ""
    if len(block) > 12:
        candidate = block[12].strip()
        if candidate and not re.fullmatch(r"\d{2}\s+\w+\s+\d{4}", candidate):
            comment_codes = candidate

    adjustment, null_run = _apply_comment_codes(comment_codes, raw_margin=margin)

    distance_code = block[9].strip() if len(block) > 9 else None
    distance_match = re.match(r"^(\d{4})(MS|SS)?$", distance_code or "")
    distance = int(distance_match.group(1)) if distance_match else None
    race_name = block[10].strip() if len(block) > 10 else None

    return HorseRun(
        horse_id=horse_id,
        run_date=block[0].strip(),
        track_code=block[1].strip() if len(block) > 1 else None,
        finish_position=int(block[2].strip()) if len(block) > 2 and re.fullmatch(r"\d+", block[2].strip()) else None,
        barrier=block[3].strip() if len(block) > 3 else None,
        margin=margin,
        mile_rate=block[5].strip() if len(block) > 5 and re.fullmatch(r"\d:\d{2}\.\d", block[5].strip()) else None,
        driver_name=block[6].strip() if len(block) > 6 else None,
        trainer_name=block[7].strip() if len(block) > 7 else None,
        stake=_parse_stake(block[8].strip() if len(block) > 8 else ""),
        distance=distance,
        distance_code=distance_code,
        race_name=race_name,
        start_price=_parse_price(block[11].strip() if len(block) > 11 else ""),
        comment_codes=comment_codes or None,
        comment_adjustment=adjustment,
        null_run=null_run,
        adjusted_margin=None if null_run or margin is None else round(margin + adjustment, 2),
        race_type="TRIAL" if race_name and "TRIAL" in race_name.upper() else "RACE",
    )


def _extract_horse_id(html: str) -> str | None:
    match = re.search(r"horseId=(\d+)", html)
    return match.group(1) if match else None


def _extract_horse_name(text: str, source_name: str | None) -> str:
    if source_name:
        stem = Path(source_name).stem
        maybe_name = re.sub(r"_\d+$", "", stem).replace("_", " ").strip()
        if maybe_name:
            return maybe_name
    for line in text.splitlines()[:40]:
        if re.fullmatch(r"[A-Z0-9\s\'\-\(\)\.]{4,}", line):
            return _clean_spaces(line.title())
    return "Unknown Horse"


def _extract_nr(text: str) -> int | None:
    match = re.search(r"Class\s*\n(NR\d+)", text)
    return int(match.group(1)[2:]) if match else None


def _extract_summary(text: str, label: str) -> tuple[int, int, int, int] | None:
    match = re.search(rf"{re.escape(label)}\nSummary:\n([\d\-]+)", text)
    if not match:
        return None
    parts = match.group(1).split("-")
    return tuple(int(part) for part in parts) if len(parts) == 4 else None


def _dedupe_runners(runners: list[RunnerInfo]) -> list[RunnerInfo]:
    seen: set[tuple[str, int, str]] = set()
    deduped: list[RunnerInfo] = []
    for runner in runners:
        key = (runner.meeting_code, runner.race_number, runner.horse_id)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(runner)
    return deduped


def _extract_race_name(block: str) -> str | None:
    plain = _clean_spaces(re.sub(r"<[^>]+>", " ", block))
    match = re.search(r"Race\s+\d+\s+(.*?)(?:\s+\d{4}M|\s+\$[\d,]+|\s+Mobile|\s+Standing|$)", plain, re.IGNORECASE)
    return match.group(1).strip()[:180] if match else None


def _extract_form_race_name(header_html: str) -> str | None:
    match = re.search(r'<td class="raceTitle">(.*?)</td>', header_html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    plain = _clean_spaces(re.sub(r"<[^>]+>", " ", match.group(1)))
    return plain or None


def _parse_hrnsw_meeting_text(text: str) -> dict[str, str] | None:
    match = re.fullmatch(r"(.+?)\s+(Day|Night|Twilight)\s+(\d{2}/\d{2}/\d{4})", text.strip(), re.IGNORECASE)
    if not match:
        return None
    track_name, session, meeting_date = match.groups()
    return {
        "track_name": _clean_spaces(track_name).title(),
        "session": session.title(),
        "meeting_date": meeting_date,
    }


def _extract_meeting_code_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    query_mc = parse_qs(parsed.query).get("mc")
    if query_mc:
        return query_mc[0].upper()
    match = re.search(r"\b([A-Z]{2}\d{6})\b", url, re.IGNORECASE)
    return match.group(1).upper() if match else None


def _dedupe_hrnsw_entries(entries: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    deduped: list[dict[str, str]] = []
    for entry in entries:
        code = entry["meeting_code"]
        if code in seen:
            continue
        seen.add(code)
        deduped.append(entry)
    return deduped


def _extract_form_distance(info_html: str) -> int | None:
    match = re.search(r'<td class="distance">(\d{4})\s+METRES</td>', info_html, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _extract_form_conditions(info_html: str) -> str | None:
    values = re.findall(r'<div class="race_track_data">\s*(.*?)\s*</div>', info_html, re.IGNORECASE | re.DOTALL)
    cleaned = []
    for value in values:
        plain = _clean_spaces(re.sub(r"<[^>]+>", " ", value))
        if plain.startswith("Track:") or plain.startswith("Track Record:"):
            continue
        if plain:
            cleaned.append(plain)
    return " | ".join(cleaned) if cleaned else None


def _extract_form_start_type(info_html: str) -> str | None:
    match = re.search(r'<td class="start">([^<]+)</td>', info_html, re.IGNORECASE)
    return _clean_spaces(match.group(1)) if match else None


def _extract_race_distance(block: str) -> int | None:
    match = re.search(r"\b(1609|1710|1720|1730|1740|1770|2090|2100|2200|2260|2300)\s*M\b", block, re.IGNORECASE)
    if match:
        return int(match.group(1))
    match = re.search(r"\b(1609|1710|1720|1730|1740|1770|2090|2100|2200|2260|2300)(?:MS|SS)\b", block, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _extract_class_name(block: str) -> str | None:
    plain = _clean_spaces(re.sub(r"<[^>]+>", " ", block))
    match = re.search(r"\b(NR\s*\d+[^$<]{0,80}|CLASS\s*[A-Z0-9]+[^$<]{0,80}|C\d[\w\s\-]{0,40})", plain, re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_form_horse_nr(horse_block: str) -> int | None:
    """Extract the horse's individual NR from the form page horse block.

    The NR appears as: <div class="horse_class">NR36</div>
    Sometimes includes an apprentice rating: NR48 (A43) — we take only the base NR.
    """
    match = re.search(r'<div class="horse_class">\s*NR\s*(\d+)', horse_block, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _extract_runner_number(snippet: str) -> int | None:
    match = re.search(r"\b(?:No\.?|Number|Runner)\s*[:#]?\s*(\d{1,2})\b", snippet, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _extract_barrier(snippet: str) -> str | None:
    match = re.search(r"\b(?:Barrier|Draw)\s*[:#]?\s*([A-Za-z]?\d{1,2})\b", snippet, re.IGNORECASE)
    return match.group(1).upper() if match else None


def _extract_driver(snippet: str) -> str | None:
    plain = _clean_spaces(re.sub(r"<[^>]+>", " ", snippet))
    match = re.search(r"\bDriver\s*:?\s*([A-Z][A-Za-z\.\'\-\s]{3,40})", plain)
    return match.group(1).strip() if match else None


def _extract_driver_link(snippet: str) -> str | None:
    match = re.search(r'<div class="driver">.*?<a href="([^"]+/racing/driverlink/[^"]+|/racing/driverlink/[^"]+)"', snippet, re.IGNORECASE | re.DOTALL)
    return match.group(1) if match else None


def _extract_trainer_link(snippet: str) -> str | None:
    match = re.search(r'<div class="trainer">.*?<a href="([^"]+/racing/trainerlink/[^"]+|/racing/trainerlink/[^"]+|[^"]+/racing/trainers/[^"]+|/racing/trainers/[^"]+)"', snippet, re.IGNORECASE | re.DOTALL)
    return match.group(1) if match else None


def _extract_trainer(snippet: str) -> str | None:
    plain = _clean_spaces(re.sub(r"<[^>]+>", " ", snippet))
    match = re.search(r"\bTrainer\s*:?\s*([A-Z][A-Za-z\.\'\-\s]{3,40})", plain)
    return match.group(1).strip() if match else None


def _extract_scratched(snippet: str) -> bool:
    return bool(re.search(r"\bSCR(?:ATCHED)?\b", snippet, re.IGNORECASE))


def _extract_price(snippet: str) -> float | None:
    match = re.search(r"\$(\d+\.\d{2})", snippet)
    return float(match.group(1)) if match else None


def _extract_cell_text(row_html: str, class_name: str) -> str | None:
    match = re.search(rf'<td class="{re.escape(class_name)}"[^>]*>(.*?)</td>', row_html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return _clean_spaces(re.sub(r"<[^>]+>", " ", match.group(1))) or None


def _extract_cell_anchor_text(row_html: str, class_name: str) -> str | None:
    match = re.search(
        rf'<td class="{re.escape(class_name)}[^"]*"[^>]*>.*?<a [^>]*>(.*?)</a>',
        row_html,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return _extract_cell_text(row_html, class_name)
    return _clean_spaces(re.sub(r"<[^>]+>", " ", match.group(1))) or None


def _extract_cell_anchor_href(row_html: str, class_name: str) -> str | None:
    match = re.search(
        rf'<td class="{re.escape(class_name)}[^"]*"[^>]*>.*?<a [^>]*href="([^"]+)"',
        row_html,
        re.IGNORECASE | re.DOTALL,
    )
    return match.group(1) if match else None


def _extract_cell_int(row_html: str, class_name: str) -> int | None:
    value = _extract_cell_text(row_html, class_name)
    return int(value) if value and value.isdigit() else None


def _extract_cell_price(row_html: str, class_name: str) -> float | None:
    value = _extract_cell_text(row_html, class_name)
    if not value:
        return None
    match = re.search(r"\$(\d+\.\d{2})", value)
    return float(match.group(1)) if match else None


def _clean_driver_name(name: str | None) -> str | None:
    if not name:
        return None
    return re.sub(r"\s*\([^)]*\)\s*$", "", name).strip() or None


def _extract_recent_lines_from_horse_block(
    horse_block: str,
    meeting_code: str,
    race_number: int,
    horse_id: str,
) -> list[RunnerRecentLine]:
    line_pattern = re.compile(r'<tr class="form_line">(?P<line>.*?)</tr>', flags=re.IGNORECASE | re.DOTALL)

    recent_lines: list[RunnerRecentLine] = []
    for idx, match in enumerate(line_pattern.finditer(horse_block), start=1):
        parsed = _parse_recent_line_html(match.group("line"))
        if not parsed:
            continue

        track_code = parsed["track_code"].upper()
        q1, q2, q3, q4 = _parse_quarters(parsed["quarters"])
        first_half = round(q1 + q2, 2) if q1 is not None and q2 is not None else None
        raw_comment = parsed["comment"]
        finish_position = _parse_form_place(parsed["form_place"])
        raw_margin = _parse_recent_line_margin(parsed["line_text"], finish_position)
        comment_adjustment, tempo_adjustment, null_run = _apply_form_line_text_rules(raw_comment, first_half, float(parsed["last_half"]), raw_margin)
        adjusted_margin = None if null_run or raw_margin is None else round(raw_margin + comment_adjustment + tempo_adjustment, 2)
        recent_lines.append(
            RunnerRecentLine(
                meeting_code=meeting_code,
                race_number=race_number,
                horse_id=horse_id,
                line_index=idx,
                run_date=_normalize_compact_date(parsed["date"]),
                track_name=TRACK_CODE_MAP.get(track_code, track_code.title()),
                track_code=track_code,
                distance=int(parsed["distance"]),
                condition=_normalize_track_condition(parsed["condition"]),
                last_half=float(parsed["last_half"]),
                mile_rate=parsed["mile_rate"],
                first_half=first_half,
                q1=q1,
                q2=q2,
                q3=q3,
                q4=q4,
                raw_comment=raw_comment or None,
                finish_position=finish_position,
                raw_margin=raw_margin,
                run_purse=parsed.get("purse"),
                line_nr_ceiling=parsed.get("line_nr_ceiling"),
                line_race_age=parsed.get("line_race_age"),
                run_sp=parsed.get("run_sp"),
                comment_adjustment=comment_adjustment,
                tempo_adjustment=tempo_adjustment,
                null_run=null_run,
                adjusted_margin=adjusted_margin,
            )
        )
    return recent_lines


def _parse_recent_line_html(line_html: str) -> dict[str, str] | None:
    form_place_match = re.search(r'<td[^>]*class="form_place">\s*(?P<form_place>.*?)\s*</td>', line_html, re.IGNORECASE | re.DOTALL)
    results_match = re.search(
        r'<a class="results_link"[^>]*>\s*(?P<track>[A-Z]+)\s+(?P<date>\d{2}[A-Za-z]{3}\d{2})\s*</a>'
        r'\s*(?P<distance>\d{4})(?:MS|SS)\s+\((?P<condition>[^)]+)\)'
        r'(?:,\s*\$(?P<purse>[\d,]+))?',
        line_html,
        re.IGNORECASE | re.DOTALL,
    )
    sectional_match = re.search(
        r'<span class="bolded">\s*(?P<mile_rate>\d:\d{2}\.\d),\s*(?P<last_half>\d{2}\.\d)\s*</span>\s*\((?P<quarters>[^)]+)\)',
        line_html,
        re.IGNORECASE | re.DOTALL,
    )
    if not form_place_match or not results_match or not sectional_match:
        return None

    plain = _clean_spaces(re.sub(r"<[^>]+>", " ", line_html))
    comment = plain
    quarter_text = f"{sectional_match.group('mile_rate')}, {sectional_match.group('last_half')} ({sectional_match.group('quarters')})"
    if quarter_text in comment:
        comment = comment.split(quarter_text, 1)[-1].strip()

    raw_purse = results_match.group("purse")
    purse = float(raw_purse.replace(",", "")) if raw_purse else None
    line_nr_ceiling = _parse_line_nr_ceiling(line_html)
    sp_match = re.search(r'</a>\s*\$(\d+\.?\d*)(?:\s+fav)?[,\s]', line_html, re.IGNORECASE)
    run_sp = float(sp_match.group(1)) if sp_match else None
    age_match = re.search(r'(\d)yo', line_html, re.IGNORECASE)
    line_race_age = f"{age_match.group(1)}yo" if age_match else None
    return {
        "form_place": form_place_match.group("form_place"),
        "track_code": results_match.group("track"),
        "date": results_match.group("date"),
        "distance": results_match.group("distance"),
        "condition": results_match.group("condition"),
        "mile_rate": sectional_match.group("mile_rate"),
        "last_half": sectional_match.group("last_half"),
        "quarters": sectional_match.group("quarters"),
        "comment": comment,
        "line_text": plain,
        "purse": purse,
        "line_nr_ceiling": line_nr_ceiling,
        "line_race_age": line_race_age,
        "run_sp": run_sp,
    }


def _parse_line_nr_ceiling(line_html: str) -> int | None:
    """Extract the NR ceiling from a form-line HTML snippet.

    Handles all three formats used on harness.org.au:
      NR up to 45     → 45
      NR 40 to 43     → 43  (banded race, ceiling is upper bound)
      NR.45           → 45  (LTW format)
    Returns None for non-NR classes (Maiden, R-grades, etc.).
    """
    m = re.search(r'NR\s+up\s+to\s+(\d+)', line_html, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r'NR\s+(\d+)\s+to\s+(\d+)', line_html, re.IGNORECASE)
    if m:
        return int(m.group(2))
    m = re.search(r'NR\.(\d+)', line_html, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _normalize_compact_date(value: str) -> str:
    match = re.fullmatch(r"(\d{2})([A-Za-z]{3})(\d{2})", value)
    if not match:
        return value
    day, month, year = match.groups()
    year_full = f"20{year}"
    return f"{int(day)} {month.title()} {year_full}"


def _normalize_track_condition(value: str) -> str:
    upper = value.strip().upper()
    if upper in {"FAST", "GOOD", "FIRM"}:
        return "Good"
    if upper in {"SLOW", "HEAVY", "WET", "RAIN AFFECTED", "SOFT"}:
        return "Slow"
    return value.title().strip()


def _extract_form_race_purse(info_html: str) -> float | None:
    """Extract the total race purse from the raceMoreInfo section.

    The HTML looks like:
      <span class="race_prizemoney">$9,792 - 1st $5,184, ...</span>

    Returns the leading dollar amount as a float, e.g. 9792.0, or None.
    """
    match = re.search(r'<span class="race_prizemoney">\s*\$([\d,]+)', info_html, re.IGNORECASE)
    if not match:
        return None
    return float(match.group(1).replace(",", ""))


def _extract_form_bmr(horse_block: str) -> str | None:
    """Extract career BMR from a form page horse block.

    The HTML looks like:
      <span title="Best Mile Rate">BMR:</span> <span class="form_stats">TR1:57.1MS</span>

    Returns the raw value string e.g. "TR1:57.1MS", or None if absent.
    """
    match = re.search(
        r'<span title="Best Mile Rate">BMR:</span>\s*<span class="form_stats">([^<]+)</span>',
        horse_block,
        re.IGNORECASE,
    )
    return _clean_spaces(match.group(1)) if match else None


def _extract_form_bmr_dist_rge(horse_block: str) -> str | None:
    """Extract distance-range BMR from a form page horse block.

    The HTML looks like:
      <div class="form_stats_item" title="1609m to 1800m">BMRDistRge: <span class="form_stats">TR1:57.1MS</span></div>

    Returns the raw value string e.g. "TR1:57.1MS", or None if absent.
    """
    match = re.search(
        r'BMRDistRge:\s*<span class="form_stats">([^<]+)</span>',
        horse_block,
        re.IGNORECASE,
    )
    return _clean_spaces(match.group(1)) if match else None


def _extract_form_stats_summary(horse_block: str, label: str) -> tuple[int, int, int, int] | None:
    patterns = {
        "Career": r"Career:\s*<span class=\"form_stats\">([\d\-]+)</span>",
        "TS": r"<span title=\"This Season\">TS:</span>\s*<span class=\"form_stats\">([\d\-]+)</span>",
        "LS": r"<span title=\"Last Season\">LS:</span>\s*<span class=\"form_stats\">([\d\-]+)</span>",
        "DistRge": r"DistRge:\s*<span class=\"form_stats\">([\d\-]+)</span>",
    }
    pattern = patterns.get(label)
    if not pattern:
        return None
    match = re.search(pattern, horse_block, re.IGNORECASE)
    if not match:
        return None
    parts = match.group(1).split("-")
    if len(parts) != 4:
        return None
    try:
        return tuple(int(part) for part in parts)
    except ValueError:
        return None


def _parse_quarters(value: str) -> tuple[float | None, float | None, float | None, float | None]:
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 4:
        return None, None, None, None
    try:
        return tuple(float(part) for part in parts)  # type: ignore[return-value]
    except ValueError:
        return None, None, None, None


def _apply_form_line_text_rules(
    raw_comment: str,
    first_half: float | None,
    last_half: float | None,
    raw_margin: float | None,
) -> tuple[float, float, bool]:
    comment = raw_comment.lower()
    comment_adjustment = 0.0
    tempo_adjustment = 0.0
    null_run = False

    if "behind leader" in comment or "behind lead at bell" in comment:
        comment_adjustment += 7.5
    if "held up" in comment or "no clear run" in comment:
        comment_adjustment -= 7.5
    if "outside leader" in comment or "death seat" in comment:
        comment_adjustment -= 10.0
    if "3 wide no cover" in comment or "three wide no cover" in comment:
        comment_adjustment -= 10.0
    if "checked" in comment or "inconvenienced" in comment:
        if raw_margin is not None and raw_margin > 20.0:
            null_run = True
        else:
            comment_adjustment -= 10.0
    if "three wide early" in comment or "three wide middle" in comment or "3 wide early" in comment or "3 wide middle" in comment:
        comment_adjustment -= 5.0
    if "1 out 4 back" in comment or "1 out 5 back" in comment or "1 out 6 back" in comment:
        comment_adjustment -= 7.5

    if "flat tyre" in comment or "flat tire" in comment:
        if raw_margin is None or raw_margin <= 30.0:
            comment_adjustment -= 10.0

    if "contacted sulky" in comment:
        if raw_margin is not None and raw_margin > 20.0:
            null_run = True
        else:
            comment_adjustment -= 7.5

    if "locked wheels" in comment:
        null_run = True
    if (
        "checked and broke" in comment
        or "broke in score up" in comment
        or ("broke" in comment and "broke gear" not in comment and "broken gear" not in comment)
    ):
        null_run = True

    if first_half is not None and last_half is not None and abs(first_half - last_half) <= 2.0:
        tempo_adjustment = -1.5

    return comment_adjustment, tempo_adjustment, null_run


def _parse_form_place(value: str) -> int | None:
    plain = _clean_spaces(re.sub(r"<[^>]+>", " ", value))
    match = re.match(r"(\d+)-", plain)
    if match:
        return int(match.group(1))
    if "last win" in plain.lower():
        return 1
    return None


def _parse_recent_line_margin(value: str, finish_position: int | None) -> float | None:
    plain = _clean_spaces(value).lower()
    if finish_position == 1:
        return 0.0
    # Explicit numeric margin: "btn 9.9m"
    match = re.search(r"btn\s+([\d\.]+)m", plain)
    if match:
        return float(match.group(1))
    # Short-head margin codes immediately after "btn" — must be caught before the
    # fallback regex, which would otherwise match the track distance (e.g. 1980ms).
    match = re.search(r"btn\s+(shfhd|hfhd|sh|hd)\b", plain)
    if match:
        return 0.05 if match.group(1) == "shfhd" else 0.1
    # Structured line format: "$price, margin, wnr/Nth"
    match = re.search(r"\$\d+(?:\.\d{2})?,\s*([\d\.]+m|hd|hfhd|shfhd|sh)\s*,\s*(?:wnr|\d+(?:st|nd|rd|th))", plain)
    if match:
        plain = match.group(1)
    if plain in {"hd", "hfhd", "sh"}:
        return 0.1
    if plain == "shfhd":
        return 0.05
    # Last-resort fallback — capped at 50m to prevent track distances (e.g. 1609m,
    # 1980m) from being mistaken for margins when other patterns fail.
    match = re.search(r"([\d\.]+)m", plain)
    if match:
        val = float(match.group(1))
        return val if val <= 50.0 else None
    return None


def _is_excluded_race(race_name: str | None) -> bool:
    return bool(race_name and any(keyword in race_name.upper() for keyword in EXCLUDED_RACE_KEYWORDS))


def _parse_margin(value: str) -> float | None:
    if value in {"HD", "HFHD", "SH"}:
        return 0.1
    if value == "SHFHD":
        return 0.05
    match = re.match(r"^([\d\.]+)m$", value)
    return float(match.group(1)) if match else None


def _parse_stake(value: str) -> float | None:
    match = re.match(r"^\$([\d,]+)$", value)
    return float(match.group(1).replace(",", "")) if match else None


def _parse_price(value: str) -> float | None:
    match = re.match(r"^\$([\d]+\.[\d]{2})$", value)
    return float(match.group(1)) if match else None


def _parse_prizemoney(value: str) -> float | None:
    clean = re.sub(r"[$,\s]", "", value.strip())
    try:
        return float(clean) if clean else None
    except ValueError:
        return None


def _parse_results_price(value: str) -> float | None:
    match = re.search(r"\$\s*([\d]+\.[\d]{2})", value)
    return float(match.group(1)) if match else None


def _parse_results_margin(value: str) -> float | None:
    plain = value.strip().upper()
    if not plain:
        return None
    if plain in {"HD", "HFHD", "SH"}:
        return 0.1
    if plain == "SHFHD":
        return 0.05
    match = re.search(r"([\d]+(?:\.\d+)?)", plain)
    return float(match.group(1)) if match else None


def _apply_comment_codes(comment_codes: str, raw_margin: float | None = None) -> tuple[float, bool]:
    total = 0.0
    null_run = False
    for part in comment_codes.upper().split():
        if part == "RR":
            continue
        if part == "CTS":
            if raw_margin is not None and raw_margin > 20.0:
                null_run = True
            else:
                total -= 7.5
        elif part in NULL_RUN_CODES:
            null_run = True
        elif part in CODES:
            total += CODES[part]
        else:
            total += _decode_positional(part)
    return total, null_run


def _decode_positional(code: str) -> float:
    if re.fullmatch(r"1[2-9]", code):
        return -7.5 if int(code[1]) >= 4 else -10.0
    if re.fullmatch(r"[4-9]", code):
        return -7.5
    if re.fullmatch(r"[1-9][0-9]", code) and int(code) >= 10:
        return -7.5
    return 0.0


def _clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_results(results: list[ResultRunner]) -> list[ResultRunner]:
    seen: set[tuple[str, int, str]] = set()
    deduped: list[ResultRunner] = []
    for result in results:
        key = (result.meeting_code, result.race_number, result.horse_name.upper())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped


def parse_driver_page_html(html: str, driver_name: str) -> dict[str, object] | None:
    """Parse a driver profile page and return season/career win rate stats.

    Extracts from the statrow header divs:
      <div class="text-small">Season Win %</div><div>26%</div>
      <div class="text-small">Career Win %</div><div>18%</div>

    Also extracts current-season starts/wins from the season stats table.
    Returns None if neither rate can be found.
    """
    season_win_rate = _extract_driver_stat_pct(html, "Season Win %")
    career_win_rate = _extract_driver_stat_pct(html, "Career Win %")
    if season_win_rate is None and career_win_rate is None:
        return None

    season_starts, season_wins = _extract_driver_season_stats(html)
    return {
        "driver_name": driver_name,
        "season_starts": season_starts,
        "season_wins": season_wins,
        "season_win_rate": season_win_rate,
        "career_win_rate": career_win_rate,
    }


def parse_trainer_page_html(html: str, trainer_name: str) -> dict[str, object] | None:
    season_win_rate = _extract_profile_stat_pct(html, "Season Win %")
    career_win_rate = _extract_profile_stat_pct(html, "Career Win %")
    if season_win_rate is None and career_win_rate is None:
        return None

    season_starts, season_wins = _extract_profile_season_stats(html)
    return {
        "trainer_name": trainer_name,
        "season_starts": season_starts,
        "season_wins": season_wins,
        "season_win_rate": season_win_rate,
        "career_win_rate": career_win_rate,
    }


def parse_trainer_links_from_fields_html(html: str) -> dict[str, str]:
    links: dict[str, str] = {}
    for match in re.finditer(
        r'<div class="trainer">.*?<a href="(?P<link>[^"]+/racing/trainerlink/[^"]+|/racing/trainerlink/[^"]+|[^"]+/racing/trainers/[^"]+|/racing/trainers/[^"]+)">(?P<name>[^<]+)</a>',
        html,
        re.IGNORECASE | re.DOTALL,
    ):
        name = _clean_spaces(match.group("name"))
        if not name:
            continue
        links[_normalize_person_key(name)] = match.group("link")
    return links


def _extract_profile_stat_pct(html: str, label: str) -> float | None:
    # Rendered HTML has whitespace inside the value div, so match loosely after the label
    pattern = re.escape(label) + r".*?(\d+)%"
    m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
    return int(m.group(1)) / 100.0 if m else None


def _extract_driver_stat_pct(html: str, label: str) -> float | None:
    return _extract_profile_stat_pct(html, label)


def _extract_profile_season_stats(html: str) -> tuple[int | None, int | None]:
    # Find the season table (Season / Starts / Wins / Places / Stakes)
    m = re.search(r"<th[^>]*>Season</th>.*?<tbody>(.*?)</tbody>", html, re.DOTALL | re.IGNORECASE)
    if not m:
        return None, None
    tbody = m.group(1)
    row_m = re.search(r"<tr>(.*?)</tr>", tbody, re.DOTALL | re.IGNORECASE)
    if not row_m:
        return None, None
    cells = re.findall(r"<td[^>]*>([^<]+)</td>", row_m.group(1))
    if len(cells) < 3:
        return None, None
    starts_m = re.search(r"(\d[\d,]*)", cells[1])
    wins_m = re.search(r"(\d[\d,]*)", cells[2])
    starts = int(starts_m.group(1).replace(",", "")) if starts_m else None
    wins = int(wins_m.group(1).replace(",", "")) if wins_m else None
    return starts, wins


def _extract_driver_season_stats(html: str) -> tuple[int | None, int | None]:
    return _extract_profile_season_stats(html)


def _normalize_person_key(name: str) -> str:
    return " ".join(name.upper().split())
