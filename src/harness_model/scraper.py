from __future__ import annotations

import time
from pathlib import Path


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def fetch_rendered_html(
    url: str,
    wait_ms: int = 5000,
    retries: int = 3,
    backoff_s: float = 3.0,
    wait_selector: str | None = None,
    selector_timeout_ms: int = 20000,
) -> str:
    """Fetch a JS-rendered page via headless Chromium.

    Retries transient failures (navigation timeouts, browser launch hiccups)
    with linear backoff so unattended cloud runs survive a flaky network or a
    momentary block. Raises after the final attempt.

    wait_selector: if set, wait for this CSS selector to appear (up to
    selector_timeout_ms) before capturing — more reliable than a fixed delay for
    content that loads late (e.g. a results table that renders slowly). Missing
    selector is non-fatal: it falls through to capture whatever rendered so the
    caller can validate/retry.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is required. Install with 'pip install playwright' "
            "and 'python -m playwright install chromium'."
        ) from exc

    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                context = browser.new_context(user_agent=USER_AGENT)
                page = context.new_page()
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                if wait_selector:
                    try:
                        page.wait_for_selector(wait_selector, timeout=selector_timeout_ms)
                    except Exception:  # noqa: BLE001 — selector never appeared; capture anyway
                        pass
                page.wait_for_timeout(wait_ms)
                html = page.content()
                browser.close()
                return html
        except Exception as exc:  # noqa: BLE001 — transient nav/launch errors
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(
        f"fetch_rendered_html failed for {url} after {retries} attempts: {last_exc}"
    ) from last_exc


def fetch_hrnsw_results_search_html(track_value: str, wait_ms: int = 5000) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is required. Install with 'pip install playwright' "
            "and 'python -m playwright install chromium'."
        ) from exc

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()
        page.goto(build_hrnsw_results_index_url(), timeout=45000, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)
        page.select_option("#ContentPlaceHolderMain_ContentPlaceHolderContent_ddlSearchTrack", value=str(track_value))
        page.click("#ContentPlaceHolderMain_ContentPlaceHolderContent_btnSearch")
        page.wait_for_timeout(wait_ms)
        html = page.content()
        browser.close()
        return html


def save_html(html: str, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    return output


def build_meeting_url(meeting_code: str) -> str:
    return f"https://www.harness.org.au/form.cfm?mc={meeting_code}"


def build_hrnsw_results_index_url() -> str:
    return "https://www.hrnsw.com.au/racing/results"


def build_horse_url(horse_id: str) -> str:
    return f"https://www.harness.org.au/racing/horse-search/?horseId={horse_id}"


def build_results_url(meeting_code: str) -> str:
    return f"https://www.harness.org.au/racing/fields/race-fields/?mc={meeting_code}"


def build_fields_url(meeting_code: str) -> str:
    return f"https://www.harness.org.au/racing/fields/race-fields/?mc={meeting_code}"


def build_driver_url(driver_name: str) -> str:
    slug = driver_name.lower().strip().replace(" ", "-")
    return f"https://www.harness.org.au/racing/drivers/{slug}/"


def driver_name_to_slug(driver_name: str) -> str:
    return driver_name.lower().strip().replace(" ", "-")


def build_trainer_url(trainer_name: str) -> str:
    slug = trainer_name.lower().strip().replace(" ", "-")
    return f"https://www.harness.org.au/racing/trainers/{slug}/"


def trainer_name_to_slug(trainer_name: str) -> str:
    return trainer_name.lower().strip().replace(" ", "-")


def is_rate_limited_html(html: str) -> bool:
    lowered = html.lower()
    return "rate limit exceeded" in lowered or "access denied" in lowered


def is_valid_meeting_html(html: str) -> bool:
    lowered = html.lower()
    if is_rate_limited_html(html):
        return False
    if "an error has occurred" in lowered:
        return False
    return 'class="racefieldtable"' in lowered or "horse-search/?horseid=" in lowered


def is_valid_results_html(html: str) -> bool:
    """True if the page actually contains a results table.

    The results table (class "raceFieldTable resultTable") only appears once a
    race is official and rendered. A fields-only or not-yet-rendered page has no
    resultTable, so parsing it would silently yield zero results — this lets the
    fetcher reject/retry such pages instead of ingesting nothing.
    """
    lowered = html.lower()
    if is_rate_limited_html(html):
        return False
    return "resulttable" in lowered


def is_valid_horse_html(html: str) -> bool:
    lowered = html.lower()
    if is_rate_limited_html(html):
        return False
    if "an error has occurred" in lowered:
        return False
    return "performance records" in lowered or "best winning mile rate" in lowered


def is_valid_driver_html(html: str) -> bool:
    lowered = html.lower()
    if is_rate_limited_html(html):
        return False
    return "season win %" in lowered or "career win %" in lowered


def is_valid_trainer_html(html: str) -> bool:
    lowered = html.lower()
    if is_rate_limited_html(html):
        return False
    return "season win %" in lowered or "career win %" in lowered
