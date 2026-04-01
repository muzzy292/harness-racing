from __future__ import annotations

from pathlib import Path


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def fetch_rendered_html(url: str, wait_ms: int = 5000) -> str:
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
        page.goto(url, timeout=45000, wait_until="domcontentloaded")
        page.wait_for_timeout(wait_ms)
        html = page.content()
        browser.close()
        return html


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
