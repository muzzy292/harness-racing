"""
Harness Driver Profile Fetcher
================================
Reads a race form HTML file, extracts all driver names,
constructs their profile URLs, and saves each driver page.

Driver URL format: https://www.harness.org.au/racing/drivers/[firstname-lastname]/

HOW TO USE:
1. Run fetch_race_v2.py first to save the race form page
2. Run this script
3. Select your saved race form file
4. Files saved to Desktop/drivers_MEETINGCODE_TIMESTAMP/
5. Upload the folder here alongside your horse profiles

FIRST-TIME SETUP (one time only):
    pip install playwright
    py -m playwright install chromium   (Windows)
    python3 -m playwright install chromium  (Mac)
"""

import os
import re
import sys
import time
from datetime import datetime
from html.parser import HTMLParser


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style', 'nav', 'header', 'footer'):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ('script', 'style', 'nav', 'header', 'footer'):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            s = data.strip()
            if s:
                self.text.append(s)


def driver_name_to_slug(name):
    """'Seaton Grima' → 'seaton-grima'. Abbreviated names return None."""
    name = name.strip()
    parts = name.split()
    if len(parts) < 2 or len(parts[0]) <= 2:
        return None
    return '-'.join(p.lower() for p in parts if len(p) > 1)


def extract_drivers_from_race_form(filepath):
    """
    Extract full driver names from driverlink anchors in a race form HTML.
    Returns list of (full_name, slug) tuples, deduplicated.
    """
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        html = f.read()

    # Full names linked via driverlink hrefs
    link_names = re.findall(
        r'href=["\'](?:https?://www\.harness\.org\.au)?/racing/driverlink/[A-Z0-9]+["\']'
        r'[^>]*>\s*([A-Z][a-z]+(?:\s[A-Z][a-z]+)+)\s*</a>',
        html
    )

    seen = {}
    for name in link_names:
        name = name.strip()
        if name and name not in seen:
            slug = driver_name_to_slug(name)
            if slug:
                seen[name] = slug

    return [(name, slug) for name, slug in seen.items()]


def fetch_driver_page(playwright_page, slug):
    url = f'https://www.harness.org.au/racing/drivers/{slug}/'
    try:
        playwright_page.goto(url, timeout=45000, wait_until='domcontentloaded')
        playwright_page.wait_for_timeout(5000)
        try:
            playwright_page.wait_for_selector('table', timeout=10000)
            playwright_page.wait_for_timeout(1500)
        except Exception:
            pass
        return playwright_page.content(), url
    except Exception as e:
        return None, url


def is_valid_driver_page(html):
    return (html and len(html) > 5000
            and ('Latest Drives' in html or 'Career Win' in html))


def find_race_files(desktop):
    files = [f for f in os.listdir(desktop)
             if f.startswith('race_') and f.endswith('.html')]
    files.sort(reverse=True)
    return [os.path.join(desktop, f) for f in files]


def get_meeting_code(filepath):
    m = re.search(r'race_([A-Z0-9]+)_', os.path.basename(filepath))
    return m.group(1) if m else 'meeting'


def main():
    print('=' * 58)
    print('  Harness Driver Profile Fetcher')
    print('=' * 58)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print('\n  Playwright not installed.')
        print('  Run:  pip install playwright')
        print('  Then: py -m playwright install chromium')
        input('\n  Press Enter to close...')
        sys.exit()

    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
    recent  = find_race_files(desktop)

    if recent:
        print('\n  Recent race files on Desktop:')
        for i, f in enumerate(recent[:5], 1):
            print(f'    {i}. {os.path.basename(f)}')
        print()
        user_input = input('  Enter number or paste path: ').strip().strip('"')
        if user_input.isdigit() and 1 <= int(user_input) <= len(recent):
            race_file = recent[int(user_input) - 1]
        elif os.path.isfile(user_input):
            race_file = user_input
        else:
            print('  File not found.')
            input('\n  Press Enter to close...')
            sys.exit()
    else:
        race_file = input('  Paste path to race form file: ').strip().strip('"')
        if not os.path.isfile(race_file):
            print('  File not found.')
            input('\n  Press Enter to close...')
            sys.exit()

    print(f'\n  Using: {os.path.basename(race_file)}')

    drivers = extract_drivers_from_race_form(race_file)
    if not drivers:
        print('\n  No full driver names found.')
        print('  Make sure this is a race form or fields/results page.')
        input('\n  Press Enter to close...')
        sys.exit()

    print(f'\n  Found {len(drivers)} drivers:')
    for name, slug in drivers:
        print(f'    {name:<28} → racing/drivers/{slug}/')

    mc          = get_meeting_code(race_file)
    timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir  = os.path.join(desktop, f'drivers_{mc}_{timestamp}')
    os.makedirs(output_dir, exist_ok=True)

    print(f'\n  Saving to: drivers_{mc}_{timestamp}')
    print()

    success = 0
    failed  = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        for i, (name, slug) in enumerate(drivers, 1):
            print(f'  [{i}/{len(drivers)}] {name}...', end=' ', flush=True)
            html, url = fetch_driver_page(page, slug)

            if html and is_valid_driver_page(html):
                path = os.path.join(output_dir, f'driver_{slug}.html')
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(html)
                print('done')
                success += 1
            else:
                print('FAILED')
                failed.append(name)

            if i < len(drivers):
                time.sleep(1.5)

        browser.close()

    print()
    print(f'  Completed: {success}/{len(drivers)} drivers fetched.')
    if failed:
        print(f'  Failed: {", ".join(failed)}')
    print(f'\n  Files saved to: {output_dir}')
    print('  Upload folder to Claude alongside horse profiles.')
    print()
    input('  Press Enter to close...')


if __name__ == '__main__':
    main()
