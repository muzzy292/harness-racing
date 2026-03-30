"""
Harness Race & Horse Page Fetcher v2
=====================================
Uses a headless browser (Playwright) to fetch pages that require JavaScript,
including horse profile pages from harness.org.au.

FIRST-TIME SETUP (one time only):
1. Make sure Python is installed (see README from v1)
2. Open Terminal / Command Prompt and run these two commands:
      pip install playwright
      python -m playwright install chromium
3. That's it — you only need to do this once.

HOW TO USE:
1. Double-click this file to run it
2. Paste in any harness.org.au URL when prompted
3. Press Enter — it saves an HTML file to your Desktop
4. Upload that file here for analysis

Works with:
  - Race form guides:  https://www.harness.org.au/form.cfm?mc=...
  - Horse profiles:    https://www.harness.org.au/racing/horse-search/?horseId=...
  - Any other page on the site
"""

import os
import sys
from datetime import datetime


def fetch_with_playwright(url):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("\n  Playwright is not installed.")
        print("  Please run these commands in Terminal / Command Prompt:")
        print()
        print("      pip install playwright")
        print("      python -m playwright install chromium")
        print()
        input("  Press Enter to close...")
        sys.exit()

    print("\n  Launching browser...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print("  Loading page (this may take a few seconds)...")
        try:
            page.goto(url, timeout=45000, wait_until="domcontentloaded")
        except Exception as e:
            print(f"\n  Could not load page: {e}")
            browser.close()
            return None

        # Wait for content to render
        print("  Waiting for content to render...")
        page.wait_for_timeout(6000)

        # Extra wait for horse/driver profile pages
        if "horse-search" in url or "horseId" in url or "drivers" in url or "driverlink" in url:
            try:
                page.wait_for_selector("table", timeout=12000)
                page.wait_for_timeout(2000)
            except Exception:
                pass

        html = page.content()
        browser.close()
        return html


def save_file(html, url):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Build filename from URL
    if "horseId=" in url:
        horse_id = url.split("horseId=")[-1].split("&")[0]
        filename = f"horse_{horse_id}_{timestamp}.html"
    elif "mc=" in url:
        mc = url.split("mc=")[-1].split("&")[0]
        filename = f"race_{mc}_{timestamp}.html"
    elif "/drivers/" in url or "/driverlink/" in url:
        # Extract driver name from URL slug
        slug = url.rstrip("/").split("/")[-1]
        filename = f"driver_{slug}_{timestamp}.html"
    else:
        filename = f"harness_{timestamp}.html"

    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    if not os.path.exists(desktop):
        desktop = os.path.expanduser("~")

    filepath = os.path.join(desktop, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

    return filepath


def main():
    print("=" * 55)
    print("  Harness Page Fetcher v2 (JavaScript-enabled)")
    print("=" * 55)
    print()
    print("  Paste the URL below and press Enter.")
    print()
    print("  Examples:")
    print("  Race:  https://www.harness.org.au/form.cfm?mc=LN290326")
    print("  Horse: https://www.harness.org.au/racing/horse-search/?horseId=826028")
    print()

    url = input("  URL: ").strip()

    if not url:
        print("\n  No URL entered. Exiting.")
        input("\n  Press Enter to close...")
        sys.exit()

    if not url.startswith("http"):
        url = "https://" + url

    html = fetch_with_playwright(url)

    if html and len(html) > 2000:
        filepath = save_file(html, url)
        print(f"\n  Done! File saved to:")
        print(f"  {filepath}")
        print()
        print("  Upload that file to Claude for analysis.")
    else:
        print("\n  Page loaded but content appears empty or too short.")
        print("  The page may require a login or have extra protection.")
        if html:
            # Save anyway so user can inspect
            filepath = save_file(html, url)
            print(f"  Saved what we got to: {filepath}")

    print()
    input("  Press Enter to close...")


if __name__ == "__main__":
    main()
