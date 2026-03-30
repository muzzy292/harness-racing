"""
Harness Batch Horse Profile Fetcher
=====================================
Reads a saved race form HTML file, extracts every horse ID,
then fetches each horse's full profile page from harness.org.au.

All horse pages are saved to a folder on your Desktop ready
to upload to Claude for full-form analysis.

FIRST-TIME SETUP (one time only):
1. Make sure Python is installed
2. Open Terminal / Command Prompt and run:
      pip install playwright
      py -m playwright install chromium   (Windows)
      python3 -m playwright install chromium  (Mac)

HOW TO USE:
1. First fetch a race form page using fetch_race_v2.py
2. Double-click this script
3. Enter the path to your saved race HTML file when prompted
      (or just drag the file onto the terminal window)
4. Optionally enter a race number to fetch only that race's horses
      (leave blank to fetch all horses in the meeting)
5. Wait — it fetches each horse page one by one
6. Upload the output folder to Claude for analysis

OUTPUT:
  Desktop/horses_MEETINGCODE_TIMESTAMP/
      TOLD_YOU_TWICE_807062.html
      RAKADAN_NZ_835136.html
      MAGIC_JOE_814213.html
      ... etc
"""

import os
import re
import sys
import time
from datetime import datetime


def find_race_html_files():
    """Look for recently saved race HTML files on the Desktop."""
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    files = [f for f in os.listdir(desktop) if f.startswith("race_") and f.endswith(".html")]
    files.sort(reverse=True)
    return [os.path.join(desktop, f) for f in files]


def extract_horses(html, race_number=None):
    """Extract horse ID + name pairs. Optionally filter to a specific race number."""
    
    # Get all horse name + ID pairs
    pattern = r'horseId=(\d+)\">([A-Z][A-Z0-9\s\-]+)</a>'
    all_matches = re.findall(pattern, html)
    
    if not all_matches:
        return []

    if race_number is None:
        return [(hid, name.strip()) for hid, name in all_matches]

    # Filter to a specific race by finding the race block
    race_markers = [m.start() for m in re.finditer(r'Race\s+\d+', html)]
    
    if race_number > len(race_markers):
        print(f"\n  Race {race_number} not found. Found {len(race_markers)} races.")
        return [(hid, name.strip()) for hid, name in all_matches]

    start_idx = race_markers[race_number - 1]
    end_idx = race_markers[race_number] if race_number < len(race_markers) else len(html)
    race_html = html[start_idx:end_idx]

    race_matches = re.findall(pattern, race_html)
    return [(hid, name.strip()) for hid, name in race_matches]


def fetch_horse_page(page, horse_id, horse_name):
    """Fetch a single horse profile page using an already-open Playwright page."""
    url = f"https://www.harness.org.au/racing/horse-search/?horseId={horse_id}"
    
    try:
        page.goto(url, timeout=25000, wait_until="domcontentloaded")
        page.wait_for_timeout(3500)
        
        # Wait for the performance table to load
        try:
            page.wait_for_selector("table", timeout=8000)
            page.wait_for_timeout(1000)
        except Exception:
            pass
        
        html = page.content()
        
        # Check we got real content
        if "Performance Records" in html or "Career" in html:
            return html
        else:
            return html  # Return anyway, let user decide

    except Exception as e:
        print(f"      Error: {e}")
        return None


def save_horse_file(html, horse_id, horse_name, output_dir):
    """Save a horse page to the output folder."""
    safe_name = re.sub(r'[^A-Z0-9]', '_', horse_name.upper())
    safe_name = re.sub(r'_+', '_', safe_name).strip('_')
    filename = f"{safe_name}_{horse_id}.html"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)
    
    return filepath


def get_meeting_code(filepath):
    """Try to extract meeting code from filename."""
    name = os.path.basename(filepath)
    match = re.search(r'race_([A-Z0-9]+)_', name)
    return match.group(1) if match else "meeting"


def main():
    print("=" * 58)
    print("  Harness Batch Horse Profile Fetcher")
    print("=" * 58)
    print()

    # Find race HTML files on Desktop
    recent_files = find_race_html_files()
    
    if recent_files:
        print("  Recent race files found on your Desktop:")
        for i, f in enumerate(recent_files[:5], 1):
            print(f"    {i}. {os.path.basename(f)}")
        print()
        print("  Enter the number above, or paste a full file path:")
    else:
        print("  No race files found on Desktop.")
        print("  Paste the full path to your saved race HTML file:")
    
    print()
    user_input = input("  > ").strip().strip('"').strip("'")

    # Handle numeric shortcut
    if user_input.isdigit() and recent_files:
        idx = int(user_input) - 1
        if 0 <= idx < len(recent_files):
            race_file = recent_files[idx]
        else:
            print("\n  Invalid selection.")
            input("\n  Press Enter to close...")
            sys.exit()
    elif os.path.exists(user_input):
        race_file = user_input
    else:
        print(f"\n  File not found: {user_input}")
        input("\n  Press Enter to close...")
        sys.exit()

    print(f"\n  Using: {os.path.basename(race_file)}")

    # Read the race HTML
    with open(race_file, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    # Ask for race number filter
    print()
    print("  Enter a race number to fetch only that race (e.g. 2),")
    print("  or press Enter to fetch ALL horses in the meeting:")
    race_input = input("  Race number: ").strip()
    
    race_number = None
    if race_input.isdigit():
        race_number = int(race_input)

    # Extract horses
    horses = extract_horses(html, race_number)
    
    if not horses:
        print("\n  No horse IDs found in the file.")
        input("\n  Press Enter to close...")
        sys.exit()

    scope = f"Race {race_number}" if race_number else "full meeting"
    print(f"\n  Found {len(horses)} horses in {scope}:")
    for hid, name in horses:
        print(f"    {name} ({hid})")

    # Create output folder on Desktop
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    meeting_code = get_meeting_code(race_file)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"horses_{meeting_code}_R{race_number}_{timestamp}" if race_number else f"horses_{meeting_code}_{timestamp}"
    output_dir = os.path.join(desktop, folder_name)
    os.makedirs(output_dir, exist_ok=True)

    print(f"\n  Output folder: {folder_name}")
    print()

    # Import Playwright
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  Playwright is not installed.")
        print("  Run:  pip install playwright")
        print("  Then: py -m playwright install chromium")
        input("\n  Press Enter to close...")
        sys.exit()

    # Fetch all horse pages
    success = 0
    failed = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        for i, (horse_id, horse_name) in enumerate(horses, 1):
            print(f"  [{i}/{len(horses)}] {horse_name}...", end=" ", flush=True)
            
            horse_html = fetch_horse_page(page, horse_id, horse_name)
            
            if horse_html:
                save_horse_file(horse_html, horse_id, horse_name, output_dir)
                print("done")
                success += 1
            else:
                print("FAILED")
                failed.append(horse_name)
            
            # Polite delay between requests
            if i < len(horses):
                time.sleep(1.5)

        browser.close()

    print()
    print(f"  Completed: {success}/{len(horses)} horses fetched successfully.")
    
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    
    print()
    print(f"  Files saved to:")
    print(f"  {output_dir}")
    print()
    print("  Upload the entire folder (or individual files) to Claude.")
    print()
    input("  Press Enter to close...")


if __name__ == "__main__":
    main()
