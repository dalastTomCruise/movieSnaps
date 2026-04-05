"""
verify_movies.py — checks each URL in available_movies.txt to confirm
images can be fetched. Writes verified URLs to verified_available_movies.txt.

Usage: poetry run python3 verify_movies.py
"""

import time
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
INPUT_FILE = "available_movies.txt"
OUTPUT_FILE = "verified_available_movies.txt"
DELAY = 0.5  # seconds between requests to be polite


def has_images(url: str) -> bool:
    """Returns True if the movie page has at least one screencap image."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return False
        soup = BeautifulSoup(resp.text, "html.parser")
        for img in soup.find_all("img", src=True):
            src = img["src"].split("?")[0]
            if "b-cdn.net" in src and src.lower().endswith((".jpg", ".jpeg")):
                return True
        return False
    except Exception:
        return False


def main():
    with open(INPUT_FILE) as f:
        urls = [l.strip() for l in f if l.strip() and not l.strip().endswith(",processed")]

    print(f"Verifying {len(urls)} URLs...")

    verified = []
    failed = []

    for i, url in enumerate(urls, 1):
        ok = has_images(url)
        status = "✅" if ok else "❌"
        print(f"[{i}/{len(urls)}] {status} {url}")
        if ok:
            verified.append(url)
        else:
            failed.append(url)
        time.sleep(DELAY)

    with open(OUTPUT_FILE, "w") as f:
        for url in verified:
            f.write(url + "\n")

    print(f"\nDone. {len(verified)} verified, {len(failed)} failed.")
    print(f"Verified URLs saved to {OUTPUT_FILE}")
    if failed:
        print(f"\nFailed URLs:")
        for u in failed:
            print(f"  {u}")


if __name__ == "__main__":
    main()
