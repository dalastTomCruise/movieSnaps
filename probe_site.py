"""
Quick probe to check if movie-screencaps.com search results
are server-side rendered (SSR) or require JavaScript.

Run: python probe_site.py
"""

import requests
from bs4 import BeautifulSoup

SEARCH_QUERY = "inception"
SEARCH_URL = f"https://movie-screencaps.com/?s={SEARCH_QUERY}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def probe():
    print(f"Fetching: {SEARCH_URL}\n")
    resp = requests.get(SEARCH_URL, headers=HEADERS, timeout=15)
    print(f"Status code: {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', 'unknown')}\n")

    soup = BeautifulSoup(resp.text, "html.parser")

    # Look for common result containers
    # Adjust selectors based on what we find
    candidates = {
        "article tags": soup.find_all("article"),
        "h2 tags": soup.find_all("h2"),
        "a tags with href containing movie": [
            a for a in soup.find_all("a", href=True)
            if "movie-screencaps.com" in a["href"] and a.get_text(strip=True)
        ],
    }

    for label, results in candidates.items():
        print(f"[{label}]: found {len(results)}")
        for item in results[:3]:  # show first 3 of each
            if hasattr(item, "get_text"):
                print(f"  -> {item.get_text(strip=True)[:80]}")

    print("\n--- Raw HTML snippet (first 2000 chars) ---")
    print(resp.text[:2000])

    # Verdict
    total_hits = sum(len(v) for v in candidates.values())
    print("\n--- VERDICT ---")
    if total_hits > 0:
        print("Results appear to be in the raw HTML. requests + BeautifulSoup should work.")
    else:
        print("No results found in raw HTML. JavaScript rendering (playwright) may be needed.")


if __name__ == "__main__":
    probe()
