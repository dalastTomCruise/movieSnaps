"""
discover_movies.py — pulls all available movies from movie-screencaps.com sitemap.

Usage: poetry run python3 discover_movies.py
"""

import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}


def get_sitemap_urls(url: str) -> list[str]:
    resp = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(resp.text, "xml")
    return [loc.text.strip() for loc in soup.find_all("loc")]


def get_all_movies() -> list[str]:
    # Get sub-sitemaps
    index_urls = get_sitemap_urls("https://movie-screencaps.com/sitemap-index-1.xml")
    print(f"Found {len(index_urls)} sub-sitemaps")

    movie_urls = []
    for sitemap_url in index_urls:
        print(f"Fetching: {sitemap_url}")
        urls = get_sitemap_urls(sitemap_url)
        # Filter to movie pages only — must have exactly one path segment ending in a year
        for u in urls:
            parts = u.rstrip("/").split("/")
            if len(parts) == 4 and parts[3] and any(c.isdigit() for c in parts[3]):
                movie_urls.append(u)

    return movie_urls


if __name__ == "__main__":
    urls = get_all_movies()
    print(f"\nTotal movie pages found: {len(urls)}")

    with open("available_movies.txt", "w") as f:
        for url in urls:
            slug = url.rstrip("/").split("/")[-1]
            f.write(f"{url}\n")
    print(f"Saved {len(urls)} movie URLs to available_movies.txt")
