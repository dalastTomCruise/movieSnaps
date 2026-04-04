# scraper.py — search, page discovery, image extraction

import logging
import random
import time
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

from config import BASE_URL, DEFAULT_PAGES_TO_SCRAPE, REQUEST_DELAY_SECONDS, USER_AGENT

logger = logging.getLogger(__name__)


@dataclass
class MovieEntry:
    title: str
    url: str
    movie_id: str  # slugified, e.g. "inception-2010"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _get(session: requests.Session, url: str) -> BeautifulSoup:
    logger.info(f"GET {url}")
    resp = session.get(url, timeout=15)
    logger.info(f"  -> {resp.status_code}")
    resp.raise_for_status()
    time.sleep(REQUEST_DELAY_SECONDS)
    return BeautifulSoup(resp.text, "html.parser")


def search(query: str) -> list[MovieEntry]:
    """Search movie-screencaps.com and return matching movie entries."""
    session = _session()
    soup = _get(session, f"{BASE_URL}/?s={requests.utils.quote(query)}")

    results = []
    for article in soup.find_all("article"):
        a = article.find("a", href=True)
        img = article.find("img", alt=True)
        if not a or not img:
            continue
        title = img["alt"].strip()
        url = a["href"].rstrip("/") + "/"
        # Skip generic homepage links or empty titles
        if not title or url in (f"{BASE_URL}/", BASE_URL):
            continue
        slug = url.rstrip("/").split("/")[-1]
        results.append(MovieEntry(title=title, url=url, movie_id=slug))

    if not results:
        logger.warning(f"No results found for query: {query!r}")
    return results


def get_total_pages(movie_url: str) -> int:
    """Discover how many screencap pages exist for a movie."""
    session = _session()
    soup = _get(session, movie_url)

    # Pagination links — find the highest page number
    page_nums = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/page/" in href:
            try:
                num = int(href.rstrip("/").split("/page/")[-1])
                page_nums.append(num)
            except ValueError:
                pass

    return max(page_nums) if page_nums else 1


def sample_pages(total_pages: int, n: int = DEFAULT_PAGES_TO_SCRAPE) -> list[int]:
    """
    Sample pages spread evenly across the full range.
    Divides total pages into n equal buckets and picks one random page per bucket.
    This guarantees pages are well spread out and not clustered together.
    """
    if total_pages <= n:
        return list(range(1, total_pages + 1))
    bucket_size = total_pages // n
    pages = []
    for i in range(n):
        bucket_start = i * bucket_size + 1
        bucket_end = bucket_start + bucket_size - 1
        pages.append(random.randint(bucket_start, min(bucket_end, total_pages)))
    return pages


def get_image_urls(movie_url: str, page: int) -> list[str]:
    """Extract all screencap image URLs from a given page."""
    session = _session()
    url = f"{movie_url.rstrip('/')}/page/{page}/" if page > 1 else movie_url
    soup = _get(session, url)

    images = []
    for img in soup.find_all("img", src=True):
        src = img["src"]
        # Strip CDN thumbnail param to get full resolution
        src = src.split("?")[0]
        # Only grab actual screencap images from the CDN
        if "caps.b-cdn.net" in src and src.lower().endswith((".jpg", ".jpeg")):
            images.append(src)

    logger.info(f"  Found {len(images)} images on page {page}")
    return images
