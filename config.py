# config.py — all tunables in one place

AWS_REGION = "us-east-1"
S3_BUCKET = "movie-screencaps-game"
DYNAMO_TABLE = "movies"
BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

# Scraper settings
DEFAULT_PAGES_TO_SCRAPE = 10
TARGET_SCREENCAP_COUNT = 10
REQUEST_DELAY_SECONDS = 1.0
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

BASE_URL = "https://movie-screencaps.com"

# Image rejection rules passed to Claude
REJECTION_RULES = [
    "Title cards or text overlays showing the movie name",
    "End credits or opening credits",
    "Iconic or meme-famous scenes strongly associated with this film",
    "ANY human face visible, whether close-up or in the background — no faces at all",
    "Distinctive costumes or props that are strongly associated with this specific film",
]
