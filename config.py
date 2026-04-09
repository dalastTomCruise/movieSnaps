# config.py — all tunables in one place

AWS_REGION = "us-east-1"
S3_BUCKET = "movie-screencaps-game"
DYNAMO_TABLE = "movies"
LEADERBOARD_TABLE = "leaderboard"

BEDROCK_MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"


# Set to True to log rejection/approval reasons from Claude (slower, for debugging)
VERBOSE_AGENT = True

# Scraper settings
DEFAULT_PAGES_TO_SCRAPE = 10
MAX_EVALUATION_SECONDS = 900  # 15 minutes — write whatever was approved and move on
MAX_IMAGE_ERRORS = 50  # stop evaluating if too many CDN 404s
TARGET_SCREENCAP_COUNT = 5
SPREAD_INDEXES_PER_PAGE = 4  # images sampled per page, evenly spread across the page timeline
IMAGES_TO_SHOW_CAP = 10  # max images shown to player per movie
REQUEST_DELAY_SECONDS = 0.5
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
