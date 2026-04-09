# agent.py — Bedrock/Claude for metadata enrichment and image selection

import json
import logging
import base64
import io
import time
from dataclasses import dataclass, field

import boto3
import requests
from PIL import Image

from config import AWS_REGION, BEDROCK_MODEL_ID, VERBOSE_AGENT, MAX_EVALUATION_SECONDS, MAX_IMAGE_ERRORS

logger = logging.getLogger(__name__)

_bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)


@dataclass
class ImageEvaluation:
    index: int
    url: str
    approved: bool
    reason: str


@dataclass
class BatchResult:
    batch_num: int
    total_images: int
    evaluations: list[ImageEvaluation] = field(default_factory=list)


@dataclass
class SelectionResult:
    movie_title: str
    total_candidates: int
    approved_urls: list[str] = field(default_factory=list)

    def print_summary(self):
        logger.info(f"\nFinal: {len(self.approved_urls)}/{self.total_candidates} approved")


def _invoke(messages: list[dict], system: str) -> str:
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 8192,
        "system": system,
        "messages": messages,
    })
    resp = _bedrock.invoke_model(modelId=BEDROCK_MODEL_ID, body=body)
    result = json.loads(resp["body"].read())
    return result["content"][0]["text"]


def get_movie_metadata(title: str) -> dict:
    system = "You are a film encyclopedia. Return only valid JSON, no markdown."
    prompt = (
        f"Provide metadata for the movie \"{title}\".\n"
        "Return a JSON object with these exact keys:\n"
        "  title (string), year (integer), cast (list of top 5 actor name strings), "
        "genres (list of genre strings), synopsis (one sentence string).\n"
        "If you are unsure about any field, use null."
    )
    raw = _invoke([{"role": "user", "content": prompt}], system)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Claude returned non-JSON metadata, attempting extraction")
        start, end = raw.find("{"), raw.rfind("}") + 1
        return json.loads(raw[start:end]) if start != -1 else {}


def get_similar_movies(metadata: dict) -> list[str]:
    """Ask Haiku for 3 similar movies based on style, decade, genre, and cast."""
    title = metadata.get("title", "Unknown")
    year = metadata.get("year", "")
    cast = ", ".join(metadata.get("cast") or [])
    genres = ", ".join(metadata.get("genres") or [])
    synopsis = metadata.get("synopsis", "")

    system = "You are a film expert. Return only valid JSON, no markdown."
    prompt = (
        f"Movie: \"{title}\" ({year})\n"
        f"Genres: {genres}\n"
        f"Cast: {cast}\n"
        f"Synopsis: {synopsis}\n\n"
        f"Suggest exactly 3 similar movies that share the same era, visual style, tone, and genre.\n"
        f"Prioritize movies from within 10 years of {year} with similar cast caliber and themes.\n"
        f"Return JSON: {{\"similar_movies\": [\"Title (Year)\", \"Title (Year)\", \"Title (Year)\"]}}"
    )
    try:
        raw = _invoke([{"role": "user", "content": prompt}], system)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            s, e2 = raw.find("{"), raw.rfind("}") + 1
            data = json.loads(raw[s:e2]) if s != -1 else {}
        return data.get("similar_movies", [])
    except Exception as e:
        logger.warning(f"Failed to get similar movies: {e}")
        return []


def generate_selection_guide(metadata: dict) -> str:
    """
    Ask Haiku to produce a detailed, movie-specific evaluation prompt.
    This becomes the actual instructions the image evaluator follows.
    """
    title = metadata.get("title", "Unknown")
    year = metadata.get("year", "")
    cast = ", ".join(metadata.get("cast") or [])
    genres = ", ".join(metadata.get("genres") or [])
    synopsis = metadata.get("synopsis", "")

    system = "You are an expert film analyst. Return only plain text, no markdown, no JSON."
    prompt = (
        f"Movie: \"{title}\" ({year})\n"
        f"Genres: {genres}\n"
        f"Cast: {cast}\n"
        f"Synopsis: {synopsis}\n\n"
        f"Write a DETAILED image evaluation guide (max 300 words) for someone reviewing screencaps "
        f"from this film for a movie guessing game. The reviewer has never seen the movie and needs "
        f"your expertise to know what to approve and reject.\n\n"
        f"The guide MUST include:\n\n"
        f"1. MAIN CAST VISUAL DESCRIPTIONS — For each main cast member, describe exactly what they "
        f"look like in this film so the reviewer can spot them. Include hair color, distinctive "
        f"clothing, costumes, makeup, or physical traits. For animated films, describe the main "
        f"characters' visual design (colors, shapes, distinctive features).\n\n"
        f"2. ICONIC SYMBOLS & PROPS TO REJECT — List specific objects, logos, symbols, vehicles, "
        f"or props that would immediately identify this movie (e.g. 'the X logo', 'Wolverine's claws', "
        f"'the yellow spandex suits', 'the Cerebro helmet'). These must be rejected.\n\n"
        f"3. ICONIC SCENES TO REJECT — Describe 3-5 specific scenes or locations that are so famous "
        f"they'd give the movie away instantly.\n\n"
        f"4. GOOD PICKS — List 5-8 types of shots that would work well: generic environments, "
        f"background details, non-distinctive props, crowd scenes without main cast, etc. "
        f"Be specific to this film's setting and era.\n\n"
        f"5. PEOPLE RULES — Explain that background extras, crowds, and non-main-cast people "
        f"are ALLOWED (about 20% of picks should include these). Only main cast members must be rejected. "
        f"For animated films, only main characters must be rejected — background animated people are fine.\n\n"
        f"Be extremely specific to THIS movie. Use your knowledge of the film."
    )

    try:
        guide = _invoke([{"role": "user", "content": prompt}], system)
        logger.info(f"Generated selection guide for '{title}':\n{guide}")
        return guide
    except Exception as e:
        logger.warning(f"Failed to generate selection guide: {e}")
        return ""


def _load_image_b64(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://movie-screencaps.com/"}, timeout=1)
    resp.raise_for_status()
    img = Image.open(io.BytesIO(resp.content))
    if max(img.size) > 1568:
        img.thumbnail((1568, 1568), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return base64.standard_b64encode(buf.getvalue()).decode("utf-8")


def _evaluate_batch(urls: list[str], prompt: str, system: str, batch_label: str) -> list[dict]:
    """
    Run a batch of image URLs through Haiku with the given prompt.
    Returns list of {"url": str, "has_people": bool} for approved images.
    """
    approved = []
    batch_size = 20
    image_errors = 0

    for batch_start in range(0, len(urls), batch_size):
        if isinstance(urls[0], dict):
            batch = urls[batch_start: batch_start + batch_size]
            batch_urls = [u["url"] for u in batch]
        else:
            batch_urls = urls[batch_start: batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        logger.info(f"  [{batch_label}] Batch {batch_num} ({len(batch_urls)} images)...")

        content = [{"type": "text", "text": prompt}]
        valid_indices = []

        for idx, url in enumerate(batch_urls):
            try:
                b64 = _load_image_b64(url)
                content.append({"type": "text", "text": f"Image {idx}:"})
                content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}})
                valid_indices.append((idx, url))
            except Exception as e:
                logger.warning(f"Skipping {url}: {e}")
                image_errors += 1
                if image_errors >= MAX_IMAGE_ERRORS:
                    logger.warning("Too many image errors — stopping batch")
                    break

        try:
            raw = _invoke([{"role": "user", "content": content}], system)
        except Exception as e:
            if "ThrottlingException" in str(e) or "Too many tokens" in str(e):
                for attempt in range(5):
                    wait = (2 ** attempt) * 5  # 5s, 10s, 20s, 40s, 80s
                    logger.warning(f"Throttled, retrying in {wait}s...")
                    time.sleep(wait)
                    try:
                        raw = _invoke([{"role": "user", "content": content}], system)
                        break
                    except Exception:
                        if attempt == 4:
                            logger.warning(f"Batch {batch_num} failed after retries")
                            continue
            else:
                logger.warning(f"Batch {batch_num} failed: {e}")
                continue

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            s, e2 = raw.find("{"), raw.rfind("}") + 1
            data = json.loads(raw[s:e2]) if s != -1 else {}

        idx_to_url = {idx: url for idx, url in valid_indices}
        for item in data.get("evaluations", []):
            i = item.get("index")
            a = item.get("approved", False)
            reason = item.get("reason", "")
            has_people = item.get("has_people", False)
            has_cast = item.get("has_cast", False)
            iconic_scene = item.get("iconic_scene", False)
            url = idx_to_url.get(i)
            if url is None:
                continue
            tags = []
            if has_cast:
                tags.append("🎭")
            elif has_people:
                tags.append("👤")
            else:
                tags.append("🏠")
            if iconic_scene:
                tags.append("⭐")
            tag_str = "".join(tags)
            result = {"url": url, "has_people": has_people, "has_cast": has_cast, "iconic_scene": iconic_scene}
            approved.append(result)
            if VERBOSE_AGENT:
                logger.info(f"    {tag_str} [{i}] {reason}")
            else:
                logger.info(f"    {tag_str} [{i}] tagged")

    return approved


def select_screencaps(movie_title: str, image_urls: list[str], metadata: dict = None) -> SelectionResult:
    """
    Single-pass Haiku tagger. No rejection — every image is kept and tagged with:
      has_people: bool — any person visible (cast or extras)
      has_cast: bool — main cast member recognizable
      iconic_scene: bool — recognizable/famous location or moment
    """
    system = (
        "You are a fast image tagger for a movie guessing game. "
        "Tag every image — never reject. "
        "Return only valid JSON, no markdown."
    )

    movie_context = ""
    cast_list = []
    if metadata:
        cast_list = metadata.get("cast") or []
        cast = ", ".join(cast_list)
        genres = ", ".join(metadata.get("genres") or [])
        synopsis = metadata.get("synopsis", "")
        year = metadata.get("year", "")
        movie_context = (
            f"Movie: \"{movie_title}\" ({year}) — {genres}\n"
            f"Cast: {cast}\n"
            f"Synopsis: {synopsis}\n\n"
        )

    # Generate movie-specific guide for cast identification
    selection_guide = ""
    if metadata:
        logger.info("Generating movie-specific selection guide...")
        selection_guide = generate_selection_guide(metadata)
        if selection_guide:
            selection_guide = f"\nGUIDE:\n{selection_guide}\n"

    result = SelectionResult(movie_title=movie_title, total_candidates=len(image_urls))

    logger.info(f"\nTagging {len(image_urls)} images...")
    prompt = (
        f"{movie_context}"
        f"{selection_guide}"
        f"Tag every image. Do NOT reject any — tag all of them.\n\n"
        f"For each image, determine:\n"
        f"- has_people: true if ANY person is visible (cast, extras, crowds, silhouettes)\n"
        f"- has_cast: true if a MAIN CAST MEMBER is recognizable (face, distinctive costume, or features). "
        f"Main cast: {', '.join(cast_list) if cast_list else 'unknown'}\n"
        f"- iconic_scene: true if this is a famous/recognizable location or moment from the film\n\n"
        f"Images numbered from 0. "
        f"Return JSON: {{\"evaluations\": [{{\"index\": int, \"has_people\": bool, \"has_cast\": bool, \"iconic_scene\": bool}}]}}"
    )

    tagged = _evaluate_batch(image_urls, prompt, system, "Tag")
    logger.info(f"Tagged {len(tagged)}/{len(image_urls)} images")

    # Stats
    n_cast = sum(1 for t in tagged if t.get("has_cast"))
    n_people = sum(1 for t in tagged if t.get("has_people") and not t.get("has_cast"))
    n_empty = sum(1 for t in tagged if not t.get("has_people"))
    n_iconic = sum(1 for t in tagged if t.get("iconic_scene"))
    logger.info(f"  🎭 cast: {n_cast} | 👤 extras: {n_people} | 🏠 empty: {n_empty} | ⭐ iconic: {n_iconic}")

    result.approved_urls = tagged
    return result
