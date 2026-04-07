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
    """
    Ask Haiku to produce a tailored image selection guide based on the movie's metadata.
    Returns a plain-text guide used as the evaluation prompt for this specific movie.
    """
    title = metadata.get("title", "Unknown")
    year = metadata.get("year", "")
    cast = ", ".join(metadata.get("cast") or [])
    genres = ", ".join(metadata.get("genres") or [])
    synopsis = metadata.get("synopsis", "")

    system = "You are an expert movie screencap curator. Return only plain text, no markdown, no JSON."
    prompt = (
        f"Movie: \"{title}\" ({year})\n"
        f"Genres: {genres}\n"
        f"Cast: {cast}\n"
        f"Synopsis: {synopsis}\n\n"
        f"Write a concise image selection guide (max 150 words) for a movie guessing game curator "
        f"who is reviewing screencaps from this film. The guide should:\n"
        f"1. Note whether this is a people-heavy film, animation, nature documentary, etc. and how strict to be about rejecting people\n"
        f"2. List 3-5 specific things to APPROVE (e.g. 'empty diner interiors', 'desert landscapes', 'close-ups of props')\n"
        f"3. List 3-5 specific things to REJECT that would give this movie away (e.g. 'the briefcase', 'the gimp mask', 'Uma Thurman's face')\n"
        f"4. One sentence on the overall visual style to look for\n\n"
        f"Be specific to THIS movie. Do not be generic."
    )

    try:
        guide = _invoke([{"role": "user", "content": prompt}], system)
        logger.info(f"Generated selection guide for '{title}':\n{guide}")
        return guide
    except Exception as e:
        logger.warning(f"Failed to generate selection guide: {e}")
        return ""


def _load_image_b64(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://movie-screencaps.com/"}, timeout=10)
    resp.raise_for_status()
    img = Image.open(io.BytesIO(resp.content))
    if max(img.size) > 1568:
        img.thumbnail((1568, 1568), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return base64.standard_b64encode(buf.getvalue()).decode("utf-8")


def _evaluate_batch(urls: list[str], prompt: str, system: str, batch_label: str) -> list[str]:
    """
    Run a batch of image URLs through Haiku with the given prompt.
    Returns the list of URLs that were approved.
    """
    approved = []
    batch_size = 20
    image_errors = 0

    for batch_start in range(0, len(urls), batch_size):
        batch = urls[batch_start: batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        logger.info(f"  [{batch_label}] Batch {batch_num} ({len(batch)} images)...")

        content = [{"type": "text", "text": prompt}]
        valid_indices = []

        for idx, url in enumerate(batch):
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
                    wait = (2 ** attempt) * 30
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
            url = idx_to_url.get(i)
            if url is None:
                continue
            if a:
                approved.append(url)
                if VERBOSE_AGENT:
                    logger.info(f"    ✅ [{i}] {reason}")
                else:
                    logger.info(f"    ✅ [{i}] approved")
            elif VERBOSE_AGENT:
                logger.info(f"    ❌ [{i}] {reason}")

    return approved


def select_screencaps(movie_title: str, image_urls: list[str], metadata: dict = None) -> SelectionResult:
    """
    Two-pass Haiku evaluation:
      Pass 1 — filter all candidates, approve scene/object shots with no people
      Pass 2 — re-check pass 1 approvals, remove any people that slipped through
    All images surviving both passes are saved — no cap.
    """
    movie_context = ""
    if metadata:
        cast = ", ".join(metadata.get("cast") or [])
        genres = ", ".join(metadata.get("genres") or [])
        synopsis = metadata.get("synopsis", "")
        year = metadata.get("year", "")
        movie_context = (
            f"Movie: \"{movie_title}\" ({year}) — {genres}\n"
            f"Cast: {cast}\n"
            f"Synopsis: {synopsis}\n\n"
        )

    # Generate a movie-specific selection guide
    selection_guide = ""
    if metadata:
        logger.info("Generating movie-specific selection guide...")
        selection_guide = generate_selection_guide(metadata)
        if selection_guide:
            selection_guide = f"\nSELECTION GUIDE FOR THIS MOVIE:\n{selection_guide}\n"

    result = SelectionResult(movie_title=movie_title, total_candidates=len(image_urls))

    # --- Pass 1: scene/object filter ---
    logger.info(f"\nPass 1 — evaluating {len(image_urls)} candidates...")
    pass1_system = (
        "You are an image reviewer for a movie guessing game. "
        "REJECT any image containing a person or human body part. "
        "APPROVE only images of environments, objects, animals, or scenes with zero humans. "
        "Return only valid JSON, no markdown."
    )
    pass1_prompt = (
        f"{movie_context}"
        f"{selection_guide}"
        f"ABSOLUTE RULE: If ANY person, human body part, silhouette, shadow, or reflection of a person "
        f"is visible anywhere in the image — REJECT IT. No exceptions.\n\n"
        f"✅ APPROVE only: cities, landscapes, nature, animals, signs, objects, buildings, vehicles, props, empty rooms.\n"
        f"❌ REJECT: any person visible, title cards, credits, blurry images.\n\n"
        f"Candidates numbered from 0. "
        f"Return JSON: {{\"evaluations\": [{{\"index\": int, \"approved\": bool{', \"reason\": str' if VERBOSE_AGENT else ''}}}]}}"
    )

    pass1_approved = _evaluate_batch(image_urls, pass1_prompt, pass1_system, "Pass 1")
    logger.info(f"Pass 1 result: {len(pass1_approved)}/{len(image_urls)} approved")

    if not pass1_approved:
        logger.warning("No images survived pass 1")
        return result

    # --- Pass 2: people verification ---
    logger.info(f"\nPass 2 — verifying {len(pass1_approved)} images for people...")
    pass2_system = (
        "You are a strict people detector. "
        "Your only job: does this image contain any person, human body part, silhouette, shadow of a person, or reflection of a person? "
        "Return only valid JSON, no markdown."
    )
    pass2_prompt = (
        f"For each image below, check ONLY: is any person or human body part visible anywhere?\n"
        f"This includes: faces, hands, feet, arms, legs, torsos, silhouettes, shadows of people, reflections of people.\n"
        f"If YES — approved: false. If NO person at all — approved: true.\n\n"
        f"Images numbered from 0. "
        f"Return JSON: {{\"evaluations\": [{{\"index\": int, \"approved\": bool{', \"reason\": str' if VERBOSE_AGENT else ''}}}]}}"
    )

    pass2_approved = _evaluate_batch(pass1_approved, pass2_prompt, pass2_system, "Pass 2")
    logger.info(f"Pass 2 result: {len(pass2_approved)}/{len(pass1_approved)} survived")

    result.approved_urls = pass2_approved
    return result
