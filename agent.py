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

from config import AWS_REGION, BEDROCK_MODEL_ID, TARGET_SCREENCAP_COUNT, VERBOSE_AGENT, MAX_EVALUATION_SECONDS, MAX_IMAGE_ERRORS

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

    @property
    def approved(self) -> list[ImageEvaluation]:
        return [e for e in self.evaluations if e.approved]

    @property
    def rejected(self) -> list[ImageEvaluation]:
        return [e for e in self.evaluations if not e.approved]


@dataclass
class SelectionResult:
    movie_title: str
    total_candidates: int
    batches: list[BatchResult] = field(default_factory=list)
    approved_urls: list[str] = field(default_factory=list)

    def print_summary(self):
        logger.info(f"\n=== Selection Summary for '{self.movie_title}' ===")
        for batch in self.batches:
            logger.info(f"\nBatch {batch.batch_num} ({batch.total_images} images):")
            for e in batch.evaluations:
                status = "✅ APPROVED" if e.approved else "❌ REJECTED"
                logger.info(f"  [{e.index}] {status}: {e.reason}")
        logger.info(f"\nFinal: {len(self.approved_urls)}/{self.total_candidates} approved")


def _invoke(messages: list[dict], system: str) -> str:
    """Call Claude via Bedrock and return the text response."""
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
    """Ask Claude to fill in movie metadata. Returns a dict with year, cast, genres."""
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
        # Try to extract JSON block if wrapped in text
        start, end = raw.find("{"), raw.rfind("}") + 1
        return json.loads(raw[start:end]) if start != -1 else {}


def select_screencaps(movie_title: str, image_urls: list[str], metadata: dict = None) -> SelectionResult:
    """
    Send image URLs to Claude in batches for evaluation.
    Returns a SelectionResult with full approval/rejection metadata.
    """
    system = (
        "You are a passionate film fan and expert movie trivia game designer. "
        "You deeply understand cinema — genres, directors, iconic imagery, and what makes each film unique. "
        "Your job is to select screencaps for a movie guessing game where players try to identify the film from images alone. "
        "Think like a fan who loves this movie: pick frames that are visually interesting and hint at the film's world, "
        "but don't make it too easy. The best picks are ones that a fan would recognize and smile at, "
        "while a casual viewer might need a few more clues. "
        "Return only valid JSON, no markdown."
    )

    # Build movie context from metadata
    movie_context = ""
    if metadata:
        cast = ", ".join(metadata.get("cast") or [])
        genres = ", ".join(metadata.get("genres") or [])
        synopsis = metadata.get("synopsis", "")
        year = metadata.get("year", "")
        movie_context = (
            f"\nMOVIE DETAILS (use this to identify what to filter out):\n"
            f"  Year: {year}\n"
            f"  Genres: {genres}\n"
            f"  Cast: {cast}\n"
            f"  Synopsis: {synopsis}\n"
            f"  Known iconic elements to ALWAYS REJECT: any props, locations, or visual elements "
            f"that are strongly associated with this specific film based on your knowledge.\n"
        )

    result = SelectionResult(movie_title=movie_title, total_candidates=len(image_urls))
    batch_size = 20
    start_time = time.time()
    image_errors = 0

    for batch_start in range(0, len(image_urls), batch_size):
        if len(result.approved_urls) >= TARGET_SCREENCAP_COUNT:
            break

        # Timeout check — write what we have and move on
        elapsed = time.time() - start_time
        if elapsed > MAX_EVALUATION_SECONDS:
            logger.warning(f"Evaluation timeout after {elapsed:.0f}s — saving {len(result.approved_urls)} approved images and moving on")
            break

        # Too many CDN errors — this movie's images are mostly dead
        if image_errors >= MAX_IMAGE_ERRORS:
            logger.warning(f"Too many image errors ({image_errors}) — saving {len(result.approved_urls)} approved images and moving on")
            break

        batch = image_urls[batch_start: batch_start + batch_size]
        still_needed = TARGET_SCREENCAP_COUNT - len(result.approved_urls)
        batch_num = batch_start // batch_size + 1
        logger.info(f"Evaluating batch {batch_num} ({len(batch)} images), need {still_needed} more...")

        # Build context of already-approved images to prevent duplicates
        approved_context = ""
        if result.approved_urls:
            approved_context = (
                f"\n\nYou have already approved {len(result.approved_urls)} images shown above as 'ALREADY APPROVED'. "
                f"Do NOT approve anything that shares the same room, scene, setting, objects, "
                f"lighting, color palette, or visual theme as any already-approved image.\n"
            )

        content = [
            {
                "type": "text",
                "text": (
                    f"Movie: \"{movie_title}\"\n"
                    f"{movie_context}\n"
                    f"You are selecting screencaps for a movie guessing game. Players must identify \"{movie_title}\" from images alone.\n\n"
                    f"Think like a devoted fan of this film who wants to make the game CHALLENGING but FAIR. "
                    f"You know every scene intimately. Your goal is to pick frames that give subtle hints — "
                    f"not frames that scream the movie's name. A good pick makes someone think 'hmm, this looks familiar...' "
                    f"not 'oh that's obviously from X'. Prioritize atmosphere, production design, and visual storytelling "
                    f"over anything that directly identifies the film.\n\n"
                    f"⚠️ HARD RULES — NO EXCEPTIONS:\n"
                    f"1. NO PEOPLE — zero humans in the shot. Not main cast, not extras, not silhouettes, not partial body parts. If any person is visible in the frame at all, REJECT it. No exceptions.\n"
                    f"2. NO TITLE CARDS, text overlays, or credits.\n"
                    f"3. NO OBVIOUS GIVEAWAYS — reject frames showing:\n"
                    f"   - Superhero suits, signature weapons, or character-defining outfits (e.g. Batman's cowl, Iron Man's armor, a Jedi's lightsaber)\n"
                    f"   - The central theme item or symbol of the film (e.g. the One Ring, the DeLorean, the shark)\n"
                    f"   - Any scene so famous it's become a cultural reference for this film\n"
                    f"   - The film's most iconic/signature props or costumes that immediately identify it\n"
                    f"   KEY LOCATIONS: You MAY show a key location from the film ONLY if no cast members are in the shot. An empty Overlook Hotel corridor is fine. Jack Nicholson in the Overlook Hotel is not.\n"
                    f"   Use your deep knowledge of this film — be strict. If a frame would make someone immediately say the movie title, REJECT it.\n"
                    f"4. NO SIMILAR IMAGES — each approved frame must be completely distinct from all others:\n"
                    f"   - Different location, building, room, or environment — never two shots from the same space\n"
                    f"   - Different visual setting type (no two diner shots, no two street shots, no two warehouse shots)\n"
                    f"   - Different mood, lighting, and color palette\n"
                    f"   - Different type of shot (wide, close, interior, exterior)\n"
                    f"   Before approving any image, compare it against ALL already-approved images. If it shares the same building, room, street, or environment as any approved image, REJECT it.\n"
                    f"5. NO BLURRY or out-of-focus images.\n\n"
                    f"✅ GREAT picks are frames that:\n"
                    f"- Show props, objects, or details that hint at the film's world (a weapon, a vehicle, a piece of furniture, a sign)\n"
                    f"- Capture environments, locations, and architecture with NO people present\n"
                    f"- Close-ups on iconic objects or details that a fan would recognize\n"
                    f"- Wide establishing shots of key locations — empty streets, rooms, landscapes\n"
                    f"- Would make someone think 'I've seen this somewhere...' rather than 'that's obviously [movie]'\n\n"
                    f"When unsure if something is too obvious or has any person in it, REJECT. Subtle > recognizable. Empty > populated.\n\n"
                    f"I need {still_needed} more approved images. New candidates are numbered from 0.\n\n"
                    f"For EVERY candidate, provide evaluation. Return JSON:\n"
                    f"  evaluations: list of {{'index': int, 'approved': bool{', \"reason\": str' if VERBOSE_AGENT else ''}}}\n"
                ),
            }
        ]

        # First inject already-approved images as reference
        for i, approved_url in enumerate(result.approved_urls):
            try:
                img_resp = requests.get(approved_url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://movie-screencaps.com/"}, timeout=10)
                img_resp.raise_for_status()
                img = Image.open(io.BytesIO(img_resp.content))
                if max(img.size) > 1568:
                    img.thumbnail((1568, 1568), Image.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=85)
                b64 = base64.standard_b64encode(buf.getvalue()).decode("utf-8")
                content.append({"type": "text", "text": f"ALREADY APPROVED {i}:"})
                content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}})
            except Exception as e:
                logger.warning(f"Could not load approved image for context: {e}")

        valid_indices = []
        for idx, url in enumerate(batch):
            try:
                img_resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://movie-screencaps.com/"}, timeout=10)
                img_resp.raise_for_status()

                # Resize to max 1568px on longest side for Claude's multi-image limit
                img = Image.open(io.BytesIO(img_resp.content))
                max_size = 1568
                if max(img.size) > max_size:
                    img.thumbnail((max_size, max_size), Image.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=85)
                b64 = base64.standard_b64encode(buf.getvalue()).decode("utf-8")
                fmt = "jpeg"
                content.append({"type": "text", "text": f"Image {idx}:"})
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": f"image/{fmt}",
                        "data": b64,
                    },
                })
                valid_indices.append((idx, url))
            except Exception as e:
                logger.warning(f"Skipping image {url}: {e}")
                image_errors += 1

        try:
            raw = _invoke([{"role": "user", "content": content}], system)
        except Exception as e:
            # Retry with exponential backoff on throttling
            if "ThrottlingException" in str(e) or "Too many tokens" in str(e):
                for attempt in range(5):
                    wait = (2 ** attempt) * 30  # 30s, 60s, 120s, 240s, 480s
                    logger.warning(f"Throttled on batch {batch_num}, retrying in {wait}s (attempt {attempt+1}/5)...")
                    time.sleep(wait)
                    try:
                        raw = _invoke([{"role": "user", "content": content}], system)
                        break
                    except Exception as retry_e:
                        if attempt == 4:
                            logger.warning(f"Batch {batch_num} failed after retries: {retry_e}")
                            continue
            else:
                logger.warning(f"Batch {batch_num} failed: {e}")
                continue

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            start, end = raw.find("{"), raw.rfind("}") + 1
            data = json.loads(raw[start:end]) if start != -1 else {}

        idx_to_url = {idx: url for idx, url in valid_indices}
        batch_result = BatchResult(batch_num=batch_num, total_images=len(valid_indices))

        for item in data.get("evaluations", []):
            i = item.get("index")
            approved = item.get("approved", False)
            reason = item.get("reason", "")
            url = idx_to_url.get(i)
            if url is None:
                continue
            eval_ = ImageEvaluation(index=i, url=url, approved=approved, reason=reason)
            batch_result.evaluations.append(eval_)
            if approved and len(result.approved_urls) < TARGET_SCREENCAP_COUNT:
                result.approved_urls.append(url)
                if VERBOSE_AGENT:
                    logger.info(f"  ✅ [{i}] {reason}")
                else:
                    logger.info(f"  ✅ [{i}] approved")
            else:
                if VERBOSE_AGENT:
                    logger.info(f"  ❌ [{i}] {reason}")

        result.batches.append(batch_result)

    # --- Final verification pass ---
    if result.approved_urls:
        logger.info(f"\nRunning final verification on {len(result.approved_urls)} approved images...")
        verify_content = [
            {
                "type": "text",
                "text": (
                    f"Movie: \"{movie_title}\"\n{movie_context}\n"
                    f"These {len(result.approved_urls)} images were approved for a movie guessing game. "
                    f"Do a final strict check and flag ANY that violate:\n"
                    f"1. Main cast member's face clearly visible and identifiable (frontal face of a recognizable actor)\n"
                    f"2. Title cards, text overlays, or credits\n"
                    f"3. Such an iconic/obvious scene that it immediately gives away the movie title\n"
                    f"4. Two images too similar to each other\n"
                    f"5. Blurry or unclear images\n\n"
                    f"Images numbered from 0. Return JSON: {{\"violations\": [{{\"index\": int, \"reason\": str}}]}}\n"
                    f"Return empty list if all pass."
                )
            }
        ]
        for i, url in enumerate(result.approved_urls):
            try:
                img_resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://movie-screencaps.com/"}, timeout=10)
                img_resp.raise_for_status()
                img = Image.open(io.BytesIO(img_resp.content))
                if max(img.size) > 1568:
                    img.thumbnail((1568, 1568), Image.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=85)
                b64 = base64.standard_b64encode(buf.getvalue()).decode("utf-8")
                verify_content.append({"type": "text", "text": f"Image {i}:"})
                verify_content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}})
            except Exception as e:
                logger.warning(f"Could not load image for verification: {e}")
        try:
            raw = _invoke([{"role": "user", "content": verify_content}], system)
            try:
                vdata = json.loads(raw)
            except json.JSONDecodeError:
                s, e2 = raw.find("{"), raw.rfind("}") + 1
                vdata = json.loads(raw[s:e2]) if s != -1 else {}
            violations = vdata.get("violations", [])
            if violations:
                logger.warning(f"Final check found {len(violations)} violations — removing:")
                bad_indices = {v["index"] for v in violations}
                for v in violations:
                    logger.warning(f"  ❌ Image {v['index']}: {v['reason']}")
                result.approved_urls = [url for i, url in enumerate(result.approved_urls) if i not in bad_indices]
                logger.info(f"  {len(result.approved_urls)} images remain after cleanup")
            else:
                logger.info("  ✅ All approved images passed final verification")
        except Exception as e:
            logger.warning(f"Final verification failed: {e}")

    return result
