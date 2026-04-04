# agent.py — Bedrock/Claude for metadata enrichment and image selection

import json
import logging
import base64
import io
from dataclasses import dataclass, field

import boto3
import requests
from PIL import Image

from config import AWS_REGION, BEDROCK_MODEL_ID, TARGET_SCREENCAP_COUNT

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
        "You are an expert film critic and movie trivia game designer. "
        "Your job is to evaluate screencaps for a movie guessing game. "
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

    for batch_start in range(0, len(image_urls), batch_size):
        if len(result.approved_urls) >= TARGET_SCREENCAP_COUNT:
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
                    f"You are selecting screencaps for a movie guessing game where players must identify the movie from images alone.\n\n"
                    f"⚠️ ABSOLUTE HARD RULES — ZERO EXCEPTIONS:\n"
                    f"1. NO IDENTIFIABLE FACES. If you can make out someone's identity — eyes, nose, mouth clearly visible — REJECT. Silhouettes, backs of heads, heavily obscured or distant faces are OK.\n"
                    f"2. Human forms are OK as long as you cannot identify who they are. A person walking away, a silhouette, a crowd from a distance — all fine. REJECT only if the person is clearly identifiable.\n"
                    f"3. NO title cards, text overlays, or credits.\n"
                    f"4. NO iconic props, locations, or visual elements associated with this specific film. Use your film knowledge and the movie details above to identify these.\n"
                    f"5. NO SIMILARITIES between approved images — each must be completely unique:\n"
                    f"   - Different room/location (no two interiors from the same space)\n"
                    f"   - Different setting type (no two cityscapes, no two snowy scenes, no two industrial spaces)\n"
                    f"   - Different objects (no two watches, no two vehicles of same type)\n"
                    f"   - Different lighting/mood (no two dark moody shots, no two bright daylight shots)\n"
                    f"   - Different color palette\n"
                    f"   Compare against ALL already-approved images shown above before approving.\n"
                    f"6. NO blurry, out-of-focus, or motion-blurred images.\n\n"
                    f"✅ IDEAL images show:\n"
                    f"- Pure environments, architecture, landscapes with zero human presence\n"
                    f"- Objects or details that are visually interesting but not film-specific\n"
                    f"- Each image must feel like a completely different world from the others\n\n"
                    f"When in doubt, REJECT. Quality over quantity.\n\n"
                    f"I need {still_needed} more approved images. New candidates are numbered from 0.\n\n"
                    f"For EVERY candidate, provide evaluation. Return JSON:\n"
                    f"  evaluations: list of {{'index': int, 'approved': bool, 'reason': str}}\n"
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

        try:
            raw = _invoke([{"role": "user", "content": content}], system)
        except Exception as e:
            # Retry with exponential backoff on throttling
            if "ThrottlingException" in str(e) or "Too many tokens" in str(e):
                for attempt in range(3):
                    wait = (2 ** attempt) * 10  # 10s, 20s, 40s
                    logger.warning(f"Throttled on batch {batch_num}, retrying in {wait}s (attempt {attempt+1}/3)...")
                    import time
                    time.sleep(wait)
                    try:
                        raw = _invoke([{"role": "user", "content": content}], system)
                        break
                    except Exception as retry_e:
                        if attempt == 2:
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
                logger.info(f"  ✅ [{i}] {reason}")
            else:
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
                    f"1. Any identifiable face (eyes, nose, mouth clearly visible — silhouettes/backs of heads are OK)\n"
                    f"2. Any person whose identity could be determined\n"
                    f"3. Any iconic prop or scene that gives away this specific movie\n"
                    f"4. Any two images too similar to each other\n"
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
