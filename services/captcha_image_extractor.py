"""
CAPTCHA image extractor for Alibaba's slider CAPTCHA.

Extracts the background image (with the gap/hole) and the puzzle piece image
from the live DOM so that local OpenCV gap detection can work without sending
screenshots to an external service.

Three strategies are tried in order:
  1. Canvas toDataURL  — Alibaba noCaptcha v2 renders both images into <canvas>
  2. <img> src scrape  — older variants and some regional Alibaba properties
  3. Element screenshot fallback — last resort; lower accuracy for gap detection

Public API
----------
  await extract_slider_images(pw_page) -> SliderImages | None
"""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ── CSS selectors for the CAPTCHA container ────────────────────────────────
_CONTAINER_SELECTORS = [
    "#nocaptcha",
    "#nc_1_wrapper",
    "[class*='nc-container']",
    "#baxia-dialog",
    "[class*='baxia']",
    ".nc_scale",
]

# Minimum valid image dimensions
_MIN_BG_WIDTH  = 100
_MIN_BG_HEIGHT = 50
_MIN_PIECE_SIZE = 20


@dataclass(frozen=True)
class SliderImages:
    background_bytes: bytes   # PNG/JPEG of the background (has the gap)
    piece_bytes: bytes        # PNG of the puzzle piece
    piece_initial_x: float    # screen X of the puzzle piece's left edge
    strategy: str             # "canvas" | "img_src" | "screenshot"


async def extract_slider_images(pw_page) -> Optional[SliderImages]:
    """
    Extract background and puzzle-piece images from the active CAPTCHA.

    Returns SliderImages or None if extraction fails entirely.
    """
    # First try inside any CAPTCHA iframe
    frames = [pw_page] + list(pw_page.frames)

    for frame in frames:
        result = await _try_canvas(frame)
        if result:
            logger.info("[CAPTCHA] Image extraction: canvas strategy succeeded")
            return result

        result = await _try_img_src(frame)
        if result:
            logger.info("[CAPTCHA] Image extraction: img_src strategy succeeded")
            return result

    # Last resort: screenshot the container element
    result = await _try_screenshot(pw_page)
    if result:
        logger.warning("[CAPTCHA] Image extraction: using screenshot fallback (lower accuracy)")
        return result

    logger.error("[CAPTCHA] Image extraction: all strategies failed")
    return None


# ── Strategy 1: canvas.toDataURL ──────────────────────────────────────────

async def _try_canvas(frame) -> Optional[SliderImages]:
    """
    Extract images via JS canvas.toDataURL().
    Alibaba noCaptcha v2 renders the background and piece into two <canvas> elements.
    """
    try:
        data = await frame.evaluate("""
            () => {
                const containers = [
                    document.querySelector('#nocaptcha'),
                    document.querySelector('#nc_1_wrapper'),
                    document.querySelector('[class*="nc-container"]'),
                    document.querySelector('#baxia-dialog'),
                    document.body,
                ];

                for (const container of containers) {
                    if (!container) continue;
                    const canvases = Array.from(container.querySelectorAll('canvas'));
                    if (canvases.length < 2) continue;

                    // Sort by area descending — background is largest
                    canvases.sort((a, b) =>
                        (b.width * b.height) - (a.width * a.height)
                    );

                    const bg    = canvases[0];
                    const piece = canvases[1];

                    try {
                        const bgData    = bg.toDataURL('image/png');
                        const pieceData = piece.toDataURL('image/png');

                        // Get piece's screen position
                        const pieceRect = piece.getBoundingClientRect();

                        if (bgData.length < 100 || pieceData.length < 100) continue;

                        return {
                            bg:      bgData,
                            piece:   pieceData,
                            pieceX:  pieceRect.left,
                            bgW:     bg.width,
                            bgH:     bg.height,
                            pieceW:  piece.width,
                            pieceH:  piece.height,
                        };
                    } catch (e) {
                        // Canvas may be tainted — skip
                        continue;
                    }
                }
                return null;
            }
        """)

        if not data:
            return None

        bg_bytes    = _data_uri_to_bytes(data["bg"])
        piece_bytes = _data_uri_to_bytes(data["piece"])

        if not _valid_image(bg_bytes, _MIN_BG_WIDTH, _MIN_BG_HEIGHT):
            return None
        if not _valid_image(piece_bytes, _MIN_PIECE_SIZE, _MIN_PIECE_SIZE):
            return None

        return SliderImages(
            background_bytes=bg_bytes,
            piece_bytes=piece_bytes,
            piece_initial_x=float(data.get("pieceX", 0)),
            strategy="canvas",
        )

    except Exception as e:
        logger.debug(f"[CAPTCHA] Canvas extraction failed: {e}")
        return None


# ── Strategy 2: <img> src scraping ────────────────────────────────────────

_CAPTCHA_IMG_URL_PATTERNS = (
    "captcha", "nocaptcha", "puzzle", "slide", "verify",
    "alicdn.com", "aliyun.com", "cf.aliyun",
)

async def _try_img_src(frame) -> Optional[SliderImages]:
    """
    Extract images by finding <img> tags inside the CAPTCHA container and
    downloading them (or decoding base64 data URIs).
    """
    try:
        img_data = await frame.evaluate("""
            () => {
                const containers = [
                    document.querySelector('#nocaptcha'),
                    document.querySelector('#nc_1_wrapper'),
                    document.querySelector('[class*="nc-container"]'),
                    document.querySelector('#baxia-dialog'),
                    document.body,
                ];

                for (const container of containers) {
                    if (!container) continue;
                    const imgs = Array.from(container.querySelectorAll('img'));
                    if (imgs.length < 2) continue;

                    // Filter out tiny decorative images
                    const captchaImgs = imgs.filter(img =>
                        img.naturalWidth > 50 && img.naturalHeight > 30
                    );
                    if (captchaImgs.length < 2) continue;

                    // Sort by area descending
                    captchaImgs.sort((a, b) =>
                        (b.naturalWidth * b.naturalHeight) - (a.naturalWidth * a.naturalHeight)
                    );

                    const bg    = captchaImgs[0];
                    const piece = captchaImgs[1];
                    const pieceRect = piece.getBoundingClientRect();

                    return {
                        bgSrc:    bg.src,
                        pieceSrc: piece.src,
                        pieceX:   pieceRect.left,
                    };
                }
                return null;
            }
        """)

        if not img_data:
            return None

        bg_bytes    = await _fetch_image(img_data["bgSrc"],    frame)
        piece_bytes = await _fetch_image(img_data["pieceSrc"], frame)

        if not bg_bytes or not piece_bytes:
            return None
        if not _valid_image(bg_bytes, _MIN_BG_WIDTH, _MIN_BG_HEIGHT):
            return None
        if not _valid_image(piece_bytes, _MIN_PIECE_SIZE, _MIN_PIECE_SIZE):
            return None

        return SliderImages(
            background_bytes=bg_bytes,
            piece_bytes=piece_bytes,
            piece_initial_x=float(img_data.get("pieceX", 0)),
            strategy="img_src",
        )

    except Exception as e:
        logger.debug(f"[CAPTCHA] img_src extraction failed: {e}")
        return None


async def _fetch_image(src: str, frame) -> Optional[bytes]:
    """Fetch image bytes from a URL or data URI."""
    if not src:
        return None

    if src.startswith("data:"):
        return _data_uri_to_bytes(src)

    # Fetch via httpx, passing the page cookies for authentication
    try:
        import httpx
        cookies = await frame.evaluate("""
            () => document.cookie
        """)
        cookie_header = cookies if isinstance(cookies, str) else ""
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(src, headers={"Cookie": cookie_header})
            if resp.status_code == 200:
                return resp.content
    except Exception as e:
        logger.debug(f"[CAPTCHA] Image fetch failed for {src[:60]}: {e}")

    return None


# ── Strategy 3: Element screenshot fallback ───────────────────────────────

async def _try_screenshot(pw_page) -> Optional[SliderImages]:
    """
    Screenshot the CAPTCHA container element.
    This is a fallback — the resulting image includes UI chrome (track, button,
    text) which reduces gap detection accuracy.
    """
    try:
        captcha_el = None
        for sel in _CONTAINER_SELECTORS:
            loc = pw_page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                captcha_el = loc.first
                break

        if not captcha_el:
            return None

        # Try to screenshot just the background portion and piece separately
        bg_loc    = captcha_el.locator("canvas, img").first
        piece_loc = captcha_el.locator("canvas, img").nth(1)

        if await bg_loc.count() > 0 and await piece_loc.count() > 0:
            bg_bytes    = await bg_loc.screenshot()
            piece_bytes = await piece_loc.screenshot()
            piece_bbox  = await piece_loc.bounding_box()
            piece_x     = piece_bbox["x"] if piece_bbox else 0.0
        else:
            # Full container screenshot as last resort
            bg_bytes    = await captcha_el.screenshot()
            piece_bytes = bg_bytes          # same image — detector will use contour fallback
            piece_x     = 0.0

        if not _valid_image(bg_bytes, _MIN_BG_WIDTH, _MIN_BG_HEIGHT):
            return None

        return SliderImages(
            background_bytes=bg_bytes,
            piece_bytes=piece_bytes,
            piece_initial_x=piece_x,
            strategy="screenshot",
        )

    except Exception as e:
        logger.debug(f"[CAPTCHA] Screenshot extraction failed: {e}")
        return None


# ── Utilities ──────────────────────────────────────────────────────────────

def _data_uri_to_bytes(data_uri: str) -> Optional[bytes]:
    """Decode a base64 data URI to raw bytes."""
    try:
        if "," not in data_uri:
            return None
        _, b64 = data_uri.split(",", 1)
        return base64.b64decode(b64)
    except Exception:
        return None


def _valid_image(data: Optional[bytes], min_w: int, min_h: int) -> bool:
    """Check that bytes decode to a valid image meeting minimum dimensions."""
    if not data:
        return False
    try:
        from PIL import Image
        from io import BytesIO
        img = Image.open(BytesIO(data))
        return img.width >= min_w and img.height >= min_h
    except Exception:
        return False
