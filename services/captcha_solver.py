"""
CAPTCHA detection and solving for Alibaba Cloud CAPTCHA (Taobao/Tmall/1688).

Strategy (local-first, 2Captcha as fallback):
  1. Detect CAPTCHA via DOM selectors, URL patterns, JS evaluation
  2. Extract background + puzzle-piece images from DOM (canvas/img/screenshot)
  3. Use OpenCV template matching to find the gap X position locally
  4. Calculate drag distance, simulate human-like slider drag
  5. If local solving fails and CAPTCHA_API_KEY is set → 2Captcha slider API

Alibaba's slider CAPTCHA:
  - Background image with a missing puzzle piece (gap)
  - Foreground puzzle piece must be dragged to fill the gap
  - Trajectory analysis: checks mouse speed, acceleration, noise
  - Sets cookies prefixed with nco_ / umdata / x5sec on success
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from typing import Optional

from services.captcha_image_extractor import extract_slider_images
from services.captcha_gap_detector import detect_gap_x, _MIN_CONFIDENCE

logger = logging.getLogger(__name__)

_DEBUG     = os.getenv("CAPTCHA_DEBUG", "0") == "1"
_DEBUG_DIR = "/tmp/captcha_debug"

# ── Configuration ──────────────────────────────────────────────────────────
CAPTCHA_API_KEY      = os.getenv("CAPTCHA_API_KEY", "")
CAPTCHA_SOLVE_TIMEOUT = 120   # seconds to wait for 2Captcha
MAX_SOLVE_ATTEMPTS    = 3     # total attempts (local + fallback combined)
MAX_LOCAL_ATTEMPTS    = 2     # local attempts before falling back to 2Captcha


def log(message: str, level: str = "info") -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    print(f"{ts} [CAPTCHA] [{level.upper()}] {message}")
    log_fn = getattr(logger, level.lower() if level.lower() != "warn" else "warning", logger.info)
    log_fn(message)


# ── Known Alibaba CAPTCHA selectors ───────────────────────────────────────

CAPTCHA_SELECTORS = [
    "#nocaptcha", "#nc_1_wrapper", "#nc_1__scale_text",
    "[class*='nc-container']", "[id*='nocaptcha']",
    ".nc_scale", ".nc-lang-cnt",
    "#nc_1_n1z", "#nc_1_n1t",
    "iframe[src*='captcha']", "iframe[src*='nocaptcha']", "iframe[src*='verify']",
    "#baxia-dialog", "[class*='baxia']", "#J_Punish",
    "[class*='identity-verify']", "[class*='logincheckali498']",
]

CAPTCHA_URL_PATTERNS = [
    r"login\.taobao\.com", r"sec\.taobao\.com",
    r"verify\.", r"captcha", r"punish", r"checkcode",
]


# ═══════════════════════════════════════════════════════════════════════════
# DETECTION (unchanged — already works well)
# ═══════════════════════════════════════════════════════════════════════════

async def detect_captcha(pw_page) -> dict:
    """Check if a CAPTCHA is present. Returns {detected, type, details}."""
    result = {"detected": False, "type": None, "details": {}}

    url = pw_page.url
    for pattern in CAPTCHA_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            result.update({"detected": True, "type": "url_redirect",
                           "details": {"url": url, "pattern": pattern}})
            log(f"CAPTCHA detected via URL redirect: {url}")
            return result

    for sel in CAPTCHA_SELECTORS:
        try:
            el = pw_page.locator(sel)
            if await el.count() > 0 and await el.first.is_visible():
                result.update({
                    "detected": True,
                    "type": "slider" if "nc_" in sel or "nocaptcha" in sel.lower() else "generic",
                    "details": {"selector": sel},
                })
                log(f"CAPTCHA detected via selector: {sel}")
                return result
        except Exception:
            continue

    try:
        has_nc = await pw_page.evaluate("""
            () => {
                if (window.NoCaptcha || window.AWSC || window.nc) return 'global';
                if (document.querySelector('#nc_1_n1z, .nc_scale, [class*="nc-container"]')) return 'dom';
                if (document.querySelector('#baxia-dialog, [class*="baxia"]')) return 'baxia';
                return null;
            }
        """)
        if has_nc:
            result.update({
                "detected": True,
                "type": "slider" if has_nc in ("global", "dom") else "baxia",
                "details": {"method": f"js_eval:{has_nc}"},
            })
            log(f"CAPTCHA detected via JS: {has_nc}")
            return result
    except Exception:
        pass

    return result


def detect_captcha_from_html(html: str) -> bool:
    """Quick HTML-only check for CAPTCHA indicators (sync)."""
    indicators = [
        "nocaptcha", "nc_1_wrapper", "nc_scale", "nc-lang-cnt",
        "baxia-dialog", "J_Punish", "identity-verify",
        "noCaptcha", "AWSC.use", "slider-captcha",
    ]
    html_lower = html.lower()
    return any(ind.lower() in html_lower for ind in indicators)


# ═══════════════════════════════════════════════════════════════════════════
# LOCAL SOLVING — OpenCV gap detection
# ═══════════════════════════════════════════════════════════════════════════

async def _solve_locally(pw_page) -> bool:
    """
    Solve the slider CAPTCHA using local OpenCV gap detection.

    Steps:
      1. Extract background + piece images from the DOM
      2. Detect gap X in background image pixel space
      3. Scale gap X to screen/viewport coordinates
      4. Drag slider to the gap
    """
    log("Local solve: extracting CAPTCHA images from DOM...")
    images = await extract_slider_images(pw_page)
    if not images:
        log("Local solve: image extraction failed", "warn")
        return False

    log(f"Local solve: images extracted via '{images.strategy}'. Running gap detection...")
    detection = detect_gap_x(images.background_bytes, images.piece_bytes)
    if not detection:
        log("Local solve: gap detection returned None", "warn")
        return False

    log(f"Local solve: gap_x={detection.gap_x}px, confidence={detection.confidence:.2f}, method={detection.method}")

    if detection.confidence < _MIN_CONFIDENCE and detection.method != "fallback_center":
        log(f"Local solve: confidence {detection.confidence:.2f} below threshold — skipping drag", "warn")
        return False

    # ── Coordinate scaling ──────────────────────────────────────────────────
    # gap_x is in background image pixel space.
    # We must scale to the rendered DOM element width.
    #
    # drag_end_screen_x = container_left + (gap_x / bg_image_width) × track_rendered_width

    captcha_frame, captcha_el = await _find_captcha_container(pw_page)
    container_bbox = await captcha_el.bounding_box() if captcha_el else None

    try:
        from PIL import Image
        from io import BytesIO
        bg_pil = Image.open(BytesIO(images.background_bytes))
        bg_image_width = bg_pil.width
    except Exception:
        bg_image_width = detection.gap_x + 1   # avoid division by zero

    if container_bbox and bg_image_width > 0:
        scale           = container_bbox["width"] / bg_image_width
        target_screen_x = container_bbox["x"] + detection.gap_x * scale
        target_screen_y = container_bbox["y"] + container_bbox["height"] / 2
        log(f"Local solve: drag target=({target_screen_x:.0f}, {target_screen_y:.0f}) "
            f"[scale={scale:.2f}]")
    else:
        # No container bbox — use raw gap_x; Y will be overridden by slider button's Y
        target_screen_x = float(detection.gap_x)
        target_screen_y = 0.0  # placeholder; _drag_slider uses slider button Y
        log(f"Local solve: drag target=({target_screen_x:.0f}, ?) [no scale — container not found]")

    return await _drag_slider(
        pw_page,
        captcha_frame,
        captcha_el,
        {"x": target_screen_x, "y": target_screen_y},
    )


_CONTAINER_SELS = [
    "#nocaptcha", "#nc_1_wrapper", "[class*='nc-container']",
    "#baxia-dialog", "[class*='baxia']", ".nc_scale",
    "#J_Punish", "[class*='identity-verify']",
    "[id*='nocaptcha']", "[class*='nc-lang-cnt']",
    "[class*='captcha']", "[id*='captcha']",
    "[class*='verify']", "[id*='verify']",
]

# JS that finds the container element wrapping the largest CAPTCHA image
_JS_FIND_CONTAINER = """
() => {
    // Walk up from any large <img> that looks like a CAPTCHA background
    const imgs = Array.from(document.querySelectorAll('img'));
    const large = imgs
        .filter(img => img.naturalWidth > 100 && img.naturalHeight > 50)
        .sort((a, b) => (b.naturalWidth * b.naturalHeight) - (a.naturalWidth * a.naturalHeight));
    if (!large.length) return null;

    let el = large[0];
    for (let i = 0; i < 6; i++) {   // walk up max 6 levels
        el = el.parentElement;
        if (!el) break;
        const rect = el.getBoundingClientRect();
        // Container must be reasonably sized and visible
        if (rect.width >= 200 && rect.height >= 80 && rect.width <= 800) {
            return el.id ? '#' + el.id
                 : el.className ? '.' + el.className.split(' ')[0]
                 : null;
        }
    }
    return null;
}
"""


async def _find_captcha_container(pw_page):
    """
    Return (frame, first_visible_CAPTCHA_container_locator) searching main page
    and all iframes.  Returns (None, None) if nothing found.
    """
    frames = [pw_page] + list(pw_page.frames)
    for frame in frames:
        # Pass 1: known CSS selectors
        for sel in _CONTAINER_SELS:
            try:
                loc = frame.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    return frame, loc.first
            except Exception:
                continue

        # Pass 2: JS-derive selector from the largest img's parent chain
        try:
            derived_sel = await frame.evaluate(_JS_FIND_CONTAINER)
            if derived_sel:
                loc = frame.locator(derived_sel)
                if await loc.count() > 0:
                    log(f"Container found via JS image-parent walk: {derived_sel}")
                    return frame, loc.first
        except Exception:
            pass

    log("CAPTCHA container not found in any frame — proceeding without bounding box", "warn")
    return None, None


# ═══════════════════════════════════════════════════════════════════════════
# 2CAPTCHA FALLBACK — Slider API (not coordinatescaptcha)
# ═══════════════════════════════════════════════════════════════════════════

def _to_jpeg(image_bytes: bytes, max_kb: int = 100) -> Optional[bytes]:
    """
    Convert image bytes to a JPEG that fits within max_kb.

    2Captcha rejects WEBP and oversized images (error 15).
    Tries progressively lower quality until the file is small enough.
    Returns None if conversion fails.
    """
    try:
        from PIL import Image
        from io import BytesIO

        img = Image.open(BytesIO(image_bytes)).convert("RGB")

        for quality in (85, 70, 55, 40):
            buf = BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            data = buf.getvalue()
            if len(data) <= max_kb * 1024:
                return data

        # Last resort: resize to half dimensions
        w, h = img.size
        img = img.resize((w // 2, h // 2), Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=60, optimize=True)
        return buf.getvalue()

    except Exception as exc:
        logger.debug(f"[CAPTCHA] _to_jpeg failed: {exc}")
        return None


async def _solve_with_2captcha_slider(pw_page) -> bool:
    """
    Fallback: submit the background image to 2Captcha's slider task API.

    Uses the modern JSON task API (not the legacy in.php form), with
    task type ImageToCoordinates or the slider-specific flow.
    2Captcha returns a slide distance (pixels from left edge of track).
    """
    if not CAPTCHA_API_KEY:
        return False

    import base64
    import httpx

    log("2Captcha fallback: extracting background image...")
    images = await extract_slider_images(pw_page)
    if not images:
        log("2Captcha fallback: image extraction failed", "warn")
        return False

    # 2Captcha only accepts JPEG/PNG/BMP/GIF, max ~100 KB.
    # Re-encode as JPEG to handle WEBP and any oversized images.
    jpeg_bytes = _to_jpeg(images.background_bytes)
    if not jpeg_bytes:
        log("2Captcha fallback: could not convert background image to JPEG", "error")
        return False

    log(f"2Captcha fallback: image {len(jpeg_bytes) // 1024}KB ({images.strategy})")
    screenshot_b64 = base64.b64encode(jpeg_bytes).decode()

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # CoordinatesTask: worker clicks on the gap position and returns {x, y}
            resp = await client.post("https://api.2captcha.com/createTask", json={
                "clientKey": CAPTCHA_API_KEY,
                "task": {
                    "type": "CoordinatesTask",
                    "body": screenshot_b64,
                    "comment": "This is a slider CAPTCHA background image. "
                               "Click on the CENTER of the dark gap/hole where the puzzle piece should go.",
                },
            })
            data = resp.json()

        if data.get("errorId", 1) != 0:
            log(f"2Captcha createTask error: {data}", "error")
            return False

        task_id = data.get("taskId")
        if not task_id:
            log("2Captcha: no taskId in response", "error")
            return False

        log(f"2Captcha task: {task_id} — waiting for solution...")
        coords = await _poll_2captcha_task(task_id)
        if not coords:
            return False

        log(f"2Captcha solution: x={coords['x']}, y={coords['y']}")
        captcha_frame, captcha_el = await _find_captcha_container(pw_page)
        return await _drag_slider(pw_page, captcha_frame, captcha_el, coords)

    except Exception as e:
        log(f"2Captcha fallback failed: {e}", "error")
        return False


async def _poll_2captcha_task(task_id) -> Optional[dict]:
    """Poll 2Captcha getTaskResult until solved or timed out."""
    import httpx
    deadline = time.time() + CAPTCHA_SOLVE_TIMEOUT

    async with httpx.AsyncClient(timeout=30) as client:
        while time.time() < deadline:
            await asyncio.sleep(5)
            resp = await client.post("https://api.2captcha.com/getTaskResult", json={
                "clientKey": CAPTCHA_API_KEY,
                "taskId":    task_id,
            })
            data = resp.json()

            if data.get("errorId", 0) != 0:
                log(f"2Captcha poll error: {data}", "error")
                return None

            status = data.get("status")
            if status == "ready":
                solution = data.get("solution", {})

                # ImageToCoordinates returns: {"coordinates": [{"x": 123, "y": 45}]}
                coords_list = solution.get("coordinates") or solution.get("clicks")
                if isinstance(coords_list, list) and coords_list:
                    c = coords_list[0]
                    if isinstance(c, dict):
                        return {"x": int(c.get("x", 0)), "y": int(c.get("y", 0))}

                # Some task types return x/y directly
                if "x" in solution:
                    return {"x": int(solution["x"]), "y": int(solution.get("y", 0))}

                log(f"2Captcha: unrecognised solution shape: {solution!r}", "error")
                return None

            if status != "processing":
                log(f"2Captcha unexpected status: {status}", "error")
                return None

    log("2Captcha solve timed out", "error")
    return None


# ═══════════════════════════════════════════════════════════════════════════
# MOUSE DRAG — Human-like slider movement (unchanged)
# ═══════════════════════════════════════════════════════════════════════════

_JS_FIND_SLIDER_BTN = """
() => {
    // Find the leftmost small interactive element — the slider handle starts at the left
    const candidates = Array.from(document.querySelectorAll('div,span,button,a,i,em'));
    const handles = candidates.filter(el => {
        const r  = el.getBoundingClientRect();
        const st = window.getComputedStyle(el);
        if (r.width <= 5 || r.height <= 5) return false;                 // too tiny
        if (r.width > 120 || r.height > 120) return false;               // too large
        if (st.display === 'none' || st.visibility === 'hidden') return false;
        const cur = st.cursor;
        const cls = (el.className || '').toString().toLowerCase();
        const id  = (el.id || '').toLowerCase();
        if (cur === 'pointer' || cur === 'grab' || cur === 'move' || cur === 'ew-resize') return true;
        if (cls.match(/btn|button|slider|handle|drag|n1z|nc_|move/)) return true;
        if (id.match(/n1z|btn|slider|handle|drag/)) return true;
        return false;
    });
    if (!handles.length) return null;
    // Pick leftmost visible element (slider starts at left edge of track)
    handles.sort((a, b) => a.getBoundingClientRect().left - b.getBoundingClientRect().left);
    const best = handles[0];
    const r = best.getBoundingClientRect();
    return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
}
"""


async def _js_find_slider_button(pw_page, search_frames: list) -> Optional[dict]:
    """Use JS to locate a slider-button-like element when CSS selectors fail."""
    for frame in search_frames:
        try:
            coords = await frame.evaluate(_JS_FIND_SLIDER_BTN)
            if coords and coords.get("x") and coords.get("y"):
                return coords
        except Exception:
            continue
    return None


_SLIDER_BTN_SELECTORS = [
    "#nc_1_n1z", "#nc_2_n1z",
    "[id$='n1z']",
    ".nc_btn", ".btn_slide", ".btn-slide",
    "[class*='btn_slide']", "[class*='nc_btn']",
    "[class*='move-target']", "[class*='slider-button']",
    "[class*='nc-lang-cnt'] [class*='btn']",
]


async def _drag_slider(pw_page, captcha_frame, captcha_el, target_coords: dict) -> bool:
    """
    Simulate a human-like slider drag from the slider button to target_coords.

    target_coords must be in page *viewport* (screen) coordinates.
    """
    try:
        # Search the frame where the CAPTCHA lives first, then fall back to main page
        slider_btn = None
        search_frames = []
        if captcha_frame is not None:
            search_frames.append(captcha_frame)
        for f in [pw_page] + list(pw_page.frames):
            if f not in search_frames:
                search_frames.append(f)

        for frame in search_frames:
            for sel in _SLIDER_BTN_SELECTORS:
                try:
                    loc = frame.locator(sel)
                    if await loc.count() > 0:
                        slider_btn = loc.first
                        break
                except Exception:
                    continue
            if slider_btn:
                break

        if not slider_btn and captcha_el:
            slider_btn = captcha_el.locator(
                "[class*='btn'], [class*='slider'], [id*='n1z'], [class*='handle'], [class*='drag']"
            ).first

        # JS fallback: scan for any small interactive element that looks like a drag handle
        if not slider_btn or await slider_btn.count() == 0:
            js_btn_coords = await _js_find_slider_button(pw_page, search_frames)
            if js_btn_coords:
                log(f"Slider button found via JS scan: {js_btn_coords}")
                # Inject a synthetic drag using page.mouse directly
                start_x = float(js_btn_coords["x"])
                start_y = float(js_btn_coords["y"])
                end_x   = float(target_coords["x"])
                end_y   = start_y

                distance = end_x - start_x
                if distance < 5:
                    log(f"Drag distance too small ({distance:.0f}px) — skipping", "warn")
                    return False

                log(f"Dragging (JS-found btn): ({start_x:.0f},{start_y:.0f}) → ({end_x:.0f},{end_y:.0f}) = {distance:.0f}px")
                trajectory = _generate_human_trajectory(start_x, start_y, end_x, end_y)
                mouse = pw_page.mouse
                await mouse.move(start_x, start_y)
                await asyncio.sleep(random.uniform(0.1, 0.3))
                await mouse.down()
                await asyncio.sleep(random.uniform(0.05, 0.15))
                for point in trajectory:
                    await mouse.move(point["x"], point["y"])
                    await asyncio.sleep(point["delay"])
                await asyncio.sleep(random.uniform(0.05, 0.2))
                await mouse.up()
                await asyncio.sleep(random.uniform(1.5, 3.0))
                still_captcha = await detect_captcha(pw_page)
                return not still_captcha["detected"]

            log("Slider button not found via CSS or JS scan", "warn")
            return False

        await slider_btn.scroll_into_view_if_needed()
        await asyncio.sleep(random.uniform(0.3, 0.6))
        bbox = await slider_btn.bounding_box()
        if not bbox:
            log("Slider button has no bounding box", "warn")
            return False

        start_x = bbox["x"] + bbox["width"] / 2
        start_y = bbox["y"] + bbox["height"] / 2
        end_x   = float(target_coords["x"])
        end_y   = start_y   # horizontal slider — Y stays the same

        distance = end_x - start_x
        if distance < 5:
            log(f"Drag distance too small ({distance:.0f}px) — skipping", "warn")
            return False

        log(f"Dragging slider: ({start_x:.0f},{start_y:.0f}) → ({end_x:.0f},{end_y:.0f}) = {distance:.0f}px")

        trajectory = _generate_human_trajectory(start_x, start_y, end_x, end_y)
        mouse = pw_page.mouse
        await mouse.move(start_x, start_y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
        await mouse.down()
        await asyncio.sleep(random.uniform(0.05, 0.15))

        for point in trajectory:
            await mouse.move(point["x"], point["y"])
            await asyncio.sleep(point["delay"])

        await asyncio.sleep(random.uniform(0.05, 0.2))
        await mouse.up()
        await asyncio.sleep(random.uniform(1.5, 3.0))

        still_captcha = await detect_captcha(pw_page)
        if not still_captcha["detected"]:
            log("CAPTCHA solved successfully!")
            return True
        log("CAPTCHA still present after drag — solve may have failed", "warn")
        return False

    except Exception as e:
        log(f"Slider drag failed: {e}", "error")
        return False


def _generate_human_trajectory(
    start_x: float, start_y: float,
    end_x: float,   end_y: float,
) -> list[dict]:
    """Generate a human-like mouse trajectory (ease-out cubic + overshoot)."""
    distance = end_x - start_x
    steps    = max(20, int(distance / 3))
    points   = []

    for i in range(steps + 1):
        t      = i / steps
        eased  = 1 - (1 - t) ** 3    # ease-out cubic
        x      = start_x + distance * eased
        y_wobble = random.gauss(0, 1.5) if 0.1 < t < 0.9 else 0
        y      = start_y + y_wobble

        if t < 0.1:
            delay = random.uniform(0.015, 0.035)
        elif t > 0.85:
            delay = random.uniform(0.02, 0.05)
        else:
            delay = random.uniform(0.005, 0.02)

        points.append({"x": x, "y": y, "delay": delay})

    overshoot = random.uniform(5, 15)
    points.append({"x": end_x + overshoot, "y": start_y + random.gauss(0, 1),
                   "delay": random.uniform(0.03, 0.06)})
    points.append({"x": end_x + random.uniform(-2, 2), "y": start_y + random.gauss(0, 0.5),
                   "delay": random.uniform(0.04, 0.08)})
    return points


# ═══════════════════════════════════════════════════════════════════════════
# DEBUG — DOM snapshot
# ═══════════════════════════════════════════════════════════════════════════

_JS_DUMP_DOM = """
() => {
    const snap = {
        url:     location.href,
        title:   document.title,
        frames:  [],
        captcha_elements: [],
        all_images: [],
        interactive_small: [],
    };

    // ── helper ──────────────────────────────────────────────────────────
    function elInfo(el) {
        const r  = el.getBoundingClientRect();
        const st = window.getComputedStyle(el);
        return {
            tag:      el.tagName.toLowerCase(),
            id:       el.id   || null,
            classes:  el.className ? el.className.toString().split(' ').filter(Boolean) : [],
            rect:     { x: Math.round(r.left), y: Math.round(r.top),
                        w: Math.round(r.width), h: Math.round(r.height) },
            visible:  r.width > 0 && r.height > 0
                      && st.display !== 'none' && st.visibility !== 'hidden',
            cursor:   st.cursor,
            src:      el.src  || null,
            href:     el.href || null,
            naturalW: el.naturalWidth  || null,
            naturalH: el.naturalHeight || null,
        };
    }

    // ── CAPTCHA-keyword elements ─────────────────────────────────────────
    const kwRe = /captcha|nocaptcha|baxia|slider|verify|punish|nc_|n1z|identity/i;
    document.querySelectorAll('*').forEach(el => {
        const combined = (el.id || '') + ' ' + (el.className ? el.className.toString() : '');
        if (kwRe.test(combined)) snap.captcha_elements.push(elInfo(el));
    });

    // ── All <img> elements ───────────────────────────────────────────────
    document.querySelectorAll('img').forEach(el => snap.all_images.push(elInfo(el)));

    // ── Small interactive elements (potential slider buttons) ────────────
    document.querySelectorAll('div,span,button,a,i,em').forEach(el => {
        const r  = el.getBoundingClientRect();
        const st = window.getComputedStyle(el);
        if (r.width < 5 || r.height < 5) return;
        if (r.width > 150 || r.height > 150) return;
        const cur = st.cursor;
        const cls = (el.className || '').toString().toLowerCase();
        const id  = (el.id || '').toLowerCase();
        if (cur === 'pointer' || cur === 'grab' || cur === 'move' || cur === 'ew-resize'
            || cls.match(/btn|slider|handle|drag|n1z|nc_/)
            || id.match(/n1z|btn|slider|handle|drag/)) {
            snap.interactive_small.push(elInfo(el));
        }
    });

    // ── iframes present ──────────────────────────────────────────────────
    document.querySelectorAll('iframe').forEach(el => {
        snap.frames.push({ src: el.src, id: el.id, classes: el.className });
    });

    return snap;
}
"""


async def dump_captcha_dom(pw_page) -> None:
    """
    Snapshot all CAPTCHA-related DOM elements across every frame and write
    them to /tmp/captcha_debug/dom_dump_<timestamp>.json.

    Only runs when CAPTCHA_DEBUG=1.
    """
    if not _DEBUG:
        return

    os.makedirs(_DEBUG_DIR, exist_ok=True)
    ts    = time.strftime("%Y%m%dT%H%M%S")
    frames = [pw_page] + list(pw_page.frames)
    result = {"timestamp": ts, "frame_count": len(frames), "frames": []}

    for i, frame in enumerate(frames):
        frame_label = f"frame_{i}"
        try:
            frame_url = frame.url
        except Exception:
            frame_url = "unknown"

        try:
            snap = await frame.evaluate(_JS_DUMP_DOM)
        except Exception as exc:
            snap = {"error": str(exc)}

        result["frames"].append({"index": i, "url": frame_url, "snapshot": snap})

    path = os.path.join(_DEBUG_DIR, f"dom_dump_{ts}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)

    log(f"DOM dump saved → {path}")


# ═══════════════════════════════════════════════════════════════════════════
# HIGH-LEVEL ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

async def handle_captcha(pw_page, job_id: str = None) -> bool:
    """
    Detect whether the current page is blocked by a CAPTCHA.

    Returns True when the page is clear, False when a CAPTCHA or
    verification redirect is present.
    """
    detection = await detect_captcha(pw_page)
    if not detection["detected"]:
        return True

    captcha_type = detection["type"]
    log(f"CAPTCHA detected: type={captcha_type}, details={detection['details']}")

    # Dump DOM snapshot for debugging (only when CAPTCHA_DEBUG=1)
    await dump_captcha_dom(pw_page)

    if captcha_type == "url_redirect":
        log("Redirected to verification page — cookies may be expired", "warn")
    else:
        log("Verification challenge present — scrape should skip this page", "warn")

    return False
