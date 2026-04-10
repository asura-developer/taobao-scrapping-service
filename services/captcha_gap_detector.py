"""
CAPTCHA slider gap detector using OpenCV.

Given the background image (with the gap/hole) and the puzzle piece image,
finds the X position of the gap in the background so the slider can be
dragged to the correct position.

Two detection strategies:
  1. Template matching on Canny edge maps  (primary — robust against color filters)
  2. Contour detection on background only  (fallback when piece image is unavailable)

Public API
----------
  detect_gap_x(background_bytes, piece_bytes) -> GapDetectionResult | None

Debug mode
----------
  Set env var CAPTCHA_DEBUG=1 to save intermediate images to /tmp/captcha_debug/.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from io import BytesIO
from typing import Optional

logger = logging.getLogger(__name__)

_DEBUG = os.getenv("CAPTCHA_DEBUG", "0") == "1"
_DEBUG_DIR = "/tmp/captcha_debug"

# Minimum confidence to accept a template-match result
_MIN_CONFIDENCE = 0.30

# Expected gap size range (pixels in background image space)
_GAP_MIN_W, _GAP_MAX_W = 30, 120
_GAP_MIN_H, _GAP_MAX_H = 30, 160

# Gap is never in the leftmost 10% or rightmost 5% of the background
_GAP_X_MIN_FRAC = 0.10
_GAP_X_MAX_FRAC = 0.95


@dataclass(frozen=True)
class GapDetectionResult:
    gap_x: int        # left-edge X of gap in background image pixel space
    confidence: float # 0.0–1.0
    method: str       # "template_match" | "contour" | "fallback_center"


def detect_gap_x(
    background_bytes: bytes,
    piece_bytes: bytes,
) -> Optional[GapDetectionResult]:
    """
    Find the gap X position in the background image.

    Returns GapDetectionResult or None if detection completely fails.
    """
    try:
        import cv2
        import numpy as np
    except ImportError:
        logger.error("[CAPTCHA] opencv-python-headless not installed. Run: pip install opencv-python-headless")
        return None

    bg_img    = _bytes_to_cv2(background_bytes)
    piece_img = _bytes_to_cv2(piece_bytes)

    if bg_img is None:
        logger.error("[CAPTCHA] Gap detector: could not decode background image")
        return None

    if _DEBUG:
        _save_debug(bg_img, "01_background.png")
        if piece_img is not None:
            _save_debug(piece_img, "02_piece.png")

    # Strategy 1: template matching (requires both images)
    if piece_img is not None:
        result = _template_match(bg_img, piece_img)
        if result and result.confidence >= _MIN_CONFIDENCE:
            logger.info(
                f"[CAPTCHA] Gap detected via template matching: x={result.gap_x}, "
                f"confidence={result.confidence:.2f}"
            )
            return result
        _conf = result.confidence if result else 0
        logger.debug(
            f"[CAPTCHA] Template match confidence too low ({_conf:.2f}), trying contour fallback"
        )

    # Strategy 2: contour detection on background only
    result = _contour_detect(bg_img)
    if result:
        logger.info(
            f"[CAPTCHA] Gap detected via contour: x={result.gap_x}, "
            f"confidence={result.confidence:.2f}"
        )
        return result

    # Strategy 3: brightness-based column scan (Alibaba gap is noticeably darker)
    result = _brightness_detect(bg_img)
    if result:
        logger.info(
            f"[CAPTCHA] Gap detected via brightness: x={result.gap_x}, "
            f"confidence={result.confidence:.2f}"
        )
        return result

    # Strategy 4: geometric center fallback (very rough, better than nothing)
    h, w = bg_img.shape[:2]
    center_x = int(w * 0.55)   # gap is usually slightly right of center
    logger.warning(f"[CAPTCHA] All detection strategies failed — using geometric fallback x={center_x}")
    return GapDetectionResult(gap_x=center_x, confidence=0.0, method="fallback_center")


# ── Strategy 1: Template matching on Canny edge maps ─────────────────────

def _template_match(bg_img, piece_img) -> Optional[GapDetectionResult]:
    import cv2
    import numpy as np

    bg_h, bg_w = bg_img.shape[:2]
    pc_h, pc_w = piece_img.shape[:2]

    # Scale piece to match background height if they differ significantly
    if abs(pc_h - bg_h) > 10 and bg_h > 0:
        scale = bg_h / pc_h
        new_w = max(1, int(pc_w * scale))
        piece_img = cv2.resize(piece_img, (new_w, bg_h), interpolation=cv2.INTER_AREA)
        pc_h, pc_w = piece_img.shape[:2]

    # Piece must be smaller than background
    if pc_w >= bg_w or pc_h > bg_h:
        logger.debug("[CAPTCHA] Piece image larger than background — skipping template match")
        return None

    # Convert both to grayscale
    bg_gray    = _to_gray(bg_img)
    piece_gray = _to_gray(piece_img)

    # Extract alpha mask from piece if available (RGBA image)
    piece_mask = None
    if len(piece_img.shape) == 3 and piece_img.shape[2] == 4:
        piece_mask = piece_img[:, :, 3]
        if piece_mask.max() == 0:
            piece_mask = None  # blank alpha — ignore

    # Apply Canny edge detection — robust against Alibaba's color manipulation
    bg_edges    = cv2.Canny(bg_gray,    80, 180)
    piece_edges = cv2.Canny(piece_gray, 80, 180)

    if _DEBUG:
        _save_debug(bg_edges,    "03_bg_edges.png")
        _save_debug(piece_edges, "04_piece_edges.png")

    # Template match — try Canny edges first, then raw grayscale
    result_map = None
    try:
        if piece_mask is not None:
            mask_resized = cv2.resize(piece_mask, (piece_edges.shape[1], piece_edges.shape[0]))
            result_map = cv2.matchTemplate(
                bg_edges, piece_edges,
                cv2.TM_CCORR_NORMED,
                mask=mask_resized,
            )
        else:
            result_map = cv2.matchTemplate(bg_edges, piece_edges, cv2.TM_CCOEFF_NORMED)
    except cv2.error as e:
        logger.debug(f"[CAPTCHA] matchTemplate (edges) failed: {e}")

    # If Canny edges produce a low-confidence map, also try raw grayscale
    edge_max = float(result_map.max()) if result_map is not None else 0.0
    if edge_max < _MIN_CONFIDENCE:
        try:
            gray_map = cv2.matchTemplate(bg_gray, piece_gray, cv2.TM_CCOEFF_NORMED)
            if gray_map.max() > edge_max:
                result_map = gray_map
                logger.debug("[CAPTCHA] Using raw-grayscale template map (better than edges)")
        except cv2.error:
            pass

    if result_map is None:
        return None

    if _DEBUG:
        # Normalize heatmap to 0-255 for saving
        norm_map = cv2.normalize(result_map, None, 0, 255, cv2.NORM_MINMAX, cv2.CV_8U)
        _save_debug(norm_map, "05_match_heatmap.png")

    _, max_val, _, max_loc = cv2.minMaxLoc(result_map)
    gap_x = int(max_loc[0])

    # Reject matches at the extreme edges (unlikely to be real gap positions)
    min_x = int(bg_w * _GAP_X_MIN_FRAC)
    max_x = int(bg_w * _GAP_X_MAX_FRAC)
    if not (min_x <= gap_x <= max_x):
        logger.debug(f"[CAPTCHA] Template match gap_x={gap_x} outside valid range [{min_x},{max_x}]")
        # Try second-best location by masking the best and re-searching
        result_map_masked = result_map.copy()
        mask_r = min(20, pc_w // 2)
        x1 = max(0, gap_x - mask_r)
        x2 = min(result_map.shape[1], gap_x + mask_r)
        result_map_masked[:, x1:x2] = 0
        _, max_val2, _, max_loc2 = cv2.minMaxLoc(result_map_masked)
        if max_val2 > 0.3 and (min_x <= max_loc2[0] <= max_x):
            gap_x  = int(max_loc2[0])
            max_val = float(max_val2)
        else:
            max_val = float(max_val) * 0.5  # penalise out-of-range result

    return GapDetectionResult(
        gap_x=gap_x,
        confidence=float(np.clip(max_val, 0.0, 1.0)),
        method="template_match",
    )


# ── Strategy 3: Brightness / darkness column scan ────────────────────────

def _contour_detect(bg_img) -> Optional[GapDetectionResult]:
    import cv2
    import numpy as np

    bg_gray = _to_gray(bg_img)
    h, w = bg_gray.shape[:2]

    # Blur to reduce noise, then Canny
    blurred = cv2.GaussianBlur(bg_gray, (5, 5), 0)
    edges   = cv2.Canny(blurred, 60, 160)

    if _DEBUG:
        _save_debug(edges, "06_contour_edges.png")

    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return None

    candidates = []
    x_min = int(w * _GAP_X_MIN_FRAC)
    x_max = int(w * _GAP_X_MAX_FRAC)

    for cnt in contours:
        x, y, cw, ch = cv2.boundingRect(cnt)
        # Filter by size
        if not (_GAP_MIN_W <= cw <= _GAP_MAX_W and _GAP_MIN_H <= ch <= _GAP_MAX_H):
            continue
        # Filter by X position (gap is not at the very left or right)
        if not (x_min <= x <= x_max):
            continue
        # Aspect ratio: roughly square-ish
        aspect = cw / ch
        if not (0.3 <= aspect <= 2.5):
            continue
        area = cv2.contourArea(cnt)
        candidates.append((area, x, cw, ch))

    if not candidates:
        return None

    # Pick the largest candidate (gap typically has the most edge area)
    candidates.sort(reverse=True)
    _, best_x, best_w, best_h = candidates[0]

    # Confidence: normalised by how many candidates there are (fewer = more confident)
    confidence = max(0.3, 0.7 - (len(candidates) - 1) * 0.05)

    return GapDetectionResult(
        gap_x=best_x,
        confidence=float(confidence),
        method="contour",
    )


# ── Strategy 3: Brightness / darkness column scan ────────────────────────

def _brightness_detect(bg_img) -> Optional[GapDetectionResult]:
    """
    Find the gap by scanning for the darkest column band in the background.

    Alibaba's CAPTCHA renders the gap as a noticeably darker region
    (shadow + missing texture). Scanning column means works even when
    template matching fails due to style variations.
    """
    import cv2
    import numpy as np

    bg_h, bg_w = bg_img.shape[:2]
    bg_gray = _to_gray(bg_img)

    # Work on the vertical centre (exclude top/bottom 20% which have UI chrome)
    y_start = int(bg_h * 0.15)
    y_end   = int(bg_h * 0.85)
    roi     = bg_gray[y_start:y_end, :].astype(np.float32)

    # Column-wise mean intensity
    col_means = np.mean(roi, axis=0)

    # Smooth to merge adjacent dark columns into one region
    smooth_w = max(3, bg_w // 20) | 1   # ensure odd
    col_smooth = cv2.GaussianBlur(
        col_means.reshape(1, -1), (1, smooth_w), 0
    ).flatten()

    x_min = int(bg_w * _GAP_X_MIN_FRAC)
    x_max = int(bg_w * _GAP_X_MAX_FRAC)
    valid  = col_smooth[x_min:x_max]

    if valid.size == 0:
        return None

    min_idx = int(np.argmin(valid))
    gap_x   = x_min + min_idx

    gap_val  = float(col_smooth[gap_x])
    mean_val = float(np.mean(valid))

    if mean_val <= 0:
        return None

    # How much darker is the gap column vs the image mean?
    darkness_ratio = 1.0 - (gap_val / mean_val)

    # Require at least 8% darkness difference to avoid noise
    if darkness_ratio < 0.08:
        return None

    confidence = float(np.clip(darkness_ratio * 1.5, 0.0, 0.75))

    if _DEBUG:
        import os
        os.makedirs(_DEBUG_DIR, exist_ok=True)
        # Draw gap on debug copy
        debug_img = bg_img.copy()
        cv2.line(debug_img, (gap_x, 0), (gap_x, bg_h), (0, 0, 255), 2)
        _save_debug(debug_img, "07_brightness_gap.png")

    return GapDetectionResult(gap_x=gap_x, confidence=confidence, method="brightness")


# ── Utilities ──────────────────────────────────────────────────────────────

def _bytes_to_cv2(data: bytes):
    """Decode image bytes to an OpenCV numpy array (BGR or BGRA)."""
    try:
        import cv2
        import numpy as np
        arr = np.frombuffer(data, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_UNCHANGED)
        return img
    except Exception:
        return None


def _to_gray(img) -> "np.ndarray":
    """Convert an OpenCV image to grayscale regardless of channel count."""
    import cv2
    if len(img.shape) == 2:
        return img
    if img.shape[2] == 4:
        return cv2.cvtColor(img, cv2.COLOR_BGRA2GRAY)
    return cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)


def _save_debug(img, filename: str) -> None:
    """Save an OpenCV image to the debug directory."""
    import cv2
    import os
    os.makedirs(_DEBUG_DIR, exist_ok=True)
    path = os.path.join(_DEBUG_DIR, filename)
    cv2.imwrite(path, img)
    logger.debug(f"[CAPTCHA] Debug image saved: {path}")
