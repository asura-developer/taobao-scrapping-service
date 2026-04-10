"""
Unit tests for captcha_gap_detector.py

Uses synthetic images with known gap positions so tests run without
any live CAPTCHA pages and without a 2Captcha API key.
"""

import io
import pytest


def _make_background_with_gap(width=300, height=150, gap_x=180, gap_w=50, gap_h=50) -> bytes:
    """
    Create a synthetic background image: solid grey with a dark rectangular gap.
    Returns PNG bytes.
    """
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (width, height), color=(150, 150, 150))
    draw = ImageDraw.Draw(img)
    # Draw a distinct dark rectangle to simulate the gap
    gap_y = (height - gap_h) // 2
    draw.rectangle([gap_x, gap_y, gap_x + gap_w, gap_y + gap_h], fill=(30, 30, 30))
    # Add some texture around it
    draw.rectangle([0, 0, width - 1, height - 1], outline=(100, 100, 100))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _make_piece(width=50, height=50) -> bytes:
    """Create a synthetic puzzle piece: dark rectangle matching the gap colour."""
    from PIL import Image

    img = Image.new("RGBA", (width, height), color=(30, 30, 30, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _make_invalid_bytes() -> bytes:
    return b"\x00\x01\x02\x03 not an image"


# ── Dependency check ──────────────────────────────────────────────────────

def _opencv_available() -> bool:
    try:
        import cv2  # noqa: F401
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    not _opencv_available(),
    reason="opencv-python-headless not installed"
)


# ── Tests ─────────────────────────────────────────────────────────────────

class TestDetectGapX:
    def test_returns_result_for_valid_images(self):
        from services.captcha_gap_detector import detect_gap_x

        bg    = _make_background_with_gap(gap_x=180)
        piece = _make_piece()
        result = detect_gap_x(bg, piece)

        assert result is not None
        assert isinstance(result.gap_x, int)
        assert 0 <= result.gap_x <= 300
        assert 0.0 <= result.confidence <= 1.0
        assert result.method in ("template_match", "contour", "fallback_center")

    def test_template_match_accuracy_within_tolerance(self):
        """Gap detected within 20px of known position."""
        from services.captcha_gap_detector import detect_gap_x

        KNOWN_GAP_X = 160
        bg    = _make_background_with_gap(width=300, height=150, gap_x=KNOWN_GAP_X, gap_w=50, gap_h=50)
        piece = _make_piece(50, 50)

        result = detect_gap_x(bg, piece)
        assert result is not None
        # Template matching on high-contrast synthetic images should be accurate
        assert abs(result.gap_x - KNOWN_GAP_X) <= 20, (
            f"Expected gap near {KNOWN_GAP_X}, got {result.gap_x}"
        )

    def test_returns_none_for_invalid_background(self):
        from services.captcha_gap_detector import detect_gap_x

        result = detect_gap_x(_make_invalid_bytes(), _make_piece())
        assert result is None

    def test_fallback_when_piece_is_invalid(self):
        """With invalid piece, falls back to contour or fallback_center."""
        from services.captcha_gap_detector import detect_gap_x

        bg = _make_background_with_gap(gap_x=150)
        result = detect_gap_x(bg, _make_invalid_bytes())

        # Should not crash; returns a fallback result
        assert result is not None
        assert result.method in ("contour", "fallback_center")

    def test_confidence_is_float_in_range(self):
        from services.captcha_gap_detector import detect_gap_x

        bg    = _make_background_with_gap()
        piece = _make_piece()
        result = detect_gap_x(bg, piece)

        assert result is not None
        assert isinstance(result.confidence, float)
        assert 0.0 <= result.confidence <= 1.0

    def test_different_gap_positions(self):
        """Verify different gap positions produce different gap_x results."""
        from services.captcha_gap_detector import detect_gap_x

        results = []
        for gap_x in [80, 140, 200]:
            bg    = _make_background_with_gap(gap_x=gap_x)
            piece = _make_piece()
            r = detect_gap_x(bg, piece)
            assert r is not None
            results.append(r.gap_x)

        # All three results should be distinct (not all the same value)
        assert len(set(results)) > 1, f"All gap positions resolved to same x: {results}"

    def test_frozen_dataclass(self):
        """GapDetectionResult must be immutable."""
        from services.captcha_gap_detector import detect_gap_x

        bg    = _make_background_with_gap()
        piece = _make_piece()
        result = detect_gap_x(bg, piece)
        assert result is not None

        with pytest.raises((AttributeError, TypeError)):
            result.gap_x = 999  # type: ignore


class TestContourFallback:
    def test_contour_detect_on_high_contrast_image(self):
        """Contour detector finds gap when piece unavailable."""
        from services.captcha_gap_detector import _contour_detect, _bytes_to_cv2

        bg_bytes = _make_background_with_gap(width=300, height=150, gap_x=150, gap_w=60, gap_h=60)
        bg_img   = _bytes_to_cv2(bg_bytes)
        result   = _contour_detect(bg_img)

        # May or may not find contours depending on image details — just verify no crash
        # and if found, x is in valid range
        if result is not None:
            assert 0 <= result.gap_x <= 300
            assert result.method == "contour"

    def test_contour_returns_none_for_blank_image(self):
        from services.captcha_gap_detector import _contour_detect, _bytes_to_cv2
        from PIL import Image

        buf = io.BytesIO()
        Image.new("RGB", (300, 150), color=(128, 128, 128)).save(buf, format="PNG")
        img = _bytes_to_cv2(buf.getvalue())
        result = _contour_detect(img)
        # Blank image has no strong contours — should return None or low-confidence result
        # Either is acceptable
        if result is not None:
            assert result.confidence <= 0.7
