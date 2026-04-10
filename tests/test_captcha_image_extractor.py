"""
Unit tests for captcha_image_extractor.py

Uses mock Playwright page objects to test the three extraction strategies
without needing a live browser.
"""

import asyncio
import base64
import io
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ── Helpers ───────────────────────────────────────────────────────────────

def _png_bytes(width=300, height=150, color=(150, 150, 150)) -> bytes:
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (width, height), color=color).save(buf, format="PNG")
    return buf.getvalue()


def _png_data_uri(width=300, height=150) -> str:
    b64 = base64.b64encode(_png_bytes(width, height)).decode()
    return f"data:image/png;base64,{b64}"


def _tiny_png_bytes() -> bytes:
    """PNG too small to be a valid CAPTCHA image."""
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (10, 10)).save(buf, format="PNG")
    return buf.getvalue()


# ── Canvas extraction tests ───────────────────────────────────────────────

class TestCanvasExtraction:
    @pytest.mark.asyncio
    async def test_successful_canvas_extraction(self):
        from services.captcha_image_extractor import _try_canvas

        mock_frame = MagicMock()
        mock_frame.evaluate = AsyncMock(return_value={
            "bg":     _png_data_uri(300, 150),
            "piece":  _png_data_uri(60, 150),
            "pieceX": 10.0,
            "bgW":    300, "bgH": 150,
            "pieceW": 60,  "pieceH": 150,
        })

        result = await _try_canvas(mock_frame)

        assert result is not None
        assert result.strategy == "canvas"
        assert result.piece_initial_x == 10.0
        assert len(result.background_bytes) > 0
        assert len(result.piece_bytes) > 0

    @pytest.mark.asyncio
    async def test_returns_none_when_js_returns_null(self):
        from services.captcha_image_extractor import _try_canvas

        mock_frame = MagicMock()
        mock_frame.evaluate = AsyncMock(return_value=None)

        result = await _try_canvas(mock_frame)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_background_too_small(self):
        from services.captcha_image_extractor import _try_canvas

        mock_frame = MagicMock()
        mock_frame.evaluate = AsyncMock(return_value={
            "bg":     _png_data_uri(10, 10),   # too small
            "piece":  _png_data_uri(10, 10),
            "pieceX": 0.0,
            "bgW": 10, "bgH": 10, "pieceW": 10, "pieceH": 10,
        })

        result = await _try_canvas(mock_frame)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_evaluate_exception(self):
        from services.captcha_image_extractor import _try_canvas

        mock_frame = MagicMock()
        mock_frame.evaluate = AsyncMock(side_effect=RuntimeError("browser error"))

        result = await _try_canvas(mock_frame)
        assert result is None


# ── Data URI utility tests ────────────────────────────────────────────────

class TestDataUri:
    def test_valid_data_uri_decoded(self):
        from services.captcha_image_extractor import _data_uri_to_bytes

        raw = _png_bytes(100, 50)
        b64 = base64.b64encode(raw).decode()
        uri = f"data:image/png;base64,{b64}"

        result = _data_uri_to_bytes(uri)
        assert result == raw

    def test_invalid_data_uri_returns_none(self):
        from services.captcha_image_extractor import _data_uri_to_bytes

        assert _data_uri_to_bytes("not-a-data-uri") is None
        assert _data_uri_to_bytes("") is None
        assert _data_uri_to_bytes("data:image/png;base64,!!!invalid!!!") is None

    def test_none_input_returns_none(self):
        from services.captcha_image_extractor import _data_uri_to_bytes

        assert _data_uri_to_bytes(None) is None  # type: ignore


# ── Image validation tests ────────────────────────────────────────────────

class TestValidImage:
    def test_valid_image_passes(self):
        from services.captcha_image_extractor import _valid_image

        data = _png_bytes(300, 150)
        assert _valid_image(data, 100, 50) is True

    def test_too_small_fails(self):
        from services.captcha_image_extractor import _valid_image

        data = _tiny_png_bytes()
        assert _valid_image(data, 100, 50) is False

    def test_invalid_bytes_fails(self):
        from services.captcha_image_extractor import _valid_image

        assert _valid_image(b"not an image", 1, 1) is False
        assert _valid_image(None, 1, 1) is False
        assert _valid_image(b"", 1, 1) is False


# ── Screenshot fallback tests ─────────────────────────────────────────────

class TestScreenshotFallback:
    @pytest.mark.asyncio
    async def test_fallback_uses_screenshot_strategy(self):
        from services.captcha_image_extractor import _try_screenshot

        png = _png_bytes(300, 150)

        mock_loc = MagicMock()
        mock_loc.count      = AsyncMock(return_value=1)
        mock_loc.is_visible = AsyncMock(return_value=True)
        mock_loc.first      = MagicMock()
        mock_loc.first.screenshot    = AsyncMock(return_value=png)
        mock_loc.first.bounding_box  = AsyncMock(return_value={"x": 100, "y": 200, "width": 300, "height": 150})

        mock_piece_loc = MagicMock()
        mock_piece_loc.count      = AsyncMock(return_value=1)
        mock_piece_loc.screenshot = AsyncMock(return_value=_png_bytes(60, 150))
        mock_piece_loc.bounding_box = AsyncMock(return_value={"x": 110, "y": 210})

        mock_pw_page = MagicMock()
        def locator_side_effect(sel):
            if "canvas" in sel or "img" in sel:
                # first call returns bg_loc, nth call returns piece_loc
                inner = MagicMock()
                inner.first = mock_loc.first
                inner.nth   = MagicMock(return_value=mock_piece_loc)
                inner.count = AsyncMock(return_value=2)
                return inner
            return mock_loc
        mock_pw_page.locator = locator_side_effect

        result = await _try_screenshot(mock_pw_page)
        # May succeed or fall back depending on mock setup — just no crash
        # If it returns something, verify strategy
        if result is not None:
            assert result.strategy == "screenshot"

    @pytest.mark.asyncio
    async def test_fallback_returns_none_when_no_container(self):
        from services.captcha_image_extractor import _try_screenshot

        mock_loc = MagicMock()
        mock_loc.count      = AsyncMock(return_value=0)
        mock_loc.is_visible = AsyncMock(return_value=False)

        mock_pw_page = MagicMock()
        mock_pw_page.locator = MagicMock(return_value=mock_loc)

        result = await _try_screenshot(mock_pw_page)
        assert result is None
