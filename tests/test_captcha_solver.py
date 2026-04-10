"""
Unit tests for captcha_solver.py (orchestration flow).
"""

import asyncio
import io
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from services.captcha_solver import detect_captcha_from_html, handle_captcha
from services.captcha_solver import _solve_locally


# ── detect_captcha_from_html (sync) ──────────────────────────────────────

class TestDetectCaptchaFromHtml:
    def test_detects_nocaptcha_string(self):
        assert detect_captcha_from_html("<div id='nocaptcha'>slider</div>") is True

    def test_detects_awsc_script(self):
        assert detect_captcha_from_html("<script>AWSC.use('nc')</script>") is True

    def test_detects_baxia_dialog(self):
        assert detect_captcha_from_html("<div id='baxia-dialog'></div>") is True

    def test_no_captcha_in_normal_html(self):
        assert detect_captcha_from_html("<html><body><h1>Products</h1></body></html>") is False

    def test_empty_string(self):
        assert detect_captcha_from_html("") is False


# ── handle_captcha orchestration ─────────────────────────────────────────

class TestHandleCaptcha:
    async def test_returns_true_when_no_captcha_detected(self):
        mock_page = MagicMock()
        mock_page.url = "https://s.taobao.com/search?q=phone"
        mock_page.locator = MagicMock(return_value=MagicMock(
            count=AsyncMock(return_value=0),
            is_visible=AsyncMock(return_value=False),
        ))
        mock_page.evaluate = AsyncMock(return_value=None)

        result = await handle_captcha(mock_page)
        assert result is True

    async def test_returns_false_when_captcha_detected(self):
        with patch("services.captcha_solver.detect_captcha") as mock_detect, \
             patch("services.captcha_solver.dump_captcha_dom", new_callable=AsyncMock) as mock_dump:

            mock_detect.return_value = {"detected": True, "type": "slider", "details": {}}

            result = await handle_captcha(MagicMock())

        assert result is False
        mock_dump.assert_called_once()

    async def test_url_redirect_returns_false_immediately(self):
        with patch("services.captcha_solver.detect_captcha") as mock_detect, \
             patch("services.captcha_solver.dump_captcha_dom", new_callable=AsyncMock) as mock_dump:

            mock_detect.return_value = {
                "detected": True,
                "type": "url_redirect",
                "details": {"url": "https://login.taobao.com"},
            }

            result = await handle_captcha(MagicMock())

        assert result is False
        mock_dump.assert_called_once()


# ── _solve_locally ────────────────────────────────────────────────────────

class TestSolveLocally:
    async def test_returns_false_when_image_extraction_fails(self):
        with patch("services.captcha_solver.extract_slider_images") as mock_extract:
            mock_extract.return_value = None
            result = await _solve_locally(MagicMock())
        assert result is False

    async def test_returns_false_when_gap_detection_fails(self):
        from services.captcha_image_extractor import SliderImages

        buf = io.BytesIO()
        from PIL import Image
        Image.new("RGB", (300, 150)).save(buf, format="PNG")
        png = buf.getvalue()

        mock_images = SliderImages(
            background_bytes=png, piece_bytes=png,
            piece_initial_x=10.0, strategy="canvas",
        )

        with patch("services.captcha_solver.extract_slider_images") as mock_extract, \
             patch("services.captcha_solver.detect_gap_x") as mock_detect:
            mock_extract.return_value = mock_images
            mock_detect.return_value  = None
            result = await _solve_locally(MagicMock())

        assert result is False
