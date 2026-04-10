#!/usr/bin/env python3

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("\n❌  playwright not installed.")
    print("    Run:  pip install playwright && playwright install chromium\n")
    sys.exit(1)

# ── Cookie / URL maps (mirrors COOKIE_FILES and TEST_URLS in load-cookies.js) ─
COOKIE_FILES = {
    "taobao": "cookies.json",
    "tmall":  "cookies.json",        # same file — shared account
    "1688":   "cookies-1688.json",
}

TEST_URLS = {
    "taobao": "https://www.taobao.com",
    "tmall":  "https://www.tmall.com",
    "1688":   "https://www.1688.com",
}

# ── Resolve --platform arg ─────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Cookie load test — verifies saved session is still valid")
parser.add_argument("--platform", default="taobao",
                    choices=["taobao", "tmall", "1688"],
                    help="Platform whose cookies to test")
args = parser.parse_args()
platform    = args.platform.lower()
cookie_file = COOKIE_FILES.get(platform, "cookies.json")
test_url    = TEST_URLS.get(platform, "https://www.taobao.com")

UTILS_DIR = Path(__file__).parent


async def load_cookies():
    print(f"\n🍪 Cookie Loading Test — platform: {platform}")
    print("=" * 50)

    cookie_path = UTILS_DIR / cookie_file

    if not cookie_path.exists():
        print(f"\n❌ Error: {cookie_file} not found!")
        print(f"   Run: python utils/login_helper.py --platform {platform}\n")
        sys.exit(1)

    print(f"📂 Loading cookies from: {cookie_path}")
    cookies: list = json.loads(cookie_path.read_text())
    print(f"✓ Loaded {len(cookies)} cookies")

    # Check expiry (mirrors the expired-cookie check in load-cookies.js)
    now     = int(time.time())
    expired = [c for c in cookies if c.get("expires", -1) > 0 and c["expires"] < now]
    if expired:
        print(f"⚠️  {len(expired)} cookies have expired — consider re-logging in")

    print("\n🌐 Launching browser…")

    async with async_playwright() as pw:
        # headless=False so you can visually confirm login state
        browser = await pw.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        context = await browser.new_context(viewport={"width": 1200, "height": 900})
        page    = await context.new_page()

        # Apply cookies — mirrors page.setCookie(...cookies) in Puppeteer
        print(f"🍪 Setting {platform} cookies…")
        await context.add_cookies(cookies)
        print("✓ Cookies applied\n")

        print(f"🔍 Testing login state on {test_url}…")
        await page.goto(test_url, wait_until="networkidle")
        await page.wait_for_timeout(2000)

        # Mirror the page.evaluate() login-detection heuristic
        is_logged_in: bool = await page.evaluate("""() => {
            const loginLinks = document.querySelectorAll('a[href*="login"],a[href*="signin"]');
            const hasLoginButton = Array.from(loginLinks).some(link =>
                link.textContent.includes('登录') ||
                link.textContent.toLowerCase().includes('login') ||
                link.textContent.toLowerCase().includes('sign in')
            );
            return !hasLoginButton;
        }""")

        if is_logged_in:
            print("✅ Login state preserved! You are logged in.\n")
        else:
            print("⚠️  Login state not detected. Cookies may have expired.\n")
            print(f"   Re-login: python utils/login_helper.py --platform {platform}\n")

        print("Press Enter to close browser…")
        await asyncio.get_event_loop().run_in_executor(None, input)

        await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(load_cookies())
    except KeyboardInterrupt:
        print("\n\n⚠️  Cancelled by user.\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        sys.exit(1)