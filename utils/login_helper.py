#!/usr/bin/env python3
"""
Login helper — saves browser cookies for scraping.

Supports two modes:
  1. Interactive (local dev):  Opens a visible browser for manual login.
     python utils/login_helper.py --platform taobao

  2. Headless (deployed server):  No display needed.
     - Import cookies via API:  POST /api/scraper/import-cookies
     - QR code login via API:   POST /api/scraper/qr-login/{platform}
     - Or run this script with --headless to get a QR code in the terminal.
     python utils/login_helper.py --platform taobao --headless
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("\n❌  playwright not installed.")
    print("    Run:  pip install playwright && playwright install chromium\n")
    sys.exit(1)

# ── Platform config ──────────────────────────────────────────────────────
PLATFORM_CONFIG = {
    "taobao": {
        "label":      "Taobao",
        "loginUrl":   "https://login.taobao.com/member/login.jhtml",
        "verifyUrl":  "https://www.taobao.com",
        "cookieFile": "cookies.json",
        "note":       "Taobao & Tmall share the same Alibaba account — one login covers both.",
    },
    "tmall": {
        "label":      "Tmall",
        "loginUrl":   "https://login.tmall.com",
        "verifyUrl":  "https://www.tmall.com",
        "cookieFile": "cookies.json",
        "note":       "Tmall & Taobao share the same Alibaba account. Cookies saved to cookies.json.",
    },
    "1688": {
        "label":      "1688",
        "loginUrl":   "https://passport.1688.com/member/signin.htm",
        "verifyUrl":  "https://www.1688.com",
        "cookieFile": "cookies-1688.json",
        "note":       "1688 uses a separate B2B account. Cookies saved to cookies-1688.json.",
    },
}

# ── Args ─────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Login helper — saves browser cookies for scraping")
parser.add_argument("--platform", default="taobao",
                    choices=["taobao", "tmall", "1688"],
                    help="Platform to log in to")
parser.add_argument("--headless", action="store_true",
                    help="Headless mode: capture QR code for terminal-based login (no display needed)")
parser.add_argument("--import-file", type=str, default=None,
                    help="Import cookies from a JSON file (e.g. exported by a browser extension)")
args = parser.parse_args()
platform = args.platform.lower()
config   = PLATFORM_CONFIG[platform]

UTILS_DIR = Path(__file__).parent


async def import_cookies_from_file(filepath: str):
    """Import cookies from a JSON file exported by a browser extension."""
    print(f"\n📥 Importing cookies for {config['label']} from {filepath}")
    print("=" * 50)

    path = Path(filepath)
    if not path.exists():
        print(f"❌ File not found: {filepath}")
        sys.exit(1)

    try:
        cookies = json.loads(path.read_text())
        if not isinstance(cookies, list):
            print("❌ Cookie file must contain a JSON array of cookie objects")
            sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        sys.exit(1)

    valid = [c for c in cookies if isinstance(c, dict) and "name" in c and "value" in c]
    if not valid:
        print("❌ No valid cookies found (each must have 'name' and 'value')")
        sys.exit(1)

    cookie_path = UTILS_DIR / config["cookieFile"]
    cookie_path.write_text(json.dumps(valid, indent=2))
    print(f"✅ Imported {len(valid)} cookies → {cookie_path}")
    print(f"\n📝 Start the server with: python main.py")
    print("✓ Done!\n")


async def headless_qr_login():
    """Headless QR code login — captures QR and saves to file for scanning."""
    print(f"\n🔐 {config['label']} Headless QR Login")
    print("=" * 50)
    print(f"\n📝 {config['note']}\n")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        context = await browser.new_context(viewport={"width": 1200, "height": 900})
        page = await context.new_page()

        print(f"🌐 Navigating to {config['label']} login page…")
        await page.goto(config["loginUrl"], wait_until="networkidle", timeout=30000)

        # Try to switch to QR code tab
        for selector in [
            'text="扫码登录"', 'text="二维码登录"',
            '.qrcode-login', '.login-switch',
            'a:has-text("扫码")',
        ]:
            try:
                el = page.locator(selector).first
                if await el.is_visible(timeout=2000):
                    await el.click()
                    await page.wait_for_timeout(1500)
                    break
            except Exception:
                continue

        await page.wait_for_timeout(2000)

        # Save QR screenshot
        qr_path = UTILS_DIR / f"qr-{platform}.png"
        await page.screenshot(path=str(qr_path))
        print(f"\n📸 QR code screenshot saved to: {qr_path}")
        print(f"   Open this image and scan with your {config['label']} mobile app.\n")

        # Also try to find and save just the QR element
        for qr_sel in [
            '#J_QRCodeImg img', '.qrcode-img img', 'img[src*="qrcode"]',
            '.login-qrcode img', '#qrcode img', '.qr-img',
        ]:
            try:
                el = page.locator(qr_sel).first
                if await el.is_visible(timeout=1000):
                    qr_only_path = UTILS_DIR / f"qr-{platform}-code.png"
                    await el.screenshot(path=str(qr_only_path))
                    print(f"   QR code only: {qr_only_path}")
                    break
            except Exception:
                continue

        print("\n⏳ Waiting for you to scan the QR code…")
        print("   (Checking every 3 seconds for login success)\n")

        # Poll for login completion
        max_wait = 180  # 3 minutes
        elapsed = 0
        while elapsed < max_wait:
            await page.wait_for_timeout(3000)
            elapsed += 3
            current_url = page.url
            if not any(kw in current_url for kw in ("login", "signin", "passport")):
                print("✅ Login detected!")
                break

            # Check for QR expiry
            for sel in ['.qrcode-expired', 'text="二维码已过期"', 'text="已过期"']:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=300):
                        print("⚠️  QR code expired. Refreshing page…")
                        await page.reload(wait_until="networkidle")
                        await page.wait_for_timeout(2000)
                        await page.screenshot(path=str(qr_path))
                        print(f"   New QR screenshot saved to: {qr_path}")
                        break
                except Exception:
                    continue

            dots = "." * ((elapsed // 3) % 4)
            print(f"   Waiting{dots} ({elapsed}s / {max_wait}s)", end="\r")
        else:
            print(f"\n⚠️  Timed out after {max_wait}s. Try again or use --import-file instead.")
            await browser.close()
            return

        # Save cookies
        cookies = await context.cookies()
        cookie_path = UTILS_DIR / config["cookieFile"]
        cookie_path.write_text(json.dumps(cookies, indent=2))
        print(f"\n💾 Saved {len(cookies)} cookies → {cookie_path}")

        await browser.close()

    print(f"\n📝 Next steps:")
    print(f"   Start the server: python main.py\n")
    print("✓ Done!\n")


async def interactive_login():
    """Original interactive login — opens a visible browser window."""
    print(f"\n🔐 {config['label']} Login Helper")
    print("=" * 50)
    print(f"\n📝 {config['note']}\n")

    user_data_dir = UTILS_DIR / "browser-data"
    user_data_dir.mkdir(parents=True, exist_ok=True)
    print(f"✓ Browser session dir: {user_data_dir}")
    print("🌐 Opening browser…\n")

    async with async_playwright() as pw:
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            viewport={"width": 1200, "height": 900},
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )

        page = context.pages[0] if context.pages else await context.new_page()

        print("📋 Instructions:")
        print(f"   1. The browser has navigated to the {config['label']} login page")
        print("   2. Complete the login process in the browser window")
        print("   3. Once logged in, browse around to confirm your session")
        print("   4. Come back here and press Enter when done\n")

        await page.goto(config["loginUrl"], wait_until="networkidle")
        print(f"✓ Opened {config['label']} login page")
        print("\n⏳ Waiting for you to complete login…")
        print("   (Press Enter when done, or Ctrl+C to cancel)\n")

        await asyncio.get_event_loop().run_in_executor(None, input)

        print("\n💾 Saving session…")

        current_url = page.url
        cookies     = await context.cookies()

        print("\n📊 Session Info:")
        print(f"   URL:            {current_url}")
        print(f"   Cookies count:  {len(cookies)}")

        cookie_path = UTILS_DIR / config["cookieFile"]
        try:
            cookie_path.write_text(json.dumps(cookies, indent=2))
            print(f"   Cookies file:   {cookie_path}")
            print(f"   ✓ Cookies saved to {config['cookieFile']}")
        except Exception as e:
            print(f"   ⚠️  Failed to save {config['cookieFile']}: {e}")

        if "login" in current_url or "signin" in current_url:
            print("\n⚠️  Warning: Still on login page. Login may not be complete.")
        else:
            print("\n✅ Login appears successful!")

        await context.close()

    print("\n📝 Next steps:")
    if platform == "1688":
        print("   1688 cookies are saved to cookies-1688.json")
        print("   The scraper will use this file exclusively for 1688 jobs.")
    else:
        print("   Taobao & Tmall cookies are saved to cookies.json")
        print("   Both taobao and tmall scraping jobs will use this file.")
        print("   For 1688, run: python utils/login_helper.py --platform 1688")
    print("\n   Start your server with:")
    print("      python main.py")
    print("\n✓ Done!\n")


if __name__ == "__main__":
    try:
        if args.import_file:
            asyncio.run(import_cookies_from_file(args.import_file))
        elif args.headless:
            asyncio.run(headless_qr_login())
        else:
            asyncio.run(interactive_login())
    except KeyboardInterrupt:
        print("\n\n⚠️  Cancelled by user.\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        sys.exit(1)
