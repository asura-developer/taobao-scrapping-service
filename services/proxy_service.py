"""
Proxy rotation service.

Manages a pool of proxies and rotates them across scraping requests.

Supports two modes:
  1. **Static pool** — Multiple proxy IPs that we rotate ourselves.
     Load from PROXY_LIST env, PROXY_FILE env, or proxies.txt.
     Format: protocol://user:pass@host:port  or  host:port (defaults to http)

  2. **Rotating gateway** — A single endpoint where the *provider* rotates IPs.
     Set PROXY_GATEWAY env var (e.g. http://user:pass@gate.smartproxy.com:7777).
     Optional: PROXY_GATEWAY_STICKY=true to use sticky sessions per-job.

Automatically marks static proxies as failed after consecutive errors and
rotates to healthy ones. Failed proxies are retried after a cooldown.
"""

import asyncio
import os
import random
import re
import time
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Cooldown before retrying a failed proxy (seconds)
_FAIL_COOLDOWN = 300  # 5 minutes
# Max consecutive failures before marking proxy as dead
_MAX_CONSECUTIVE_FAILS = 3
_PROVIDER_REFRESH_INTERVAL = 300  # 5 minutes
_PROVIDER_MIN_AVAILABLE = 2
_DEFAULT_STICKY_TEMPLATE = "{username}-session-{session}"
_DEFAULT_STICKY_MINUTES = 15


def _normalize_sticky_session(session_id: str) -> str:
    """Keep sticky session IDs short and proxy-safe for username injection."""
    if not session_id:
        return ""
    normalized = re.sub(r"[^a-zA-Z0-9]", "", session_id)
    return normalized[:24] or "session1"


def _build_sticky_username(
    username: Optional[str],
    sticky_session: Optional[str],
    sticky_template: Optional[str] = None,
    sticky_minutes: int = _DEFAULT_STICKY_MINUTES,
) -> Optional[str]:
    if not username or not sticky_session:
        return username

    template = (sticky_template or _DEFAULT_STICKY_TEMPLATE).strip() or _DEFAULT_STICKY_TEMPLATE
    session = _normalize_sticky_session(sticky_session)

    try:
        return template.format(
            username=username,
            session=session,
            minutes=max(1, int(sticky_minutes or _DEFAULT_STICKY_MINUTES)),
        )
    except Exception:
        logger.warning("Invalid proxy sticky template '%s'; falling back to default", template)
        return _DEFAULT_STICKY_TEMPLATE.format(username=username, session=session)


@dataclass
class ProxyEntry:
    url: str                          # full URL: protocol://user:pass@host:port
    host: str = ""
    port: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = "http"
    is_gateway: bool = False          # rotating gateway — skip health-based rotation
    source: str = "static"            # static | provider | gateway

    # Health tracking
    total_requests: int = 0
    total_failures: int = 0
    consecutive_failures: int = 0
    last_failure_at: float = 0.0
    is_dead: bool = False

    def mark_success(self):
        self.total_requests += 1
        self.consecutive_failures = 0

    def mark_failure(self):
        self.total_requests += 1
        self.total_failures += 1
        self.consecutive_failures += 1
        self.last_failure_at = time.time()
        # Gateway proxies never go "dead" — the provider handles rotation
        if not self.is_gateway and self.consecutive_failures >= _MAX_CONSECUTIVE_FAILS:
            self.is_dead = True

    def is_available(self) -> bool:
        if self.is_gateway:
            return True
        if not self.is_dead:
            return True
        # Retry dead proxies after cooldown
        if time.time() - self.last_failure_at > _FAIL_COOLDOWN:
            self.is_dead = False
            self.consecutive_failures = 0
            return True
        return False

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 1.0
        return (self.total_requests - self.total_failures) / self.total_requests

    def to_playwright_proxy(
        self,
        sticky_session: str = None,
        sticky_template: str = None,
        sticky_minutes: int = _DEFAULT_STICKY_MINUTES,
    ) -> dict:
        """Convert to Playwright proxy format.

        For gateway proxies with sticky sessions, the username can be rebuilt
        using a provider-specific template.
        """
        proxy = {"server": f"{self.protocol}://{self.host}:{self.port}"}
        username = self.username
        if sticky_session and username and self.is_gateway:
            username = _build_sticky_username(
                username,
                sticky_session,
                sticky_template=sticky_template,
                sticky_minutes=sticky_minutes,
            )
        if username:
            proxy["username"] = username
        if self.password:
            proxy["password"] = self.password
        return proxy

    def to_httpx_url(
        self,
        sticky_session: str = None,
        sticky_template: str = None,
        sticky_minutes: int = _DEFAULT_STICKY_MINUTES,
    ) -> str:
        """Full proxy URL for httpx/requests.

        For sticky sessions, injects session ID into the username.
        """
        if sticky_session and self.username and self.is_gateway:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(self.url)
            new_user = _build_sticky_username(
                parsed.username,
                sticky_session,
                sticky_template=sticky_template,
                sticky_minutes=sticky_minutes,
            )
            netloc = f"{new_user}:{parsed.password}@{parsed.hostname}:{parsed.port}" if parsed.password else f"{new_user}@{parsed.hostname}:{parsed.port}"
            return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
        return self.url

    def to_url(self) -> str:
        """Full proxy URL for requests/httpx."""
        return self.url


def _parse_proxy(raw: str, is_gateway: bool = False) -> Optional[ProxyEntry]:
    """Parse a proxy string into a ProxyEntry."""
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None

    # Add protocol if missing
    if "://" not in raw:
        raw = f"http://{raw}"

    try:
        from urllib.parse import urlparse
        parsed = urlparse(raw)
        protocol = parsed.scheme or "http"
        host = parsed.hostname
        port = parsed.port
        username = parsed.username
        password = parsed.password

        if not host or not port:
            logger.warning(f"Invalid proxy (missing host/port): {raw}")
            return None

        return ProxyEntry(
            url=raw,
            host=host,
            port=port,
            username=username,
            password=password,
            protocol=protocol,
            is_gateway=is_gateway,
        )
    except Exception as e:
        logger.warning(f"Failed to parse proxy '{raw}': {e}")
        return None


class ProxyService:
    def __init__(self):
        self._proxies: list[ProxyEntry] = []
        self._gateway: Optional[ProxyEntry] = None
        self._provider_config: Optional[dict] = None
        self._sticky_sessions: bool = False
        self._sticky_template: str = _DEFAULT_STICKY_TEMPLATE
        self._sticky_minutes: int = _DEFAULT_STICKY_MINUTES
        self._current_index: int = 0
        self._enabled: bool = False
        self._provider_task: Optional[asyncio.Task] = None
        self._provider_lock = asyncio.Lock()
        self._last_provider_refresh_at: float = 0.0
        self._load_proxies()

    def _load_proxies(self):
        """Load proxies from env vars or proxies.txt file."""

        # ── JSON provider API (dynamic pool fetched from provider URL) ──
        provider_url = os.getenv("PROXY_PROVIDER_URL", "").strip()
        if provider_url:
            provider_protocol = os.getenv("PROXY_PROVIDER_PROTOCOL", "http").strip().lower() or "http"
            provider_username = os.getenv("PROXY_PROVIDER_USERNAME", "").strip()
            provider_password = os.getenv("PROXY_PROVIDER_PASSWORD", "").strip()
            provider_refresh_interval = os.getenv("PROXY_PROVIDER_REFRESH_INTERVAL_SEC", str(_PROVIDER_REFRESH_INTERVAL)).strip()
            provider_min_available = os.getenv("PROXY_PROVIDER_MIN_AVAILABLE", str(_PROVIDER_MIN_AVAILABLE)).strip()

            ok = self.set_provider(
                provider_url,
                protocol=provider_protocol,
                username=provider_username,
                password=provider_password,
                refresh_interval_sec=int(provider_refresh_interval or _PROVIDER_REFRESH_INTERVAL),
                min_available=int(provider_min_available or _PROVIDER_MIN_AVAILABLE),
            )
            if ok:
                logger.info(
                    "Proxy provider configured from env: %s (protocol=%s)",
                    provider_url,
                    provider_protocol,
                )

        # ── Rotating gateway (single endpoint, provider rotates IPs) ──
        gateway_url = os.getenv("PROXY_GATEWAY", "").strip()
        if gateway_url:
            entry = _parse_proxy(gateway_url, is_gateway=True)
            if entry:
                self._gateway = entry
                self._enabled = True
                self._sticky_sessions = os.getenv("PROXY_GATEWAY_STICKY", "").lower() in ("true", "1", "yes")
                self._sticky_template = (
                    os.getenv("PROXY_GATEWAY_STICKY_TEMPLATE", _DEFAULT_STICKY_TEMPLATE).strip()
                    or _DEFAULT_STICKY_TEMPLATE
                )
                self._sticky_minutes = max(
                    1,
                    int(os.getenv("PROXY_GATEWAY_STICKY_MINUTES", str(_DEFAULT_STICKY_MINUTES)).strip() or _DEFAULT_STICKY_MINUTES),
                )
                logger.info(
                    f"Proxy gateway configured: {entry.host}:{entry.port} "
                    f"(sticky={'yes' if self._sticky_sessions else 'no'})"
                )

        # ── Static proxy pool ─────────────────────────────────────────
        proxies_raw: list[str] = []

        # 1. From PROXY_LIST env var (comma-separated)
        proxy_list = os.getenv("PROXY_LIST", "")
        if proxy_list:
            proxies_raw.extend(proxy_list.split(","))

        # 2. From PROXY_FILE env var (path to file)
        proxy_file = os.getenv("PROXY_FILE", "")
        if proxy_file and Path(proxy_file).exists():
            proxies_raw.extend(Path(proxy_file).read_text().splitlines())

        # 3. From proxies.txt in project root
        default_file = Path(__file__).parent.parent / "proxies.txt"
        if default_file.exists() and not proxy_file:
            proxies_raw.extend(default_file.read_text().splitlines())

        # Parse all
        for raw in proxies_raw:
            entry = _parse_proxy(raw)
            if entry:
                self._proxies.append(entry)

        if self._proxies:
            self._enabled = True
            logger.info(f"Static proxy pool: {len(self._proxies)} proxies loaded")

        if not self._enabled:
            logger.info("Proxy rotation disabled: no proxies configured")

    async def load_provider_from_db(self, db) -> bool:
        if self._provider_config:
            logger.info("Skipping DB proxy provider load because env config is already set")
            return True
        try:
            doc = await db.app_config.find_one({"key": "proxy_provider"}, {"_id": 0, "value": 1})
        except Exception as e:
            logger.warning(f"Failed to load proxy provider config from DB: {e}")
            return False

        value = (doc or {}).get("value")
        if not isinstance(value, dict) or not value.get("url"):
            return False

        ok = self.set_provider(
            value["url"],
            protocol=value.get("protocol", "http"),
            username=value.get("username") or "",
            password=value.get("password") or "",
            refresh_interval_sec=value.get("refreshIntervalSec", _PROVIDER_REFRESH_INTERVAL),
            min_available=value.get("minAvailable", _PROVIDER_MIN_AVAILABLE),
        )
        if ok:
            logger.info("Loaded proxy provider config from DB")
        return ok

    async def save_provider_to_db(self, db) -> bool:
        if not self._provider_config:
            return False
        try:
            await db.app_config.update_one(
                {"key": "proxy_provider"},
                {"$set": {"value": self._provider_config, "updatedAt": time.time()}},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to save proxy provider config to DB: {e}")
            return False

    async def clear_provider_from_db(self, db) -> bool:
        try:
            await db.app_config.delete_one({"key": "proxy_provider"})
            return True
        except Exception as e:
            logger.warning(f"Failed to clear proxy provider config from DB: {e}")
            return False

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def has_gateway(self) -> bool:
        return self._gateway is not None

    @property
    def proxy_count(self) -> int:
        count = len(self._proxies)
        if self._gateway:
            count += 1
        return count

    @property
    def available_count(self) -> int:
        count = sum(1 for p in self._proxies if p.is_available())
        if self._gateway:
            count += 1
        return count

    @property
    def provider_available_count(self) -> int:
        return sum(1 for p in self._proxies if p.source == "provider" and p.is_available())

    def get_next(self, session_id: str = None) -> Optional[ProxyEntry]:
        """Get next available proxy.

        If a gateway is configured, it always returns the gateway.
        Otherwise, uses round-robin with health checks on the static pool.

        Args:
            session_id: Optional job/session ID for sticky sessions.
                        Only used with gateway proxies when PROXY_GATEWAY_STICKY=true.
        """
        if not self._enabled:
            return None

        # Gateway takes priority — the provider handles rotation
        if self._gateway:
            return self._gateway

        # Static pool — weighted random by success rate
        available = [p for p in self._proxies if p.is_available()]
        if not available:
            # All proxies dead — reset and try again
            logger.warning("All proxies exhausted — resetting health status")
            for p in self._proxies:
                p.is_dead = False
                p.consecutive_failures = 0
            available = self._proxies

        weights = [max(p.success_rate, 0.1) for p in available]
        return random.choices(available, weights=weights, k=1)[0]

    def get_random(self) -> Optional[ProxyEntry]:
        """Get a random available proxy."""
        if not self._enabled:
            return None
        if self._gateway:
            return self._gateway
        available = [p for p in self._proxies if p.is_available()]
        if not available:
            return self.get_next()  # triggers reset
        return random.choice(available)

    def get_playwright_proxy(self, session_id: str = None) -> Optional[dict]:
        """Get a proxy formatted for Playwright's launch(proxy=...).

        Convenience method that handles sticky sessions automatically.
        """
        entry = self.get_next(session_id)
        if not entry:
            return None
        sticky = session_id if (self._sticky_sessions and entry.is_gateway) else None
        return entry.to_playwright_proxy(
            sticky_session=sticky,
            sticky_template=self._sticky_template,
            sticky_minutes=self._sticky_minutes,
        )

    def get_httpx_url(self, session_id: str = None) -> Optional[str]:
        """Get a proxy URL for httpx/requests.

        Convenience method that handles sticky sessions automatically.
        """
        entry = self.get_next(session_id)
        if not entry:
            return None
        sticky = session_id if (self._sticky_sessions and entry.is_gateway) else None
        return entry.to_httpx_url(
            sticky_session=sticky,
            sticky_template=self._sticky_template,
            sticky_minutes=self._sticky_minutes,
        )

    def mark_success(self, proxy: ProxyEntry):
        """Mark a proxy request as successful."""
        proxy.mark_success()

    def mark_failure(self, proxy: ProxyEntry):
        """Mark a proxy request as failed."""
        proxy.mark_failure()
        if proxy.is_dead:
            logger.warning(
                f"Proxy {proxy.host}:{proxy.port} marked dead "
                f"({proxy.consecutive_failures} consecutive failures)"
            )

    def get_stats(self) -> dict:
        """Get proxy pool statistics."""
        all_proxies = list(self._proxies)
        if self._gateway:
            all_proxies = [self._gateway] + all_proxies

        return {
            "enabled": self._enabled,
            "mode": "gateway" if self._gateway else ("pool" if self._proxies else "none"),
            "stickySessions": self._sticky_sessions,
            "stickyTemplate": self._sticky_template if self._sticky_sessions else None,
            "stickyMinutes": self._sticky_minutes if self._sticky_sessions else None,
            "provider": self._provider_config,
            "total": len(all_proxies),
            "available": self.available_count,
            "dead": sum(1 for p in all_proxies if p.is_dead),
            "proxies": [
                {
                    "host": f"{p.host}:{p.port}",
                    "protocol": p.protocol,
                    "type": "gateway" if p.is_gateway else p.source,
                    "requests": p.total_requests,
                    "failures": p.total_failures,
                    "successRate": round(p.success_rate * 100, 1),
                    "dead": p.is_dead,
                }
                for p in all_proxies
            ],
        }

    def add_proxy(self, raw: str, is_gateway: bool = False) -> bool:
        """Add a proxy at runtime."""
        entry = _parse_proxy(raw, is_gateway=is_gateway)
        if not entry:
            return False
        if is_gateway:
            entry.source = "gateway"
            self._gateway = entry
        else:
            entry.source = "static"
            self._proxies.append(entry)
        self._enabled = True
        return True

    def set_gateway(self, raw: str, sticky: bool = False) -> bool:
        """Set or replace the rotating gateway proxy."""
        entry = _parse_proxy(raw, is_gateway=True)
        if not entry:
            return False
        entry.source = "gateway"
        self._gateway = entry
        self._sticky_sessions = sticky
        self._sticky_template = (
            os.getenv("PROXY_GATEWAY_STICKY_TEMPLATE", _DEFAULT_STICKY_TEMPLATE).strip()
            or _DEFAULT_STICKY_TEMPLATE
        )
        self._sticky_minutes = max(
            1,
            int(os.getenv("PROXY_GATEWAY_STICKY_MINUTES", str(_DEFAULT_STICKY_MINUTES)).strip() or _DEFAULT_STICKY_MINUTES),
        )
        self._enabled = True
        logger.info(f"Gateway proxy set: {entry.host}:{entry.port} (sticky={'yes' if sticky else 'no'})")
        return True

    def remove_gateway(self):
        """Remove the gateway proxy."""
        self._gateway = None
        self._sticky_sessions = False
        self._sticky_template = _DEFAULT_STICKY_TEMPLATE
        self._sticky_minutes = _DEFAULT_STICKY_MINUTES
        self._enabled = len(self._proxies) > 0

    def remove_proxy(self, host_port: str) -> bool:
        """Remove a proxy by host:port."""
        # Check if it's the gateway
        if self._gateway and f"{self._gateway.host}:{self._gateway.port}" == host_port:
            self.remove_gateway()
            return True
        before = len(self._proxies)
        self._proxies = [p for p in self._proxies if f"{p.host}:{p.port}" != host_port]
        self._enabled = len(self._proxies) > 0 or self._gateway is not None
        return len(self._proxies) < before

    def set_provider(
        self,
        url: str,
        protocol: str = "http",
        username: str = "",
        password: str = "",
        refresh_interval_sec: int = _PROVIDER_REFRESH_INTERVAL,
        min_available: int = _PROVIDER_MIN_AVAILABLE,
    ) -> bool:
        if not url or not url.strip():
            return False
        protocol = (protocol or "http").strip().lower()
        if protocol not in ("http", "https", "socks5"):
            return False
        self._provider_config = {
            "url": url.strip(),
            "protocol": protocol,
            "username": username.strip() or None,
            "password": password.strip() or None,
            "refreshIntervalSec": max(30, int(refresh_interval_sec or _PROVIDER_REFRESH_INTERVAL)),
            "minAvailable": max(1, int(min_available or _PROVIDER_MIN_AVAILABLE)),
        }
        return True

    def clear_provider(self):
        self._provider_config = None
        self._proxies = [p for p in self._proxies if p.source != "provider"]
        self._last_provider_refresh_at = 0.0
        self._enabled = len(self._proxies) > 0 or self._gateway is not None

    async def refresh_provider_proxies(self, replace_existing: bool = True) -> dict:
        if not self._provider_config:
            return {"success": False, "message": "No proxy provider configured"}
        async with self._provider_lock:
            import httpx

            try:
                async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                    resp = await client.get(self._provider_config["url"])
                    resp.raise_for_status()
                    data = resp.json()
            except Exception as e:
                logger.warning(f"Provider proxy refresh failed: {e}")
                return {"success": False, "message": f"Provider fetch failed: {e}"}

            if isinstance(data, dict):
                for key in ("data", "result", "proxies", "list"):
                    if isinstance(data.get(key), list):
                        data = data[key]
                        break

            if not isinstance(data, list):
                return {"success": False, "message": "Provider response must be a JSON array or object containing a proxy list"}

            entries: list[ProxyEntry] = []
            protocol = self._provider_config["protocol"]
            username = self._provider_config.get("username")
            password = self._provider_config.get("password")

            for item in data:
                if not isinstance(item, dict):
                    continue
                host = item.get("host") or item.get("ip") or item.get("server")
                port = item.get("port")
                if not host or not port:
                    continue

                auth = ""
                if username:
                    auth = username
                    if password:
                        auth += f":{password}"
                    auth += "@"
                raw = f"{protocol}://{auth}{host}:{port}"
                entry = _parse_proxy(raw)
                if not entry:
                    continue
                entry.source = "provider"
                entries.append(entry)

            if not entries:
                return {"success": False, "message": "Provider returned no usable proxies"}

            if replace_existing:
                self._proxies = [p for p in self._proxies if p.source != "provider"]

            existing = {f"{p.protocol}://{p.host}:{p.port}" for p in self._proxies}
            added = 0
            for entry in entries:
                key = f"{entry.protocol}://{entry.host}:{entry.port}"
                if key in existing:
                    continue
                self._proxies.append(entry)
                existing.add(key)
                added += 1

            self._enabled = len(self._proxies) > 0 or self._gateway is not None
            self._last_provider_refresh_at = time.time()
            logger.info(f"Provider proxy refresh complete: {added} added, {len(entries)} fetched")
            return {
                "success": True,
                "message": f"Loaded {added} provider proxies",
                "fetched": len(entries),
                "added": added,
            }

    async def ensure_provider_proxies(self, force: bool = False, reason: str = "") -> dict:
        if not self._provider_config:
            return {"success": False, "message": "No proxy provider configured"}

        refresh_interval = int(self._provider_config.get("refreshIntervalSec", _PROVIDER_REFRESH_INTERVAL))
        min_available = int(self._provider_config.get("minAvailable", _PROVIDER_MIN_AVAILABLE))
        now = time.time()
        stale = (now - self._last_provider_refresh_at) >= refresh_interval if self._last_provider_refresh_at else True
        low = self.provider_available_count < min_available

        if force or stale or low:
            logger.info(
                "Refreshing provider proxies%s%s%s",
                f" [{reason}]" if reason else "",
                " because pool is low" if low else "",
                " because cache is stale" if stale else "",
            )
            return await self.refresh_provider_proxies(replace_existing=True)

        return {
            "success": True,
            "message": "Provider proxy pool is healthy",
            "fetched": 0,
            "added": 0,
        }

    async def start(self):
        if not self._provider_config or self._provider_task:
            return
        await self.ensure_provider_proxies(force=True, reason="startup")
        self._provider_task = asyncio.create_task(self._provider_refresh_loop())

    async def stop(self):
        if not self._provider_task:
            return
        self._provider_task.cancel()
        try:
            await self._provider_task
        except asyncio.CancelledError:
            pass
        self._provider_task = None

    async def _provider_refresh_loop(self):
        try:
            while True:
                refresh_interval = int((self._provider_config or {}).get("refreshIntervalSec", _PROVIDER_REFRESH_INTERVAL))
                await asyncio.sleep(max(30, refresh_interval))
                if not self._provider_config:
                    continue
                result = await self.ensure_provider_proxies(reason="background")
                if not result.get("success"):
                    logger.warning("Background provider refresh failed: %s", result.get("message"))
        except asyncio.CancelledError:
            return


# Singleton
proxy_service = ProxyService()
