"""
API Key authentication middleware.

Protects all /api/* endpoints with a bearer token or X-API-Key header.
The API key is set via the API_KEY environment variable.
If API_KEY is not set, authentication is disabled (open access).

Usage:
  - Header: Authorization: Bearer <key>
  - Header: X-API-Key: <key>
  - Query param: ?api_key=<key>
"""

import os
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

API_KEY = os.getenv("API_KEY", "")

# Paths that don't require authentication
PUBLIC_PATHS = {
    "/",
    "/health",
    "/docs",
    "/openapi.json",
    "/redoc",
}


def is_auth_enabled() -> bool:
    return bool(API_KEY)


def _extract_key(request: Request) -> str:
    """Extract API key from request headers or query params."""
    # 1. Authorization: Bearer <key>
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:].strip()

    # 2. X-API-Key header
    api_key_header = request.headers.get("x-api-key", "")
    if api_key_header:
        return api_key_header.strip()

    # 3. Query parameter
    return request.query_params.get("api_key", "").strip()


class APIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip auth if not enabled
        if not is_auth_enabled():
            return await call_next(request)

        # Skip public paths
        path = request.url.path
        if path in PUBLIC_PATHS:
            return await call_next(request)

        # Skip non-API paths (static files)
        if not path.startswith("/api/"):
            return await call_next(request)

        # Validate key
        key = _extract_key(request)
        if key != API_KEY:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=401,
                content={"success": False, "error": "Invalid or missing API key"},
            )

        return await call_next(request)
