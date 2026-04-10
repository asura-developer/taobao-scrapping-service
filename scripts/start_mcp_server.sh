#!/usr/bin/env bash
# Launch Scrapling MCP server for AI-assisted scraping
# Usage:
#   ./scripts/start_mcp_server.sh          # stdio mode (for Claude Code / Cursor)
#   ./scripts/start_mcp_server.sh http     # HTTP mode on port 8000

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Prefer project venv, fallback to system
if [ -f "$PROJECT_DIR/.venv/bin/scrapling" ]; then
    SCRAPLING="$PROJECT_DIR/.venv/bin/scrapling"
elif [ -f "$PROJECT_DIR/scrapling-env/bin/scrapling" ]; then
    SCRAPLING="$PROJECT_DIR/scrapling-env/bin/scrapling"
else
    SCRAPLING="$(which scrapling 2>/dev/null || true)"
fi

if [ -z "$SCRAPLING" ]; then
    echo "Error: scrapling CLI not found. Run: pip install 'scrapling[ai]'" >&2
    exit 1
fi

MODE="${1:-stdio}"

if [ "$MODE" = "http" ]; then
    echo "Starting Scrapling MCP server (HTTP mode on :8000)..."
    exec "$SCRAPLING" mcp --http --port 8000
else
    # stdio mode — used by Claude Code, Cursor, etc.
    exec "$SCRAPLING" mcp
fi
