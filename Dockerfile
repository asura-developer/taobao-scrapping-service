FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PORT=3000

# System dependencies for Playwright/Chromium and OpenCV
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libasound2 libpango-1.0-0 libpangocairo-1.0-0 \
    libcairo2 libx11-6 libx11-xcb1 libxcb1 libxext6 libxrender1 \
    libxi6 libxtst6 fonts-liberation fonts-noto-color-emoji \
    libglib2.0-0 libsm6 \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt

# Install Playwright Chromium browser
RUN python -m playwright install chromium

# Copy application source
COPY . .

# Create writable app directories used at runtime
RUN mkdir -p /app/utils /app/data /ms-playwright

EXPOSE 3000

CMD ["sh", "-c", "python -m uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
