FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --extra dapp-crawler

# Install Playwright Chromium browser
RUN uv run playwright install --with-deps chromium

COPY workers/ workers/
COPY services/ services/
COPY db/ db/
COPY utils/ utils/

CMD ["uv", "run", "--no-sync", "python", "-m", "workers.dapp_crawl_worker"]
