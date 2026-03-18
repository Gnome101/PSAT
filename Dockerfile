FROM python:3.10-slim

# Install system deps needed by solc/slither
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cache layer)
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY main.py ./
COPY services/ services/
COPY utils/ utils/

ENTRYPOINT ["uv", "run", "python", "main.py"]
