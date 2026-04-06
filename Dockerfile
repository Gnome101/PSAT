FROM node:22-alpine AS site-builder

WORKDIR /site

COPY site/package.json site/package-lock.json ./
RUN npm ci

COPY site/ ./
RUN npm run build


FROM python:3.10-slim AS backend

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/root/.foundry/bin:/app/.venv/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://foundry.paradigm.xyz | bash \
    && /root/.foundry/bin/foundryup

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

COPY main.py api.py alembic.ini ./
COPY db/ db/
COPY workers/ workers/
COPY start_workers.sh ./
RUN chmod +x start_workers.sh
COPY services/ services/
COPY schemas/ schemas/
COPY utils/ utils/
COPY site/ site/
COPY --from=site-builder /site/dist /app/site/dist

EXPOSE 8000

CMD ["uv", "run", "--no-sync", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
