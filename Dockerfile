
FROM python:3.11-slim
# Pinned to match the local uv version; bump deliberately, never use :latest
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app

# Dependencies first so this layer is cached when only code changes
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY app/ ./
CMD ["/app/.venv/bin/python", "-u", "main.py"]
