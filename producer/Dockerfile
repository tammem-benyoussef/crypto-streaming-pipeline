FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY producer ./producer

ENV PATH="/app/.venv/bin:$PATH"
RUN useradd --create-home appuser
USER appuser

CMD ["python", "-m", "producer.main"]