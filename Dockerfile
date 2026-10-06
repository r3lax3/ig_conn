# syntax=docker/dockerfile:1

# Bookworm: Playwright 1.63 installs Chromium's system libraries for it
ARG PYTHON_IMAGE=python:3.12-slim-bookworm

FROM ghcr.io/astral-sh/uv:0.12.22 AS uv

FROM ${PYTHON_IMAGE} AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /src
# dependencies first: this layer is rebuilt only when the lock changes
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM ${PYTHON_IMAGE}
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
COPY --from=build /app/.venv /app/.venv
# headless shell only (the login runs headless); --with-deps pulls its libraries from apt
RUN playwright install --with-deps --only-shell chromium \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --user-group connector
USER connector
WORKDIR /home/connector
CMD ["ig-connector"]
