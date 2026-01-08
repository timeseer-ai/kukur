FROM python:3.13 AS builder
ENV APP_ROOT=/usr/src/app
WORKDIR ${APP_ROOT}

RUN apt-get update \
 && apt-get --yes install \
    unixodbc-dev \
 && rm -rf /var/lib/apt/lists/*

RUN --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --all-extras --no-dev --no-install-project --no-editable

FROM python:3.13-slim
ENV APP_ROOT=/usr/src/app
WORKDIR ${APP_ROOT}

### Allow running as non-root
COPY docker-entrypoint.sh /entrypoint.sh
RUN chmod g=u /etc/passwd \
 && chgrp -R 0 ${APP_ROOT} \
 && chmod -R g=u ${APP_ROOT}
ENTRYPOINT ["/entrypoint.sh"]

RUN apt-get update \
 && apt-get --yes install \
    unixodbc tdsodbc \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder ${APP_ROOT}/.venv ${APP_ROOT}/.venv

COPY kukur ${APP_ROOT}/kukur

USER 1001
ENV PATH="${APP_ROOT}/.venv/bin:$PATH"

CMD ["python", "-m", "kukur.cli"]
