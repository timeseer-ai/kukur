FROM python:3.8 as builder
WORKDIR /usr/src/app

RUN apt-get update \
 && apt-get --yes install \
    unixodbc-dev \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --prefix=/install --no-warn-script-location -r requirements.txt

FROM python:3.8-slim
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
    unixodbc \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

COPY kukur ${APP_ROOT}/kukur

USER 1001

CMD ["python", "-m", "kukur.cli"]
