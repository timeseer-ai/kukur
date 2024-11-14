FROM alpine/curl:8.1.2

COPY data.ndjson /data.ndjson
COPY import-data.sh /import-data.sh

ENTRYPOINT ["sh", "/import-data.sh"]