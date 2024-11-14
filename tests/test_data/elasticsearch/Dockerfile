FROM alpine/curl:8.1.2

COPY data.ndjson /data.ndjson

# RUN curl -X POST http://localhost:9200/_bulk --data-binary @data.ndjson -H 'Content-Type: application/json'
COPY import-data.sh /import-data.sh

ENTRYPOINT ["sh", "/import-data.sh"]