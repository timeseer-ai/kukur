#!/bin/sh
code=$(curl --head --silent --output /dev/null -w %{http_code} 'elasticsearch:9200/tsai_data')
if [ "$code" = 404 ]; then
    curl -X POST elasticsearch:9200/_bulk --data-binary @data.ndjson -H 'Content-Type: application/json'
fi
