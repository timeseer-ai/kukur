#!/bin/bash

curl -X POST http://localhost:9200/_bulk --data-binary @data.ndjson -H 'Content-Type: application/json'
