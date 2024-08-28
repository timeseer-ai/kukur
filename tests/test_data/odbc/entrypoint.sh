#!/bin/bash

(
    set -x
    for i in {1..50}; do
        /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -No -i setup.sql
        if [ $? -eq 0 ]; then
            break
        else
            sleep 1
        fi
    done
) &

exec "$@"
