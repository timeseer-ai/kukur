FROM mcr.microsoft.com/mssql/server:2019-latest

ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=Timeseer!AI

COPY entrypoint.sh /entrypoint.sh
COPY setup.sql /setup.sql

ENTRYPOINT ["/entrypoint.sh"]

CMD ["/opt/mssql/bin/sqlservr"]
