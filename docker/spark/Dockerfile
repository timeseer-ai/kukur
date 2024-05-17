FROM timeseer/kukur:latest

ARG ODBC_URL=https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/odbc/2.8.0/SimbaSparkODBC-2.8.0.1002-Debian-64bit.zip

USER 0

RUN apt-get update \
 && apt-get --yes install curl unzip libsasl2-modules-gssapi-mit \
 && curl -o driver.zip ${ODBC_URL} \
 && unzip driver.zip *.deb \
 && dpkg -i *.deb \
 && rm *.deb \
 && rm *.zip \
 && rm -rf /var/lib/apt/lists/*

USER 1001
