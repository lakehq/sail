FROM python:3.11-slim

ARG PYSAIL_VERSION
ARG PYSPARK_VERSION=4.1.0

RUN python3 -m pip install --no-cache-dir "pysail==${PYSAIL_VERSION}"
RUN python3 -m pip install --no-cache-dir "pyspark[connect]==${PYSPARK_VERSION}"

ENTRYPOINT ["/usr/local/bin/sail"]
