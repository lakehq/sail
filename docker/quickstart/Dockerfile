FROM python:3.11-slim

ARG PYSAIL_VERSION

RUN python3 -m pip install --no-cache-dir "pysail[spark]==${PYSAIL_VERSION}"

ENTRYPOINT ["/usr/local/bin/sail"]
