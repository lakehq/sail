ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}-slim

ARG PYSAIL_VERSION
ARG PYSPARK_VERSION=4.2.0

RUN python3 -m pip install --no-cache-dir "pysail==${PYSAIL_VERSION}"
RUN python3 -m pip install --no-cache-dir "pyspark-client==${PYSPARK_VERSION}"

ENTRYPOINT ["/usr/local/bin/sail"]
