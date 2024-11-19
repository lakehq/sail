FROM python:3.11-slim

RUN python3 -m pip install --no-cache-dir "pysail[spark]"

ENTRYPOINT ["/usr/local/bin/sail"]
