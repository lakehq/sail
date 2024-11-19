FROM python:3.11-slim

RUN python3 -m pip install --no-cache-dir "pysail[spark]==v0.2.0.dev0"

ENTRYPOINT ["/usr/local/bin/sail"]
