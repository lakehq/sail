FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    bash \
    krb5-admin-server \
    krb5-kdc \
    krb5-user && \
    rm -rf /var/lib/apt/lists/*

COPY kdc-entrypoint.sh /usr/local/bin/kdc-entrypoint.sh
COPY bootstrap-kdc.sh /usr/local/bin/bootstrap-kdc.sh

RUN chmod +x /usr/local/bin/kdc-entrypoint.sh /usr/local/bin/bootstrap-kdc.sh && \
    mkdir -p /artifacts /etc/krb5kdc /var/lib/krb5kdc

EXPOSE 88
EXPOSE 88/udp

ENTRYPOINT ["/usr/local/bin/kdc-entrypoint.sh"]
