#!/usr/bin/env bash
set -euo pipefail

REALM="${KERBEROS_REALM:-SAIL.TEST}"
KDC_HOSTNAME="${KDC_HOSTNAME:-sail-kerberos-kdc}"
SERVICE_PRINCIPAL="${1:?missing service principal}"
CLIENT_PRINCIPAL="${2:?missing client principal}"
SERVICE_KEYTAB="${3:?missing service keytab path}"
CLIENT_KEYTAB="${4:?missing client keytab path}"

kadmin.local -q "addprinc -randkey ${SERVICE_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${CLIENT_PRINCIPAL}"

mkdir -p "$(dirname "${SERVICE_KEYTAB}")" "$(dirname "${CLIENT_KEYTAB}")" /artifacts
rm -f "${SERVICE_KEYTAB}" "${CLIENT_KEYTAB}"

kadmin.local -q "ktadd -k ${SERVICE_KEYTAB} ${SERVICE_PRINCIPAL}"
kadmin.local -q "ktadd -k ${CLIENT_KEYTAB} ${CLIENT_PRINCIPAL}"
chmod 600 "${SERVICE_KEYTAB}" "${CLIENT_KEYTAB}"

cat > /artifacts/krb5.conf <<EOF
[libdefaults]
 default_realm = ${REALM}
 dns_lookup_kdc = false
 dns_lookup_realm = false
 rdns = false
 dns_canonicalize_hostname = false
 ignore_acceptor_hostname = true
 ticket_lifetime = 24h
 forwardable = true

[realms]
 ${REALM} = {
  kdc = ${KDC_HOSTNAME}:88
 }

[domain_realm]
 ${KDC_HOSTNAME} = ${REALM}
 .${KDC_HOSTNAME} = ${REALM}
 localhost = ${REALM}
 .local = ${REALM}
EOF
