#!/usr/bin/env bash
set -euo pipefail

REALM="${KERBEROS_REALM:-SAIL.TEST}"
KDC_HOSTNAME="${KDC_HOSTNAME:-sail-kerberos-kdc}"
MASTER_PASSWORD="${KERBEROS_MASTER_PASSWORD:-sail-master-password}"

cat > /etc/krb5.conf <<EOF
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

cat > /etc/krb5kdc/kdc.conf <<EOF
[kdcdefaults]
 kdc_ports = 88
 kdc_tcp_ports = 88

[realms]
 ${REALM} = {
  acl_file = /etc/krb5kdc/kadm5.acl
  database_name = /var/lib/krb5kdc/principal
  admin_keytab = FILE:/etc/krb5kdc/kadm5.keytab
  key_stash_file = /etc/krb5kdc/stash
  max_life = 24h
  max_renewable_life = 7d
  default_principal_flags = +preauth
 }
EOF

printf '*/admin@%s *\n' "${REALM}" > /etc/krb5kdc/kadm5.acl

if [ ! -f /var/lib/krb5kdc/principal ]; then
  kdb5_util create -s -P "${MASTER_PASSWORD}"
fi

krb5kdc
kadmind -nofork &

echo "KDC ready"
wait $!
