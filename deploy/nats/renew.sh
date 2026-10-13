#!/bin/bash
set -euo pipefail
umask 077
root=${SKVOZ_NATS_STATE:-/var/lib/skvoz-nats}
: "${SKVOZ_ACME_EMAIL:?}" "${SKVOZ_ACME_TERMS_AGREED:?}"
[[ $SKVOZ_ACME_TERMS_AGREED == true ]]
address=$(cat "$root/address")
if [[ -f $root/tls/current/certificate.pem ]] && openssl x509 -in "$root/tls/current/certificate.pem" -checkend 172800 -noout >/dev/null; then
 systemctl stop skvoz-nats-retry.timer || true
 exit 0
fi
# Only a confirmed unknown, unregistered account can be reset. lego5.5.2
# writes this exact pretty-JSON metadata before its first network request.
recover_unregistered_account() {
 local server=${SKVOZ_ACME_DIRECTORY:-https://acme-v02.api.letsencrypt.org/directory}
 local host account key saved
 local -a metadata
 [[ $server =~ ^https?://[[:graph:]]+$ && $server != *'"'* && $server != *'\'* ]] || return 1
 [[ $SKVOZ_ACME_EMAIL =~ ^[a-zA-Z0-9_.+@-]+$ ]] || return 1
 host=${server#*://}; host=${host%%/*}; host=${host%%\?*}; host=${host%%\#*}
 [[ -n $host && $host != . && $host != .. && $host != *'@'* ]] || return 1
 account="$root/acme/accounts/${host//:/_}/$SKVOZ_ACME_EMAIL/account.json"
 key="${account%/*}/$SKVOZ_ACME_EMAIL.key"
 [[ -f $account && ! -L $account && -f $key && ! -L $key ]] || return 1
 [[ $(stat -c %s "$account") -le 16384 ]] || return 1
 mapfile -t metadata < "$account"
 # Require the complete pinned top-level layout; ambiguous, corrupt, nested
 # or registered metadata is never treated as an unregistered account.
 [[ ${#metadata[@]} == 7 || ${#metadata[@]} == 8 ]] || return 1
 [[ ${metadata[0]} == '{' && ${metadata[1]} == $'\t'\"id\":\ \"$SKVOZ_ACME_EMAIL\", &&
    ${metadata[2]} == $'\t'\"email\":\ \"$SKVOZ_ACME_EMAIL\", &&
    ${metadata[3]} =~ ^$'\t'\"keyType\":\ \"(rsa2048|rsa3072|rsa4096|rsa8192|ec256|ec384)\",$ &&
    ${metadata[4]} == $'\t'\"server\":\ \"$server\", && ${metadata[-1]} == '}' ]] || return 1
 if [[ ${#metadata[@]} == 8 ]]; then
  [[ ${metadata[5]} =~ ^$'\t'\"origin\":\ \"(command|configuration|migration)\",$ ]] || return 1
 fi
 [[ ${metadata[-2]} == $'\t"registration": null' ]] || return 1
 saved=$(mktemp -d "$root/acme/unregistered-XXXXXXXX") || return 1
 # Preserve the orphan key and metadata, not certificates or registered state.
 mv "$key" "$saved/account.key" || return 1
 mv "$account" "$saved/account.json" || return 1
 printf 'Unregistered ACME account preserved; the next scheduled retry can register again.\n' >&2
}
# One issuance attempt retains the original150s budget. Recovery prepares the
# next existing retry; it never adds a second issuance attempt here.
log=$(mktemp "$root/renew-XXXXXXXX.log")
trap 'rm -f "$log"' EXIT
if timeout --kill-after=2 150 lego run --renew-force --path "$root/acme" \
 --server "${SKVOZ_ACME_DIRECTORY:-https://acme-v02.api.letsencrypt.org/directory}" \
 --email "$SKVOZ_ACME_EMAIL" --accept-tos --domains "$address" --profile shortlived \
 --http --http.address "${SKVOZ_ACME_LISTEN:-0.0.0.0:80}" --cert.timeout 120 --no-random-sleep 2>"$log"; then
 cat "$log" >&2
else
 status=$?
 cat "$log" >&2
 if grep -Fq 'urn:ietf:params:acme:error:accountDoesNotExist' "$log"; then
  recover_unregistered_account || true
 fi
 exit "$status"
fi
name=${address//:/_}
/usr/local/lib/skvoz-nats/tls.sh apply "$root/acme/certificates/$name.crt" "$root/acme/certificates/$name.key"
systemctl stop skvoz-nats-retry.timer || true
