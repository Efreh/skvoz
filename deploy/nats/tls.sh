#!/bin/bash
set -euo pipefail
umask 077
root=${SKVOZ_NATS_STATE:-/var/lib/skvoz-nats}
[[ $root =~ ^/[a-zA-Z0-9/_-]+$ ]]
trust=${SKVOZ_NATS_CA:-/etc/ssl/certs/ca-certificates.crt}
identity_inputs() {
 address=$(cat "$root/address") || return 1
 [[ -n $address ]] || return 1
 verify=(-verify_hostname "$address")
 [[ $address != *:* && ! $address =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || verify=(-verify_ip "$address")
}
validate() {
 local cert=$1 key=$2
 local san
 identity_inputs || return 1
 san=$(openssl x509 -in "$cert" -ext subjectAltName -noout) || return 1
 if [[ ${verify[0]} == -verify_hostname ]]; then [[ $san == *DNS:* ]] || return 1; else [[ $san == *"IP Address:"* ]] || return 1; fi
 openssl verify -CAfile "$trust" -untrusted "$cert" -purpose sslserver "${verify[@]}" "$cert" >/dev/null || return 1
 local cert_key key_public
 cert_key=$(openssl x509 -in "$cert" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum) || return 1
 key_public=$(openssl pkey -in "$key" -pubout -outform DER | sha256sum) || return 1
 [[ $cert_key == "$key_public" ]] || return 1
 openssl x509 -in "$cert" -checkend 0 -noout >/dev/null || return 1
}
expiry() { date -u -d "$(openssl x509 -in "$1" -enddate -noout | cut -d= -f2-)" +%s; }
probe_serial() {
 identity_inputs || return 1
 port=$(cat "$root/leaf-port") || return 1
 [[ $port =~ ^[0-9]{1,5}$ ]] && (( 10#$port>=1 && 10#$port<=65535 )) || return 1
 timeout --kill-after=1 5 bash -euo pipefail -c '
  address=$1; port=$2; trust=$3; serial=$4; shift 4
  while :; do
   if served=$(openssl s_client -connect "127.0.0.1:$port" -servername "$address" -verify_return_error "$@" -CAfile "$trust" </dev/null 2>/dev/null | openssl x509 -serial -noout) && [[ $served == "$serial" ]]; then exit 0; fi
   sleep 0.05
  done
 ' _ "$address" "$port" "$trust" "$1" "${verify[@]}"
}
schedule() {
 local end=$1 serial=${2#serial=}
 [[ $end =~ ^[0-9]+$ && $serial =~ ^[0-9A-Fa-f]{1,64}$ ]] || return 1
 if systemctl is-active --quiet "skvoz-nats-expire-$serial.timer"; then return; fi
 systemd-run --collect --quiet --unit="skvoz-nats-expire-$serial" \
  --on-calendar="@$end" --timer-property=AccuracySec=1s \
  --setenv="SKVOZ_NATS_STATE=$root" --setenv="SKVOZ_NATS_CA=$trust" \
  --property=NoNewPrivileges=true --property=ProtectHome=true \
  --property=ProtectSystem=strict --property="ReadWritePaths=$root/activation.lock" \
  /usr/local/lib/skvoz-nats/tls.sh expire "$2"
}
stop_expired() {
 systemctl kill --kill-whom=main --signal=KILL skvoz-nats.service || true
 systemctl stop skvoz-nats.service
}
replace_link() {
 local link
 link=$(mktemp "$root/tls/link-XXXXXXXX") || return 1
 rm -f "$link" || return 1
 ln -s "$1" "$link" || { rm -f "$link"; return 1; }
 mv -Tf "$link" "$root/tls/current" || { rm -f "$link"; return 1; }
}
case ${1:-} in
 check)
  validate "$root/tls/current/certificate.pem" "$root/tls/current/key.pem"
  ;;
 apply)
  [[ $# == 3 ]]
  exec 9>>"$root/apply.lock"; flock -w 3 9
  generation=$(mktemp -d "$root/tls/generation-XXXXXXXX")
  trap 'rm -rf "$generation"' EXIT
  install -m 0600 "$2" "$generation/certificate.pem"
  install -m 0600 "$3" "$generation/key.pem"
  validate "$generation/certificate.pem" "$generation/key.pem"
  chown --reference="$root/nats.conf" "$generation" "$generation/certificate.pem" "$generation/key.pem"
  end=$(expiry "$generation/certificate.pem")
  start=$(date -u -d "$(openssl x509 -in "$generation/certificate.pem" -startdate -noout | cut -d= -f2-)" +%s)
  (( end>start && end-start<=576000 ))
  serial=$(openssl x509 -in "$generation/certificate.pem" -serial -noout)
  old=$(readlink "$root/tls/current" || true)
  touched=false
  activation_open=false
  old_serial=""
  old_end=""
  [[ -z $old ]] || old_serial=$(openssl x509 -in "$old/certificate.pem" -serial -noout)
  [[ -z $old ]] || old_end=$(expiry "$old/certificate.pem")
  replace_link "$generation"
  commit_expiry() {
   if [[ $activation_open == false ]]; then exec 8>>"$root/activation.lock" || return 1; activation_open=true; fi
   flock -w 1 8 || return 1
   schedule "$1" "$2" || return 1
   printf '%s %s\n' "$1" "$2" > "$root/expiry.next" || return 1
   mv -f "$root/expiry.next" "$root/expiry" || return 1
   sync -f "$root/expiry" || return 1
   flock -u 8 || return 1
  }
  restore() {
   if [[ -n $old ]]; then
    if ! replace_link "$old"; then
     [[ $touched == false ]] || stop_expired || true
     printf 'Unable to restore previous TLS generation link\n' >&2; return 1
    fi
   elif ! rm -f "$root/tls/current"; then
    [[ $touched == false ]] || stop_expired || true
    printf 'Unable to remove unconfirmed TLS generation link\n' >&2; return 1
   fi
   if [[ $touched == true ]] && systemctl is-active --quiet skvoz-nats.service; then
    if [[ -n $old ]] && validate "$old/certificate.pem" "$old/key.pem" && systemctl kill --kill-whom=main --signal=HUP skvoz-nats.service && probe_serial "$old_serial"; then
     # Restore both serving material and its original absolute expiry fence.
     if ! commit_expiry "$old_end" "$old_serial"; then stop_expired || true; fi
    else stop_expired || true; fi
   fi
  }
  trap 'restore; rm -rf "$generation"' EXIT
  nats-server -t -c "$root/nats.conf"
  if systemctl is-active --quiet skvoz-nats.service; then
   # A nonzero systemctl result does not prove that HUP was undelivered.
   touched=true
   systemctl kill --kill-whom=main --signal=HUP skvoz-nats.service
   probe_serial "$serial"
  fi
  # Only a confirmed serving generation extends the independent expiry fence.
  exec 8>>"$root/activation.lock"; activation_open=true; flock -w 1 8
  if systemctl is-active --quiet skvoz-nats.service; then probe_serial "$serial"; fi
  commit_expiry "$end" "$serial"
  if [[ -n $old_serial && $old_serial != "$serial" ]]; then systemctl stop "skvoz-nats-expire-${old_serial#serial=}.timer" || true; fi
  trap - EXIT
  # Keep the committed and previous generation; delete only this tool's directory names.
  for prior in "$root"/tls/generation-*; do
   [[ $prior == "$generation" || $prior == "$old" || ! -d $prior ]] || rm -rf "$prior"
  done
  ;;
 schedule)
  read -r end serial < "$root/expiry"
  validate "$root/tls/current/certificate.pem" "$root/tls/current/key.pem"
  [[ $(openssl x509 -in "$root/tls/current/certificate.pem" -serial -noout) == "$serial" ]]
  [[ $end == "$(expiry "$root/tls/current/certificate.pem")" ]]
  schedule "$end" "$serial"
  ;;
 expire)
  [[ $# == 2 && $2 =~ ^serial=[0-9A-Fa-f]{1,64}$ ]]
  if ! { exec 8>>"$root/activation.lock"; }; then stop_expired; exit 1; fi
  if ! flock -w 1 8; then
   if read -r end serial < "$root/expiry" && [[ $end =~ ^[0-9]+$ && $serial != "$2" ]]; then exit 0; fi
   stop_expired; exit 1
  fi
  if ! read -r end serial < "$root/expiry" || [[ ! $end =~ ^[0-9]+$ ]]; then
   stop_expired; exit 1
  fi
  # A stale timer cannot act after the serving generation was committed.
  [[ $serial == "$2" ]] || exit 0
  (( $(date +%s)>=end )) || exit 0
  stop_expired
  ;;
 *) printf 'Usage: tls.sh check|apply CERT KEY|schedule|expire SERIAL\n' >&2; exit 2 ;;
esac
