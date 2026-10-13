#!/bin/bash
set -euo pipefail
umask 077
# Public keys come from skvoz-server auth-public, never from private seed files.
: "${SKVOZ_ISSUER:?}" "${SKVOZ_XKEY:?}" "${SKVOZ_NAMESPACE:?}" "${SKVOZ_ADDRESS:?}"
[[ $SKVOZ_ISSUER =~ ^A[A-Z2-7]{55}$ && $SKVOZ_XKEY =~ ^X[A-Z2-7]{55}$ ]]
[[ $SKVOZ_NAMESPACE =~ ^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)*$ && ${#SKVOZ_NAMESPACE} -le 256 ]]
awk -F. '{for(i=1;i<=NF;i++)if(length($i)>64)exit 1}' <<< "$SKVOZ_NAMESPACE"
[[ $SKVOZ_ADDRESS =~ ^[a-zA-Z0-9:.-]+$ && ${#SKVOZ_ADDRESS} -le 253 ]]
client=${SKVOZ_CLIENT_PORT:-4222}; leaf=${SKVOZ_LEAF_PORT:-7422}
[[ $client =~ ^[0-9]{1,5}$ && $leaf =~ ^[0-9]{1,5}$ ]]
(( client>0 && client<=65535 && leaf>0 && leaf<=65535 && client!=leaf ))
root=${SKVOZ_NATS_STATE:-/var/lib/skvoz-nats}
[[ $root =~ ^/[a-zA-Z0-9/_-]+$ && ! $root =~ /\.\.(/|$) ]]
[[ ! -e $root/nats.conf && ! -e $root/leaf-remotes.json ]]
install -d -m 0700 "$root" "$root/tls"
id=$(openssl rand -hex 8)
password() { openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n'; }
app=$(password); auth=$(password); sys=$(password)
app_user=__skvoz_leaf_${id}_app; auth_user=__skvoz_leaf_${id}_auth; sys_user=__skvoz_leaf_${id}_sys
cat > "$root/nats.conf" <<CONFIG
server_name: "skvoz-entry-$id"
listen: "0.0.0.0:$client"
http: "127.0.0.1:8222"
max_payload: 65588
max_pending: 4194304
max_connections: 1312
write_deadline: 2s
system_account: SYS
tls {cert_file:"$root/tls/current/certificate.pem",key_file:"$root/tls/current/key.pem",handshake_first:false}
accounts {
 APP {users:[{user:"$app_user",password:"$app",allowed_connection_types:[LEAFNODE],permissions:{publish:{allow:["$SKVOZ_NAMESPACE.>","skvoz.enroll.>"]},subscribe:{allow:["$SKVOZ_NAMESPACE.>","skvoz.enroll.>"]}}}]}
 AUTH {users:[{user:"$auth_user",password:"$auth",allowed_connection_types:[LEAFNODE],permissions:{publish:{allow:["\$SYS._INBOX.>","_INBOX.>"]},subscribe:{allow:["\$SYS.REQ.USER.AUTH"]}}}]}
 SYS {users:[{user:"$sys_user",password:"$sys",allowed_connection_types:[LEAFNODE],permissions:{publish:{allow:["\$SYS.REQ.SERVER.*.KICK","\$SYS.REQ.SERVER.*.CONNZ","\$SYS.REQ.SERVER.PING.IDZ"]},subscribe:{allow:["_INBOX.skvoz.sys.>","\$SYS._INBOX.>","\$SYS.ACCOUNT.APP.CONNECT","\$SYS.ACCOUNT.APP.DISCONNECT"]}}}]}
}
authorization {timeout:3s,auth_callout:{account:AUTH,issuer:"$SKVOZ_ISSUER",xkey:"$SKVOZ_XKEY",auth_users:["$app_user","$auth_user","$sys_user"]}}
leafnodes {listen:"0.0.0.0:$leaf",compression:off,tls:{cert_file:"$root/tls/current/certificate.pem",key_file:"$root/tls/current/key.pem",handshake_first:true}}
CONFIG
cat > "$root/leaf-remotes.json" <<CONFIG
{"v":1,"entries":[{"address":"$SKVOZ_ADDRESS","port":$leaf,"ca_pem":null,"users":{"APP":{"username":"$app_user","password":"$app"},"AUTH":{"username":"$auth_user","password":"$auth"},"SYS":{"username":"$sys_user","password":"$sys"}}}]}
CONFIG
printf '%s\n' "$SKVOZ_ADDRESS" > "$root/address"
printf '%s\n' "$leaf" > "$root/leaf-port"
# Each entry owns separate native credentials and independent ACME state.
printf 'Configuration saved. Transfer leaf-remotes.json privately to the full server.\n'
