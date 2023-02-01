#!/usr/bin/env bash

set -e -x

ENDPOINT_IP="$1"
ROUTE="$2"
CLIENT_PROC="boringtun-cli"
IFACE=wg0
SELF_PRIV_KEY="MOv7hICJE+V3fc2PqQQBOyWXQgG1hogB7VYdJwJLMkA="
ENDPOINT_PUB_KEY="Ib6tSFEy7NSFSe8VmiBAG4P818TscYwYfxYvHB18CV8="

# fixme: default route causes permission denied error on sysctl
CONFIG=$(
  cat <<-END
[Interface]
Address = 10.0.0.2/24
ListenPort = 51820
PrivateKey = $SELF_PRIV_KEY

[Peer]
PublicKey = $ENDPOINT_PUB_KEY
Endpoint = $ENDPOINT_IP:51820
AllowedIPs = $ROUTE
END
)
echo "$CONFIG" >>"$IFACE.conf"

iptables -I OUTPUT -o eth0 -d "$ENDPOINT_IP" -j ACCEPT
iptables -A OUTPUT -o eth0 -j DROP

$CLIENT_PROC --disable-drop-privileges -f "$IFACE" &
./wg-quick.sh up "./$IFACE.conf" &
wait
