#!/usr/bin/env bash

set -e -x

IFACE=wg0
SELF_PRIV_KEY="UBzTEjX7tMYzt1tPOiJeajPmAdKm0fTHtqC+x9kJ5WQ="
PEER_PUB_KEY="Wp819/AlaNviIUZ4KvTBoOUp6VWTG53CIPkuSdn4FiM="

CONFIG=$(
  cat <<-END
[Interface]
Address = 10.0.0.1/24
ListenPort = 51820
PrivateKey = $SELF_PRIV_KEY

PostUp = iptables -A FORWARD -i $IFACE -j ACCEPT; iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
PostDown = iptables -D FORWARD -i $IFACE -j ACCEPT; iptables -t nat -D POSTROUTING -o eth0 -j MASQUERADE

[Peer]
PublicKey = $PEER_PUB_KEY
AllowedIPs = 10.0.0.0/24
END
)
echo "$CONFIG" >>"$IFACE.conf"

on_exit() {
  set +e

  wg show
  wg-quick down "./$IFACE.conf"
}

wg-quick up "./$IFACE.conf"

trap on_exit EXIT TERM INT
sleep infinity
