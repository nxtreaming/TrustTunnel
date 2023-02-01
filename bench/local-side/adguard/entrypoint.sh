#!/usr/bin/env bash

set -e -x

ENDPOINT_HOSTNAME="$1"
ENDPOINT_IP="$2"
PROTOCOL="$3"
MODE="$4"
if [[ "$MODE" == "socks" ]]; then
  SOCKS_PORT_FIRST="$5"
  SOCKS_PORT_LAST="$6"
fi

COMMON_CONFIG=$(
  cat <<-END
    "server_info": {
        "hostname": "$ENDPOINT_HOSTNAME",
        "addresses": ["$ENDPOINT_IP:4433"],
        "username": "premium",
        "password": "premium",
        "skip_cert_verify": true,
        "upstream_protocol": "$PROTOCOL",
        "upstream_fallback_protocol": "$PROTOCOL"
    },
    "listener_type": "$MODE",
    "killswitch_enabled": true,
    "vpn_mode": "general",
    "loglevel": "info",
END
)

iptables -I OUTPUT -o eth0 -d "$ENDPOINT_IP" -j ACCEPT
iptables -A OUTPUT -o eth0 -j DROP

if [[ "$MODE" == "tun" ]]; then
  {
    echo "{"
    echo "$COMMON_CONFIG"
    echo "
    \"tun_info\": {
        \"excluded_routes\": [],
        \"included_routes\": [
            \"0.0.0.0/0\",
            \"2000::/3\"
        ],
        \"mtu_size\": 1500
    }"
    echo "}"
  } >>standalone_client.conf
  ./standalone_client >>/tmp/vpn.log 2>&1
else
  for port in $(seq "$SOCKS_PORT_FIRST" "$SOCKS_PORT_LAST"); do
    {
      echo "{"
      echo "$COMMON_CONFIG"
      echo "
    \"socks_info\": {
        \"socks_user\": \"\",
        \"socks_pass\": \"\",
        \"socks_host\": \"127.0.0.1\",
        \"socks_port\": \"$port\"
    }"
      echo "}"
    } >>"standalone_client-$port.conf"
    ./standalone_client --config "./standalone_client-$port.conf" >>"/tmp/vpn-$port.log" 2>&1 &
  done

  wait
fi
