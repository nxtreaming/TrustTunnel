#!/usr/bin/env bash

set -e -x

NGINX_CONF_FILENAME="$1"
NGINX_DOWNLOAD_DIR="$2"
NGINX_1GB_FILENAME="1GiB.dat"
NGINX_100KB_FILENAME="100KiB.dat"

mkdir "$NGINX_DOWNLOAD_DIR"
dd bs=$((1024 * 1024)) count=1024 if=/dev/random of="$NGINX_DOWNLOAD_DIR/$NGINX_1GB_FILENAME"
dd bs=1024 count=128 if=/dev/random of="$NGINX_DOWNLOAD_DIR/$NGINX_100KB_FILENAME"

nginx -g 'daemon off;' -c "$NGINX_CONF_FILENAME" -p "$(pwd)" &
iperf3 -s &
wait
