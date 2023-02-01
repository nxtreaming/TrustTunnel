# syntax=docker/dockerfile:1
FROM bench-common

RUN apt install -y iptables wireguard && \
    cargo install boringtun-cli

ADD entrypoint.sh \
    /bench/

WORKDIR /bench/

ENV WG_QUICK_USERSPACE_IMPLEMENTATION=boringtun
ENTRYPOINT ./entrypoint.sh
