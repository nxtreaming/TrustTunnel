# syntax=docker/dockerfile:1
FROM bench-ls

ARG WG_QUICK_URL="https://raw.githubusercontent.com/WireGuard/wireguard-tools/71799a8f6d1450b63071a21cad6ed434b348d3d5/src/wg-quick/linux.bash"

RUN apt install -y iptables wireguard && \
    cargo install boringtun-cli

ADD entrypoint.sh \
    wg-quick.patch \
    /bench/
ADD "$WG_QUICK_URL" /bench/wg-quick.sh

WORKDIR /bench/
RUN chmod +x wg-quick.sh && \
    patch wg-quick.sh wg-quick.patch

ENV WG_QUICK_USERSPACE_IMPLEMENTATION=boringtun
ENTRYPOINT ["./entrypoint.sh"]
CMD ["endpoint_ip", "route"]
