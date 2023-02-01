# syntax=docker/dockerfile:1
FROM bench-common

ARG ENDPOINT_DIR="vpn-libs-endpoint"
ARG ENDPOINT_HOSTNAME="endpoint.bench"
ARG CONFIG_FILE="vpn.conf"
ARG LOG_LEVEL="info"

RUN apt install -y openssl

COPY $ENDPOINT_DIR /bench/$ENDPOINT_DIR

WORKDIR /bench/
RUN cd "$ENDPOINT_DIR" && \
    cargo build --release --bin vpn_endpoint && \
    mv ./target/release/vpn_endpoint /bench/ && \
    cd examples/my_vpn && \
    openssl req -config openssl.conf -new -x509 -sha256 -newkey rsa:2048 -nodes -days 1000 \
        -keyout /bench/key.pem -out /bench/cert.pem \
        -subj "/C=de/CN=$ENDPOINT_HOSTNAME" \
        -addext "subjectAltName = DNS:*.$ENDPOINT_HOSTNAME"

RUN echo "{\n\
  \"listen_address\": \"[::]:4433\",\n\
  \"tunnel_tls_host_info\": {\n\
    \"hostname\": \"$ENDPOINT_HOSTNAME\",\n\
    \"cert_chain_path\": \"cert.pem\",\n\
    \"private_key_path\": \"key.pem\"\n\
  },\n\
  \"allow_private_network_connections\": true,\n\
  \"listen_protocols\": [\n\
    {\n\
      \"http1\": {}\n\
    },\n\
    {\n\
      \"http2\": {}\n\
    },\n\
    {\n\
      \"quic\": {}\n\
    }\n\
  ]\n\
}" >>$CONFIG_FILE

ENV LOG_LEVEL=$LOG_LEVEL \
    CONFIG_FILE=$CONFIG_FILE \
    RUST_BACKTRACE=1
ENTRYPOINT ./vpn_endpoint -l "$LOG_LEVEL" "$CONFIG_FILE" >>/tmp/vpn.log 2>&1
