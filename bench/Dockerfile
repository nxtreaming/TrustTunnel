# syntax=docker/dockerfile:1
FROM python:3.11-slim-bullseye

ARG RUST_CHANNEL=1.62

# Install the required utilities
RUN apt update && \
    apt install -y build-essential cmake curl git gnupg lsb-release iperf3 iproute2 \
    net-tools software-properties-common wget

# Install Rust and Cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_CHANNEL}
ENV PATH="/root/.cargo/bin:${PATH}"

CMD bash
