# Architecture

The benchmark consists of 3 isolated parts:

* `remote-side` - acts as HTTP and iperf servers for the benchmark
* `middle-box` - acts as a VPN endpoint host, either WireGuard or AdGuard
* `local-side` - acts as a benchmark running host, can establish tunnels to the server
  residing on the remote side through the VPN endpoint

# How to run

## Single host

1) Build docker images
   ```shell
   cd ./bench
   ./single_host.sh build --client=<vpn-libs.git> --endpoint=<vpn-libs-endpoint.git>
   ```

   This command prepares all the parts to run on the current host. To see the full set of
   the available options run:
   ```shell
   ./single_host.sh --help
   ```
2) Run the benchmark
   ```shell
   ./single_host.sh run
   ```

   This command runs all the parts of the benchmark on the current host.

## Separate hosts

Assume IP addresses of `host_1`, `host_2` and `host_3` are 1.1.1.1, 2.2.2.2 and 3.3.3.3 respectively.

1) Running `host_1` as a remote side
   ```shell
   scp Dockerfile user@1.1.1.1:~
   scp -r remote-side user@1.1.1.1:~
   ssh user@1.1.1.1
   docker build -t bench-common .
   docker build -t bench-rs ./remote-side
   docker run -d -p 8080:8080 -p 5201:5201 -p 5201:5201/udp bench-rs
   ```
2) Running `host_2` as a middle box
   ```shell
   scp Dockerfile user@2.2.2.2:~
   git clone <vpn-libs-endpoint.git> ./middle-box/adguard-rust/vpn-libs-endpoint
   scp -r middle-box user@2.2.2.2:~
   ssh user@2.2.2.2
   docker build -t bench-common .
   ```

    * WireGuard
       ```shell
       docker build -t bench-mb-wg ./middle-box/wireguard
       docker run -d \
         --cap-add=NET_ADMIN --cap-add=SYS_MODULE --device=/dev/net/tun \
         -p 51820:51820/udp \
         bench-mb-wg
       ```
    * AdGuard
       ```shell
       docker build \
         --build-arg ENDPOINT_HOSTNAME=endpoint.bench \
         -t bench-mb-ag ./middle-box/adguard-rust/
       docker run -d \
         --cap-add=NET_ADMIN --cap-add=SYS_MODULE --device=/dev/net/tun \
         -p 4433:4433 -p 4433:4433/udp \
         bench-mb-ag
       ```
3) Run the benchmark from `host_3`
   ```shell
   scp Dockerfile user@3.3.3.3:~
   git clone <vpn-libs.git> ./local-side/adguard/vpn-libs
   scp -r local-side user@3.3.3.3:~
   ssh user@3.3.3.3
   docker build -t bench-common .
   docker build -t bench-ls ./local-side
   ```

   * No VPN
      ```shell
      ./local-side/bench.sh no-vpn bridge 1.1.1.1 results/no-vpn
      ```
   * WireGuard
      ```shell
      docker build -t bench-ls-wg ./local-side/wireguard
      ./local-side/bench.sh wg bridge 1.1.1.1 results/wg 2.2.2.2
      ```
   * AdGuard
      ```shell
      docker build -t bench-ls-ag ./local-side/adguard
      ./local-side/bench.sh ag bridge 1.1.1.1 results/ag 2.2.2.2 endpoint.bench 
      ```
