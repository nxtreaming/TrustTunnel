#!/usr/bin/env bash

HELP_MSG="
Usage:  bench.sh <type=no-vpn|wg|ag> <network> <remote_ip> <results_dir_path> [<endpoint_ip> <endpoint_hostname>]
"

set -e -x
set -o pipefail

LOCAL_IMAGE="bench-ls"
LOCAL_AG_IMAGE="bench-ls-ag"
LOCAL_WG_IMAGE="bench-ls-wg"
CONTAINER_RESULTS_DIR_PATH="/bench/results"
JOB_NUMS=(1 2 4)

run_test() {
  local set_up_test_suite_cmd="$1"
  shift
  local tear_down_test_suite_cmd="$1"
  shift
  local results_host_dir_path="$1"
  shift

  local container

  container=$(eval "$set_up_test_suite_cmd")
  # Otherwise the bench is not able to connect to the server
  sleep 3
  docker exec -w /bench "$container" ./local_side.py "$@"
  mkdir -p "$results_host_dir_path"
  docker cp "$container:$CONTAINER_RESULTS_DIR_PATH/." "$results_host_dir_path"
  docker rm -f "$container"
  eval "$tear_down_test_suite_cmd"
}

run_through_tun() {
  local set_up_test_suite_cmd="$1"
  local tear_down_test_suite_cmd="$2"
  local results_host_dir_path="$3"
  local remote_ip="$4"

  for jobs_num in "${JOB_NUMS[@]}"; do
    echo "Running download test with ${jobs_num} parallel jobs..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
      --output "$CONTAINER_RESULTS_DIR_PATH/lf-dl-$jobs_num.json" \
      --jobs "$jobs_num" \
      --download "http://$remote_ip:8080/download/1GiB.dat"
    echo "...done"

    echo "Running upload test with ${jobs_num} parallel jobs..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
      --output "$CONTAINER_RESULTS_DIR_PATH/lf-ul-$jobs_num.json" \
      --jobs "$jobs_num" \
      --upload "http://$remote_ip:8080/upload"
    echo "...done"

    echo "Running iperf download over TCP test with ${jobs_num} parallel jobs..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
      --output "$CONTAINER_RESULTS_DIR_PATH/iperf-tcp-dl-$jobs_num.json" \
      --jobs "$jobs_num" \
      --iperf "$remote_ip" \
      --iperf-proto tcp \
      --iperf-dir download
    echo "...done"

    echo "Running iperf upload over TCP test with ${jobs_num} parallel jobs..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
      --output "$CONTAINER_RESULTS_DIR_PATH/iperf-tcp-ul-$jobs_num.json" \
      --jobs "$jobs_num" \
      --iperf "$remote_ip" \
      --iperf-proto tcp \
      --iperf-dir upload
    echo "...done"

    echo "Running iperf download over UDP test with ${jobs_num} parallel jobs..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
      --output "$CONTAINER_RESULTS_DIR_PATH/iperf-udp-dl-$jobs_num.json" \
      --jobs "$jobs_num" \
      --iperf "$remote_ip" \
      --iperf-proto udp \
      --iperf-dir download
    echo "...done"

    echo "Running iperf upload over UDP test with ${jobs_num} parallel jobs..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
      --output "$CONTAINER_RESULTS_DIR_PATH/iperf-udp-ul-$jobs_num.json" \
      --jobs "$jobs_num" \
      --iperf "$remote_ip" \
      --iperf-proto udp \
      --iperf-dir upload
    echo "...done"
  done
}

run_through_proxy() {
  local set_up_test_suite_cmd="$1"
  local tear_down_test_suite_cmd="$2"
  local results_host_dir_path="$3"
  local remote_ip="$4"
  local proxy_hostname="$5"

  common_script_args=(--proxy "https://premium:premium@$proxy_hostname:4433")

  for jobs_num in "${JOB_NUMS[@]}"; do
    echo "Running download test with ${jobs_num} parallel jobs..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
      "${common_script_args[@]}" \
      --output "$CONTAINER_RESULTS_DIR_PATH/lf-dl-$jobs_num.json" \
      --jobs "$jobs_num" \
      --download "http://$remote_ip:8080/download/1GiB.dat"
    echo "...done"
    echo "Running upload test with ${jobs_num} parallel jobs..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
      "${common_script_args[@]}" \
      --output "$CONTAINER_RESULTS_DIR_PATH/lf-ul-$jobs_num.json" \
      --jobs "$jobs_num" \
      --upload "http://$remote_ip:8080/upload"
    echo "...done"
  done

  echo "Running small files download test..."
  run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$results_host_dir_path" \
    "${common_script_args[@]}" \
    --output "$CONTAINER_RESULTS_DIR_PATH/sf-dl.json" \
    --jobs 1000 \
    --download "http://$remote_ip:8080/download/100KiB.dat"
  echo "...done"
}

run_no_vpn() {
  local network="$1"
  local remote_ip="$2"
  local output_dir_path="$3"

  echo "Running bench without any VPN..."
  local set_up_test_suite_cmd="docker run -it -d --network=$network $LOCAL_IMAGE"
  local tear_down_test_suite_cmd=""
  run_through_tun "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$output_dir_path" "$remote_ip" ""

  echo "Running small files download test..."
  run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$output_dir_path" \
    --output "$CONTAINER_RESULTS_DIR_PATH/sf-dl.json" \
    --jobs 1000 \
    --download "http://$remote_ip:8080/download/100KiB.dat"
  echo "...done"

  echo "Bench without any VPN is done"
}

run_through_wg() {
  local network="$1"
  local remote_ip="$2"
  local endpoint_ip="$3"
  local output_dir="$4"
  local container

  local set_up_test_suite_cmd="docker run -d \
    --cap-add=NET_ADMIN --cap-add=SYS_MODULE --device=/dev/net/tun \
    --network=$network \
    $LOCAL_WG_IMAGE \
    $endpoint_ip $remote_ip/32"
  local tear_down_test_suite_cmd=""
  echo "Running bench through WireGuard tunnel..."
  run_through_tun "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$output_dir" \
    "$remote_ip"

  echo "Running small files download test..."
  run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$output_dir" \
    --output "$CONTAINER_RESULTS_DIR_PATH/sf-dl.json" \
    --jobs 1000 \
    --download "http://$remote_ip:8080/download/100KiB.dat"
  echo "...done"

  echo "...done"
}

run_through_ag() {
  local network="$1"
  local remote_ip="$2"
  local endpoint_hostname="$3"
  local endpoint_ip="$4"
  local output_dir="$5"

  local set_up_test_suite_cmd="docker run -it -d --add-host=$endpoint_hostname:$endpoint_ip --network=$network $LOCAL_IMAGE"
  local tear_down_test_suite_cmd=""
  echo "Running bench through AdGuard http1 proxy..."
  run_through_proxy "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$output_dir/http1/" \
    "$remote_ip" "$endpoint_hostname"
  echo "...done"

  for protocol in http2 http3; do
    set_up_test_suite_cmd="docker run -d \
      --cap-add=NET_ADMIN --cap-add=SYS_MODULE --device=/dev/net/tun \
      --add-host=$endpoint_hostname:$endpoint_ip \
      --network=$network \
      $LOCAL_AG_IMAGE \
      $endpoint_hostname $endpoint_ip $protocol tun"
    tear_down_test_suite_cmd=""
    echo "Running bench through AgGuard ${protocol} tunnel..."
    run_through_tun "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$output_dir/${protocol}/" \
      "$remote_ip"

    set_up_test_suite_cmd="{
      container=\$(docker run -d \
        --cap-add=NET_ADMIN --cap-add=SYS_MODULE --device=/dev/net/tun \
        --add-host=$endpoint_hostname:$endpoint_ip \
        --network=$network \
        $LOCAL_AG_IMAGE \
        $endpoint_hostname $endpoint_ip $protocol socks 1080 1179)
      sleep 10
      echo \$container
    }"
    echo "Running small files download test..."
    run_test "$set_up_test_suite_cmd" "$tear_down_test_suite_cmd" "$output_dir/${protocol}/" \
      --output "$CONTAINER_RESULTS_DIR_PATH/sf-dl.json" \
      --jobs 10 \
      --download "http://$remote_ip:8080/download/100KiB.dat" \
      --proxy "socks5://127.0.0.1" \
      --socks-ports-range "(1080,1179)"
    echo "...done"

    echo "...done"
  done
}

type="$1"
network="$2"
remote_ip="$3"
output_dir="$4"
endpoint_ip="$5"
endpoint_hostname="$6"

if [[ "$type" == "no-vpn" ]]; then
  run_no_vpn "$network" "$remote_ip" "$output_dir"
elif [[ "$type" == "wg" ]]; then
  run_through_wg "$network" "$remote_ip" "$endpoint_ip" "$output_dir"
elif [[ "$type" == "ag" ]]; then
  run_through_ag "$network" "$remote_ip" "$endpoint_hostname" "$endpoint_ip" "$output_dir"
else
  echo "$HELP_MSG"
  exit 1
fi
