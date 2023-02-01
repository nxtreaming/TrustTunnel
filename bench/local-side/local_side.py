#!/usr/bin/env python3

import json
import os
import subprocess
import sys
import threading
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path


def parse_socks_ports_range(text):
    if len(text) <= 3 or text[0] != '(' or text[-1] != ')' or ',' not in text:
        raise ValueError('invalid syntax')

    (first, last) = text[1:-1].split(',')
    return int(first), int(last)


PARSER = ArgumentParser()
PARSER.add_argument("-j", "--jobs", default=1, help="Parallel jobs number. Default is 1.", type=int)
PARSER.add_argument("-x", "--proxy",
                    help="""
                    URL of the endpoint as a proxy for the HTTP transfer tests.
                    The syntax is the same as the corresponding curl option.
                    In case it's not specified, it's assumed that network traffic is tunneled
                    through an endpoint (for example, via a VPN client).
                    If proxy authorization is required, the URL should contain the credentials
                    (e.g., https://login:password@myproxy.org).
                    """)
PARSER.add_argument("-d", "--download", help="URL for the file download test.", type=str)
PARSER.add_argument("-u", "--upload",
                    help="""
                    URL for the file upload test. Shouldn't contain a file name, it is generated automatically.
                    """,
                    type=str)
PARSER.add_argument("--iperf", help="iperf server `IP[:PORT]` for test. Requires iperf3 installed.", type=str)
PARSER.add_argument("--iperf-proto", help="iperf test protocol.", type=str, choices=["tcp", "udp"])
PARSER.add_argument("--iperf-dir", help="iperf test direction.", type=str, choices=["download", "upload"])
PARSER.add_argument("-o", "--output", help="Output file. Filled with JSON encoded results.", type=str)
PARSER.add_argument("--socks-ports-range",
                    help="""
                    Syntax: (int,int) (e.g., `--socks-ports-range (1,42)`)
                    Has sense in case a SOCKS5 proxy is specified (i.e., `--proxy socks5[h]://...`).
                    The proxy URL MUST NOT contain a port number. Instead, the script generates
                    parallel requests to destination through the SOCKS5 proxies running on the same
                    host.
                    For example, `--download http://1.1.1.1:80/file.txt --proxy socks5://127.0.0.1 --socks-ports-range (1080,1081)`
                    runs one request for `http://1.1.1.1:80/file.txt` through each of 2 SOCKS5
                    proxies (`127.0.0.1:1080` and `127.0.0.1:1081`) in parallel.
                    Adding `--jobs N` argument causes repeating the request N times through each proxy (i.e.,
                    `--jobs 2 --download http://1.1.1.1:80/file.txt --proxy socks5://127.0.0.1 --socks-ports-range (1080,1081)`
                    runs the request 2 times through each proxy).
                    """,
                    type=parse_socks_ports_range)

ARGS = PARSER.parse_args()


class HttpMeasurements:
    jobs_num: int = 0
    failed_num: int = 0
    avg_speed_MBps: float = 0.0
    errors: [str] = []

    def __init__(self, jobs_num, speed_samples, errors):
        assert jobs_num == len(speed_samples) + len(errors)

        self.jobs_num = jobs_num
        self.failed_num = len(errors)
        if len(speed_samples) > 0:
            self.avg_speed_MBps = sum(speed_samples) / len(speed_samples)
        self.errors = errors

    def __str__(self):
        return f"""
        Jobs number:       {self.jobs_num}
        Failed jobs:       {self.failed_num}
        Average speed:     {self.avg_speed_MBps:.2f}MB/s
        """

    def __repr__(self):
        return str(self)


class TcpIperfMeasurements:
    jobs_num: int = 0
    avg_sender_speed_MBps: float = 0.0
    sender_total_retries: int = 0
    avg_receiver_speed_MBps: float = 0.0

    def __init__(self, jobs_num, sender_speed_MBps, sender_retries, receiver_speed_MBps):
        self.jobs_num = jobs_num
        self.avg_sender_speed_MBps = sender_speed_MBps / jobs_num
        self.sender_total_retries = sender_retries
        self.avg_receiver_speed_MBps = receiver_speed_MBps / jobs_num

    def __str__(self):
        return f"""
        Jobs number:               {self.jobs_num}
        Average sender speed:      {self.avg_sender_speed_MBps:.2f}MB/s
        Sender total retries:      {self.sender_total_retries}
        Average receiver speed:    {self.avg_receiver_speed_MBps:.2f}MB/s
        """

    def __repr__(self):
        return str(self)


class UdpIperfMeasurements:
    jobs_num: int = 0
    avg_sender_speed_MBps: float = 0.0
    sender_total_sent_bytes: float = 0.0
    avg_receiver_speed_MBps: float = 0.0
    receiver_total_received_bytes: float = 0.0
    total_lost_data_percent: float = 0.0

    def __init__(self, jobs_num, sender_speed_MBps, sender_sent_bytes, receiver_speed_MBps, receiver_received_bytes):
        self.jobs_num = jobs_num
        self.avg_sender_speed_MBps = sender_speed_MBps / jobs_num
        self.sender_total_sent_bytes = sender_sent_bytes
        self.avg_receiver_speed_MBps = receiver_speed_MBps / jobs_num
        self.receiver_total_received_bytes = receiver_received_bytes

        assert sender_sent_bytes >= receiver_received_bytes
        self.total_lost_data_percent = round(100 * (sender_sent_bytes - receiver_received_bytes) / sender_sent_bytes, 2)

    def __str__(self):
        return f"""
        Jobs number:                   {self.jobs_num}
        Average sender speed:          {self.avg_sender_speed_MBps:.2f}MB/s
        Sender total sent bytes:       {self.sender_total_sent_bytes}
        Average receiver speed:        {self.avg_receiver_speed_MBps:.2f}MB/s
        Receiver total received bytes: {self.receiver_total_received_bytes}
        Total lost data:               {self.total_lost_data_percent}%
        """

    def __repr__(self):
        return str(self)


def run_file_download_test(url, parallel_files, proxy_url):
    print(f"{datetime.now().time()} | Running file download test...")

    args = ["curl", "--output", "/dev/null", "--fail", "--insecure", url]
    if proxy_url is not None:
        args.extend(["--proxy-insecure", "--proxy", proxy_url])
    processes = []
    print(f"{datetime.now().time()} | Running command '{args}' in parallel {parallel_files}...")
    for _ in range(0, parallel_files):
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        processes.append(proc)

    speeds_MBps = []
    errors = []
    for p in processes:
        if p.wait() != 0:
            message = p.stdout.read().decode("utf-8")
            print(f"{datetime.now().time()} | Transfer failed:\n{message}")
            errors.append(message)
            continue

        output = p.stdout.read().decode("utf-8")
        speed_str = output.splitlines()[-1].split()[6]
        if 'k' in speed_str:
            speeds_MBps.append(float(speed_str.replace('k', '')) / 1024)
        elif 'M' in speed_str:
            speeds_MBps.append(float(speed_str.replace('M', '')))
        elif 'G' in speed_str:
            speeds_MBps.append(1024 * float(speed_str.replace('G', '')))
        else:
            speeds_MBps.append(float(speed_str) / (1024 * 1024))

    print(f"{datetime.now().time()} | File download test is done")
    return HttpMeasurements(
        parallel_files,
        speeds_MBps,
        errors,
    )


def run_file_upload_test(url, parallel_files, proxy_url):
    print(f"{datetime.now().time()} | Running file upload test...")

    args = ["curl", "-X", "PUT", "-T", os.environ['UPLOAD_FILENAME'], "--fail", "--insecure"]
    if proxy_url is not None:
        args.extend(["--proxy-insecure", "--proxy", proxy_url])
    processes = []
    print(f"{datetime.now().time()} | Running command '{args}' in parallel {parallel_files}...")
    for _ in range(0, parallel_files):
        proc = subprocess.Popen(
            args + [f"{url}/{os.environ['UPLOAD_FILENAME']}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        processes.append(proc)

    speeds_MBps = []
    errors = []
    for p in processes:
        if p.wait() != 0:
            message = p.stdout.read().decode("utf-8")
            print(f"{datetime.now().time()} | Transfer failed:\n{message}")
            errors.append(message)
            continue

        output = p.stdout.read().decode("utf-8")
        speed_str = output.splitlines()[-1].split()[7]
        if 'k' in speed_str:
            speeds_MBps.append(float(speed_str.replace('k', '')) / 1024)
        elif 'M' in speed_str:
            speeds_MBps.append(float(speed_str.replace('M', '')))
        elif 'G' in speed_str:
            speeds_MBps.append(1024 * float(speed_str.replace('G', '')))
        else:
            speeds_MBps.append(float(speed_str) / (1024 * 1024))

    print(f"{datetime.now().time()} | File upload test is done")
    return HttpMeasurements(
        parallel_files,
        speeds_MBps,
        errors,
    )


def run_iperf_test(server, proto, dir, parallel_threads):
    args = ["iperf3", "--no-delay", "--zerocopy", "--client", server, "--parallel", str(parallel_threads),
            "--format", "M"]

    extra_args = {
        ("tcp", "download"): ["--reverse"],
        ("tcp", "upload"): [],
        ("udp", "download"): ["--reverse", "--udp", "--bitrate", "0"],
        ("udp", "upload"): ["--udp", "--bitrate", "0"],
    }
    args.extend(extra_args[(proto, dir)])

    print(f"{datetime.now().time()} | Running iperf test '{args}'...")

    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
        if proc.wait() != 0:
            message = f"iperf test failed:\n{proc.stdout.read().decode('utf-8')}"
            print(f"{datetime.now().time()} | {message}")
            return {"error": message}

        results = proc.stdout.read().decode("utf-8")
        print(f"{datetime.now().time()} | iperf output:\n{results}")

    print(f"{datetime.now().time()} | iperf test is done")

    result_lines = {
        "sender": None,
        "receiver": None,
    }
    for line in reversed(results.splitlines()):
        if ((parallel_threads > 1 and line.startswith("[SUM]")) or (parallel_threads == 1 and line.startswith('['))) \
                and (line.endswith("sender") or line.endswith("receiver")):
            parts = line.split()
            if parallel_threads == 1:
                parts = [parts[0] + parts[1]] + parts[2:]
            result_lines[parts[-1]] = parts
            if result_lines["sender"] is not None and result_lines["receiver"] is not None:
                break
        elif "Interval" in line and "Transfer" in line and "Bitrate" in line:
            print(f"{datetime.now().time()} | Not all results are found in iperf output:\n{results}")
            sys.exit(1)

    if proto == "tcp":
        try:
            ret = TcpIperfMeasurements(
                parallel_threads,
                sender_speed_MBps=float(result_lines["sender"][-4] if len(result_lines["sender"]) == 9
                                        else result_lines["sender"][-3]),
                sender_retries=int(result_lines["sender"][-2] if len(result_lines["sender"]) == 9 else 0),
                receiver_speed_MBps=float(result_lines["receiver"][-3]),
            )
        except Exception as e:
            print(f"{datetime.now().time()} | Failed to parse iperf output: {e}:\n{result_lines}")
            sys.exit(1)
    else:
        def transfer_unit_to_multiplier(text: str):
            text = text.lower()
            x = 1.0 / 8 if text.endswith("bits") else 1

            if text.startswith('k'):
                return 1024 * x
            if text.startswith('m'):
                return 1024 * 1024 * x
            if text.startswith('g'):
                return 1024 * 1024 * 1024 * x
            if text.startswith('t'):
                return 1024 * 1024 * 1024 * 1024 * x

            return x

        try:
            ret = UdpIperfMeasurements(
                parallel_threads,
                sender_speed_MBps=float(result_lines["sender"][5]),
                sender_sent_bytes=float(
                    result_lines["sender"][3]) * transfer_unit_to_multiplier(result_lines["sender"][4]),
                receiver_speed_MBps=float(result_lines["receiver"][5]),
                receiver_received_bytes=float(
                    result_lines["receiver"][3]) * transfer_unit_to_multiplier(result_lines["receiver"][4]),
            )
        except Exception as e:
            print(f"{datetime.now().time()} | Failed to parse iperf output: {e}:\n{result_lines}")
            sys.exit(1)
    print(f"{datetime.now().time()} | iperf test is done")
    return ret


RESULTS = {}

socks5_proxies = []
if ARGS.proxy and ARGS.socks_ports_range:
    no_scheme = ARGS.proxy.removeprefix("socks5://").removeprefix("socks5h://")
    if len(ARGS.proxy) != len(no_scheme):
        if (']' in no_scheme and not no_scheme.endswith(']')) or ':' in no_scheme:
            print(f"{datetime.now().time()} | "
                  f"SOCKS5 proxy url must not contain port in case ports range is specified")
            sys.exit(1)

    for port in range(ARGS.socks_ports_range[0], ARGS.socks_ports_range[1] + 1):
        socks5_proxies.append(f"{ARGS.proxy}:{port}")


class ThreadReturningValue(threading.Thread):

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args):
        threading.Thread.join(self, *args)
        return self._return


if ARGS.download:
    if len(socks5_proxies) > 0:
        threads = list(map(
            lambda proxy: ThreadReturningValue(target=run_file_download_test, args=(ARGS.download, ARGS.jobs, proxy)),
            socks5_proxies))
        for t in threads:
            t.start()

        speed_samples = []
        errors = []
        for x in map(lambda x: x.join(), threads):
            speed_samples.extend([x.avg_speed_MBps] * (x.jobs_num - x.failed_num))
            errors.extend(x.errors)
        r = HttpMeasurements(jobs_num=len(socks5_proxies) * ARGS.jobs, speed_samples=speed_samples, errors=errors)
    else:
        r = run_file_download_test(ARGS.download, ARGS.jobs, ARGS.proxy)
    print(f"{datetime.now().time()} | HTTP download test results: {r}")
    RESULTS["http_download"] = r

if ARGS.upload:
    if len(socks5_proxies) > 0:
        threads = list(map(
            lambda proxy: ThreadReturningValue(target=run_file_upload_test, args=(ARGS.upload, ARGS.jobs, proxy)),
            socks5_proxies))
        for t in threads:
            t.start()

        speed_samples = []
        errors = []
        for x in map(lambda x: x.join(), threads):
            speed_samples.extend([x.avg_speed_MBps] * (x.jobs_num - x.failed_num))
            errors.extend(x.errors)
        r = HttpMeasurements(jobs_num=len(socks5_proxies) * ARGS.jobs, speed_samples=speed_samples, errors=errors)
    else:
        r = run_file_upload_test(ARGS.upload, ARGS.jobs, ARGS.proxy)
    print(f"{datetime.now().time()} | HTTP upload test results: {r}")
    RESULTS["http_upload"] = r

if ARGS.iperf:
    r = run_iperf_test(ARGS.iperf, ARGS.iperf_proto, ARGS.iperf_dir, ARGS.jobs)
    print(f"{datetime.now().time()} | iperf ({ARGS.iperf_proto}, {ARGS.iperf_dir}) test results: {r}")
    RESULTS[f"iperf_{ARGS.iperf_proto}_{ARGS.iperf_dir}"] = r

if ARGS.output:
    Path(os.path.dirname(ARGS.output)).mkdir(parents=True, exist_ok=True)
    try:
        os.remove(ARGS.output)
    except OSError:
        pass

    RESULTS = {key: val for key, val in RESULTS.items() if val is not None}
    if len(RESULTS) > 0:
        with open(ARGS.output, "w+") as f:
            json.dump(RESULTS, fp=f, indent=2, default=lambda o: o.__dict__)
