#!/usr/bin/env python3
"""Run SKVOZ checks/demo with one disposable, authenticated INFO-before-TLS NATS."""

import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from qualification import qualify
from daemon_qualification import qualify as qualify_daemon
from network_qualification import qualify as qualify_network
from network_runtime_qualification import qualify as qualify_network_runtime
from tcp_capacity_qualification import qualify as qualify_tcp_capacity
from tcp_capacity_resources import qualify as qualify_tcp_resources

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "nats:2.15.0-alpine@sha256:ac8f88a6494bffc2c2a5289a0ca61cb28a9145c11ba5677cf24265d07f46d8d4"


def command(args, **kwargs):
    return subprocess.run(args, check=True, cwd=ROOT, **kwargs)


def capture(args):
    return command(args, capture_output=True, text=True).stdout.strip()


def certificates(directory):
    def openssl(*args):
        command(["openssl", *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=SKVOZ disposable test CA", "-addext", "basicConstraints=critical,CA:TRUE",
            "-keyout", str(directory / "ca.key"), "-out", str(directory / "ca.pem"))
    openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
            "-keyout", str(directory / "server.key"), "-out", str(directory / "server.csr"))
    (directory / "server.ext").write_text("basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n"
        "subjectAltName=DNS:localhost,IP:127.0.0.1\n")
    openssl("x509", "-req", "-days", "1", "-in", str(directory / "server.csr"),
            "-CA", str(directory / "ca.pem"), "-CAkey", str(directory / "ca.key"),
            "-CAcreateserial", "-extfile", str(directory / "server.ext"), "-out", str(directory / "server.pem"))
    # A second, unrelated CA is used for a real negative trust check.
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=SKVOZ wrong CA", "-addext", "basicConstraints=critical,CA:TRUE",
            "-keyout", str(directory / "wrong-ca.key"), "-out", str(directory / "wrong-ca.pem"))
    (directory / "wrong-name.ext").write_text((directory / "server.ext").read_text().replace("DNS:localhost,IP:127.0.0.1", "DNS:wrong.invalid"))
    for name, days, extension in [("wrong-name", "1", "wrong-name.ext"), ("expired", "-1", "server.ext")]:
        openssl("x509", "-req", "-days", days, "-in", str(directory / "server.csr"),
            "-CA", str(directory / "ca.pem"), "-CAkey", str(directory / "ca.key"),
            "-CAcreateserial", "-extfile", str(directory / extension), "-out", str(directory / f"{name}.pem"))
    for path in directory.iterdir():
        path.chmod(0o600)
    for name in ["ca.key", "wrong-ca.key", "server.csr", "server.ext", "wrong-name.ext", "ca.srl"]:
        (directory / name).unlink(missing_ok=True)


def ready(url, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url + "/healthz", timeout=0.5) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.05)
    raise RuntimeError("NATS readiness deadline exceeded")


def ready_inside(container, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            result = subprocess.run(["docker", "exec", container, "wget", "-q", "-O", "-", "-T", "1",
                                     "http://127.0.0.1:8222/healthz"], cwd=ROOT, capture_output=True,
                                    timeout=min(1, remaining))
            if result.returncode == 0:
                return
        except subprocess.TimeoutExpired:
            pass
        time.sleep(min(.05, max(0, deadline - time.monotonic())))
    raise RuntimeError("NATS internal readiness deadline exceeded")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["check", "demo", "load", "tcp", "qualify", "daemon", "network", "network-runtime", "tcp-capacity"], nargs="?", default="check")
    parser.add_argument("--offline", action="store_true", help="Use cached Cargo dependencies and Docker image")
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--streams-per-client", type=int, default=10)
    parser.add_argument("--active-per-client", type=int, default=2)
    parser.add_argument("--bytes", type=int, default=65536)
    parser.add_argument("--duration", type=int, default=2, help="Bounded idle/soak hold per qualification wave (seconds)")
    parser.add_argument("--delay-ms", type=int, default=0, help="Added roundtrip TCP forwarding delay for qualification clients")
    parser.add_argument("--slow-reader-delay-ms", type=int, default=0)
    parser.add_argument("--churn-rounds", type=int, default=1)
    parser.add_argument("--max-app-rss-mib", type=int, default=1024)
    parser.add_argument("--report-directory", type=Path, help="Copy non-secret network qualification evidence to this directory")
    parser.add_argument("--network-performance-only", action="store_true", help="Run matched network throughput diagnostics without functional/loss groups")
    parser.add_argument("--network-functional-only", action="store_true", help="Run network functional/security/loss groups without throughput measurements")
    parser.add_argument("--network-profile", action="store_true", help="Collect a separate bounded broker CPU profile after network measurements")
    parser.add_argument("--tcp-capacity-hold", type=int, default=2, help="TCP capacity hold with new exchanges (0..1800 seconds)")
    parser.add_argument("--tcp-capacity-cycles", type=int, default=0, help="Additional real open/close cycles (0..10000)")
    parser.add_argument("--tcp-capacity-server-resources", action="store_true", help="Separate real Ruby/NATS/runtime server container, 16 consumers ×64 idle +32 active +1 stalled")
    parser.add_argument("--tcp-capacity-benchmark", action="store_true", help="Matched useful upload/download, 1/16 streams, five 30-second runs")
    parser.add_argument("--tcp-capacity-negatives", action="store_true", help="Real slow DNS, setup cancellation, full proxy failures and same/other peer stalled isolation")
    parser.add_argument("--tcp-capacity-baseline-binary", type=Path)
    parser.add_argument("--tcp-capacity-baseline-profile", type=Path)
    args = parser.parse_args()
    if not 0 <= args.tcp_capacity_hold <= 1800 or not 0 <= args.tcp_capacity_cycles <= 10000:
        parser.error("TCP capacity parameters exceed the bounded experiment scope")
    if bool(args.tcp_capacity_baseline_binary) != bool(args.tcp_capacity_baseline_profile):
        parser.error("baseline binary and matching profile must be supplied together")
    if args.mode == "tcp-capacity" and not (1 <= args.clients <= 16
            and 1 <= args.streams_per_client <= 512
            and 0 <= args.active_per_client <= 512
            and args.clients * (args.streams_per_client + args.active_per_client) <= 2048):
        parser.error("TCP capacity requires 1..16 clients, 1..512 streams per client and at most 2048 total streams")
    if (args.mode == "tcp-capacity" and not (args.tcp_capacity_baseline_binary
            or args.tcp_capacity_benchmark or args.tcp_capacity_server_resources or args.tcp_capacity_negatives)
            and not (args.streams_per_client + 64 <= 512
                and args.clients * args.streams_per_client + 64 <= 2048
                and args.streams_per_client + args.active_per_client < 512
                and args.clients * (args.streams_per_client + args.active_per_client) < 2048)):
        parser.error("positive TCP capacity requires per-client idle +64 burst and idle +active +1 fresh <=512, total idle +64 <=2048 and total idle +active +1 <=2048")
    if args.tcp_capacity_server_resources and (args.mode != "tcp-capacity" or args.tcp_capacity_benchmark
            or args.tcp_capacity_baseline_binary or args.clients != 16 or args.streams_per_client != 64 or args.active_per_client != 2):
        parser.error("server resources require tcp-capacity mode, 16 clients ×64 idle and 2 active per client")
    if args.tcp_capacity_benchmark and (args.mode != "tcp-capacity" or args.clients != 1):
        parser.error("TCP capacity benchmark requires tcp-capacity mode with exactly one client")
    if args.tcp_capacity_negatives and (args.mode != "tcp-capacity" or args.tcp_capacity_benchmark
            or args.tcp_capacity_server_resources or args.tcp_capacity_baseline_binary):
        parser.error("TCP capacity negatives require an independent current runtime fixture")
    if (args.network_performance_only or args.network_functional_only or args.network_profile) and args.mode != "network":
        parser.error("network diagnostic options require network mode")
    if args.network_performance_only and args.network_functional_only:
        parser.error("network performance-only and functional-only are mutually exclusive")
    if args.network_functional_only and args.network_profile:
        parser.error("network-functional-only skips profiler workloads")
    if not (1 <= args.clients <= 512 and 1 <= args.streams_per_client <= 512
            and 0 <= args.active_per_client <= 512
            and args.clients * args.streams_per_client <= 65536 and 1 <= args.duration <= 60 and 0 <= args.delay_ms <= 200 and 1 <= args.churn_rounds <= 10
            and 0 <= args.slow_reader_delay_ms <= 2000
            and ((args.bytes+8191)//8192)*args.slow_reader_delay_ms <= 60000
            and 64 <= args.max_app_rss_mib <= 4096 and 0 <= args.active_per_client <= args.streams_per_client and 0 <= args.bytes <= 2*1024*1024):
        parser.error("load parameters exceed the bounded experiment scope")
    for executable in ["docker", "cargo", "openssl"] + (["ruby"] if args.mode in ("check", "daemon") else []):
        if not shutil.which(executable):
            raise RuntimeError(f"Required executable is missing: {executable}")
    command(["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cargo = ["cargo"]
    extra = ["--locked"] + (["--offline"] if args.offline else [])
    if args.mode == "tcp-capacity":
        if not args.tcp_capacity_baseline_binary:
            command(cargo + ["build", "--release", "-p", "skvoz-network", "--features", "linux-runtime", *extra])
        command(["docker", "build", "-f", "testbench/fixtures/network-runtime/Dockerfile", "-t", "skvoz-network:runtime-fixture", "."])
        if args.tcp_capacity_server_resources:
            command(["docker", "build", "--target", "tests", "-f", "connectors/server/Dockerfile", "-t", "skvoz-server:tcp-capacity-tests", "."])
            qualify_tcp_resources(ROOT, args)
        elif args.tcp_capacity_negatives:
            qualify_tcp_capacity(ROOT, args, certificates, mode='setup')
            qualify_tcp_capacity(ROOT, args, certificates, mode='overload')
            qualify_tcp_capacity(ROOT, args, certificates, mode='reduced')
        else:
            qualify_tcp_capacity(ROOT, args, certificates)
        return
    if args.mode == "network-runtime":
        command(cargo + ["build", "--release", "-p", "skvoz-network", "-p", "skvoz-network-helper",
                         "--features", "skvoz-network/linux-runtime", *extra])
        command(["docker", "build", "-f", "testbench/fixtures/network-runtime/Dockerfile",
                 "-t", "skvoz-network:runtime-fixture", "."])
        qualify_network_runtime(ROOT, args, certificates)
        return
    if args.mode == "check":
        command(cargo + ["fmt", "--all", "--", "--check"])
        # Check the portable library separately: workspace FFI enables linux-runtime.
        command(cargo + ["test", "-p", "skvoz-network", "--no-default-features", "--all-targets", *extra])
        command(cargo + ["clippy", "--workspace", "--exclude", "skvoz-ubuntu-client", "--all-targets", "--features", "skvoz-testbench/real-nats,skvoz-daemon/real-nats", *extra, "--", "-D", "warnings"])
    if args.mode in ("check", "daemon"):
        command(cargo + ["build", "--release", "-p", "skvoz-daemon", *extra])
    if args.mode == "qualify":
        command(cargo + ["build", "--release", "-p", "skvoz-testbench", *extra])
    if args.mode == "network":
        command(cargo + ["build", "--release", "-p", "skvoz-testbench", "--bin", "network_probe", *extra])
    token = secrets.token_hex(8)
    container = f"skvoz-testbench-{token}"
    user_password, consumer_password = secrets.token_hex(24), secrets.token_hex(24)
    identity_minimum = 132 if args.mode == "check" else (5 if args.mode == "network" else 2)
    mesh_passwords = [secrets.token_hex(24) for _ in range(max(identity_minimum, args.clients+1))]
    old_mask = os.umask(0o077)
    try:
        private_parent = ROOT / "temp" if args.mode == "network" else None
        if private_parent:
            private_parent.mkdir(mode=0o700, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="skvoz-nats-", dir=private_parent) as temporary:
            directory = Path(temporary)
            certificates(directory)
            (directory / "empty-trust").mkdir()
            mesh_users = []
            for peer_id, password in enumerate(mesh_passwords):
                publish = (f"skvoz.mesh.{token}.*.*.*.0.*" if peer_id == 0
                           else f"skvoz.mesh.{token}.*.0.*.{peer_id}.*")
                subscribe = f"skvoz.mesh.{token}.*.{peer_id}.*.*.*"
                runtime = f"skvoz.runtime.{token}.*"
                runtime_publish = ([f"{runtime}.join.*.0", f"{runtime}.lane.*.*.0.*.0.*"] if peer_id == 0 else
                    [f"{runtime}.join.0.{peer_id}", f"{runtime}.lane.0.*.{peer_id % 8}.*.{peer_id}.*"])
                runtime_subscribe = ([f"{runtime}.join.0.*", f"{runtime}.lane.0.*.*.*.*.*"] if peer_id == 0 else
                    [f"{runtime}.join.{peer_id}.*", f"{runtime}.lane.{peer_id}.*.*.*.*.*"])
                publish = '\", \"'.join([publish, *runtime_publish])
                subscribe = '\", \"'.join([subscribe, *runtime_subscribe])
                mesh_users.append(f'{{ user: "p{peer_id}", password: "{password}", permissions: {{ publish: ["{publish}"], subscribe: ["{subscribe}"] }} }}')
            daemon_password = secrets.token_hex(24)
            runtime = f"skvoz.runtime.{token}.ipc"
            shared_publish = '", "'.join([f"{runtime}.join.0.{peer}" for peer in (1, 2)] + [f"{runtime}.lane.0.*.{peer}.*.{peer}.*" for peer in (1, 2)])
            shared_subscribe = '", "'.join([f"{runtime}.join.{peer}.*" for peer in (1, 2)] + [f"{runtime}.lane.{peer}.*.*.*.*.*" for peer in (1, 2)])
            mesh_users.append(f'{{ user: "daemon-devices", password: "{daemon_password}", permissions: {{ publish: ["{shared_publish}"], subscribe: ["{shared_subscribe}"] }} }}')
            mesh_authorization = ",\n".join(mesh_users)
            config = f'''server_name: "skvoz-testbench"
port: 4222
http_port: 8222
max_payload: 65588
max_pending: 4MB
max_connections: {len(mesh_passwords)*3+64}
write_deadline: "2s"
tls {{
  cert_file: "/bench/server.pem"
  key_file: "/bench/server.key"
  ca_file: "/bench/ca.pem"
  handshake_first: false
  timeout: 2
}}
authorization {{
  users: [
    {{ user: "user", password: "{user_password}", permissions: {{
      publish: ["skvoz.bench.{token}.*.1.s.0.s"]
      subscribe: ["skvoz.bench.{token}.*.0.s.*.*"]
    }} }},
    {{ user: "consumer", password: "{consumer_password}", permissions: {{
      publish: ["skvoz.bench.{token}.*.0.s.1.s"]
      subscribe: ["skvoz.bench.{token}.*.1.s.*.*"]
    }} }},
    {mesh_authorization}
  ]
}}
'''
            (directory / "nats.conf").write_text(config)
            with socket.socket() as listener, socket.socket() as monitoring_listener:
                listener.bind(("127.0.0.1", 0))
                monitoring_listener.bind(("127.0.0.1", 0))
                broker_port = listener.getsockname()[1]
                fixed_monitor_port = monitoring_listener.getsockname()[1]
            created = False
            bootstrap = f"skvoz-testbench-{token}-profile"
            bootstrap_created = False
            try:
                if args.network_profile:
                    command(["docker", "network", "create", "--internal", "--label",
                             f"skvoz.testbench.run={token}", bootstrap], stdout=subprocess.DEVNULL)
                    bootstrap_created = True
                command(["docker", "create", "--name", container,
                    "--label", f"skvoz.testbench.run={token}", "--read-only", "--cap-drop=ALL",
                    "--security-opt=no-new-privileges:true", "--user", f"{os.getuid()}:{os.getgid()}",
                    "--memory=128m", "--cpus=1", *(["--network", bootstrap] if args.network_profile else []),
                    *([] if args.network_profile else ["--publish", f"127.0.0.1:{broker_port}:4222",
                        "--publish", f"127.0.0.1:{fixed_monitor_port}:8222"]),
                    "--mount", f"type=bind,source={directory},target=/bench,readonly",
                    "--pull=never" if args.offline else "--pull=missing", IMAGE, "--config", "/bench/nats.conf",
                    *(["--profile", "8223"] if args.network_profile else [])], stdout=subprocess.DEVNULL)
                created = True
                command(["docker", "start", container], stdout=subprocess.DEVNULL)
                if args.network_profile:
                    info = json.loads(capture(["docker", "inspect", container]))[0]
                    assert not info["HostConfig"]["PortBindings"], "Profile broker host ports published"
                    host = info["NetworkSettings"]["Networks"][bootstrap]["IPAddress"]
                    assert host, "Profile bootstrap IP missing"
                    port = "4222"
                    monitor = f"http://{host}:8222"
                    ready_inside(container)
                    version = json.loads(capture(["docker", "exec", container, "wget", "-q", "-O", "-", "-T", "2",
                                                   "http://127.0.0.1:8222/varz"]))["version"]
                    endpoint = "isolated internal container port4222"
                else:
                    host = "127.0.0.1"
                    port = capture(["docker", "port", container, "4222/tcp"]).rsplit(":", 1)[1]
                    monitor_port = capture(["docker", "port", container, "8222/tcp"]).rsplit(":", 1)[1]
                    monitor = f"http://127.0.0.1:{monitor_port}"
                    ready(monitor)
                    with urllib.request.urlopen(monitor + "/varz", timeout=2) as response:
                        version = json.load(response)["version"]
                    endpoint = f"loopback port {port}"
                print(f"Real NATS {version}: INFO before TLS, provisioned identities, {endpoint}", flush=True)
                env = os.environ | {
                    "SKVOZ_NATS_URL": f"tls://{host}:{port}", "SKVOZ_NATS_CA": str(directory / "ca.pem"),
                    "SKVOZ_NATS_WRONG_CA": str(directory / "wrong-ca.pem"),
                    "SKVOZ_TRUST_EMPTY_DIR": str(directory / "empty-trust"),
                    "SKVOZ_NATS_FIXTURE_DIR": str(directory),
                    "SKVOZ_NATS_USER_PASSWORD": user_password, "SKVOZ_NATS_CONSUMER_PASSWORD": consumer_password,
                    "SKVOZ_DAEMON_PASSWORD": daemon_password,
                    "SKVOZ_NATS_RUN_TOKEN": token, "SKVOZ_NATS_CONTAINER": container, "SKVOZ_NATS_MONITOR": monitor,
                    "SKVOZ_NATS_BOOTSTRAP_NETWORK": bootstrap if args.network_profile else "",
                }
                env.update({f"SKVOZ_NATS_P{peer_id}_PASSWORD": password for peer_id, password in enumerate(mesh_passwords)})
                if args.mode == "check":
                    command(cargo + ["test", "--workspace", "--exclude", "skvoz-ubuntu-client", "--all-targets", "--features", "skvoz-testbench/real-nats,skvoz-daemon/real-nats", *extra,
                        "--", "--test-threads=1", "--nocapture"], env=env)
                    # The final Rust test restarts Docker without awaiting broker readiness.
                    ready(monitor)
                    qualify_daemon(ROOT, directory, env, args)
                elif args.mode == "daemon":
                    command(cargo + ["test", "-p", "skvoz-daemon", "--features", "real-nats", *extra, "--", "--test-threads=1", "--nocapture"], env=env)
                    ready(monitor)
                    qualify_daemon(ROOT, directory, env, args)
                elif args.mode == "qualify":
                    qualify(ROOT, directory, env, args)
                elif args.mode == "network":
                    qualify_network(ROOT, directory, env, args)
                else:
                    command(cargo + ["run", "-p", "skvoz-testbench", *extra, "--", args.mode,
                        str(args.clients), str(args.streams_per_client), str(args.active_per_client), str(args.bytes)], env=env)
            except BaseException:
                if created:
                    command(["docker", "logs", "--tail=100", container])
                raise
            finally:
                try:
                    if created:
                        command(["docker", "rm", "--force", container], stdout=subprocess.DEVNULL)
                        print("Testbench container removed", flush=True)
                finally:
                    if bootstrap_created:
                        command(["docker", "network", "rm", bootstrap], stdout=subprocess.DEVNULL)
    finally:
        os.umask(old_mask)


if __name__ == "__main__":
    main()
