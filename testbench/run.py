#!/usr/bin/env python3
"""Run SKVOZ checks/demo with one disposable, authenticated TLS-first NATS."""

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["check", "demo", "load", "tcp", "qualify", "daemon"], nargs="?", default="check")
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
    args = parser.parse_args()
    if not (1 <= args.clients <= 512 and 1 <= args.streams_per_client <= 512
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
    if args.mode == "check":
        command(cargo + ["fmt", "--all", "--", "--check"])
        command(cargo + ["clippy", "--workspace", "--all-targets", "--features", "skvoz-testbench/real-nats,skvoz-daemon/real-nats", *extra, "--", "-D", "warnings"])
    if args.mode in ("check", "daemon"):
        command(cargo + ["build", "--release", "-p", "skvoz-daemon", *extra])
    if args.mode == "qualify":
        command(cargo + ["build", "--release", "-p", "skvoz-testbench", *extra])
    token = secrets.token_hex(8)
    container = f"skvoz-testbench-{token}"
    user_password, consumer_password = secrets.token_hex(24), secrets.token_hex(24)
    mesh_passwords = [secrets.token_hex(24) for _ in range(max(132 if args.mode == "check" else 2, args.clients+1))]
    old_mask = os.umask(0o077)
    try:
        with tempfile.TemporaryDirectory(prefix="skvoz-nats-") as temporary:
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
  handshake_first: true
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
            try:
                command(["docker", "create", "--name", container,
                    "--label", f"skvoz.testbench.run={token}", "--read-only", "--cap-drop=ALL",
                    "--security-opt=no-new-privileges:true", "--user", f"{os.getuid()}:{os.getgid()}",
                    "--memory=128m", "--cpus=1", "--publish", f"127.0.0.1:{broker_port}:4222",
                    "--publish", f"127.0.0.1:{fixed_monitor_port}:8222", "--mount", f"type=bind,source={directory},target=/bench,readonly",
                    "--pull=never" if args.offline else "--pull=missing", IMAGE, "--config", "/bench/nats.conf"], stdout=subprocess.DEVNULL)
                created = True
                command(["docker", "start", container], stdout=subprocess.DEVNULL)
                port = capture(["docker", "port", container, "4222/tcp"]).rsplit(":", 1)[1]
                monitor_port = capture(["docker", "port", container, "8222/tcp"]).rsplit(":", 1)[1]
                monitor = f"http://127.0.0.1:{monitor_port}"
                ready(monitor)
                with urllib.request.urlopen(monitor + "/varz", timeout=2) as response:
                    version = json.load(response)["version"]
                print(f"Real NATS {version}: TLS-first, provisioned identities, loopback port {port}", flush=True)
                env = os.environ | {
                    "SKVOZ_NATS_URL": f"tls://127.0.0.1:{port}", "SKVOZ_NATS_CA": str(directory / "ca.pem"),
                    "SKVOZ_NATS_WRONG_CA": str(directory / "wrong-ca.pem"),
                    "SKVOZ_TRUST_EMPTY_DIR": str(directory / "empty-trust"),
                    "SKVOZ_NATS_FIXTURE_DIR": str(directory),
                    "SKVOZ_NATS_USER_PASSWORD": user_password, "SKVOZ_NATS_CONSUMER_PASSWORD": consumer_password,
                    "SKVOZ_DAEMON_PASSWORD": daemon_password,
                    "SKVOZ_NATS_RUN_TOKEN": token, "SKVOZ_NATS_CONTAINER": container, "SKVOZ_NATS_MONITOR": monitor,
                }
                env.update({f"SKVOZ_NATS_P{peer_id}_PASSWORD": password for peer_id, password in enumerate(mesh_passwords)})
                if args.mode == "check":
                    command(cargo + ["test", "--workspace", "--all-targets", "--features", "skvoz-testbench/real-nats,skvoz-daemon/real-nats", *extra,
                        "--", "--test-threads=1", "--nocapture"], env=env)
                    qualify_daemon(ROOT, directory, env, args)
                elif args.mode == "daemon":
                    command(cargo + ["test", "-p", "skvoz-daemon", "--features", "real-nats", *extra, "--", "--test-threads=1", "--nocapture"], env=env)
                    qualify_daemon(ROOT, directory, env, args)
                elif args.mode == "qualify":
                    qualify(ROOT, directory, env, args)
                else:
                    command(cargo + ["run", "-p", "skvoz-testbench", *extra, "--", args.mode,
                        str(args.clients), str(args.streams_per_client), str(args.active_per_client), str(args.bytes)], env=env)
            except BaseException:
                if created:
                    command(["docker", "logs", "--tail=100", container])
                raise
            finally:
                if created:
                    command(["docker", "rm", "--force", container], stdout=subprocess.DEVNULL)
                    print("Testbench container removed", flush=True)
    finally:
        os.umask(old_mask)


if __name__ == "__main__":
    main()
