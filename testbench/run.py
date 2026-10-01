#!/usr/bin/env python3
"""Run SKVOZ checks/demo with one disposable, authenticated TLS-first NATS."""

import argparse
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

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
    for path in directory.iterdir():
        path.chmod(0o600)
    for name in ["ca.key", "wrong-ca.key", "server.csr", "server.ext", "ca.srl"]:
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
    parser.add_argument("mode", choices=["check", "demo"], nargs="?", default="check")
    parser.add_argument("--offline", action="store_true", help="Use cached Cargo dependencies and Docker image")
    args = parser.parse_args()
    for executable in ["docker", "cargo", "openssl"]:
        if not shutil.which(executable):
            raise RuntimeError(f"Required executable is missing: {executable}")
    command(["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cargo = ["cargo"]
    extra = ["--locked"] + (["--offline"] if args.offline else [])
    if args.mode == "check":
        command(cargo + ["fmt", "--all", "--", "--check"])
        command(cargo + ["clippy", "--workspace", "--all-targets", "--features", "skvoz-testbench/real-nats", *extra, "--", "-D", "warnings"])
    token = secrets.token_hex(8)
    container = f"skvoz-testbench-{token}"
    user_password, consumer_password = secrets.token_hex(24), secrets.token_hex(24)
    old_mask = os.umask(0o077)
    try:
        with tempfile.TemporaryDirectory(prefix="skvoz-nats-") as temporary:
            directory = Path(temporary)
            certificates(directory)
            config = f'''server_name: "skvoz-testbench"
port: 4222
http_port: 8222
max_payload: 65564
max_pending: 4MB
max_connections: 16
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
      publish: ["skvoz.bench.{token}.*.consumer"]
      subscribe: ["skvoz.bench.{token}.*.user"]
    }} }},
    {{ user: "consumer", password: "{consumer_password}", permissions: {{
      publish: ["skvoz.bench.{token}.*.user"]
      subscribe: ["skvoz.bench.{token}.*.consumer"]
    }} }}
  ]
}}
'''
            (directory / "nats.conf").write_text(config)
            created = False
            try:
                command(["docker", "create", "--name", container,
                    "--label", f"skvoz.testbench.run={token}", "--read-only", "--cap-drop=ALL",
                    "--security-opt=no-new-privileges:true", "--user", f"{os.getuid()}:{os.getgid()}",
                    "--memory=128m", "--cpus=1", "--publish", "127.0.0.1::4222",
                    "--publish", "127.0.0.1::8222", "--mount", f"type=bind,source={directory},target=/bench,readonly",
                    "--pull=never" if args.offline else "--pull=missing", IMAGE, "--config", "/bench/nats.conf"], stdout=subprocess.DEVNULL)
                created = True
                command(["docker", "start", container], stdout=subprocess.DEVNULL)
                port = capture(["docker", "port", container, "4222/tcp"]).rsplit(":", 1)[1]
                monitor_port = capture(["docker", "port", container, "8222/tcp"]).rsplit(":", 1)[1]
                monitor = f"http://127.0.0.1:{monitor_port}"
                ready(monitor)
                with urllib.request.urlopen(monitor + "/varz", timeout=2) as response:
                    version = json.load(response)["version"]
                print(f"Real NATS {version}: TLS-first, two roles, loopback port {port}", flush=True)
                env = os.environ | {
                    "SKVOZ_NATS_URL": f"tls://127.0.0.1:{port}", "SKVOZ_NATS_CA": str(directory / "ca.pem"),
                    "SKVOZ_NATS_WRONG_CA": str(directory / "wrong-ca.pem"),
                    "SKVOZ_NATS_USER_PASSWORD": user_password, "SKVOZ_NATS_CONSUMER_PASSWORD": consumer_password,
                    "SKVOZ_NATS_RUN_TOKEN": token, "SKVOZ_NATS_CONTAINER": container, "SKVOZ_NATS_MONITOR": monitor,
                }
                if args.mode == "check":
                    command(cargo + ["test", "--workspace", "--all-targets", "--features", "skvoz-testbench/real-nats", *extra,
                        "--", "--test-threads=1", "--nocapture"], env=env)
                else:
                    command(cargo + ["run", "-p", "skvoz-testbench", *extra], env=env)
            except BaseException:
                if created:
                    command(["docker", "logs", "--tail=100", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                raise
            finally:
                if created:
                    command(["docker", "rm", "--force", container], stdout=subprocess.DEVNULL)
                    print("Testbench container removed", flush=True)
    finally:
        os.umask(old_mask)


if __name__ == "__main__":
    main()
