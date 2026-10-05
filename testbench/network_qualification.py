"""Disposable Linux TUN/Core/NATS feasibility qualification, called by run.py."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import re
from pathlib import Path
import secrets
import shutil
import stat
import subprocess
import sys
import time

IMAGE = "skvoz-network:e1-fixture"
PROFILE_FETCH_SCRIPT = '''import sys,urllib.request
with urllib.request.urlopen(sys.argv[1],timeout=30) as response:
 raw=response.read(16777217)
 assert len(raw)<=16777216,'CPU profile exceeds bound'
 with open('/bench/nats-cpu.pprof','wb') as profile: profile.write(raw)
print(len(raw))'''


def create_profile(directory):
    profile = directory / "nats-cpu.pprof"
    with profile.open("xb"):
        pass
    profile.chmod(0o600)
    info = profile.lstat()
    assert stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid(), "Profile must be host-owned regular file"
    assert stat.S_IMODE(info.st_mode) == 0o600 and info.st_size == 0, "Profile precreation permissions/size"
    return profile


def verify_profile(profile):
    info = profile.lstat()
    assert stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid(), "Profile ownership changed"
    assert stat.S_IMODE(info.st_mode) == 0o600, "Profile permissions changed"
    assert 0 < info.st_size <= 16777216, "Profile size outside bound"


def export_evidence(directory, destination, report):
    failures = []
    report_path = directory / "network-report.json"
    try:
        destination.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Preserve all measurements before attempting any optional profile/log.
        shutil.copy2(report_path, destination / report_path.name)
    except OSError as error:
        failures.append({"file": report_path.name, "error": repr(error)})
    for path in sorted(directory.iterdir()):
        if (path.suffix == ".log" or path.name.endswith("-snapshot.txt")
                or path.name in ("nats-cpu.pprof", "nats-cpu-flat.txt", "nats-cpu-cum.txt")):
            try:
                shutil.copy2(path, destination / path.name)
            except OSError as error:
                failures.append({"file": path.name, "error": repr(error)})
    if failures:
        report["export_failures"] = failures
        print("NETWORK EVIDENCE EXPORT FAILURES " + json.dumps(failures), file=sys.stderr, flush=True)
        try:
            report_path.write_text(json.dumps(report, indent=2))
            shutil.copy2(report_path, destination / report_path.name)
        except OSError as error:
            print("NETWORK REPORT EXPORT FAILURE " + repr(error), file=sys.stderr, flush=True)
    return failures


class Stand:
    def __init__(self, root, directory, env):
        self.root, self.directory, self.env = root, directory, env
        self.token = secrets.token_hex(4)
        self.prefix = "skvoz-network-" + self.token
        self.containers, self.networks = [], []
        self.broker = env["SKVOZ_NATS_CONTAINER"]
        self.commands = []
        self.results = []
        self.subnet = int(self.token[:2], 16)
        self.underlay = f"198.18.{self.subnet}"
        self.egress = f"198.19.{self.subnet}"
        self.v6 = f"2001:db8:{int(self.token[:4], 16):x}:{int(self.token[4:], 16):x}"
        self.target4, self.target6 = self.egress + ".30", self.v6 + "::30"
        self.roles = {}

    def run(self, *args, check=True, timeout=45, input=None, env=None):
        self.commands.append(list(args))
        result = subprocess.run(args, cwd=self.root, text=True, capture_output=True,
                                input=input, timeout=timeout, env=env)
        if check and result.returncode:
            raise RuntimeError(f"Command failed: {args!r}\n{result.stdout}\n{result.stderr}")
        return result

    def execute(self, role, *args, **options):
        interactive = ["--interactive"] if options.get("input") is not None else []
        return self.run("docker", "exec", *interactive, self.roles[role], *args, **options)

    def launch(self, role, command):
        return self.run("docker", "exec", "--detach", self.roles[role], "sh", "-c", command)

    def network(self, suffix, *args):
        name = self.prefix + "-" + suffix
        self.run("docker", "network", "create", "--internal", "--label",
                 "skvoz.testbench.network=" + self.token, *args, name)
        self.networks.append(name)
        return name

    def container(self, role, network, address, v6=None, forwarding=False):
        name = self.prefix + "-" + role
        args = ["docker", "run", "--detach", "--name", name, "--label",
                "skvoz.testbench.network=" + self.token, "--pull=never", "--network", network,
                "--ip", address, "--cap-drop=ALL", "--cap-add=NET_ADMIN", "--cap-add=NET_RAW",
                "--cap-add=DAC_OVERRIDE",
                "--device=/dev/net/tun", "--memory=1g", "--cpus=2", "--pids-limit=128",
                "--sysctl=net.ipv4.ipfrag_high_thresh=4194304", "--sysctl=net.ipv4.ipfrag_time=15",
                "--sysctl=net.ipv6.ip6frag_high_thresh=4194304", "--sysctl=net.ipv6.ip6frag_time=15",
                "--mount", f"type=bind,source={self.directory},target=/bench",
                "--mount", f"type=bind,source={self.directory / 'network_probe'},target=/probe,readonly"]
        if v6:
            args += ["--ip6", v6]
        if forwarding:
            args += ["--sysctl=net.ipv4.ip_forward=1", "--sysctl=net.ipv6.conf.all.forwarding=1"]
        self.run(*args, IMAGE)
        self.containers.append(name)
        self.roles[role] = name
        return name

    def wait(self, predicate, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.05)
        raise TimeoutError("Network testbench readiness deadline")

    def log(self, role):
        path = self.directory / (role + ".log")
        return path.read_text() if path.exists() else ""

    def status(self, role):
        for line in reversed(self.log(role).splitlines()):
            if line.startswith("STATUS "):
                return json.loads(line[7:])
        return {}

    def setup(self):
        underlay = self.network("underlay", "--subnet", self.underlay + ".0/24")
        egress = self.network("egress", "--ipv6", "--subnet", self.egress + ".0/24",
                              "--subnet", self.v6 + "::/64")
        self.run("docker", "network", "connect", "--ip", self.underlay + ".2", underlay, self.broker)
        self.container("server", underlay, self.underlay + ".20", forwarding=True)
        self.run("docker", "network", "connect", "--ip", self.egress + ".20", "--ip6",
                 self.v6 + "::20", egress, self.roles["server"])
        self.container("client", underlay, self.underlay + ".10")
        self.container("other", underlay, self.underlay + ".11")
        self.container("target", egress, self.target4, self.target6)
        self.execute("target", "ip", "-6", "route", "add", "2001:db8:100::/64", "via", self.v6 + "::20")
        self.execute("target", "ip", "route", "add", "192.0.2.0/24", "via", self.egress + ".20")
        self.launch("target", "exec python3 /traffic.py server >/bench/target.log 2>&1")
        self.wait(lambda: "TARGET READY" in self.log("target"))
        for role in ("client", "other"):
            self.execute(role, "ip", "route", "del", "default", check=False)
        # A real consumer must fail before TUN/routing admission.
        for address in (self.target4, self.target6):
            result = self.execute("client", "ping", "-c", "1", "-W", "1", address, check=False)
            assert result.returncode != 0, "Direct client-to-target path exists"
        self.results.append({"group": "no-direct-path", "status": "passed"})
        if not self.performance_only:
            for address in (self.target4, self.target6):
                self.workload("direct-tcp-" + address, "server", "tcp", address, "--bytes", "1048576")
                self.workload("direct-udp-" + address, "server", "udp", address, "--bytes", "64",
                              "--samples", "100", "--interval", ".005")
        profile = {
            "url": "tls://" + self.underlay + ".2:4222", "tls_server_name": "localhost",
            "ca_file": "/bench/ca.pem", "namespace": "skvoz.runtime." + self.env["SKVOZ_NATS_RUN_TOKEN"] + ".network",
            "mtu": 1500, "interface": "skvoztun0", "ready_file": "/bench/client-ready",
            "grants": {str(peer): self.session_config(peer) for peer in (1, 2, 3, 4)},
        }
        profile["grants"]["3"]["families"] = [4]
        profile["grants"]["3"]["source_grants"] = ["192.0.2.12/32"]
        profile["grants"]["3"]["routes"] = [self.egress + ".0/24"]
        profile["grants"]["3"]["egress"]["ipv6"] = "none"
        profile["grants"]["4"]["packet_queue_records"] = 1
        self.profile = profile
        for role, peer in (("server", 0), ("client", 1), ("other", 2)):
            config = profile | {"peer": peer, "password": self.env[f"SKVOZ_NATS_P{peer}_PASSWORD"],
                                "ready_file": f"/bench/{role}-ready"}
            path = self.directory / (role + ".json")
            path.write_text(json.dumps(config))
            path.chmod(0o600)
            self.launch(role, f"exec /probe /bench/{role}.json >/bench/{role}.log 2>&1")
            self.wait(lambda role=role: "TUN READY" in self.log(role))
        self.configure_tun()

    def session_config(self, peer):
        return {"session": "0" * 32, "families": [4, 6],
                "source_grants": [f"192.0.2.{9+peer}/32", f"2001:db8:100::{9+peer}/128"],
                "routes": [self.egress + ".0/24", self.v6 + "::/64"],
                "dns_servers": [self.target4], "mtu": 1500, "channels": 1,
                "packet_queue_bytes": 262144, "packet_queue_records": 256,
                "setup_timeout_ms": 15000, "egress": {"ipv4": "nat44", "ipv6": "routed"}}

    def configure_tun(self):
        for role in ("server", "client", "other"):
            self.execute(role, "ip", "link", "set", "skvoztun0", "up")
        for role, peer in (("client", 1), ("other", 2)):
            self.execute(role, "ip", "addr", "add", f"192.0.2.{9+peer}/32", "dev", "skvoztun0")
            self.execute(role, "ip", "-6", "addr", "add", f"2001:db8:100::{9+peer}/128", "dev", "skvoztun0", "nodad")
            self.execute(role, "ip", "route", "add", self.egress + ".0/24", "dev", "skvoztun0")
            self.execute(role, "ip", "-6", "route", "add", self.v6 + "::/64", "dev", "skvoztun0")
            guard = f'''table inet capture {{
set direct_baseline_targets {{ type ipv4_addr; }}
chain output {{ type filter hook output priority 0; policy accept;
ip daddr @direct_baseline_targets oifname "eth0" counter accept
ip daddr {self.egress}.0/24 oifname != "skvoztun0" counter drop
ip6 daddr {self.v6}::/64 oifname != "skvoztun0" counter drop
}}
}}'''
            self.execute(role, "nft", "-f", "-", input=guard)
            assert "drop" in self.execute(role, "nft", "list", "table", "inet", "capture").stdout
        for peer in (1, 2, 4):
            self.execute("server", "ip", "route", "add", f"192.0.2.{9+peer}/32", "dev", "skvoztun0", "mtu", "1500")
            self.execute("server", "ip", "-6", "route", "add", f"2001:db8:100::{9+peer}/128", "dev", "skvoztun0", "mtu", "1500")
        firewall = f'''table inet skvoz {{
set direct_baseline_sources {{ type ipv4_addr; }}
set adversary_sources {{ type ipv4_addr; }}
chain forward {{ type filter hook forward priority 0; policy drop;
ct state established,related counter accept
iifname "eth0" ip saddr @direct_baseline_sources ip daddr {self.egress}.0/24 counter accept
iifname "skvoztun0" ip saddr @adversary_sources ip daddr {self.target4} counter accept
iifname "skvoztun0" ip saddr {{ 192.0.2.10, 192.0.2.11, 192.0.2.13 }} ip daddr {self.egress}.0/24 counter accept
iifname "skvoztun0" ip6 saddr {{ 2001:db8:100::10, 2001:db8:100::11, 2001:db8:100::13 }} ip6 daddr {self.v6}::/64 counter accept
counter drop
}}
}}
table ip skvoz_nat {{ chain postrouting {{ type nat hook postrouting priority srcnat; policy accept;
ip saddr 192.0.2.0/24 ip protocol {{ tcp, udp, icmp }} ip daddr {self.egress}.0/24 masquerade
ip saddr {self.underlay}.10 ip daddr {self.egress}.0/24 masquerade
}}
}}'''
        self.execute("server", "nft", "-f", "-", input=firewall)
        assert "masquerade" in self.execute("server", "nft", "list", "table", "ip", "skvoz_nat").stdout
        for role in ("client", "other"):
            (self.directory / (role + "-ready")).touch(mode=0o600)
            self.wait(lambda role=role: "SESSION ACTIVE" in self.log(role))

    def workload(self, group, role, *args):
        result = self.execute(role, "python3", "/traffic.py", *args)
        value = json.loads(result.stdout)
        self.results.append({"group": group, "status": "passed", "measurement": value})
        print("NETWORK " + group + " " + json.dumps(value), flush=True)
        return value

    def scenarios(self):
        for address in (self.target4, self.target6):
            suffix = "ipv6" if ":" in address else "ipv4"
            self.workload("tcp-" + suffix, "client", "tcp", address, "--bytes", "1048576")
            self.workload("udp-" + suffix, "client", "udp", address, "--bytes", "1200")
            self.workload("fragment-udp-" + suffix, "client", "udp", address, "--bytes", "60000")
            self.execute("client", "ping", "-c", "3", "-W", "2", address)
            self.results.append({"group": "icmp-" + suffix, "status": "passed"})
            self.workload("protocol143-" + suffix, "client", "raw", address)
            self.workload("peer2-tcp-" + suffix, "other", "tcp", address, "--bytes", "16384")
        observed = [json.loads(line.removeprefix("OBSERVED ")) for line in self.log("target").splitlines()
                    if line.startswith("OBSERVED ")]
        for protocol in ("tcp", "udp", "icmp"):
            for family, source in ((4, self.egress + ".20"), (6, "2001:db8:100::10")):
                assert {"protocol": protocol, "family": family, "source": source} in observed, "Target egress source mismatch"
        for family, source in ((4, "192.0.2.10"), (6, "2001:db8:100::10")):
            assert {"protocol": 143, "family": family, "source": source} in observed, "Generic protocol routed source mismatch"
        self.results.append({"group": "target-observed-nat44-routed-ipv6-protocol143", "status": "passed", "observations": observed})
        previous_forbidden = self.status("client").get("forbidden", 0)
        self.workload("source-spoof-attempt", "client", "spoof", self.target4, "--source", "192.0.2.11")
        self.wait(lambda: self.status("client").get("forbidden", 0) > previous_forbidden, timeout=3)
        assert "666f7262696464656e2d736f75726365" not in self.log("target"), "Spoof reached target"
        self.workload("healthy-after-spoof", "other", "tcp", self.target4, "--bytes", "16384")
        self.results.append({"group": "cross-peer-source-isolation", "status": "passed",
                             "scope": "Client NetworkEngine enqueue guard; malicious server ingress is a separate raw-Core group"})
        self.execute("client", "ip", "route", "add", "192.0.2.11/32", "dev", "skvoztun0")
        peer_attempt = self.execute("client", "ping", "-c", "1", "-W", "1", "192.0.2.11", check=False)
        assert peer_attempt.returncode != 0, "Client-to-client forwarding bypassed fixture ACL"
        self.results.append({"group": "client-to-client-destination-drop", "status": "passed"})
        # Router MTU decrease must produce real PMTU feedback, with smaller traffic progressing.
        self.execute("server", "ip", "link", "set", "eth1", "mtu", "1280")
        self.execute("target", "ip", "link", "set", "eth0", "mtu", "1280")
        for address in (self.target4, self.target6):
            oversized = self.execute("client", "ping", "-c", "2", "-W", "2", "-M", "do", "-s", "1400", address, check=False)
            assert oversized.returncode != 0, "PMTU oversized packet unexpectedly succeeded"
            assert any(word in oversized.stdout + oversized.stderr for word in ("mtu", "MTU", "too big", "Frag needed")), "Missing PMTU feedback"
            self.execute("client", "ping", "-c", "2", "-W", "2", "-s", "1200", address)
            self.results.append({"group": "pmtu-" + address, "status": "passed"})
        self.execute("server", "ip", "link", "set", "eth1", "mtu", "1500")
        self.execute("target", "ip", "link", "set", "eth0", "mtu", "1500")
        if not self.functional_only:
            self.initial_performance()
            self.duration_performance()
        if self.profile_enabled:
            self.profile_broker()
        self.remote_adversary()
        self.tiny_queue()
        self.missing_family()
        self.resources()
        self.broker_loss()

    def initial_performance(self):
        # Matched target interfaces: RTT is imposed in these namespaces only.
        for role, interface in (("server", "eth1"), ("target", "eth0")):
            self.execute(role, "tc", "qdisc", "replace", "dev", interface, "root", "netem", "delay", "500us")
        count = "8388608"
        self.workload("rtt1ms-ip-warmup", "client", "tcp", self.target4, "--bytes", count)
        direct, tunneled = [], []
        self.set_direct(True)
        try:
            self.workload("rtt1ms-direct-warmup", "client", "tcp", self.target4, "--bytes", count)
            for sample in range(3):
                direct.append(self.workload(f"rtt1ms-direct-{sample}", "client", "tcp", self.target4, "--bytes", count))
            baseline = self.workload("rtt1ms-direct-interactive", "client", "udp", self.target4,
                                     "--bytes", "64", "--samples", "200", "--interval", ".005")
        finally:
            self.set_direct(False)
        for sample in range(3):
            tunneled.append(self.workload(f"rtt1ms-ip-{sample}", "client", "tcp", self.target4, "--bytes", count))
        direct_median = sorted(item["mbit_s"] for item in direct)[1]
        ip_median = sorted(item["mbit_s"] for item in tunneled)[1]
        capacity = min(item["mbit_s"] for item in tunneled)
        offered = capacity * .6
        with ThreadPoolExecutor(max_workers=2) as pool:
            bulk = pool.submit(self.workload, "rtt1ms-paced-bulk", "client", "tcp", self.target4,
                               "--bytes", count, "--rate-mbit", str(offered))
            interactive = self.workload("rtt1ms-interactive-udp", "other", "udp", self.target4,
                                         "--bytes", "64", "--samples", "200", "--interval", ".005")
            bulk.result()
        measurement = {"direct_median_mbit_s": direct_median, "ip_median_mbit_s": ip_median,
                       "ratio": ip_median / direct_median, "offered_mbit_s": offered,
                       "added_p95_ms": interactive["p95_ms"] - baseline["p95_ms"],
                       "added_p99_ms": interactive["p99_ms"] - baseline["p99_ms"],
                       "additional_loss_percent": (interactive["lost"] - baseline["lost"]) / 2}
        self.results.append({"group": "initial-throughput-hol", "status": "diagnostic", "measurement": measurement,
                             "initial_thresholds_met": ip_median >= 25 and ip_median / direct_median >= .5
                             and measurement["added_p95_ms"] <= 20 and measurement["added_p99_ms"] <= 50
                             and measurement["additional_loss_percent"] <= .5,
                             "scope": "3x8MiB TCP echo; 200 UDP samples; shorter than E6 qualification"})

    def performance_telemetry(self):
        containers = {}
        for role in ("server", "client", "target", "broker"):
            container = self.broker if role == "broker" else self.roles[role]
            raw = self.run("docker", "exec", container, "sh", "-c",
                           "cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/cpu.stat").stdout.splitlines()
            cpu = dict(line.split() for line in raw[2:])
            containers[role] = {"memory_bytes": int(raw[0]), "lifetime_peak_memory_bytes": int(raw[1]),
                                "cpu_usage_usec": int(cpu["usage_usec"])}
        return {"containers": containers, "threads": self.thread_cpu(), "broker": self.broker_monitor(),
                "probe_status": {role: self.status(role) for role in ("server", "client")},
                "status_scope": "Last once-per-second probe sample; cgroup memory peak is since container creation"}

    def thread_cpu(self):
        script = '''import json,os,pathlib
result=[]
for process in pathlib.Path('/proc').iterdir():
 if not process.name.isdigit() or int(process.name)==os.getpid(): continue
 try:
  name=process.joinpath('comm').read_text().strip()
  if name not in ('probe','python3'): continue
  for task in process.joinpath('task').iterdir():
   fields=task.joinpath('stat').read_text().rsplit(')',1)[1].split()
   result.append({'pid':int(process.name),'tid':int(task.name),'name':name,
                  'user_ticks':int(fields[11]),'system_ticks':int(fields[12]),
                  'start_ticks':int(fields[19]),'processor':int(fields[36])})
 except (FileNotFoundError,ProcessLookupError): pass
print(json.dumps({'clock_ticks_per_second':os.sysconf('SC_CLK_TCK'),'tasks':result}))'''
        result = {role: json.loads(self.execute(role, "python3", "-c", script).stdout)
                  for role in ("server", "client", "target")}
        lines = self.run("docker", "exec", self.broker, "sh", "-c",
                         'for task in /proc/1/task/[0-9]*; do cat "$task/stat"; done').stdout.splitlines()
        result["broker"] = {"clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "tasks": []}
        for line in lines:
            fields = line.rsplit(")", 1)[1].split()
            result["broker"]["tasks"].append({"pid": 1, "tid": int(line.split("(", 1)[0]),
                "name": "nats-server", "user_ticks": int(fields[11]), "system_ticks": int(fields[12]),
                "start_ticks": int(fields[19]), "processor": int(fields[36])})
        return result

    def broker_monitor(self):
        script = '''import json,sys,urllib.request
result={}
for endpoint in ('varz','connz'):
 with urllib.request.urlopen(sys.argv[1]+'/'+endpoint,timeout=2) as response:
  raw=response.read(1048577)
  assert len(raw)<=1048576,'Monitoring reply too large'
  value=json.loads(raw)
 if endpoint=='varz':
  result['server']={key:value[key] for key in ('version','go','cpu','mem','connections','in_msgs','out_msgs',
     'in_bytes','out_bytes','slow_consumers','max_payload','max_pending','tls_required') if key in value}
 else:
  connections=value.get('connections',[])
  assert len(connections)<=64,'Connection diagnostic bound'
  result['connections']=[{key:c[key] for key in ('cid','in_msgs','out_msgs','in_bytes','out_bytes',
     'pending_bytes','subscriptions','tls_version','tls_cipher_suite') if key in c} for c in connections]
print(json.dumps(result))'''
        return json.loads(self.execute("client", "python3", "-c", script,
                                       "http://" + self.underlay + ".2:8222").stdout)

    def offload_inventory(self):
        inventory = {}
        for role in ("server", "client", "other", "target"):
            links = json.loads(self.execute(role, "ip", "-j", "link").stdout)
            inventory[role] = {link["ifname"]: self.execute(role, "ethtool", "-k", link["ifname"], check=False).stdout
                               for link in links}
        self.results.append({"group": "offload-inventory", "status": "diagnostic", "measurement": inventory,
                             "scope": "Read-only inventory; no NIC/TUN offload settings changed"})

    def duration_sample(self, group, seconds):
        before = self.performance_telemetry()
        value = self.workload(group, "client", "tcp-duration", self.target4, "--seconds", str(seconds))
        after = self.performance_telemetry()
        self.results[-1]["telemetry"] = {"before": before, "after": after,
            "cpu_usage_usec_delta": {role: after["containers"][role]["cpu_usage_usec"] -
                                    before["containers"][role]["cpu_usage_usec"] for role in before["containers"]}}
        return value

    def profile_broker(self):
        assert not self.run("docker", "port", self.broker, "8223/tcp", check=False).stdout.strip(), "Profiler published to host"
        before = self.performance_telemetry()
        profile = create_profile(self.directory)
        with ThreadPoolExecutor(max_workers=2) as pool:
            bulk = pool.submit(self.workload, "profile-observed-ip-load", "client", "tcp-duration", self.target4,
                               "--seconds", "25")
            self.execute("client", "python3", "-c", PROFILE_FETCH_SCRIPT,
                         "http://" + self.underlay + ".2:8223/debug/pprof/profile?seconds=20", timeout=35)
            bulk.result()
        after = self.performance_telemetry()
        verify_profile(profile)
        go = shutil.which("go") or "/usr/local/go/bin/go"
        go_cache = self.directory / "go-cache"
        go_cache.mkdir(mode=0o700)
        go_env = os.environ | {"GOCACHE": str(go_cache), "GOTOOLCHAIN": "local"}
        for order, flags in (("flat", []), ("cum", ["-cum"])):
            result = self.run(go, "tool", "pprof", "-top", "-nodecount=40", *flags, str(profile), timeout=30, env=go_env)
            (self.directory / ("nats-cpu-" + order + ".txt")).write_text(result.stdout + result.stderr)
        self.results.append({"group": "broker-cpu-profile", "status": "diagnostic",
                             "profile_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
                             "seconds": 20, "before": before, "after": after,
                             "scope": "Separate25s IP workload with20s observer CPU profile; not gate throughput samples"})

    def core_capacity(self):
        before = self.performance_telemetry()
        namespace = self.profile["namespace"] + "-core"
        for role, peer in (("server", 0), ("client", 3)):
            config = self.profile | {"namespace": namespace, "peer": peer,
                                     "mode": "core-echo-" + role, "seconds": 10, "warmup_seconds": 5,
                                     "password": self.env[f"SKVOZ_NATS_P{peer}_PASSWORD"],
                                     "stop_file": "/bench/core-echo.stop"}
            path = self.directory / ("core-" + role + ".json")
            path.write_text(json.dumps(config))
            path.chmod(0o600)
        self.launch("server", "exec /probe /bench/core-server.json >/bench/core-server.log 2>&1")
        try:
            self.wait(lambda: "CORE ECHO READY" in self.log("core-server"))
            result = self.execute("client", "/probe", "/bench/core-client.json", timeout=45)
            (self.directory / "core-client.log").write_text(result.stdout + result.stderr)
            records = [json.loads(line[len("CORE ECHO "):]) for line in result.stdout.splitlines()
                       if line.startswith("CORE ECHO {")]
            assert len(records) == 1 and records[0]["verified_pattern"] and records[0]["verified_offsets"]
            self.wait(lambda: "CORE ECHO STOPPED" in self.log("core-server"))
            after = self.performance_telemetry()
            self.results.append({"group": "generic-core-capacity", "status": "diagnostic", "measurement": records[0],
                                 "before": before, "after": after,
                                 "scope": "5s warmup plus10s rawCore echo;16KiB DATA,64KiB credit; same containers/broker/caps; separate namespace; no TUN or target/netem leg"})
        finally:
            (self.directory / "core-echo.stop").touch(mode=0o600)


    def duration_performance(self):
        # Equal-duration diagnostic: same endpoints/routes, no baseline rate cap.
        direct, tunneled = [], []
        self.set_direct(True)
        try:
            self.duration_sample("duration-direct-warmup", 5)
            for sample in range(3):
                direct.append(self.duration_sample(f"duration-direct-{sample}", 10))
        finally:
            self.set_direct(False)
        self.duration_sample("duration-ip-warmup", 5)
        for sample in range(3):
            tunneled.append(self.duration_sample(f"duration-ip-{sample}", 10))
        direct_median = sorted(item["mbit_s"] for item in direct)[1]
        ip_median = sorted(item["mbit_s"] for item in tunneled)[1]
        self.results.append({"group": "duration-throughput", "status": "diagnostic",
                             "measurement": {"direct_median_mbit_s": direct_median, "ip_median_mbit_s": ip_median,
                                             "ratio": ip_median / direct_median},
                             "throughput_thresholds_met": ip_median >= 25 and ip_median / direct_median >= .5,
                             "scope": "5s warmup plus 3x10s per path; bounded ordered TCP upload/echo; not full E6"})

    def set_direct(self, enabled):
        if enabled:
            self.execute("server", "nft", "add", "element", "inet", "skvoz", "direct_baseline_sources",
                         "{", self.underlay + ".10", "}")
            self.execute("client", "nft", "add", "element", "inet", "capture", "direct_baseline_targets",
                         "{", self.target4, "}")
            self.execute("client", "ip", "route", "replace", self.egress + ".0/24", "via", self.underlay + ".20", "dev", "eth0")
            route = self.execute("client", "ip", "route", "get", self.target4).stdout
            assert "eth0" in route and self.underlay + ".20" in route, "Direct fixture route unavailable"
        else:
            self.execute("client", "ip", "route", "replace", self.egress + ".0/24", "dev", "skvoztun0")
            self.execute("client", "nft", "flush", "set", "inet", "capture", "direct_baseline_targets")
            self.execute("server", "nft", "flush", "set", "inet", "skvoz", "direct_baseline_sources")
            assert "skvoztun0" in self.execute("client", "ip", "route", "get", self.target4).stdout
            for role, table, name in (("client", "capture", "direct_baseline_targets"),
                                      ("server", "skvoz", "direct_baseline_sources")):
                assert "elements =" not in self.execute(role, "nft", "list", "set", "inet", table, name).stdout

    def tiny_queue(self):
        self.container("tiny", self.prefix + "-underlay", self.underlay + ".13")
        config = self.profile | {"peer": 4, "password": self.env["SKVOZ_NATS_P4_PASSWORD"],
                                 "ready_file": "/bench/tiny-ready"}
        path = self.directory / "tiny.json"
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        self.launch("tiny", "exec /probe /bench/tiny.json >/bench/tiny.log 2>&1")
        self.wait(lambda: "TUN READY" in self.log("tiny"))
        self.execute("tiny", "ip", "link", "set", "skvoztun0", "up")
        self.execute("tiny", "ip", "addr", "add", "192.0.2.13/32", "dev", "skvoztun0")
        self.execute("tiny", "ip", "-6", "addr", "add", "2001:db8:100::13/128", "dev", "skvoztun0", "nodad")
        self.execute("tiny", "ip", "route", "add", self.egress + ".0/24", "dev", "skvoztun0")
        (self.directory / "tiny-ready").touch(mode=0o600)
        self.wait(lambda: "SESSION ACTIVE" in self.log("tiny"))
        with ThreadPoolExecutor(max_workers=2) as pool:
            burst = pool.submit(self.workload, "tiny-queue-burst", "tiny", "burst", self.target4, "--samples", "1000")
            self.workload("healthy-peer-during-tiny-burst", "other", "udp", self.target4,
                          "--bytes", "64", "--samples", "100", "--interval", ".005")
            burst.result()
        self.wait(lambda: any("packet_drops: " in line and "packet_drops: 0," not in line for line in self.log("tiny").splitlines()))
        last = self.log("tiny").splitlines()[-1]
        assert "queued_packet_records: 0" in last and "received_packet_records: 0" in last
        self.results.append({"group": "tiny-queue-isolation-ledgers", "status": "passed",
                             "packet_queue_records": 1, "packet_queue_bytes": 262144})

    def remote_adversary(self):
        # Existing spoof group covers client enqueue policy. This actor bypasses
        # that policy and supplies real wire2 DATA to the server over raw Core.
        self.container("adversary", self.prefix + "-underlay", self.underlay + ".12")
        victim = re.search(r"SESSION ACTIVE peer=1 id=([0-9a-f]{32})", self.log("server"))
        assert victim, "Victim session absent"
        previous = self.status("server")
        marker = "adversary-" + self.token
        config = self.profile | {"peer": 3, "password": self.env["SKVOZ_NATS_P3_PASSWORD"],
                                 "mode": "adversary", "foreign_session": victim.group(1),
                                 "target": self.target4, "marker": marker}
        path = self.directory / "adversary.json"
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        self.execute("server", "ip", "route", "add", "192.0.2.12/32", "dev", "skvoztun0", "mtu", "1500")
        self.execute("server", "nft", "add", "element", "inet", "skvoz", "adversary_sources", "{", "192.0.2.12", "}")
        route = self.execute("server", "ip", "route", "get", "192.0.2.12").stdout
        assert "skvoztun0" in route, "Adversary grant return route not installed"
        assert "192.0.2.12" in self.execute("server", "nft", "list", "set", "inet", "skvoz", "adversary_sources").stdout
        try:
            self.launch("adversary", "exec /probe /bench/adversary.json >/bench/adversary.log 2>&1")
            with ThreadPoolExecutor(max_workers=2) as pool:
                healthy = pool.submit(self.workload, "victim-during-raw-core-attacks", "client", "udp", self.target4,
                                      "--bytes", "64", "--samples", "100", "--interval", ".02")
                self.wait(lambda: "ADVERSARY STOPPED" in self.log("adversary"), timeout=35)
                assert healthy.result()["lost"] == 0, "Victim lost traffic during malicious Core input"
            log = self.log("adversary")
            assert log.count("ADVERSARY REJECTED") == 3 and "ADVERSARY GOOD" in log
            target = self.log("target")
            assert (marker + "-good").encode().hex() in target, "Own good marker did not reach kernel target"
            for name in ("foreign", "checksum", "length"):
                assert (marker + "-" + name).encode().hex() not in target, "Invalid packet reached kernel target"
            def count(status, name):
                found = re.search(r"\b" + name + r": (\d+)", status.get("counters", ""))
                return int(found.group(1)) if found else 0
            self.wait(lambda: count(self.status("server"), "packet_drops") >= count(previous, "packet_drops") + 4
                      and count(self.status("server"), "invalid_packets") >= count(previous, "invalid_packets") + 3)
            after = self.status("server")
            assert "overflows: 0" in after["core"], "Core overflow during sequential malicious records"
            self.workload("victim-after-raw-core-attacks", "client", "tcp", self.target4, "--bytes", "16384")
            self.results.append({"group": "server-raw-core-malicious-ip-and-binding", "status": "passed",
                                 "scope": "IPv4 only; three forbidden channel opens, foreign source and three malformed-IP records; later own good raw echo",
                                 "before": previous, "after": after})
        finally:
            self.run("docker", "rm", "--force", self.roles["adversary"], check=False)
            self.containers.remove(self.roles["adversary"])
            self.execute("server", "ip", "route", "del", "192.0.2.12/32", check=False)
            self.execute("server", "nft", "flush", "set", "inet", "skvoz", "adversary_sources")

    def missing_family(self):
        network = self.prefix + "-underlay"
        self.container("negative", network, self.underlay + ".12")
        config = self.profile | {"peer": 3, "password": self.env["SKVOZ_NATS_P3_PASSWORD"],
                                 "ready_file": "/bench/negative-ready"}
        path = self.directory / "negative.json"
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        (self.directory / "negative-ready").touch(mode=0o600)
        self.launch("negative", "exec /probe /bench/negative.json >/bench/negative.log 2>&1")
        self.wait(lambda: "closed_sessions: 1" in self.log("negative"), timeout=20)
        assert "SESSION ACTIVE" not in self.log("negative"), "Unsupported family silently downgraded"
        self.workload("healthy-after-family-reject", "client", "tcp", self.target4, "--bytes", "16384")
        self.results.append({"group": "missing-family-rejected", "status": "passed"})

    def resources(self):
        measurements = {}
        script = '''import json,pathlib
result=[]
for path in pathlib.Path('/proc').iterdir():
 if not path.name.isdigit(): continue
 try:
  if path.joinpath('comm').read_text().strip() != 'probe': continue
  status=path.joinpath('status').read_text()
  rss=int(next(line.split()[1] for line in status.splitlines() if line.startswith('VmRSS:')))
  fields=path.joinpath('stat').read_text().split()
  result.append({'rss_kib':rss,'fds':len(list(path.joinpath('fd').iterdir())),
                 'cpu_ticks':int(fields[13])+int(fields[14])})
 except (FileNotFoundError,ProcessLookupError): pass
print(json.dumps(result))'''
        for role in ("server", "client", "other", "tiny"):
            observed = json.loads(self.execute(role, "python3", "-c", script).stdout)
            assert len(observed) == 1, "Probe process observation failed"
            limit = 512 * 1024 if role == "server" else 128 * 1024
            assert observed[0]["rss_kib"] <= limit, "Runtime RSS gate exceeded"
            measurements[role] = observed[0]
        self.results.append({"group": "bounded-runtime-rss", "status": "passed", "measurement": measurements})
        self.snapshot()

    def broker_loss(self):
        started = time.monotonic()
        self.run("docker", "stop", "--time=0", self.broker)
        def retired(role):
            lines = self.log(role).splitlines()
            return any((line.startswith("STATUS ") and json.loads(line[7:])["sessions"] == 0)
                       or (line.startswith("RUNTIME FAILURE ") and "sessions: 0" in line) for line in lines[-3:])
        self.wait(lambda: all(retired(role) for role in ("server", "client", "other")), timeout=20)
        elapsed = time.monotonic() - started
        for role in ("client", "other"):
            result = self.execute(role, "ping", "-c", "1", "-W", "1", self.target4, check=False)
            assert result.returncode != 0, "Captured traffic escaped after broker loss"
            guard = self.execute(role, "nft", "list", "table", "inet", "capture").stdout
            assert "drop" in guard, "Client guard disappeared after accidental loss"
        self.results.append({"group": "broker-loss-retirement-guard", "status": "passed", "seconds": elapsed})
        # Explicit fixture stop closes TUN and zeroes engine ownership; helper policy is an E3 gate.
        for role in ("server", "client", "other", "negative", "tiny"):
            (self.directory / (role + "-ready.stop")).touch(mode=0o600)
            self.wait(lambda role=role: "STOPPED resources=EngineResources { sessions: 0, streams: 0, queued_packet_bytes: 0, queued_packet_records: 0, received_packet_bytes: 0, received_packet_records: 0, control_bytes: 0, parser_bytes: 0 }" in self.log(role), timeout=15)
            result = self.execute(role, "ip", "link", "show", "skvoztun0", check=False)
            assert result.returncode != 0, "Native TUN descriptor/interface remained after stop"
        self.results.append({"group": "runtime-zero-ledgers-tun-cleanup", "status": "passed"})

    def snapshot(self):
        for role in ("server", "client", "other", "target"):
            info = self.execute(role, "sh", "-c", "ip -j addr; ip -j route; ip -j -6 route; nft list ruleset; cat /proc/meminfo", check=False)
            (self.directory / (role + "-snapshot.txt")).write_text(info.stdout + info.stderr)
        return self.run("docker", "stats", "--no-stream", "--format", "{{json .}}", *self.containers, self.broker).stdout

    def cleanup(self):
        for container in reversed(self.containers):
            self.run("docker", "rm", "--force", container, check=False)
        for network in reversed(self.networks):
            self.run("docker", "network", "disconnect", "--force", network, self.broker, check=False)
            self.run("docker", "network", "rm", network, check=False)


def qualify(root, directory, env, args):
    probe = directory / "network_probe"
    shutil.copy2(root / "target/release/network_probe", probe)
    probe.chmod(0o700)
    source_paths = ["Cargo.toml", "Cargo.lock", "core/Cargo.toml", "core/src/runtime.rs", "core/src/nats.rs",
                    "network/Cargo.toml", "network/native/Cargo.toml", "testbench/Cargo.toml", "testbench/run.py",
                    "testbench/network_qualification.py", "testbench/src/bin/network_probe.rs",
                    "testbench/fixtures/network/Dockerfile", "testbench/fixtures/network/traffic.py"]
    source_paths += [str(path.relative_to(root)) for component in ("core/src", "network/src", "network/native/src")
                     for path in sorted((root / component).rglob("*.rs"))]
    source_paths += [str(path.relative_to(root)) for path in sorted((root / "testbench/src/bin/network_probe").rglob("*.rs"))]
    artifacts = {"probe_sha256": hashlib.sha256(probe.read_bytes()).hexdigest(),
                 "probe_bytes": probe.stat().st_size,
                 "source_sha256": {path: hashlib.sha256((root / path).read_bytes()).hexdigest() for path in source_paths}}
    fixture = root / "testbench/fixtures/network"
    subprocess.run(["docker", "build", "--pull=false", "-t", IMAGE, str(fixture)], cwd=root, check=True)
    stand = Stand(root, directory, env)
    stand.performance_only = args.network_performance_only
    stand.functional_only = args.network_functional_only
    stand.profile_enabled = args.network_profile
    report = {"stage": "E1", "status": "failed", "limits": "Initial feasibility; E6 gates remain pending",
              "mode": "performance-only" if args.network_performance_only else "functional-only" if args.network_functional_only else "full-feasibility",
              "functional_qualification": "not_run" if args.network_performance_only else "pending",
              "performance_qualification": "not_run" if args.network_functional_only else "diagnostic",
              "artifacts": artifacts,
              "environment": {"kernel": os.uname().release, "machine": os.uname().machine,
                              "runtime": "release Rust/Core/NATS", "tun": "Linux IFF_TUN|IFF_NO_PI, no offloads",
                              "test_container": "2 CPUs / 1GiB each, root NET_ADMIN/NET_RAW/DAC_OVERRIDE",
                              "broker_container": "1 CPU / 128MiB, verified TLS INFO-before-TLS",
                              "transport_profile": {"subscription_frames": 32,"join_frames": 32,"client_frames": 16,"shards": 8},
                              "transport_bound": "Server reserves8 lanes; clients allocate1 lane. Core status uses conservative8-lane bound."}}
    primary_error = None
    try:
        stand.setup()
        if args.network_profile:
            bootstrap = env["SKVOZ_NATS_BOOTSTRAP_NETWORK"]
            assert bootstrap, "Profiler requires isolated bootstrap network"
            info = json.loads(stand.run("docker", "network", "inspect", bootstrap).stdout)[0]
            assert info["Internal"], "Profiler bootstrap is not internal"
            broker = json.loads(stand.run("docker", "inspect", stand.broker).stdout)[0]
            assert set(broker["NetworkSettings"]["Networks"]) == {bootstrap, stand.prefix + "-underlay"}, "Unexpected broker network"
            assert not broker["HostConfig"]["PortBindings"], "Profile broker host ports published"
            report["environment"]["broker_profile_network"] = {"bootstrap": bootstrap, "internal": True,
                                                               "port": 8223, "host_published": False}
        stand.offload_inventory()
        if args.network_performance_only:
            stand.initial_performance()
            stand.duration_performance()
            stand.core_capacity()
            if args.network_profile:
                stand.profile_broker()
        else:
            stand.scenarios()
            report["functional_qualification"] = "passed"
        report["resources"] = stand.snapshot()
        report["status"] = "partial"
    except BaseException as error:
        primary_error = error
        if not args.network_performance_only:
            report["functional_qualification"] = "failed"
        report["failure"] = repr(error)
        raise
    finally:
        report["groups"] = stand.results
        report["commands"] = stand.commands
        (directory / "network-report.json").write_text(json.dumps(report, indent=2))
        try:
            failures = export_evidence(directory, args.report_directory, report) if args.report_directory else []
        finally:
            stand.cleanup()
        if failures and primary_error is None:
            raise RuntimeError("Network evidence export failed; measurements preserved in network-report.json")
