"""Bounded independent-process runtime qualification; all output stays temporary."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import urllib.request


class LatencyProxy:
    """Forward real TLS bytes with per-direction delay and one 8-KiB held chunk."""
    def __init__(self, upstream, delay_ms, maximum):
        self.upstream, self.delay, self.maximum = upstream, delay_ms / 2000, maximum
        self.ready = threading.Event()
        self.loop = None
        self.port = None
        self.tasks = set()
        self.thread = threading.Thread(target=self.run, daemon=True)

    async def connection(self, reader, writer):
        if len(self.tasks) >= self.maximum:
            writer.close()
            return
        task = asyncio.current_task()
        self.tasks.add(task)
        remote = None
        try:
            remote_reader, remote = await asyncio.open_connection("127.0.0.1", self.upstream)
            async def pump(source, target):
                while True:
                    chunk = await source.read(8192)
                    if not chunk:
                        return
                    await asyncio.sleep(self.delay)
                    target.write(chunk)
                    await target.drain()
            pumps = [asyncio.create_task(pump(reader, remote)), asyncio.create_task(pump(remote_reader, writer))]
            try:
                await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for pump_task in pumps:
                    pump_task.cancel()
                await asyncio.gather(*pumps, return_exceptions=True)
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            if remote:
                remote.close()
            self.tasks.discard(task)

    async def serve(self):
        self.loop = asyncio.get_running_loop()
        server = await asyncio.start_server(self.connection, "127.0.0.1", 0, backlog=512)
        self.port = server.sockets[0].getsockname()[1]
        self.ready.set()
        try:
            await server.serve_forever()
        finally:
            server.close()
            await server.wait_closed()
            for task in list(self.tasks):
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)

    def run(self):
        try:
            asyncio.run(self.serve())
        except asyncio.CancelledError:
            pass

    def __enter__(self):
        self.thread.start()
        if not self.ready.wait(5):
            raise RuntimeError("latency proxy startup deadline")
        return self

    def __exit__(self, *args):
        if self.loop:
            self.loop.call_soon_threadsafe(lambda: [task.cancel() for task in asyncio.all_tasks(self.loop)])
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("latency proxy cleanup deadline")


def process_sample(pid):
    try:
        status = Path(f"/proc/{pid}/status").read_text()
        rss = int(next(line.split()[1] for line in status.splitlines() if line.startswith("VmRSS:")))
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return rss, int(fields[11]) + int(fields[12])
    except (OSError, StopIteration):
        return 0, 0


def qualify(root, directory, env, args):
    binary = root / "target/release/skvoz-testbench"
    jobs = []
    streams = []
    metrics = {}
    broker_peak = {"mem": 0, "cpu": 0, "connections": 0, "slow_consumers": 0}
    start = time.monotonic()
    monitor_due = 0
    def spawn(peer):
        output = open(directory / f"worker-{peer}.log", "w")
        streams.append(output)
        safe_env = {key: value for key, value in env.items() if not key.endswith("_PASSWORD")}
        safe_env[f"SKVOZ_NATS_P{peer}_PASSWORD"] = env[f"SKVOZ_NATS_P{peer}_PASSWORD"]
        safe_env["SKVOZ_QUALIFY_DIR"] = str(directory)
        safe_env["SKVOZ_WORKER_DEADLINE_SECONDS"] = str(args.churn_rounds * (args.duration + 60) + 20)
        if peer == 1:
            safe_env["SKVOZ_SLOW_READER_DELAY_MS"] = str(args.slow_reader_delay_ms)
        job = subprocess.Popen([str(binary), "runtime-worker", str(peer), str(args.clients),
            str(args.streams_per_client), str(args.active_per_client), str(args.bytes), str(args.duration)],
            cwd=root, env=safe_env, stdout=output, stderr=subprocess.STDOUT)
        jobs.append(job)
        metrics[job.pid] = {"peer": peer, "peak_rss_kib": 0, "cpu_ticks_observed": 0}
        return job

    def sample():
        nonlocal monitor_due
        if time.monotonic() - start > args.churn_rounds * (args.duration + 60) + 20:
            raise RuntimeError("qualification controller deadline")
        total = 0
        for job in jobs:
            rss, cpu = process_sample(job.pid)
            metrics[job.pid]["peak_rss_kib"] = max(metrics[job.pid]["peak_rss_kib"], rss)
            metrics[job.pid]["cpu_ticks_observed"] = max(metrics[job.pid]["cpu_ticks_observed"], cpu)
            total += rss
        if total > args.max_app_rss_mib * 1024:
            raise RuntimeError("qualification aggregate app RSS stop limit exceeded")
        if time.monotonic() >= monitor_due:
            with urllib.request.urlopen(env["SKVOZ_NATS_MONITOR"] + "/varz", timeout=2) as response:
                broker = json.load(response)
            for key in broker_peak:
                broker_peak[key] = max(broker_peak[key], broker.get(key) or 0)
            monitor_due = time.monotonic() + 0.2
        for job in jobs:
            if job.poll() not in (None, 0):
                peer = metrics[job.pid]["peer"]
                raise RuntimeError(f"worker {peer} failed: " + (directory / f"worker-{peer}.log").read_text()[-4000:])
        time.sleep(0.025)

    results = []
    proxy = LatencyProxy(int(env["SKVOZ_NATS_URL"].rsplit(":", 1)[1]), args.delay_ms, args.clients * 3 + 32)
    try:
        with proxy:
            if args.delay_ms:
                env = env | {"SKVOZ_NATS_CLIENT_URL": f"tls://127.0.0.1:{proxy.port}"}
            server = spawn(0)
            while not (directory / "server.ready").exists():
                sample()
            for round_id in range(args.churn_rounds):
                for path in directory.glob("ready.*"):
                    path.unlink()
                (directory / "go").unlink(missing_ok=True)
                clients = [spawn(peer) for peer in range(1, args.clients + 1)]
                round_start = time.monotonic()
                while not all((directory / f"ready.{peer}").exists() for peer in range(1, args.clients + 1)):
                    if time.monotonic() - round_start > 45:
                        raise RuntimeError("independent-client ready deadline")
                    sample()
                (directory / "go").write_text("go")
                while any(job.poll() is None for job in clients):
                    sample()
                results.extend(json.loads((directory / f"result.{peer}.json").read_text()) for peer in range(1, args.clients + 1))
            (directory / "stop").write_text("stop")
            while server.poll() is None:
                sample()
            server_result = json.loads((directory / "result.0.json").read_text())
            if any(result["remaining_streams"] or result["reserved_receive_bytes"] or result["shard_failures"] for result in [server_result, *results]):
                raise RuntimeError("qualification cleanup/failure counters violated")
            latency = sorted(value for result in results for value in result["completion_us"])
            percentile = lambda q: latency[(len(latency)-1)*q//100] if latency else None
            app_server = next(value for value in metrics.values() if value["peer"] == 0)
            app_clients = [value for value in metrics.values() if value["peer"] != 0]
            payload = sum(result["payload_bytes_per_direction"] for result in results) * 2
            report = {"clients": args.clients, "streams_per_client": args.streams_per_client,
                "active_per_client": args.active_per_client, "bytes_per_direction": args.bytes,
                "hold_seconds": args.duration, "slow_reader_id": 1 if args.slow_reader_delay_ms else None,
                "slow_reader_delay_ms": args.slow_reader_delay_ms,
                "slow_reader_completion_us": [value for result in results if result["id"] == 1 for value in result["completion_us"]], "churn_rounds": args.churn_rounds,
                "transport_delay_each_direction_ms": args.delay_ms / 2,
                "delay_method": "actual TCP TLS-byte forwarding, one held 8-KiB chunk/direction; includes delay-induced chunk pacing",
                "build_profile": "release", "independent_client_processes": True,
                "elapsed_seconds": round(time.monotonic()-start, 3), "payload_bytes": payload,
                "payload_bytes_per_second_including_setup_and_hold": round(payload/(time.monotonic()-start)),
                "server_app": app_server, "client_app_peak_rss_sum_kib": sum(value["peak_rss_kib"] for value in app_clients),
                "client_app_cpu_ticks_sum_observed": sum(value["cpu_ticks_observed"] for value in app_clients),
                "cpu_clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
                "broker_only_limits": {"cpu": 1, "memory_mib": 128}, "broker_observed_peak": broker_peak,
                "completion_us": {"p50": percentile(50), "p95": percentile(95), "p99": percentile(99)},
                "remaining_streams": 0, "reserved_receive_bytes": 0,
                "sample_interval_ms": 25, "measurement_scope": "process samples, not total host usage or Core-only RSS; sum of peaks is not simultaneous RSS", "server_result": server_result}
            print("QUALIFY " + json.dumps(report, sort_keys=True), flush=True)
            (directory / "qualification.json").write_text(json.dumps(report, indent=2))
    finally:
        for job in jobs:
            if job.poll() is None:
                job.terminate()
        for job in jobs:
            try:
                job.wait(timeout=3)
            except subprocess.TimeoutExpired:
                job.kill()
                job.wait(timeout=3)
        for output in streams:
            output.close()
