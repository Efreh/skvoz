"""Independent Linux daemon / Python / Ruby IPC qualification with real TLS NATS."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import select
import secrets
import signal
import socket
import struct
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'clients' / 'python'))
from skvoz_ipc import Client, encode, INCOMING, DATA, OPENED, CLOSED, REJECTED, REMOTE_FINISHED


def wait_until(function, seconds=15):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        result = function()
        if result:
            return result
        time.sleep(.01)
    raise TimeoutError('daemon qualification deadline')


def service(path):
    client = Client(path, acceptor=True)
    streams = {}
    print('SERVICE READY', flush=True)
    while True:
        for handle, state in list(streams.items()):
            pending = state['pending']
            if state['defer'] and not state['fin']:
                continue
            if pending:
                code, n, _, _ = client.request(5, handle, bytes(pending[:1024]))
                if code in (4, 9):
                    del streams[handle]
                    continue
                assert code in (0, 1, 7), ('service send', code)
                if n:
                    del pending[:n]
                    if state['echo']:
                        state['consumed'] += n
                        client.consume(handle, state['consumed'])
            if not pending and state['fin']:
                code = client.request(7, handle)[0]
                assert code in (0, 4, 7, 9), ('service finish', code)
                state['fin'] = False
        if not client.events and not select.select([client.socket], [], [], .005)[0]:
            continue
        kind, _, handle, payload = client.event()
        if kind == INCOMING:
            metadata = payload[8:]
            if metadata == b'reject':
                assert client.request(4, handle, b'destination rejected')[0] == 0
            else:
                assert client.request(3, handle)[0] == 0
                burst = metadata == b'burst'
                streams[handle] = {'pending': bytearray(bytes(range(256)) * 64 if metadata == b'credit' else b''),
                                   'consumed': 0, 'echo': metadata not in (b'burst', b'credit'), 'burst': burst, 'fire': metadata == b'fire', 'defer': metadata == b'after-fin', 'fin': False}
        elif kind == DATA:
            if handle in streams:
                state = streams[handle]
                if state['fire']:
                    for other in streams.values():
                        if other['burst']:
                            other['pending'].extend(bytes(range(256)) * 32)
                    client.consume(handle, len(payload) - 8)
                else:
                    state['pending'].extend(payload[8:])
                assert len(state['pending']) <= 8192, 'foreign host buffer exceeded receive-window budget'
        elif kind == REMOTE_FINISHED:
            if handle in streams:
                streams[handle]['fin'] = True
        elif kind == CLOSED:
            streams.pop(handle, None)


class Daemon:
    def __init__(self, binary, directory, env, peer, limits=None, overrides=None):
        self.binary = binary
        self.directory = directory
        directory.mkdir(mode=0o700)
        self.path = directory / 'core.sock'
        profile = {'ipc_path': str(self.path), 'url': env['SKVOZ_NATS_URL'],
                   'trust': 'managed_ca', 'ca_file': env['SKVOZ_NATS_CA'],
                   'username': 'p0' if peer == 0 else 'daemon-devices',
                   'password': env['SKVOZ_NATS_P0_PASSWORD'] if peer == 0 else env['SKVOZ_DAEMON_PASSWORD'],
                   'namespace': 'skvoz.runtime.' + env['SKVOZ_NATS_RUN_TOKEN'] + '.ipc',
                   'peer_id': peer, 'allowed_peers': [1, 2] if peer == 0 else [0],
                   'initiate': [] if peer == 0 else [0], 'limits': limits or {}}
        profile.update(overrides or {})
        self.profile = directory / 'profile.json'
        self.profile.write_text(json.dumps(profile))
        self.profile.chmod(0o600)
        self.log = open(directory / 'daemon.log', 'w+')
        self.process = subprocess.Popen([str(binary), '--config', str(self.profile)], stdout=self.log, stderr=self.log)
        def started():
            if self.process.poll() is not None:
                self.log.flush()
                raise RuntimeError('daemon startup failed: ' + (directory / 'daemon.log').read_text())
            return 'READY ipc=1' in (directory / 'daemon.log').read_text()
        wait_until(started)
        assert self.path.stat().st_mode & 0o777 == 0o600

    def stop(self, kill=False):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            self.process.wait(timeout=15)
        if not kill:
            assert self.process.returncode == 0
            assert not self.path.exists(), 'normal endpoint cleanup failed'
        self.log.close()


def ready_client(path):
    c = Client(str(path))
    wait_until(lambda: c.request(11, payload=struct.pack('!Q', 0))[3] == b'\1')
    return c


def open_stream(client, metadata=b'echo'):
    code, _, handle, _ = client.request(2, payload=struct.pack('!Q', 0) + metadata)
    assert code == 0, ('open failed', code)
    wait_until(lambda: _opened(client, handle))
    return handle


def _opened(client, handle):
    kind, _, key, _ = client.event()
    if key != handle:
        assert kind != DATA, 'unexpected retained DATA'
        return False
    if kind == REJECTED:
        raise RuntimeError('stream rejected')
    return kind == OPENED


def exchange(client, message, half_close=True):
    handle = open_stream(client)
    pending = memoryview(message)
    output = bytearray()
    finished = False
    end = time.monotonic() + 15
    while time.monotonic() < end:
        if pending:
            code, n, _, _ = client.request(5, handle, bytes(pending[:65536]))
            assert code in (0, 1), ('send failed', code)
            assert n <= len(pending)
            pending = pending[n:]
        elif half_close and not finished:
            assert client.request(7, handle)[0] == 0
            finished = True
        if client.events or select.select([client.socket], [], [], .005)[0]:
            kind, _, key, payload = client.event()
            assert key == handle
            if kind == DATA:
                offset, = struct.unpack('!Q', payload[:8])
                assert offset == len(output)
                output.extend(payload[8:])
                client.consume(handle, len(output))
            elif kind in (REMOTE_FINISHED, CLOSED):
                assert bytes(output) == message
                return handle
        if not half_close and bytes(output) == message:
            return handle
    raise TimeoutError('exchange deadline')


def assert_clean(client):
    wait_until(lambda: client.status()[5:8] == (0, 0, 0))


def failed_start(binary, directory, profile, overrides=None, mode='--config', permissions=0o600, expected=2):
    directory.mkdir(mode=0o700)
    data = json.loads(profile.read_text())
    data.update(overrides or {})
    data['ipc_path'] = (overrides or {}).get('ipc_path', str(directory / 'core.sock'))
    path = directory / 'profile.json'
    path.write_text(json.dumps(data))
    path.chmod(permissions)
    result = subprocess.run([str(binary), mode, str(path)], capture_output=True, timeout=8)
    assert result.returncode == expected, ('negative startup', directory.name, result.returncode, result.stderr)
    assert data['password'].encode() not in result.stdout + result.stderr
    assert data['url'].encode() not in result.stdout + result.stderr
    return result


def holder(path):
    client = ready_client(path)
    exchange(client, b'holder-live', half_close=False)
    print('HOLDER READY', flush=True)
    time.sleep(60)


def qualify(root, directory, env, args):
    binary = root / 'target' / 'release' / 'skvoz-core-daemon'
    daemons = []
    clients = []
    service_process = None
    report = {'protocol': 1, 'platform': sys.platform, 'checks': [], 'daemon_peaks_kib': {}}
    try:
        server = Daemon(binary, directory / 'ipc-server', env, 0, {'streams_per_owner': 128})
        daemons.append(server)
        service_log = open(directory / 'service.log', 'w+')
        service_process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--service', str(server.path)], stdout=service_log, stderr=service_log)
        wait_until(lambda: 'SERVICE READY' in (directory / 'service.log').read_text())
        one = Daemon(binary, directory / 'ipc-one', env, 1, {'output_bytes': 1024 * 1024, 'output_frames': 2048, 'streams_per_owner': 128, 'owners': 2})
        two = Daemon(binary, directory / 'ipc-two', env, 2)
        daemons.extend([one, two])
        a, b = ready_client(one.path), ready_client(one.path)
        clients.extend([a, b])
        message = bytes(range(256)) * 256
        handle = exchange(a, message, half_close=False)
        for kind, payload in [(5, b'bad'), (6, struct.pack('!Q', 1)), (8, b'')]:
            assert b.request(kind, handle, payload)[0] == 4, 'cross-owner operation accepted'
        assert a.request(6, handle, struct.pack('!Q', len(message) + 1))[0] == 8
        assert a.request(8, handle)[0] == 0
        while a.event()[0] != CLOSED:
            pass
        assert a.request(5, handle, b'old')[0] == 4
        report['checks'].append('owner/stale handle and excessive consume')
        try:
            Client(str(one.path))
            raise AssertionError('owner admission accepted third connection')
        except (EOFError, ConnectionError):
            pass
        report['checks'].append('bounded owner admission')
        exchange(a, bytes(range(256)) * 128)
        report['checks'].append('binary exact bytes + half-close reverse reply')
        deferred = open_stream(a, b'after-fin')
        deferred_bytes = bytes(range(256)) * 16
        rest = deferred_bytes
        while rest:
            code, n, _, _ = a.request(5, deferred, rest)
            assert code == 0 and 0 < n < len(deferred_bytes)
            rest = rest[n:]
        end = time.monotonic() + .1
        while time.monotonic() < end:
            if a.events or select.select([a.socket], [], [], .01)[0]:
                assert a.event()[0] != DATA, 'reply sent before FIN'
        assert a.request(7, deferred)[0] == 0
        after_fin_reply = bytearray()
        while True:
            kind, _, key, payload = a.event()
            assert key == deferred
            if kind == DATA:
                after_fin_reply.extend(payload[8:])
                a.consume(key, len(after_fin_reply))
            if kind in (REMOTE_FINISHED, CLOSED):
                break
        assert bytes(after_fin_reply) == deferred_bytes
        report['checks'].append('reverse reply generated ONLY after local FIN')
        assert a.request(5, open_stream(a), b'')[0] == 0
        a.close()
        clients.remove(a)
        wait_until(lambda: b.status()[5:8] == (0, 0, 0))
        report['checks'].append('active owner EOF releases reservations')
        # A client cannot guess consumption before DATA has been fully written.
        slow = ready_client(one.path)
        clients.append(slow)
        handles = [open_stream(slow, b'burst') for _ in range(64)]
        fire = open_stream(slow, b'fire')
        assert slow.request(5, fire, b'trigger')[0] == 0
        time.sleep(.5)
        last = handles[-1]
        guessed = slow.request(6, last, struct.pack('!Q', 8192))[0]
        assert guessed == 8, ('premature consume accepted', guessed)
        assert slow.status()[7] == 0  # synthetic peer output is bounded by receive credit
        slow.close()
        clients.remove(slow)
        assert_clean(b)
        report['checks'].append('queued IPC DATA delivery watermark + owner cleanup')
        status = Path('/proc/%d/status' % one.process.pid).read_text()
        report['daemon_peaks_kib'][one.directory.name] = int(next(l for l in status.splitlines() if l.startswith('VmHWM:')).split()[1])
        print('CHECKPOINT watermark: client=' + repr(b.status()), flush=True)
        credit = open_stream(b, b'credit')
        collected = bytearray()
        while len(collected) < 8192:
            kind, _, key, payload = b.event()
            if kind == DATA:
                assert key == credit
                collected.extend(payload[8:])
        end = time.monotonic() + .15
        while time.monotonic() < end:
            if b.events or select.select([b.socket], [], [], .01)[0]:
                assert b.event()[0] != DATA, 'credit returned without consumption'
        b.consume(credit, 4096)
        while len(collected) < 12288:
            kind, _, key, payload = b.event()
            if kind == DATA:
                assert key == credit
                collected.extend(payload[8:])
        b.consume(credit, 12288)
        while len(collected) < 16384:
            kind, _, key, payload = b.event()
            if kind == DATA:
                assert key == credit
                collected.extend(payload[8:])
        assert bytes(collected) == bytes(range(256)) * 64
        b.consume(credit, 16384)
        assert b.request(8, credit)[0] == 0
        assert_clean(b)
        report['checks'].append('credit stalls at8192; partial prefix consumption resumes exact bytes')
        # Independent Ruby standard-library process uses the same protocol and login.
        ruby = subprocess.run(['ruby', str(root / 'clients/ruby/echo.rb'), '--socket', str(two.path), '--peer', '0'], input=message, capture_output=True, timeout=20)
        assert ruby.returncode == 0 and ruby.stdout == message, ('Ruby echo', ruby.returncode, len(ruby.stdout), next((i for i,(a,b) in enumerate(zip(ruby.stdout,message)) if a!=b),None), ruby.stderr)
        python = subprocess.run([sys.executable, str(root / 'clients/python/echo.py'), '--socket', str(two.path), '--peer', '0'], input=message, capture_output=True, timeout=20)
        assert python.returncode == 0 and python.stdout == message, ('Python echo', python.returncode, python.stderr)
        for language, command in [('Ruby', ['ruby', str(root / 'clients/ruby/echo.rb')]), ('Python', [sys.executable, str(root / 'clients/python/echo.py')])]:
            empty = subprocess.run(command + ['--socket', str(two.path), '--peer', '0'], input=b'', capture_output=True, timeout=20)
            assert empty.returncode == 0 and empty.stdout == b'', (language, 'zero-byte echo', empty.stderr)
        with ThreadPoolExecutor(max_workers=2) as pool:
            commands = [['ruby', str(root / 'clients/ruby/echo.rb')], [sys.executable, str(root / 'clients/python/echo.py')]]
            futures = [pool.submit(subprocess.run, command + ['--socket', str(two.path), '--peer', '0'], input=message, capture_output=True, timeout=20) for command in commands]
            for future in futures:
                parallel = future.result()
                assert parallel.returncode == 0 and parallel.stdout == message, ('concurrent foreign streams', parallel.stderr)
        report['checks'].append('independent Ruby/Python zero and65536 exact-byte plus concurrent stdlib clients')
        # Restart peer1 while peer2 stays established, sharing one NATS login.
        c2 = ready_client(two.path)
        clients.append(c2)
        retained = exchange(c2, b'healthy-before', half_close=False)
        one.stop()
        daemons.remove(one)
        restarted = Daemon(binary, directory / 'ipc-one-restart', env, 1)
        daemons.append(restarted)
        assert c2.request(5, retained, b'healthy-after')[0] == 0
        reply = bytearray()
        while len(reply) < len(b'healthy-after'):
            kind, _, key, payload = c2.event()
            if kind == DATA:
                assert key == retained
                reply.extend(payload[8:])
                c2.consume(key, 14 + len(reply))
        assert bytes(reply) == b'healthy-after'
        assert c2.request(10, payload=struct.pack('!Q', 0))[0] == 0
        extra_owner = ready_client(two.path)
        clients.append(extra_owner)
        assert extra_owner.request(10, payload=struct.pack('!Q', 0))[0] == 0
        assert c2.request(5, retained, b'join-retained')[0] == 0
        while True:
            kind, _, key, payload = c2.event()
            if kind == DATA:
                assert key == retained and payload[8:] == b'join-retained'
                c2.consume(key, 14 + len(b'healthy-after') + len(b'join-retained'))
                break
        assert c2.request(8, retained)[0] == 0
        report['checks'].append('other-owner ensure JOIN preserves same-peer established stream')
        newcomer = ready_client(restarted.path)
        clients.append(newcomer)
        assert newcomer.request(5, handle, b'stale restart')[0] == 4
        exchange(newcomer, b'new-generation')
        report['checks'].append('same login explicit IDs + isolated daemon restart')
        fragmented = ready_client(restarted.path)
        clients.append(fragmented)
        fragmented.sequence += 1
        frame = encode(2, fragmented.sequence, payload=struct.pack('!Q', 0) + b'echo')
        for byte in frame:
            fragmented.socket.sendall(bytes([byte]))
        kind, request, frag_handle, payload = fragmented.read()
        assert kind == 0x8000 and request == fragmented.sequence and struct.unpack('!H', payload[:2])[0] == 0
        wait_until(lambda: _opened(fragmented, frag_handle))
        assert fragmented.request(8, frag_handle)[0] == 0
        report['checks'].append('byte-fragmented OPEN across real daemon IPC/NATS')
        # Rejection and malformed IPC affect local connection, then healthy traffic.
        rejected = ready_client(restarted.path)
        clients.append(rejected)
        code, _, reject_handle, _ = rejected.request(2, payload=struct.pack('!Q', 0) + b'reject')
        assert code == 0
        assert wait_until(lambda: rejected.event()[0] == REJECTED)
        raw = socket.socket(socket.AF_UNIX)
        raw.settimeout(2)
        raw.connect(str(restarted.path)); raw.sendall(struct.pack('!I', 0xffffffff))
        assert raw.recv(1) == b''
        raw.close()
        exchange(newcomer, b'healthy-after-malformed')
        report['checks'].append('host rejection/malformed oversize isolation')
        # Safe startup/credential failures reserve no runtime identity or endpoint.
        fifo_dir = directory / 'fifo-profile'
        fifo_dir.mkdir(mode=0o700)
        fifo = fifo_dir / 'profile.json'
        os.mkfifo(fifo, mode=0o600)
        fifo_result = subprocess.run([str(binary), '--check-config', str(fifo)], capture_output=True, timeout=2)
        assert fifo_result.returncode == 2 and b'unsafe profile file' in fifo_result.stderr
        report['checks'].append('FIFO profile rejected before blocking open')
        failed_start(binary, directory / 'bad-mode', restarted.profile, mode='--check-config', permissions=0o644)
        failed_start(binary, directory / 'bad-path', restarted.profile, {'ipc_path': 'relative.sock'}, mode='--check-config')
        failed_start(binary, directory / 'occupied-live', restarted.profile, {'ipc_path': str(restarted.path)})
        wrong = failed_start(binary, directory / 'wrong-auth', restarted.profile, {'password': secrets.token_hex(24)}, expected=3)
        assert wrong.stderr.strip() in (b'authentication failed', b'authorization failed'), wrong.stderr
        print('NEGATIVE auth: ' + wrong.stderr.decode().strip(), flush=True)
        tls = failed_start(binary, directory / 'wrong-ca', restarted.profile, {'ca_file': env['SKVOZ_NATS_WRONG_CA']}, expected=3)
        assert b'TLS failed' in tls.stderr
        failed_start(binary, directory / 'queue142', restarted.profile, {'limits': {'output_bytes': 142}}, mode='--check-config')
        control = failed_start(binary, directory / 'queue143', restarted.profile, {'limits': {'output_bytes': 143}}, mode='--check-config', expected=0)
        assert b'PROFILE valid' in control.stdout
        try:
            Client(str(server.path), acceptor=True)
            raise AssertionError('second acceptor lease accepted')
        except RuntimeError as error:
            assert 'code 5' in str(error)
        exchange(newcomer, b'healthy-after-negative-starts')
        report['checks'].append('private config/occupied endpoint/auth/TLS/mandatory143-byte response/acceptor lease')
        # A tiny/stalled IPC owner's streams cancel; another owner on same peer survives.
        restarted.stop()
        daemons.remove(restarted)
        tiny = Daemon(binary, directory / 'ipc-tiny', env, 1, {'output_bytes': 4096, 'output_frames': 4, 'streams_per_owner': 65, 'ipc_timeout_ms': 500})
        daemons.append(tiny)
        healthy = ready_client(tiny.path)
        bad = ready_client(tiny.path)
        clients.extend([healthy, bad])
        live = exchange(healthy, b'healthy-tiny-before', half_close=False)
        bad_handles = [open_stream(bad, b'burst') for _ in range(64)]
        fire = open_stream(bad, b'fire')
        assert bad.request(2, payload=struct.pack('!Q', 0) + b'overflow')[0] == 5
        assert bad.request(5, fire, b'trigger')[0] == 0
        wait_until(lambda: healthy.status()[1] == 1 and healthy.status()[5:8] == (1, 8192, 0))
        assert healthy.request(5, live, b'healthy-tiny-after')[0] == 0
        while True:
            kind, _, key, payload = healthy.event()
            if kind == DATA:
                assert key == live and payload[8:] == b'healthy-tiny-after'
                healthy.consume(key, len(b'healthy-tiny-before') + len(b'healthy-tiny-after'))
                break
        assert healthy.request(8, live)[0] == 0
        assert_clean(healthy)
        report['checks'].append('tiny4096-byte/4-frame stalled owner isolated on SAME peer;65-stream admission; cleanup0')
        # Independent foreign host SIGKILL triggers owner disconnect cancellation.
        holding_log = open(directory / 'holder.log', 'w+')
        holding = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--holder', str(tiny.path)], stdout=holding_log, stderr=holding_log)
        try:
            wait_until(lambda: 'HOLDER READY' in (directory / 'holder.log').read_text())
            holding.kill()
            holding.wait(timeout=5)
            assert_clean(healthy)
        finally:
            if holding.poll() is None:
                holding.kill(); holding.wait(timeout=5)
            holding_log.close()
        # Kill daemon with active stream; stale socket is never silently unlinked.
        crash_key = open_stream(healthy)
        status = Path('/proc/%d/status' % tiny.process.pid).read_text()
        report['daemon_peaks_kib'][tiny.directory.name] = int(next(l for l in status.splitlines() if l.startswith('VmHWM:')).split()[1])
        tiny.stop(kill=True)
        daemons.remove(tiny)
        assert tiny.path.exists()
        failed_start(binary, directory / 'occupied-stale', tiny.profile, {'ipc_path': str(tiny.path)})
        tiny.path.unlink()  # only after confirmed killed process, explicit host cleanup
        minimum = Daemon(binary, directory / 'ipc-control143', env, 1, {'output_bytes': 143})
        daemons.append(minimum)
        minimum_client = ready_client(minimum.path)
        assert minimum_client.status()[0] == 1
        minimum_client.close()
        minimum.stop()
        daemons.remove(minimum)
        report['checks'].append('real mandatory STATUS at exact143-byte output cap')
        after_crash = Daemon(binary, directory / 'ipc-after-crash', env, 1)
        daemons.append(after_crash)
        recovered = ready_client(after_crash.path)
        clients.append(recovered)
        assert recovered.request(5, crash_key, b'old-daemon')[0] == 4
        exchange(recovered, b'after-daemon-kill')
        report['checks'].append('foreign host SIGKILL + daemon SIGKILL/stale path/generation-safe new streams')
        # Real broker stop/start: old streams terminate, IPC remains, only new streams recover.
        before_loss = exchange(c2, b'before-broker-loss', half_close=False)
        subprocess.run(['docker', 'stop', '--time', '1', env['SKVOZ_NATS_CONTAINER']], check=True, stdout=subprocess.DEVNULL)
        wait_until(lambda: c2.status()[0] == 2)
        assert_clean(c2)
        assert c2.request(5, before_loss, b'old-broker')[0] == 4
        subprocess.run(['docker', 'start', env['SKVOZ_NATS_CONTAINER']], check=True, stdout=subprocess.DEVNULL)
        wait_until(lambda: c2.request(11, payload=struct.pack('!Q', 0))[3] == b'\1', seconds=30)
        exchange(c2, b'new-stream-after-broker')
        report['checks'].append('real broker restart terminal cleanup/new-stream-only recovery')
        # Observe idle process CPU/RSS, with platform and sample scope explicit.
        for daemon in daemons:
            status = Path('/proc/%d/status' % daemon.process.pid).read_text()
            report['daemon_peaks_kib'][daemon.directory.name] = int(next(l for l in status.splitlines() if l.startswith('VmHWM:')).split()[1])
        tick1 = int(Path('/proc/%d/stat' % after_crash.process.pid).read_text().split()[13])
        time.sleep(.25)
        tick2 = int(Path('/proc/%d/stat' % after_crash.process.pid).read_text().split()[13])
        report['idle_cpu_ticks_250ms'] = tick2 - tick1
        assert tick2 - tick1 < 10, 'idle driver busy spinning'
        report['checks'].append('finite workload RSS/idle observation')
        for secret in [env['SKVOZ_DAEMON_PASSWORD'], env['SKVOZ_NATS_P0_PASSWORD']]:
            for daemon in daemons:
                assert secret not in (daemon.directory / 'daemon.log').read_text()
                assert secret.encode() not in Path('/proc/%d/cmdline' % daemon.process.pid).read_bytes()
        report['checks'].append('logs/arguments secret redaction')
        print(json.dumps(report, sort_keys=True), flush=True)
        return report
    finally:
        if service_process is not None and service_process.poll() not in (None, 0):
            print('FOREIGN SERVICE FAILED: ' + (directory / 'service.log').read_text(), file=sys.stderr, flush=True)
        for client in clients:
            client.close()
        if service_process is not None:
            service_process.terminate()
            service_process.wait(timeout=10)
            service_log.close()
        for daemon in reversed(daemons):
            daemon.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--service')
    group.add_argument('--holder')
    options = parser.parse_args()
    service(options.service) if options.service else holder(options.holder)
