"""Independent real TLS/NATS TCP capacity fixture (invoked in a disposable container)."""
import argparse
import hashlib
import array
import concurrent.futures
import json
import os
from pathlib import Path
import queue
import selectors
import socket
import struct
import subprocess
import threading
import time


def exact(sock, count):
    data = bytearray()
    while len(data) < count:
        part = sock.recv(count - len(data))
        if not part:
            raise EOFError('socket closed')
        data.extend(part)
    return bytes(data)


class Owner:
    def __init__(self, binary, config, directory, peer):
        self.lock = threading.Lock()
        self.responses = queue.Queue()
        self.events = {}
        self.results = {}
        self.terminals = set()
        self.request_ids = set()
        self.duplicate_terminals = 0
        self.id = 0
        path = directory / f'runtime-{peer}.json'
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        self.socket, child = socket.socketpair()
        child.setblocking(False)
        self.log = open(directory / f'runtime-{peer}.log', 'wb')
        self.process = subprocess.Popen([binary, '--config', str(path), '--control-fd', str(child.fileno())],
            pass_fds=[child.fileno()], stdout=self.log, stderr=self.log)
        child.close()
        threading.Thread(target=self.reader, daemon=True).start()
        try:
            self.call('HELLO', {'api': 1, 'network': 3})
            until = time.monotonic() + 30
            while time.monotonic() < until:
                if self.events.get('RUNTIME_STATE', {}).get('state') == 'ready':
                    return
                if self.process.poll() is not None:
                    raise RuntimeError(f'runtime {peer} exited; inspect its log')
                time.sleep(.05)
            raise TimeoutError('runtime readiness')
        except Exception:
            self.close()
            raise

    def reader(self):
        try:
            while True:
                descriptors = []
                def frame_exact(n):
                    data = bytearray()
                    while len(data) < n:
                        part, controls, flags, _ = self.socket.recvmsg(n-len(data), socket.CMSG_SPACE(16))
                        assert not flags & socket.MSG_CTRUNC
                        for _, kind, body in controls:
                            assert kind == socket.SCM_RIGHTS
                            fds = array.array('i')
                            fds.frombytes(body)
                            descriptors.extend(fds)
                        if not part:
                            raise EOFError('owner closed')
                        data.extend(part)
                    return data
                size = struct.unpack('!I', frame_exact(4))[0]
                assert 0 < size <= 32768
                value = json.loads(frame_exact(size))
                assert value['fd_count'] == len(descriptors)
                if 'event' in value:
                    assert not descriptors
                    self.events[value['event']] = value['data']
                    if value['event'] == 'REQUEST':
                        self.request_ids.add(value['data']['id'])
                        result = value['data']['result']
                        self.results[result] = self.results.get(result, 0) + 1
                        if result not in ('opening', 'active'):
                            identity = value['data']['id']
                            if identity in self.terminals:
                                self.duplicate_terminals += 1
                            self.terminals.add(identity)
                else:
                    self.responses.put((value, descriptors))
        except Exception as error:
            for fd in descriptors:
                os.close(fd)
            self.responses.put((error, []))

    def call(self, op, args=None):
        with self.lock:
            self.id += 1
            data = json.dumps({'v': 1, 'id': self.id, 'op': op, 'args': args or {}, 'fd_count': 0}).encode()
            self.socket.sendall(struct.pack('!I', len(data)) + data)
            value, fds = self.responses.get(timeout=25)
            if isinstance(value, Exception):
                raise value
            assert value['id'] == self.id
            if value.get('error'):
                for fd in fds:
                    os.close(fd)
                raise RuntimeError(op + ': ' + value['error'])
            return value['result'], fds

    def status(self):
        return self.call('STATUS')[0]

    def close(self):
        self.socket.close()
        while not self.responses.empty():
            pending = self.responses.get_nowait()
            if isinstance(pending, Exception):
                continue
            _, fds = pending
            for fd in fds:
                os.close(fd)
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.log.close()


def target():
    poll = selectors.DefaultSelector()
    received = {}
    for port in (9000, 9001, 9002, 9003):
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1', port))
        listener.listen(4096)
        listener.setblocking(False)
        poll.register(listener, selectors.EVENT_READ, ('listen', port, None))
    while True:
        for key, mask in poll.select(.2):
            sock = key.fileobj
            kind, port, out = key.data
            try:
                if kind == 'listen':
                    peer, _ = sock.accept()
                    peer.setblocking(False)
                    received[peer] = 0
                    poll.register(peer, selectors.EVENT_WRITE if port == 9001 else selectors.EVENT_READ,
                                  ('peer', port, bytearray()))
                elif port == 9001:
                    sock.send(b'x' * 65536)
                else:
                    if mask & selectors.EVENT_READ:
                        data = sock.recv(65536)
                        if not data:
                            if port == 9002:
                                out.extend(struct.pack('!Q', received[sock]))
                            if not out:
                                raise EOFError()
                            poll.modify(sock, selectors.EVENT_WRITE, ('closing', port, out))
                            kind = 'closing'
                        elif port == 9000:
                            out.extend(data)
                        elif port == 9002:
                            assert data == b'x'*len(data)
                            received[sock] += len(data)
                        elif port == 9003:
                            out.extend(data)
                            assert len(out) <= 65536
                            if b'\r\n\r\n' in out:
                                assert out.startswith(b'GET / HTTP/1.1\r\n')
                                out[:] = b'HTTP/1.1 200 OK\r\nContent-Length: 17\r\nConnection: close\r\n\r\ncapacity response'
                                kind = 'closing'
                                poll.modify(sock, selectors.EVENT_WRITE, (kind, port, out))
                    if out and mask & selectors.EVENT_WRITE:
                        n = sock.send(out)
                        del out[:n]
                    if kind == 'closing' and not out:
                        raise EOFError()
                    if kind != 'closing':
                        poll.modify(sock, (selectors.EVENT_WRITE if len(out) >= 262144 else selectors.EVENT_READ)
                                    | (selectors.EVENT_WRITE if out else 0), (kind, port, out))
            except BlockingIOError:
                pass
            except (OSError, EOFError):
                poll.unregister(sock)
                received.pop(sock, None)
                sock.close()


def resource(owner):
    pid = owner.process.pid
    fields = {}
    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
        if ':' in line:
            k, v = line.split(':', 1)
            fields[k] = v.strip()
    stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return {'rss_kib': int(fields['VmRSS'].split()[0]), 'rss_peak_kib': int(fields['VmHWM'].split()[0]),
            'fd': len(list(Path(f'/proc/{pid}/fd').iterdir())), 'cpu_ticks': int(stat[11]) + int(stat[12]),
            'status': owner.status(), 'requests': dict(owner.results),
            'terminal_ids':len(owner.terminals), 'duplicate_terminals':owner.duplicate_terminals}


def connect(owner, kind, endpoints, port=9000):
    if kind == 'api':
        _, fds = owner.call('OPEN_TCP', {'host': '127.0.0.1', 'port': port})
        if len(fds) != 1:
            for fd in fds:
                os.close(fd)
            raise AssertionError('OPEN_TCP descriptor count')
        sock = socket.socket(fileno=fds[0])
        sock.setblocking(True)
    else:
        uri = endpoints['http' if kind in ('connect', 'http') else 'socks']
        sock = socket.create_connection(('127.0.0.1', int(uri.rsplit(':', 1)[1])), timeout=20)
        try:
            handshake(sock, kind, port)
        except Exception:
            sock.close()
            raise
    sock.settimeout(20)
    return sock


def handshake(sock, kind, port):
        if kind in ('connect', 'http'):
            if kind == 'http':
                sock.sendall(f'GET http://127.0.0.1:{port}/ HTTP/1.1\r\n\r\n'.encode())
                expected = f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n'.encode()
                assert exact(sock, len(expected)) == expected
            else:
                sock.sendall(f'CONNECT 127.0.0.1:{port} HTTP/1.1\r\n\r\n'.encode())
                header = bytearray()
                while not header.endswith(b'\r\n\r\n'):
                    header.extend(exact(sock, 1))
                assert bytes(header).startswith(b'HTTP/1.1 200 '), bytes(header)
        else:
            sock.sendall(b'\x05\x01\x00')
            assert exact(sock, 2) == b'\x05\x00'
            sock.sendall(b'\x05\x01\x00\x01\x7f\x00\x00\x01' + struct.pack('!H', port))
            assert exact(sock, 10) == b'\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00'


def exchange(sock, serial):
    payload = struct.pack('!Q', serial) + bytes(range(256)) * 4
    sock.sendall(payload)
    assert exact(sock, len(payload)) == payload


def short_http(owner, endpoints):
    port = int(endpoints['http'].rsplit(':',1)[1])
    with socket.create_connection(('127.0.0.1',port), timeout=20) as sock:
        sock.sendall(b'GET http://127.0.0.1:9003/ HTTP/1.1\r\n\r\n')
        response = bytearray()
        while True:
            data = sock.recv(4096)
            if not data:
                break
            response.extend(data)
        assert bytes(response) == b'HTTP/1.1 200 OK\r\nContent-Length: 17\r\nConnection: close\r\n\r\ncapacity response'


def configuration(template, directory, nats, peer, baseline=False):
    config = json.loads(json.dumps(template))
    core = config['core']
    core.update(url='tls://127.0.0.1:4222', tls_server_name='localhost', trust='managed_ca', ca_file=str(directory/'ca.pem'),
                username=f'p{peer}', password=nats['authorization']['users'][peer]['password'], namespace='tcp_capacity', peer_id=str(peer))
    limits = config['network']['limits']
    if peer == 0:
        config['role'] = 'server'
        config['network']['families'] = []
        core.update(membership='broker_authorized', allowed_peers=[], initiate=[])
        limits.update(ip_sessions=128, core_streams=512 if baseline else 2048, lease_identities=4096,
            core_receive_bytes=67108864 if baseline else 134217728, core_send_bytes=67108864,
            runtime_buffer_bytes=268435456 if baseline else 536870912, runtime_buffer_records=65536 if baseline else 131072)
        config['server'] = {'ipv4': None, 'ipv6': None, 'dns_servers': [], 'allow': [{'cidr':'127.0.0.0/8','protocols':'any','ports':None}],
            'deny': [], 'service_prefixes': [], 'lease_store': str(directory/'leases.json'), 'server_addresses': [], 'management_endpoints': []}
    return config


def cgroup_memory():
    data = dict(line.split() for line in Path('/sys/fs/cgroup/memory.stat').read_text().splitlines())
    return {key:int(data.get(key,0)) for key in ('anon','file','sock','kernel','kernel_stack','slab')}


def run(args):
    directory = Path(args.directory)
    baseline = args.baseline
    template = json.loads(Path(args.template).read_text())
    nats = json.loads((directory/'nats.json').read_text())
    broker = subprocess.Popen(['nats-server', '-c', str(directory/'nats.json')], stdout=open(directory/'nats.log', 'wb'), stderr=subprocess.STDOUT)
    target_process = subprocess.Popen(['python3', __file__, '--target'])
    owners = []
    sockets, active, active_threads, transient = [], [], [], []
    stop_active = threading.Event()
    report = {'baseline': baseline, 'settings':vars(args), 'groups': [], 'samples': [], 'benchmarks': []}
    report['runtime_sha256'] = hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()
    def group(name, body):
        begin = time.monotonic()
        value = body()
        report['groups'].append({'name': name, 'seconds': time.monotonic()-begin, 'result': value})
        print(json.dumps(report['groups'][-1]), flush=True)
        (directory/'report.json').write_text(json.dumps(report, indent=2))
    try:
        time.sleep(.5)
        for peer in range(args.clients + 1):
            config = configuration(template,directory,nats,peer,baseline)
            limits = config['network']['limits']
            if args.reduced and peer > 0:
                limits.update(core_streams=2,streams_per_peer=2,core_receive_bytes=131072,core_receive_peer_bytes=131072,
                    api_queue_bytes=34816,api_queue_records=6)
                limits['runtime_buffer_bytes'] = 12592896 + 2*131072 + 2097152 + 524288 + 1081344 + 34816 + 6*256
            owners.append(Owner(args.binary, config, directory, peer))
        endpoints = [owner.call('START_PROXY', {'http_bind': f'127.0.0.1:{10080+peer}', 'socks_bind': f'127.0.0.1:{11080+peer}'})[0] for peer, owner in enumerate(owners[1:])]
        report['samples'].append({'phase': 'before', 'runtime': [resource(o) for o in owners]})
        def capacity():
            failures = []
            for i in range(args.idle):
                client = i % args.clients
                kind = 'api' if args.reduced else ('connect', 'socks', 'api', 'http')[i % 4]
                try:
                    sock = connect(owners[client+1], kind, endpoints[client])
                    sockets.append(sock)
                except Exception as error:
                    failures.append({'index':i, 'kind':kind, 'error':str(error)})
            report['capacity_attempts'] = {'opened':len(sockets),'attempted':args.idle,'offered':args.idle,'failures':failures}
            assert baseline or not failures, failures
            return {'opened':len(sockets), 'offered':args.idle, 'attempted':args.idle, 'failures':failures}
        if not args.bench:
            group('capacity', capacity)
        report['samples'].append({'phase': 'idle-before-traffic', 'runtime': [resource(o) for o in owners], 'cgroup_memory':cgroup_memory(), 'socket_memory':subprocess.check_output(['ss','-m','-t','-n'],text=True)})
        if not args.bench:
            idle_before = report['samples'][-1]['runtime']
            idle_started = time.monotonic()
            time.sleep(5)
            idle_after = [resource(o) for o in owners]
            report['idle_cpu'] = {'seconds':time.monotonic()-idle_started,'ticks_per_second':os.sysconf('SC_CLK_TCK'),
                'runtime_ticks':[new['cpu_ticks']-old['cpu_ticks'] for old,new in zip(idle_before,idle_after)]}
        for i, sock in enumerate(sockets):
            exchange(sock, i)
        report['samples'].append({'phase': 'post-traffic-idle', 'runtime': [resource(o) for o in owners], 'cgroup_memory':cgroup_memory(), 'socket_memory':subprocess.check_output(['ss','-m','-t','-n'],text=True)})
        def burst():
            def attempt(i):
                sock = connect(owners[1], ('connect','socks')[i%2], endpoints[0])
                exchange(sock, i)
                return sock
            opened, failures = [], []
            with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
                futures = [pool.submit(attempt, i) for i in range(64)]
                for future in futures:
                    try:
                        opened.append(future.result())
                    except Exception as error:
                        failures.append(str(error))
            live = owners[0].status()['counters']['tcp_open']
            if not baseline:
                assert live >= len(sockets)+len(opened), (live,len(sockets),len(opened))
            for sock in opened:
                sock.close()
            assert baseline or not failures, failures
            return {'opened':len(opened), 'offered':64, 'concurrent_server_streams':live, 'failures':failures}
        if not args.bench and not args.reduced:
            group('burst64', burst)
            report['samples'].append({'phase':'after-burst', 'runtime':[resource(o) for o in owners]})
            print('AFTER BURST',json.dumps(report['samples'][-1]),flush=True)
        if args.bench:
            for streams in (1, 16):
                for direction in ('upload', 'download'):
                    for repeat in range(args.repeats):
                        connections = []
                        transient = connections
                        for _ in range(streams):
                            connections.append(connect(owners[1], 'connect', endpoints[0], 9002 if direction == 'upload' else 9001))
                        until = time.monotonic() + args.bench_seconds
                        def transfer(sock):
                            total = 0
                            while time.monotonic() < until:
                                if direction == 'upload':
                                    sock.sendall(b'x'*65536)
                                    total += 65536
                                else:
                                    data = sock.recv(65536)
                                    assert data and data == b'x'*len(data)
                                    total += len(data)
                            return total
                        begin = time.monotonic()
                        with concurrent.futures.ThreadPoolExecutor(max_workers=streams) as pool:
                            offered = list(pool.map(transfer, connections))
                        seconds = time.monotonic()-begin
                        drain = time.monotonic()
                        confirmed = []
                        for sock, count in zip(connections, offered):
                            if direction == 'upload':
                                sock.shutdown(socket.SHUT_WR)
                                actual = struct.unpack('!Q',exact(sock,8))[0]
                                assert actual == count, (actual,count)
                                confirmed.append(actual)
                            else:
                                confirmed.append(count)
                        for sock in connections:
                            sock.close()
                        total = sum(confirmed)
                        elapsed_seconds = time.monotonic()-begin
                        drain_seconds = time.monotonic()-drain
                        report['benchmarks'].append({'streams':streams,'direction':direction,'repeat':repeat,'seconds':seconds,'drain_seconds':drain_seconds,'elapsed_seconds':elapsed_seconds,'bytes':total,'offered_bytes':sum(offered),'mbit_s':total*8/elapsed_seconds/1e6})
                        print('BENCH', json.dumps(report['benchmarks'][-1]), flush=True)
                        (directory/'report.json').write_text(json.dumps(report, indent=2))
                        deadline = time.monotonic() + 20
                        while owners[1].status()['counters']['tcp_open'] or owners[0].status()['counters']['tcp_open']:
                            assert time.monotonic() < deadline, 'benchmark cleanup deadline'
                            time.sleep(.05)
        for peer in range(args.clients):
            for i in range(args.active):
                active.append(connect(owners[peer+1], ('connect','socks','api')[i%3], endpoints[peer]))
        active_errors = []
        def active_worker(sock, index):
            try:
                serial = index
                while not stop_active.is_set():
                    exchange(sock, serial)
                    serial += 32
                    time.sleep(.05)
            except Exception as error:
                active_errors.append(repr(error))
        active_threads = [threading.Thread(target=active_worker,args=(sock,index)) for index,sock in enumerate(active)]
        for thread in active_threads:
            thread.start()
        hold_begin = time.monotonic()
        expected_live = len(sockets) + len(active)
        assert args.bench or baseline or sum(o.status()['counters']['tcp_open'] for o in owners[1:]) >= expected_live
        until = hold_begin + args.hold
        iteration = 0
        while time.monotonic() < until:
            if args.reduced:
                break
            for peer in range(args.clients):
                short_http(owners[peer+1], endpoints[peer])
                for kind in ('connect','socks','http','api'):
                    sock = connect(owners[peer+1], kind, endpoints[peer])
                    exchange(sock, iteration)
                    sock.shutdown(socket.SHUT_WR)
                    assert sock.recv(1) == b''
                    sock.close()
            if iteration % 10 == 0:
                report['samples'].append({'phase':'hold', 'elapsed':args.hold-(until-time.monotonic()), 'runtime':[resource(o) for o in owners], 'cgroup_memory':cgroup_memory()})
                (directory/'report.json').write_text(json.dumps(report, indent=2))
                print('HOLD', iteration, flush=True)
            iteration += 1
            assert not active_errors, active_errors
            time.sleep(1)
        report['hold_seconds'] = time.monotonic()-hold_begin
        assert args.reduced or report['hold_seconds'] >= args.hold
        stop_active.set()
        for thread in active_threads:
            thread.join(timeout=25)
            assert not thread.is_alive()
        assert not active_errors, active_errors
        for index, sock in enumerate(sockets):
            exchange(sock, index + 50000)
        for sock in active:
            sock.close()
        survivors = sockets[-min(args.clients,len(sockets)):]
        for sock in sockets[:-len(survivors)] if survivors else sockets:
            sock.close()
        deadline = time.monotonic()+20
        close_latencies = []
        while owners[0].status()['counters']['tcp_open'] > len(survivors):
            assert time.monotonic() < deadline, 'mass close progress deadline'
            started = time.monotonic()
            for index,sock in enumerate(survivors):
                exchange(sock,index+60000)
            close_latencies.append(time.monotonic()-started)
        for sock in survivors:
            sock.close()
        while any(o.status()['counters']['tcp_open'] for o in owners):
            assert time.monotonic() < deadline, 'cleanup deadline'
            time.sleep(.05)
        report['mass_close_healthy'] = {'streams':len(survivors),'exchanges':len(close_latencies)*len(survivors),
            'max_seconds':max(close_latencies,default=0)}
        if not baseline:
            stable_until = time.monotonic() + 15
            while time.monotonic() < stable_until:
                for peer in range(args.clients):
                    short_http(owners[peer+1], endpoints[peer])
                assert all(o.status()['lifecycle'] == 'ready' for o in owners)
                time.sleep(.2)
        assert not any(o.duplicate_terminals for o in owners)
        assert all(o.request_ids <= o.terminals for o in owners[1:]), [(len(o.request_ids),len(o.terminals)) for o in owners[1:]]
        def churn():
            for cycle in range(args.cycles):
                kind = ('connect','socks','api','http')[cycle%4]
                sock = connect(owners[1],kind,endpoints[0])
                exchange(sock,cycle)
                sock.shutdown(socket.SHUT_WR)
                assert sock.recv(1) == b''
                sock.close()
                if cycle % 1000 == 0:
                    print('CHURN',cycle,flush=True)
            deadline = time.monotonic()+20
            while any(o.status()['counters']['tcp_open'] for o in owners):
                assert time.monotonic() < deadline, 'churn cleanup deadline'
                time.sleep(.05)
            assert not any(o.duplicate_terminals for o in owners)
            return {'cycles':args.cycles, 'live_after':0}
        if args.cycles:
            group('churn',churn)
        if args.lifecycle and not baseline:
            def lifecycle():
                nonlocal transient
                results = []
                for count, operation in [(512, 'STOP_PROXY'), (512, 'PREPARE_SHUTDOWN')]:
                    held = []
                    transient = held
                    old = set(owners[1].terminals)
                    for i in range(count):
                        held.append(connect(owners[1], ('connect','socks','api')[i%3], endpoints[0]))
                    assert owners[1].status()['counters']['tcp_open'] == count
                    started = time.monotonic()
                    result, fds = owners[1].call(operation)
                    elapsed = time.monotonic()-started
                    assert not fds
                    deadline = time.monotonic()+20
                    while len(owners[1].terminals-old) < count:
                        assert time.monotonic() < deadline, ('missing lifecycle terminal', operation, len(owners[1].terminals-old),count)
                        time.sleep(.01)
                    for sock in held:
                        assert sock.recv(1) == b''
                        sock.close()
                    assert owners[1].duplicate_terminals == 0
                    results.append({'operation':operation,'streams':count,'terminals':len(owners[1].terminals-old),'response':result,'operation_seconds':elapsed})
                    if operation == 'PREPARE_SHUTDOWN':
                        assert owners[1].process.wait(timeout=5) == 0
                    if operation == 'STOP_PROXY':
                        endpoints[0] = owners[1].call('START_PROXY', {'http_bind':'127.0.0.1:10080','socks_bind':'127.0.0.1:11080'})[0]
                    stable_until = time.monotonic()+15
                    while time.monotonic() < stable_until:
                        for peer,owner in enumerate(owners[1:]):
                            if owner.process.poll() is None:
                                short_http(owner,endpoints[peer])
                        time.sleep(.2)
                    deadline = time.monotonic()+20
                    while any(owner.status()['counters']['tcp_open'] for owner in owners if owner.process.poll() is None):
                        assert time.monotonic() < deadline,'lifecycle server cleanup deadline'
                        time.sleep(.05)
                    if operation == 'PREPARE_SHUTDOWN':
                        report['expected_peer_loss'] = {'peer':1,'cause':'successful PREPARE_SHUTDOWN and process exit'}
                return results
            group('mass-lifecycle',lifecycle)
        report['samples'].append({'phase':'after', 'runtime':[resource(o) if o.process.poll() is None else {'exit_code':o.process.poll(), 'requests':o.results, 'terminal_ids':len(o.terminals), 'duplicate_terminals':o.duplicate_terminals} for o in owners]})
        report['cgroup_memory_after'] = cgroup_memory()
        report['socket_memory'] = subprocess.check_output(['ss','-m','-t','-n'], text=True)
        report['cgroup_peak'] = Path('/sys/fs/cgroup/memory.peak').read_text().strip()
        if not baseline or args.bench:
            for peer in range(len(owners)):
                text = (directory/f'runtime-{peer}.log').read_text()
                if peer == 0 and report.get('expected_peer_loss'):
                    lost = report['expected_peer_loss']['peer']
                    lines = [line for line in text.splitlines() if line.startswith(f'Peer watermark timeout: peer={lost} ')]
                    assert len(lines) == 1,lines
                    report['expected_peer_loss']['diagnostics'] = lines
                    text = '\n'.join(line for line in text.splitlines() if line not in lines)
                assert all(signal not in text for signal in ('Network drive failure','Network readiness lost','Transport shard failure','Peer watermark timeout','Peer envelope sequence failure','Peer stream receive failure')), ('unexpected transport failure',peer,text)
        report['status'] = 'baseline_measured' if args.bench and baseline else 'reproduced_expected_failure' if baseline else 'passed'
    except Exception as error:
        report['status'] = 'failed'
        report['error'] = repr(error)
        report['owner_events'] = [o.events for o in owners]
        report['exit_codes'] = [o.process.poll() for o in owners]
        try:
            report['samples'].append({'phase':'failure', 'runtime':[resource(o) for o in owners]})
        except Exception as sample_error:
            report['sample_error'] = repr(sample_error)
        raise
    finally:
        stop_active.set()
        for sock in sockets + active + transient:
            sock.close()
        for thread in active_threads:
            thread.join(timeout=25)
        (directory/'report.json').write_text(json.dumps(report, indent=2))
        for owner in owners:
            owner.close()
        broker.terminate()
        target_process.terminate()
        broker.wait(timeout=5)
        target_process.wait(timeout=5)


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', action='store_true')
    parser.add_argument('--binary')
    parser.add_argument('--directory')
    parser.add_argument('--template')
    parser.add_argument('--baseline', action='store_true')
    parser.add_argument('--clients', type=int, default=1)
    parser.add_argument('--idle', type=int, default=256)
    parser.add_argument('--hold', type=int, default=0)
    parser.add_argument('--active', type=int, default=0)
    parser.add_argument('--reduced', action='store_true')
    parser.add_argument('--lifecycle', action='store_true')
    parser.add_argument('--cycles', type=int, default=0)
    parser.add_argument('--bench', action='store_true')
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--bench-seconds', type=int, default=30)
    args = parser.parse_args()
    if args.target:
        target()
    else:
        run(args)
