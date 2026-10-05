"""Real slot exhaustion, complete proxy failures and stalled/bulk peer isolation."""
import argparse
import hashlib
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import threading
import time
from tcp_capacity import Owner, configuration, connect, exchange, exact, resource, short_http, cgroup_memory


def run(args):
    directory = Path(args.directory)
    template = json.loads(Path(args.template).read_text())
    nats = json.loads((directory/'nats.json').read_text())
    broker = subprocess.Popen(['nats-server','-c',str(directory/'nats.json')],stdout=open(directory/'nats.log','wb'),stderr=subprocess.STDOUT)
    target = subprocess.Popen(['python3',str(Path(__file__).with_name('tcp_capacity.py')),'--target'])
    owners,held,bulk,workers = [],[],[],[]
    stop = threading.Event()
    report = {'groups':[],'samples':[]}
    report['runtime_sha256'] = hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()
    def sample(phase):
        report['samples'].append({'phase':phase,'runtime':[resource(o) for o in owners], 'cgroup_memory':cgroup_memory(),
            'socket_memory':subprocess.check_output(['ss','-m','-t','-n'],text=True)})
        (directory/'overload-report.json').write_text(json.dumps(report,indent=2))
    def cleanup():
        deadline = time.monotonic()+20
        while any(o.status()['counters']['tcp_open'] for o in owners):
            assert time.monotonic() < deadline,'overload cleanup deadline'
            time.sleep(.02)
    try:
        time.sleep(.5)
        owners = [Owner(args.binary,configuration(template,directory,nats,peer),directory,peer) for peer in range(3)]
        endpoints = [o.call('START_PROXY',{'http_bind':f'127.0.0.1:{10080+i}','socks_bind':f'127.0.0.1:{11080+i}'})[0] for i,o in enumerate(owners[1:])]
        for i in range(512):
            held.append(connect(owners[1],('connect','socks','http','api')[i%4],endpoints[0]))
        assert owners[1].status()['counters']['tcp_open'] == 512
        sample('full-idle')
        def refused(i):
            socks = i%2
            endpoint = endpoints[0]['socks' if socks else 'http']
            with socket.create_connection(('127.0.0.1',int(endpoint.rsplit(':',1)[1])),timeout=15) as sock:
                if socks:
                    sock.sendall(b'\x05\x01\x00')
                    assert exact(sock,2) == b'\x05\x00'
                    sock.sendall(b'\x05\x01\x00\x01\x7f\0\0\x01'+struct.pack('!H',9000))
                    value = exact(sock,10)
                    assert value == b'\x05\x01\0\x01\0\0\0\0\0\0',value
                else:
                    sock.sendall(b'CONNECT 127.0.0.1:9000 HTTP/1.1\r\n\r\n')
                    value = bytearray()
                    while True:
                        part = sock.recv(4096)
                        if not part:
                            break
                        value.extend(part)
                    assert value == b'HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n',value
                assert sock.recv(1) == b''
                return len(value)
        with concurrent.futures.ThreadPoolExecutor(max_workers=128) as pool:
            replies = list(pool.map(refused,range(128)))
        try:
            owners[1].call('OPEN_TCP',{'host':'127.0.0.1','port':9000})
            raise AssertionError('OPEN_TCP beyond native bound accepted')
        except RuntimeError as error:
            assert str(error) == 'OPEN_TCP: overloaded'
        for i,sock in enumerate(held):
            exchange(sock,i)
        short_http(owners[2],endpoints[1])
        assert owners[1].status()['counters']['tcp_open'] == 512
        report['groups'].append({'name':'512-held-burst128-failure','opened':512,'complete_replies':len(replies),'http_replies':64,'socks_replies':64})
        sample('after-overload')
        for sock in held:
            sock.close()
        held.clear()
        cleanup()
        assert owners[1].results.get('overloaded',0) == 129,owners[1].results
        bulk = [connect(owners[1],'connect',endpoints[0],9001) for _ in range(16)]
        stalled = connect(owners[1],'api',endpoints[0],9001)
        held.append(stalled)
        stalled_id = max(owners[1].request_ids)
        errors,totals = [],[0]*16
        def download(sock,index):
            sock.settimeout(.5)
            try:
                while not stop.is_set():
                    try:
                        value = sock.recv(65536)
                    except socket.timeout:
                        continue
                    assert value and value == b'x'*len(value)
                    totals[index] += len(value)
            except Exception as error:
                errors.append(repr(error))
        workers = [threading.Thread(target=download,args=(sock,i)) for i,sock in enumerate(bulk)]
        for worker in workers:
            worker.start()
        begin = time.monotonic()
        latencies = []
        while time.monotonic()-begin < 35:
            start = time.monotonic()
            for peer in (1,2):
                for kind in ('connect','socks','http','api'):
                    with connect(owners[peer],kind,endpoints[peer-1]) as sock:
                        exchange(sock,len(latencies))
                        sock.shutdown(socket.SHUT_WR)
                        assert sock.recv(1) == b''
                short_http(owners[peer],endpoints[peer-1])
            latencies.append(time.monotonic()-start)
            assert latencies[-1] < 2,latencies[-1]
            assert not errors,errors
            if len(latencies)%10 == 1:
                sample('bulk-stalled')
            time.sleep(.2)
        assert stalled_id in owners[1].terminals,(stalled_id,owners[1].results)
        assert owners[1].results.get('timeout',0) >= 1,owners[1].results
        stop.set()
        for worker in workers:
            worker.join(timeout=2)
            assert not worker.is_alive()
        assert not errors,errors
        assert all(totals),totals
        for sock in bulk+held:
            sock.close()
        cleanup()
        stable_until = time.monotonic()+15
        while time.monotonic() < stable_until:
            for peer in (1,2):
                short_http(owners[peer],endpoints[peer-1])
            assert all(o.status()['lifecycle'] == 'ready' for o in owners)
            time.sleep(.2)
        cleanup()
        report['groups'].append({'name':'stalled16bulk-peer-isolation','seconds':time.monotonic()-begin,'bulk_received_bytes':sum(totals),
            'healthy_exchanges':len(latencies)*10,'healthy_peers':[1,2],'max_healthy_seconds':max(latencies),'stalled_terminal':'timeout'})
        sample('after')
        assert all(o.request_ids <= o.terminals and o.duplicate_terminals == 0 for o in owners[1:])
        for peer in range(3):
            log = (directory/f'runtime-{peer}.log').read_text()
            assert all(signal not in log for signal in ('Network drive failure','Network readiness lost','Transport shard failure','Peer watermark timeout','Peer envelope sequence failure','Peer stream receive failure')), (peer,log)
        report['status'] = 'passed'
    except Exception as error:
        report.update(status='failed',error=repr(error))
        raise
    finally:
        stop.set()
        for sock in held+bulk:
            sock.close()
        for worker in workers:
            worker.join(timeout=2)
        (directory/'overload-report.json').write_text(json.dumps(report,indent=2))
        for owner in owners:
            owner.close()
        broker.terminate();target.terminate()
        broker.wait(timeout=5);target.wait(timeout=5)


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('directory','binary','template'):
        parser.add_argument('--'+name,required=True)
    run(parser.parse_args())
