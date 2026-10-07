"""Consumers in a separate cgroup for the ordinary Ruby server resource spec."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
from tcp_capacity import Owner, connect, exchange, resource, short_http


def run(args):
    shared = Path(args.directory)
    directory = shared/'consumers'
    directory.mkdir(mode=0o700, exist_ok=True)
    report = {'scope':'consumer container; server resource measurements are in server-report.json','samples':[]}
    report['runtime_sha256'] = hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()
    owners, held, active, workers = [], [], [], []
    stop = threading.Event()
    target = subprocess.Popen(['python3',str(Path(__file__).with_name('tcp_capacity.py')),'--target'])
    failures = []
    operation = {}
    def short(peer, phase):
        operation.clear()
        operation.update(peer=peer, operation='short_http', phase=phase, received_bytes=0)
        short_http(owners[peer], endpoints[peer], operation)
    def save(phase):
        runtimes = [resource(owner) for owner in owners]
        assert all(value['rss_peak_kib'] <= 128*1024 for value in runtimes), 'client runtime RSS limit'
        report['samples'].append({'phase':phase,'time':time.monotonic(),'runtime':runtimes})
        temporary = shared/'consumer-report.tmp'
        temporary.write_text(json.dumps(report,indent=2))
        temporary.replace(shared/'consumer-report.json')
    def stable():
        until = time.monotonic()+15
        while time.monotonic() < until:
            for peer,owner in enumerate(owners):
                short(peer, 'stable')
            assert all(owner.status()['lifecycle'] == 'ready' for owner in owners)
            time.sleep(.2)
        deadline = time.monotonic()+30
        while any(owner.status()['counters']['tcp_open'] for owner in owners):
            assert time.monotonic() < deadline,'stable cleanup deadline'
            time.sleep(.05)
    def cleanup_bounds(before):
        after = [resource(owner) for owner in owners]
        for old,new in zip(before,after):
            assert new['fd'] <= old['fd']+8,(old['fd'],new['fd'])
            assert new['status']['counters']['buffer_bytes'] <= old['status']['counters']['buffer_bytes']+1048576
        return after
    try:
        deadline = time.monotonic()+90
        while not (shared/'ready.json').exists():
            assert time.monotonic() < deadline, 'server resource fixture readiness'
            time.sleep(.1)
        settings = json.loads((shared/'ready.json').read_text())
        for i,path in enumerate(settings['profiles']):
            owners.append(Owner(args.binary,json.loads(Path(path).read_text()),directory,i))
        endpoints = [owner.call('START_PROXY',{'http_bind':f'127.0.0.1:{10080+i}','socks_bind':f'127.0.0.1:{11080+i}'})[0] for i,owner in enumerate(owners)]
        save('before')
        initial = report['samples'][-1]['runtime']
        for peer,owner in enumerate(owners):
            for i in range(64):
                held.append(connect(owner,('connect','socks','http','api')[i%4],endpoints[peer]))
        assert len(held) == 1024
        save('idle-before-traffic')
        time.sleep(5)
        for i,sock in enumerate(held):
            exchange(sock,i)
        save('post-traffic-idle')
        time.sleep(5)
        for peer,owner in enumerate(owners):
            for i in range(2):
                active.append(connect(owner,('connect','socks')[i],endpoints[peer]))
        assert len(active) == 32
        stalled = connect(owners[0],'connect',endpoints[0],9001)
        held.append(stalled)
        stalled_since = time.monotonic()
        stalled_attempts = 1
        def transfer(sock,index):
            progress = {'worker':index, 'peer':index//2, 'operation':'active_exchange'}
            try:
                while not stop.is_set():
                    exchange(sock,index,progress)
                    time.sleep(.05)
            except Exception as error:
                failures.append({**progress, 'error':repr(error)})
        workers = [threading.Thread(target=transfer,args=(sock,i)) for i,sock in enumerate(active)]
        for worker in workers:
            worker.start()
        begin = time.monotonic()
        iteration = 0
        while time.monotonic()-begin < settings['hold']:
            assert target.poll() is None, 'target exited'
            if time.monotonic()-stalled_since >= 20:
                stalled.close()
                held.remove(stalled)
                stalled = connect(owners[0],'connect',endpoints[0],9001)
                held.append(stalled)
                stalled_since = time.monotonic()
                stalled_attempts += 1
                report['stalled_attempts'] = stalled_attempts
            for peer,owner in enumerate(owners):
                short(peer, 'active-stalled')
                for kind in ('connect','socks','http','api'):
                    operation.clear()
                    operation.update(peer=peer, operation='exchange', kind=kind, phase='active-stalled', received_bytes=0)
                    with connect(owner,kind,endpoints[peer]) as sock:
                        exchange(sock,iteration,operation)
                        sock.shutdown(socket.SHUT_WR)
                        assert sock.recv(1) == b''
            assert not failures,failures
            if iteration%10 == 0:
                save('active-stalled')
                print('SERVER RESOURCE HOLD',iteration,flush=True)
            iteration += 1
            time.sleep(1)
        report['hold_seconds'] = time.monotonic()-begin
        assert report['hold_seconds'] >= settings['hold']
        stop.set()
        for worker in workers:
            worker.join(timeout=25)
            assert not worker.is_alive()
        assert not failures,failures
        for sock in held+active:
            sock.close()
        deadline = time.monotonic()+30
        while any(owner.status()['counters']['tcp_open'] for owner in owners):
            assert time.monotonic() < deadline,'resource cleanup deadline'
            time.sleep(.05)
        stable()
        report['post_hold_cleanup'] = cleanup_bounds(initial)
        save('after-hold')
        for cycle in range(args.cycles):
            peer = cycle%len(owners)
            operation.clear()
            operation.update(peer=peer, operation='exchange', phase='churn', cycle=cycle, received_bytes=0)
            with connect(owners[peer],('connect','socks','http','api')[cycle%4],endpoints[peer]) as sock:
                exchange(sock,cycle,operation)
                sock.shutdown(socket.SHUT_WR)
                assert sock.recv(1) == b''
            if cycle%1000 == 0:
                save('churn')
                print('SERVER RESOURCE CHURN',cycle,flush=True)
        deadline = time.monotonic()+30
        while any(owner.status()['counters']['tcp_open'] for owner in owners):
            assert time.monotonic() < deadline,'resource churn cleanup deadline'
            time.sleep(.05)
        assert all(owner.request_ids <= owner.terminals for owner in owners)
        assert all(owner.duplicate_terminals == 0 for owner in owners)
        stable()
        report['post_churn_cleanup'] = cleanup_bounds(initial)
        for peer in range(len(owners)):
            log = (directory/f'runtime-{peer}.log').read_text()
            assert all(signal not in log for signal in ('Network drive failure','Network readiness lost','Transport shard failure','Peer watermark timeout','Peer envelope sequence failure','Peer stream receive failure')), (peer,log)
        save('after')
        report.update(status='passed',idle=1024,active=32,stalled=1,cycles=args.cycles)
    except Exception as error:
        statuses = []
        for peer, owner in enumerate(owners):
            try:
                statuses.append({'peer':peer, 'status':owner.call('STATUS',timeout=5)[0], 'events':owner.events, 'exit_code':owner.process.poll()})
            except Exception as status_error:
                statuses.append({'peer':peer, 'error':repr(status_error), 'events':owner.events, 'exit_code':owner.process.poll()})
        report.update(status='failed',error=repr(error), operation=dict(operation), failures=list(failures), owner_status=statuses)
        raise
    finally:
        stop.set()
        for sock in held+active:
            sock.close()
        for worker in workers:
            worker.join(timeout=25)
        report['final_transfer_failures'] = list(failures)
        for peer, owner in enumerate(owners):
            owner.log.flush()
            path = directory/f'runtime-{peer}.log'
            with path.open('rb') as source:
                source.seek(max(0,path.stat().st_size-262144))
                (shared/f'consumer-runtime-{peer}.log').write_bytes(source.read(262144))
        temporary = shared/'consumer-report.tmp'
        temporary.write_text(json.dumps(report,indent=2))
        temporary.replace(shared/'consumer-report.json')
        (shared/'consumer-done.json').write_text(json.dumps({'status':report.get('status','failed')}))
        for owner in owners:
            owner.close()
        target.terminate()
        target.wait(timeout=5)


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',required=True)
    parser.add_argument('--binary',required=True)
    parser.add_argument('--cycles',type=int,default=10000)
    args = parser.parse_args()
    assert 0 <= args.cycles <= 10000
    run(args)
