"""Real bounded DNS, queued connection setup, deadlines and cancellation."""
import argparse
import hashlib
import concurrent.futures
import heapq
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import time
from tcp_capacity import Owner, configuration, exact, exchange, resource, short_http


def dns(directory):
    pending = []
    sequence = 0
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(('127.0.0.1',53))
        sock.settimeout(.01)
        with (directory/'dns-queries.jsonl').open('w') as log:
            while True:
                try:
                    query,address = sock.recvfrom(512)
                    position,labels = 12,[]
                    while query[position]:
                        count = query[position]
                        labels.append(query[position+1:position+1+count].decode('ascii'))
                        position += count+1
                    position += 1
                    kind = struct.unpack('!H',query[position:position+2])[0]
                    end = position+4
                    name = '.'.join(labels)
                    log.write(json.dumps({'host':name,'type':kind,'time':time.monotonic()})+'\n');log.flush()
                    delay = 12 if name.startswith('blocked-') else .2 if name.startswith('queued-') else 0
                    answer = b'\xc0\x0c'+struct.pack('!HHIH',1,1,30,4)+b'\x7f\0\0\x01' if kind == 1 else b''
                    response = query[:2]+struct.pack('!HHHHH',0x8180,1,int(bool(answer)),0,0)+query[12:end]+answer
                    assert len(pending) < 256,'DNS fixture pending bound'
                    sequence += 1
                    heapq.heappush(pending,(time.monotonic()+delay,sequence,address,response))
                except socket.timeout:
                    pass
                while pending and pending[0][0] <= time.monotonic():
                    _,_,address,response = heapq.heappop(pending)
                    sock.sendto(response,address)


def run(args):
    directory = Path(args.directory)
    template = json.loads(Path(args.template).read_text())
    nats = json.loads((directory/'nats.json').read_text())
    resolver = subprocess.Popen(['python3',__file__,'--dns','--directory',str(directory)])
    broker = subprocess.Popen(['nats-server','-c',str(directory/'nats.json')],stdout=open(directory/'nats.log','wb'),stderr=subprocess.STDOUT)
    target = subprocess.Popen(['python3',str(Path(__file__).with_name('tcp_capacity.py')),'--target'])
    owners,held = [],[]
    report = {'groups':[],'samples':[]}
    report['runtime_sha256'] = hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()
    def queries():
        return [json.loads(line)['host'] for line in (directory/'dns-queries.jsonl').read_text().splitlines()]
    def wait(predicate,seconds=20):
        until = time.monotonic()+seconds
        while not predicate():
            assert time.monotonic() < until,'setup fixture deadline'
            time.sleep(.02)
    def begin(kind,host):
        endpoint = endpoints[0]['http' if kind == 'connect' else 'socks']
        sock = socket.create_connection(('127.0.0.1',int(endpoint.rsplit(':',1)[1])),timeout=20)
        held.append(sock)
        if kind == 'connect':
            sock.sendall(f'CONNECT {host}:9000 HTTP/1.1\r\n\r\n'.encode())
        else:
            sock.sendall(b'\x05\x01\x00')
            assert exact(sock,2) == b'\x05\x00'
            name = host.encode()
            sock.sendall(b'\x05\x01\x00\x03'+bytes([len(name)])+name+struct.pack('!H',9000))
        return sock,kind
    def finish(item,success,overloaded=False):
        sock,kind = item
        if kind == 'connect':
            reply = bytearray()
            while not reply.endswith(b'\r\n\r\n'):
                reply.extend(exact(sock,1))
                assert len(reply) <= 256
            assert reply.startswith(b'HTTP/1.1 200 ' if success else b'HTTP/1.1 503 ' if overloaded else b'HTTP/1.1 502 '),reply
        else:
            reply = exact(sock,10)
            assert reply[:1] == b'\x05' and reply[2:] == b'\0\x01\0\0\0\0\0\0',reply
            assert (reply[1] == 0) == success,reply
            if overloaded:
                assert reply[1] == 1,reply
        if success:
            exchange(sock,123)
            sock.shutdown(socket.SHUT_WR)
        assert sock.recv(1) == b''
        sock.close()
        return len(reply)
    def cleanup():
        wait(lambda:all(o.status()['counters']['tcp_open'] == 0 for o in owners if o.process.poll() is None),30)
    try:
        time.sleep(.5)
        assert resolver.poll() is None,'DNS fixture did not start'
        owners = [Owner(args.binary,configuration(template,directory,nats,peer),directory,peer) for peer in range(4)]
        endpoints = [o.call('START_PROXY',{'http_bind':f'127.0.0.1:{10080+i}','socks_bind':f'127.0.0.1:{11080+i}'})[0] for i,o in enumerate(owners[1:])]
        report['samples'].append({'phase':'before','runtime':[resource(o) for o in owners]})
        old = [begin('connect',f'queued-old{i}.capacity.test') for i in range(8)]
        wait(lambda:len(set(queries())) >= 4)
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            more = list(pool.map(lambda i:begin(('connect','socks')[i%2],f'queued-new{i}.capacity.test'),range(56)))
            sizes = list(pool.map(lambda item:finish(item,True),old+more))
        names = list(dict.fromkeys(queries()))
        first_new = min(index for index,name in enumerate(names) if name.startswith('queued-new'))
        assert all(names.index(f'queued-old{i}.capacity.test') < first_new for i in range(8)),names
        cleanup()
        report['groups'].append({'name':'queued64-oldest-before-new-arrivals','complete_successes':len(sizes),'dns_order':names})
        before = set(owners[1].terminals)
        pending = [begin(('connect','socks')[i%2],f'blocked-stop{i}.capacity.test') for i in range(64)]
        wait(lambda:owners[1].status()['counters']['tcp_open'] == 64)
        wait(lambda:any(name.startswith('blocked-stop') for name in queries()))
        extra_size = finish(begin('connect','blocked-extra.capacity.test'),False,overloaded=True)
        assert owners[1].status()['counters']['tcp_open'] == 64
        assert 'blocked-extra.capacity.test' not in queries()
        before = set(owners[1].terminals)
        report['groups'].append({'name':'per-peer-setup-boundary65','pending_preserved':64,'complete_http503_bytes':extra_size})
        result,fds = owners[1].call('STOP_PROXY')
        assert not fds and result == {}
        for sock,_ in pending:
            assert sock.recv(1) == b''
            sock.close()
        wait(lambda:len(owners[1].terminals-before) == 64)
        cleanup()
        dns_held = resource(owners[0])
        initial_bytes = report['samples'][0]['runtime'][0]['status']['counters']['buffer_bytes']
        delta = dns_held['status']['counters']['buffer_bytes']-initial_bytes
        assert 65536 <= delta <= 16*65536+262144,delta
        report['samples'].append({'phase':'cancelled-dns-workers-still-owned','runtime':[dns_held],'charged_delta':delta})
        report['groups'].append({'name':'stop64-pending-http-socks','terminal_ids':64,'response':result})
        endpoints[0] = owners[1].call('START_PROXY',{'http_bind':'127.0.0.1:10080','socks_bind':'127.0.0.1:11080'})[0]
        deadline_started = time.monotonic()
        pending = [begin(('connect','socks')[i%2],f'blocked-deadline{i}.capacity.test') for i in range(8)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            answers = [pool.submit(finish,item,False) for item in pending]
            healthy = 0
            until = time.monotonic()+15
            while not all(answer.done() for answer in answers):
                assert time.monotonic() < until,'destination deadline not bounded'
                short_http(owners[2],endpoints[1]);healthy += 1
                time.sleep(.05)
            sizes = [answer.result() for answer in answers]
        deadline_seconds = time.monotonic()-deadline_started
        assert deadline_seconds <= 12.5,deadline_seconds
        cleanup()
        report['groups'].append({'name':'slow-dns-deadline-independent-peer','complete_failures':len(sizes),'healthy_http':healthy,'seconds':deadline_seconds,'setup_deadline_seconds':10,'failure_write_seconds':1})
        owners[3].id += 1
        command = json.dumps({'v':1,'id':owners[3].id,'op':'OPEN_TCP','args':{'host':'blocked-api.capacity.test','port':9000},'fd_count':0},separators=(',',':')).encode()
        owners[3].socket.sendall(struct.pack('!I',len(command))+command)
        wait(lambda:'blocked-api.capacity.test' in queries())
        owners[3].socket.shutdown(socket.SHUT_RDWR)
        owners[3].socket.close()
        assert owners[3].process.wait(timeout=5) == 0
        cleanup()
        report['groups'].append({'name':'pending-open-tcp-owner-eof','exit_code':0})
        until = time.monotonic()+15
        while time.monotonic() < until:
            for peer in (1,2):
                short_http(owners[peer],endpoints[peer-1])
            time.sleep(.2)
        cleanup()
        returned_started = time.monotonic()
        wait(lambda:resource(owners[0])['status']['counters']['buffer_bytes'] <= initial_bytes+4096,60)
        report['dns_reservations_returned_after_stability_seconds'] = time.monotonic()-returned_started
        fresh = begin('connect','queued-fresh.capacity.test')
        finish(fresh,True)
        cleanup()
        final_server = resource(owners[0])
        report['samples'].append({'phase':'after-real-dns-return','runtime':[final_server]})
        assert final_server['status']['counters']['buffer_bytes'] <= initial_bytes+4096,(initial_bytes,final_server)
        after = [resource(o) for o in owners[:3]]
        before = report['samples'][0]['runtime'][:3]
        for old,new in zip(before,after):
            assert new['fd'] <= old['fd']+8
            assert new['status']['counters']['buffer_bytes'] <= old['status']['counters']['buffer_bytes']+65536
        report['cleanup_bounds'] = {'before':before,'after':after,'allowed_lazy_fds':8,'allowed_fixed_ledger_delta':65536}
        expected_eof = []
        assert all(o.request_ids <= o.terminals and o.duplicate_terminals == 0 for o in owners[1:3])
        for peer in range(4):
            log = (directory/f'runtime-{peer}.log').read_text()
            if peer == 0:
                expected_eof = [line for line in log.splitlines() if line.startswith('Peer watermark timeout: peer=3 ')]
                assert len(expected_eof) == 1,expected_eof
                log = '\n'.join(line for line in log.splitlines() if line not in expected_eof)
            assert all(signal not in log for signal in ('Network drive failure','Network readiness lost','Transport shard failure','Peer watermark timeout','Peer envelope sequence failure','Peer stream receive failure')), (peer,log)
        report['expected_peer_loss'] = {'peer':3,'cause':'deliberate pending OPEN_TCP owner EOF and process exit','diagnostics':expected_eof}
        report['samples'].append({'phase':'after','runtime':[resource(o) for o in owners[:3]]})
        report['status'] = 'passed'
    except Exception as error:
        report.update(status='failed',error=repr(error))
        raise
    finally:
        for sock in held:
            sock.close()
        (directory/'setup-report.json').write_text(json.dumps(report,indent=2))
        for owner in owners:
            owner.close()
        for child in (resolver,broker,target):
            child.terminate();child.wait(timeout=5)


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dns',action='store_true')
    parser.add_argument('--directory',required=True)
    parser.add_argument('--binary')
    parser.add_argument('--template')
    args = parser.parse_args()
    dns(Path(args.directory)) if args.dns else run(args)
