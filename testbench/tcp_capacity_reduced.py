"""Real reduced-profile exhaustion, complete failures and healthy recovery."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time

from tcp_capacity import Owner, configuration, connect, exact, exchange, resource, short_http

parser = argparse.ArgumentParser()
parser.add_argument('--directory', required=True)
parser.add_argument('--template', required=True)
parser.add_argument('--binary', required=True)
args = parser.parse_args()
os.umask(0o077)
directory = Path(args.directory)
template = json.loads(Path(args.template).read_text())
nats = json.loads((directory/'nats.json').read_text())
broker = subprocess.Popen(['nats-server','-c',str(directory/'nats.json')],stdout=open(directory/'nats.log','wb'),stderr=subprocess.STDOUT)
target = subprocess.Popen(['python3',str(Path(__file__).with_name('tcp_capacity.py')),'--target'])
owners, held = [], []
report = {'runtime_sha256':hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()}

def wait(predicate):
    deadline = time.monotonic()+20
    while not predicate():
        assert time.monotonic() < deadline, 'cleanup deadline'
        time.sleep(.02)

try:
    time.sleep(.5)
    for peer in (0,1):
        cfg = configuration(template,directory,nats,peer)
        if peer:
            limits = cfg['network']['limits']
            limits.update(core_streams=2,streams_per_peer=2,core_receive_bytes=131072,core_receive_peer_bytes=131072,api_queue_bytes=34816,api_queue_records=6)
            limits['runtime_buffer_bytes'] = 12592896+2*131072+2097152+524288+1081344+34816+6*256
            report['limits'] = limits
        owners.append(Owner(args.binary,cfg,directory,peer))
    endpoints = owners[1].call('START_PROXY',{'http_bind':'127.0.0.1:10080','socks_bind':'127.0.0.1:11080'})[0]
    report['before'] = [resource(o) for o in owners]
    held = [connect(owners[1],kind,endpoints) for kind in ('api','connect')]
    for i,sock in enumerate(held): exchange(sock,i)
    assert owners[1].status()['counters']['tcp_open']==2
    try:
        owners[1].call('OPEN_TCP',{'host':'127.0.0.1','port':9000})
        raise AssertionError('third API admitted')
    except RuntimeError as error:
        assert str(error)=='OPEN_TCP: overloaded',str(error)
    with socket.create_connection(('127.0.0.1',10080),timeout=20) as sock:
        sock.sendall(b'CONNECT 127.0.0.1:9000 HTTP/1.1\r\n\r\n')
        response = bytearray()
        while sock.recv(1,socket.MSG_PEEK): response.extend(sock.recv(4096))
        assert bytes(response)==b'HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n',response
    with socket.create_connection(('127.0.0.1',11080),timeout=20) as sock:
        sock.sendall(b'\x05\x01\x00');assert exact(sock,2)==b'\x05\x00'
        sock.sendall(b'\x05\x01\x00\x01\x7f\0\0\x01\x23\x28')
        assert exact(sock,10)==b'\x05\x01\0\x01\0\0\0\0\0\0'
        assert sock.recv(1)==b''
    exchange(held[1],10)
    held[0].shutdown(socket.SHUT_WR);assert held[0].recv(1)==b'';held[0].close()
    wait(lambda:owners[1].status()['counters']['tcp_open']==1)
    fresh = connect(owners[1],'api',endpoints);held.append(fresh)
    exchange(fresh,11);exchange(held[1],12)
    for sock in held: sock.close()
    wait(lambda:all(o.status()['counters']['tcp_open']==0 for o in owners))
    until = time.monotonic()+15
    exchanges = 0
    while time.monotonic()<until:
        short_http(owners[1],endpoints);exchanges+=1
        assert all(o.status()['lifecycle']=='ready' for o in owners)
        time.sleep(.2)
    wait(lambda:all(o.status()['counters']['tcp_open']==0 for o in owners))
    assert owners[1].request_ids<=owners[1].terminals
    assert owners[1].duplicate_terminals==0
    report['after']=[resource(o) for o in owners]
    for before,after in zip(report['before'],report['after']):
        assert after['fd']<=before['fd']+8
        assert after['status']['counters']['buffer_bytes']<=before['status']['counters']['buffer_bytes']+4096
    for peer in (0,1):
        log=(directory/f'runtime-{peer}.log').read_text()
        assert all(s not in log for s in ('Network drive failure','Network readiness lost','Transport shard failure','Peer watermark timeout','Peer envelope sequence failure','Peer stream receive failure')),log
    report.update(status='passed',initial_streams=2,negative_replies=['OPEN_TCP overloaded','HTTP503 complete','SOCKS10-byte complete'],reopened=True,stability_seconds=15,healthy_exchanges=exchanges)
except Exception as error:
    report.update(status='failed',error=repr(error));raise
finally:
    for sock in held: sock.close()
    (directory/'reduced-report.json').write_text(json.dumps(report,indent=2))
    for owner in owners: owner.close()
    target.terminate();broker.terminate()
    target.wait(timeout=5);broker.wait(timeout=5)
