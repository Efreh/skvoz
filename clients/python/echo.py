#!/usr/bin/env python3
"""Send binary stdin to a provisioned peer and print its reverse reply."""
import argparse
import select
import struct
import sys
import time
from skvoz_ipc import Client, DATA, REMOTE_FINISHED, CLOSED, REJECTED


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--peer', type=int, required=True)
    args = parser.parse_args()
    client = Client(args.socket)
    deadline = time.monotonic() + 15
    client.request(10, payload=struct.pack('!Q', args.peer))
    while client.request(11, payload=struct.pack('!Q', args.peer))[3] != b'\1':
        if time.monotonic() > deadline:
            raise TimeoutError('peer readiness deadline')
        time.sleep(.01)
    code, _, handle, _ = client.request(2, payload=struct.pack('!Q', args.peer) + b'echo')
    if code:
        raise RuntimeError('open rejected: code %d' % code)
    pending = memoryview(sys.stdin.buffer.read(65537))
    if len(pending) > 65536:
        raise ValueError('example input limit is 65536 bytes')
    finished = False
    expected_bytes = len(pending)
    received_bytes = 0
    while True:
        if pending:
            code, n, _, _ = client.request(5, handle, bytes(pending))
            if code not in (0, 1, 7):
                raise RuntimeError('send rejected: code %d' % code)
            pending = pending[n:]
        elif not finished:
            if client.request(7, handle)[0] == 0:
                finished = True
        if client.events or select.select([client.socket], [], [], .01)[0]:
            kind, _, key, payload = client.event()
            if key != handle:
                raise ValueError('unexpected stream')
            if kind == DATA:
                received_bytes += len(payload) - 8
                offset, = struct.unpack('!Q', payload[:8])
                sys.stdout.buffer.write(payload[8:])
                sys.stdout.buffer.flush()
                client.consume(handle, offset + len(payload) - 8)
            if kind == REJECTED:
                raise RuntimeError('remote rejected stream')
            if kind in (REMOTE_FINISHED, CLOSED):
                if received_bytes != expected_bytes or pending:
                    raise RuntimeError('early reply end')
                break
        if time.monotonic() > deadline:
            raise TimeoutError('example transfer deadline')
    client.close()


if __name__ == '__main__':
    main()
