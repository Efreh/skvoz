"""Kernel TCP/UDP/raw-IP workloads for the disposable network testbench."""
import argparse
import hashlib
import json
import resource
import socket
import struct
import threading
import time

PORT = 9000
PATTERN = bytes(range(256)) * 512


def exact(sock, count):
    data = bytearray()
    while len(data) < count:
        part = sock.recv(count - len(data))
        if not part:
            raise EOFError('Truncated workload record')
        data.extend(part)
    return bytes(data)


def one_way_connection(sock, download):
    with sock:
        sock.settimeout(20)
        count = struct.unpack('!Q', exact(sock, 8))[0]
        assert 0 < count <= 128 * 1024 * 1024
        offset = 0
        digest = hashlib.sha256()
        while offset < count:
            if download:
                chunk = PATTERN[:min(65536, count - offset)]
                sock.sendall(chunk)
            else:
                chunk = sock.recv(min(65536, count - offset))
                assert chunk and chunk == memoryview(PATTERN)[offset % 256:offset % 256 + len(chunk)]
            digest.update(chunk)
            offset += len(chunk)
        if download:
            sock.shutdown(socket.SHUT_WR)
        else:
            assert sock.recv(1) == b'', 'Upload missing half-close'
            sock.sendall(digest.digest())


def one_way_server(family, address, download):
    listener = socket.socket(family, socket.SOCK_STREAM)
    if family == socket.AF_INET6:
        listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    listener.bind((address, PORT + (1 if download else 2)))
    listener.listen(16)
    while True:
        connection, _ = listener.accept()
        threading.Thread(target=one_way_connection, args=(connection, download), daemon=True).start()


def one_way(address, count, download):
    with socket.create_connection((address, PORT + (1 if download else 2)), timeout=20) as sock:
        sock.sendall(struct.pack('!Q', count))
        offset = 0
        digest = hashlib.sha256()
        started = time.monotonic()
        first = None
        while offset < count:
            if download:
                chunk = sock.recv(min(65536, count - offset))
                assert chunk and chunk == memoryview(PATTERN)[offset % 256:offset % 256 + len(chunk)]
                if first is None:
                    first = time.monotonic() - started
            else:
                chunk = PATTERN[:min(65536, count - offset)]
                sock.sendall(chunk)
            digest.update(chunk)
            offset += len(chunk)
        if download:
            assert sock.recv(1) == b'', 'Download missing half-close'
        else:
            sock.shutdown(socket.SHUT_WR)
            assert exact(sock, 32) == digest.digest(), 'Upload hash mismatch'
        elapsed = time.monotonic() - started
        return {'bytes': offset, 'seconds': elapsed, 'mbit_s': offset * 8 / elapsed / 1e6,
                'first_data_ms': None if first is None else first * 1000,
                'sha256': digest.hexdigest(), 'direction': 'download' if download else 'upload'}


def short_exchange(address, samples):
    assert 0 < samples <= 1000
    times = []
    with socket.create_connection((address, PORT), timeout=10) as sock:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        for _ in range(samples):
            start = time.monotonic()
            sock.sendall(PATTERN[:256])
            assert exact(sock, 256) == PATTERN[:256]
            times.append((time.monotonic() - start) * 1000)
    times.sort()
    return {'samples': samples, 'bytes': 256, 'p95_ms': times[min(samples - 1, int(samples * .95))],
            'median_ms': times[samples // 2], 'min_ms': times[0]}


def echo_stream(connection):
    with connection:
        while data := connection.recv(65536):
            connection.sendall(data)


def tcp_server(family, address):
    listener = socket.socket(family, socket.SOCK_STREAM)
    if family == socket.AF_INET6:
        listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    listener.bind((address, PORT))
    listener.listen(16)
    while True:
        connection, peer = listener.accept()
        print("OBSERVED " + json.dumps({"protocol": "tcp", "family": 6 if family == socket.AF_INET6 else 4,
                                        "source": peer[0]}), flush=True)
        threading.Thread(target=echo_stream, args=(connection,), daemon=True).start()


def datagram_server(family, address, protocol=0):
    raw = protocol != 0
    sock = socket.socket(family, socket.SOCK_RAW if raw else socket.SOCK_DGRAM, protocol)
    if family == socket.AF_INET6 and not raw:
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    if not raw:
        # Linux IP_MTU_DISCOVER/IPV6_MTU_DISCOVER=P(M)TUDISC_DONT: echo large
        # datagrams through genuine kernel fragmentation in this owning fixture.
        sock.setsockopt(socket.IPPROTO_IP if family == socket.AF_INET else socket.IPPROTO_IPV6,
                        10 if family == socket.AF_INET else 23, 0)
    sock.bind((address, 0 if raw else PORT))
    observed = set()
    while True:
        packet, peer = sock.recvfrom(65536)
        if peer[0] not in observed and len(observed) < 32:
            observed.add(peer[0])
            print("OBSERVED " + json.dumps({"protocol": 143 if raw else "udp",
                                            "family": 6 if family == socket.AF_INET6 else 4,
                                            "source": peer[0]}), flush=True)
        if raw and family == socket.AF_INET:
            packet = packet[(packet[0] & 15) * 4:]
        # Only requests are echoed; raw sockets also observe locally sent replies.
        if raw:
            if not packet.startswith(b"SKVOZ-REQUEST:"):
                continue
            print("RAW REQUEST " + json.dumps({"source": peer[0], "payload": packet.hex()}), flush=True)
            packet = packet.replace(b"SKVOZ-REQUEST:", b"SKVOZ-RESPONSE:", 1)
        sock.sendto(packet, peer)


def icmp_monitor(family):
    sock = socket.socket(family, socket.SOCK_RAW, 1 if family == socket.AF_INET else 58)
    observed = set()
    while True:
        packet, peer = sock.recvfrom(65536)
        if family == socket.AF_INET:
            packet = packet[(packet[0] & 15) * 4:]
        if family == socket.AF_INET6 and len(packet) >= 48 and packet[0] == 2:
            quote = packet[8:]
            if quote[0] >> 4 == 6:
                print("OBSERVED_PMTU " + json.dumps({
                    "family": 6, "source": peer[0],
                    "mtu": struct.unpack("!I", packet[4:8])[0],
                    "quoted_source": socket.inet_ntop(socket.AF_INET6, quote[8:24]),
                    "quoted_destination": socket.inet_ntop(socket.AF_INET6, quote[24:40]),
                }), flush=True)
        if packet and packet[0] == (8 if family == socket.AF_INET else 128) and peer[0] not in observed:
            observed.add(peer[0])
            print("OBSERVED " + json.dumps({"protocol": "icmp", "family": 6 if family == socket.AF_INET6 else 4,
                                            "source": peer[0]}), flush=True)


def server():
    for family, address in [(socket.AF_INET, "0.0.0.0"), (socket.AF_INET6, "::")]:
        for download in (True, False):
            threading.Thread(target=one_way_server, args=(family, address, download), daemon=True).start()
        for function, args in [(tcp_server, (family, address)),
                               (datagram_server, (family, address)),
                               (datagram_server, (family, address, 143))]:
            threading.Thread(target=function, args=args, daemon=True).start()
        threading.Thread(target=icmp_monitor, args=(family,), daemon=True).start()
    print("TARGET READY", flush=True)
    while True:
        time.sleep(1)


def endpoint(address):
    return socket.AF_INET6 if ":" in address else socket.AF_INET


def tcp(address, count, rate=0):
    payload = bytes(range(256)) * (count // 256) + bytes(range(count % 256))
    with socket.create_connection((address, PORT), timeout=10) as sock:
        received = bytearray()
        def reader():
            while len(received) < len(payload):
                data = sock.recv(65536)
                if not data:
                    break
                received.extend(data)
        thread = threading.Thread(target=reader)
        thread.start()
        started = time.monotonic()
        if rate:
            for offset in range(0, len(payload), 8192):
                sock.sendall(payload[offset:offset + 8192])
                remaining = (min(offset + 8192, len(payload)) * 8 / (rate * 1e6)
                             - (time.monotonic() - started))
                if remaining > 0:
                    time.sleep(remaining)
        else:
            sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        thread.join(timeout=15)
        assert not thread.is_alive(), "TCP receive deadline"
        assert received == payload, "TCP payload mismatch"
        elapsed = time.monotonic() - started
        return {"bytes": count, "seconds": elapsed, "mbit_s": count * 8 / elapsed / 1e6,
                "sha256": hashlib.sha256(received).hexdigest()}


def tcp_duration(address, duration):
    assert 0 < duration <= 30, "Duration outside bounded diagnostic range"
    chunk_size = 65536
    body = (bytes(range(256)) * 256)[:chunk_size - 8]
    sent_hash = hashlib.sha256()
    result, errors = {}, []
    before = resource.getrusage(resource.RUSAGE_SELF)
    with socket.create_connection((address, PORT), timeout=10) as sock:
        def reader():
            try:
                received_hash = hashlib.sha256()
                pending = bytearray()
                sequence = 0
                while data := sock.recv(chunk_size):
                    received_hash.update(data)
                    pending.extend(data)
                    while len(pending) >= chunk_size:
                        expected = struct.pack("!Q", sequence) + body
                        assert pending[:chunk_size] == expected, "TCP block order/content mismatch"
                        del pending[:chunk_size]
                        sequence += 1
                assert not pending, "Truncated TCP block"
                result.update(bytes=sequence * chunk_size, sha256=received_hash.hexdigest())
            except BaseException as error:
                errors.append(repr(error))
        thread = threading.Thread(target=reader)
        thread.start()
        started = time.monotonic()
        deadline = started + duration
        chunks = 0
        try:
            while time.monotonic() < deadline:
                packet = struct.pack("!Q", chunks) + body
                sock.sendall(packet)
                sent_hash.update(packet)
                chunks += 1
            sending_seconds = time.monotonic() - started
            sock.shutdown(socket.SHUT_WR)
            thread.join(timeout=15)
            assert not thread.is_alive(), "TCP duration receive deadline"
            assert not errors, "TCP duration reader failed: " + repr(errors)
            assert result["bytes"] == chunks * chunk_size, "TCP duration byte count mismatch"
            assert result["sha256"] == sent_hash.hexdigest(), "TCP duration hash mismatch"
            elapsed = time.monotonic() - started
        finally:
            # Bound exceptional teardown too; closing wakes a failed reader.
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
            thread.join(timeout=1)
    after = resource.getrusage(resource.RUSAGE_SELF)
    return result | {"requested_seconds": duration, "sending_seconds": sending_seconds,
                     "seconds": elapsed, "mbit_s": result["bytes"] * 8 / elapsed / 1e6,
                     "chunk_bytes": chunk_size, "peak_rss_kib": after.ru_maxrss,
                     "cpu_seconds": (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime),
                     "semantics": "upload plus concurrent echo; ordered fixed blocks and incremental SHA-256"}


def udp(address, count, samples=1, interval=0):
    payload = bytes(range(256)) * (count // 256) + bytes(range(count % 256))
    times = []
    lost = 0
    with socket.socket(endpoint(address), socket.SOCK_DGRAM) as sock:
        sock.settimeout(2)
        if count > 1500:
            sock.setsockopt(socket.IPPROTO_IP if endpoint(address) == socket.AF_INET else socket.IPPROTO_IPV6,
                            10 if endpoint(address) == socket.AF_INET else 23, 0)
        sock.connect((address, PORT))
        for _ in range(samples):
            started = time.monotonic()
            sock.send(payload)
            try:
                received = sock.recv(65536)
                assert received == payload, "UDP payload mismatch"
                times.append((time.monotonic() - started) * 1000)
            except TimeoutError:
                lost += 1
            time.sleep(interval)
    ordered = sorted(times)
    assert ordered, "No UDP reply"
    return {"bytes": count, "samples": samples, "lost": lost,
            "p95_ms": ordered[min(len(ordered) - 1, int(len(ordered) * .95))],
            "p99_ms": ordered[min(len(ordered) - 1, int(len(ordered) * .99))]}


def raw(address):
    family = endpoint(address)
    payload = b"SKVOZ-REQUEST:protocol143:" + bytes(range(256))
    with socket.socket(family, socket.SOCK_RAW, 143) as sock:
        sock.settimeout(5)
        sock.sendto(payload, (address, 0))
        packet, _ = sock.recvfrom(65536)
        if family == socket.AF_INET:
            packet = packet[(packet[0] & 15) * 4:]
        assert packet == payload.replace(b"SKVOZ-REQUEST:", b"SKVOZ-RESPONSE:", 1)
    return {"protocol": 143, "bytes": len(packet)}


def burst(address, count):
    with socket.socket(endpoint(address), socket.SOCK_DGRAM) as sock:
        sock.connect((address, PORT))
        sock.settimeout(.2)
        for sequence in range(count):
            sock.send(struct.pack("!I", sequence) + b"B" * 1196)
        received = set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                packet = sock.recv(65536)
                assert len(packet) == 1200 and packet[4:] == b"B" * 1196
                received.add(struct.unpack("!I", packet[:4])[0])
            except TimeoutError:
                break
    assert received, "No packet progressed under tiny queues"
    return {"offered": count, "received": len(received)}


def checksum(data):
    if len(data) % 2:
        data += b"\0"
    total = sum(struct.unpack("!" + "H" * (len(data) // 2), data))
    while total >> 16:
        total = (total & 65535) + (total >> 16)
    return (~total) & 65535


def spoof(address, source):
    payload = b"SKVOZ-REQUEST:forbidden-source"
    header = struct.pack("!BBHHHBBH4s4s", 0x45, 0, 20 + len(payload), 54321,
                         0, 64, 143, 0, socket.inet_aton(source), socket.inet_aton(address))
    header = header[:10] + struct.pack("!H", checksum(header)) + header[12:]
    with socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_RAW) as sock:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        sock.sendto(header + payload, (address, 0))
    return {"sent_spoof": source}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["server", "tcp", "tcp-duration", "tcp-download", "tcp-upload", "tcp-short", "udp", "raw", "spoof", "burst"])
    parser.add_argument("address", nargs="?")
    parser.add_argument("--bytes", type=int, default=65536)
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--interval", type=float, default=0)
    parser.add_argument("--source")
    parser.add_argument("--rate-mbit", type=float, default=0)
    parser.add_argument("--seconds", type=float, default=10)
    args = parser.parse_args()
    if args.mode == "server":
        server()
        return
    functions = {"tcp": lambda: tcp(args.address, args.bytes, args.rate_mbit),
                 "tcp-duration": lambda: tcp_duration(args.address, args.seconds),
                 "tcp-download": lambda: one_way(args.address, args.bytes, True),
                 "tcp-upload": lambda: one_way(args.address, args.bytes, False),
                 "tcp-short": lambda: short_exchange(args.address, args.samples),
                 "udp": lambda: udp(args.address, args.bytes, args.samples, args.interval),
                 "raw": lambda: raw(args.address),
                 "spoof": lambda: spoof(args.address, args.source),
                 "burst": lambda: burst(args.address, args.samples)}
    print(json.dumps(functions[args.mode]()), flush=True)


if __name__ == "__main__":
    main()
