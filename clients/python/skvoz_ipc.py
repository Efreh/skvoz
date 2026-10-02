"""Standard-library IPC v1 client; no native SKVOZ bindings required."""
import collections
import socket
import struct

HEADER = struct.Struct('!4sHHQQQ')
MAX_BODY = 65568
RESPONSE = 0x8000
INCOMING, OPENED, REJECTED, DATA, WRITABLE, REMOTE_FINISHED, CLOSED = range(0x9001, 0x9008)


def encode(kind, request, handle=0, payload=b''):
    if len(payload) > 65536:
        raise ValueError('IPC payload exceeds limit')
    body = HEADER.pack(b'SKI1', 1, kind, request, handle >> 64, handle & ((1 << 64) - 1)) + payload
    return struct.pack('!I', len(body)) + body


class Client:
    """One connection owns its handles. Keep consuming events during traffic."""
    def __init__(self, path, acceptor=False, timeout=10):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.socket.settimeout(timeout)
            self.socket.connect(path)
            self.sequence = 0
            self.events = collections.deque()
            self.event_bytes = 0
            result = self.request(1, payload=struct.pack('!HHB', 1, 1, int(acceptor)))
            if result[0] != 0:
                self.close()
                raise RuntimeError('IPC HELLO rejected: code %d' % result[0])
            self.capabilities = result[3]
        except BaseException:
            self.socket.close()
            raise

    def close(self):
        self.socket.close()

    def _exact(self, n):
        output = bytearray()
        while len(output) < n:
            part = self.socket.recv(n - len(output))
            if not part:
                raise EOFError('IPC session closed')
            output.extend(part)
        return bytes(output)

    def read(self):
        n, = struct.unpack('!I', self._exact(4))
        if not 32 <= n <= MAX_BODY:
            raise ValueError('invalid IPC frame size')
        body = self._exact(n)
        magic, version, kind, request, high, low = HEADER.unpack(body[:32])
        if magic != b'SKI1' or version != 1:
            raise ValueError('unsupported IPC frame')
        if kind != RESPONSE and not INCOMING <= kind <= CLOSED:
            raise ValueError('unknown IPC frame kind')
        return kind, request, (high << 64) | low, body[32:]

    def request(self, kind, handle=0, payload=b''):
        self.sequence += 1
        self.socket.sendall(encode(kind, self.sequence, handle, payload))
        while True:
            event = self.read()
            if event[0] == RESPONSE:
                if event[1] != self.sequence or len(event[3]) < 10:
                    raise ValueError('unexpected IPC response')
                code, value = struct.unpack('!HQ', event[3][:10])
                return code, value, event[2], event[3][10:]
            if event[1] != 0:
                raise ValueError('unexpected IPC event request')
            if len(self.events) >= 4096 or self.event_bytes + len(event[3]) > 8 * 1024 * 1024:
                raise BufferError('client event budget exceeded; drain events')
            self.events.append(event)
            self.event_bytes += len(event[3])

    def event(self):
        if self.events:
            event = self.events.popleft()
            self.event_bytes -= len(event[3])
            return event
        frame = self.read()
        if frame[0] == RESPONSE or frame[1] != 0:
            raise ValueError('unexpected IPC response')
        return frame

    def status(self):
        code, _, _, payload = self.request(9)
        if code or len(payload) != 97:
            raise RuntimeError('invalid status response')
        return struct.unpack('!B12Q', payload)

    def consume(self, handle, end):
        code, _, _, _ = self.request(6, handle, struct.pack('!Q', end))
        # CLOSED can retire ownership before the final consumption request.
        if code not in (0, 4):
            raise RuntimeError('consume rejected: code %d' % code)
        return code
