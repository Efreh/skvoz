"""Small authoritative DNS fixture for real systemd-resolved path checks."""
import json
import socket
import struct
import sys
import threading

IP4, IP6 = sys.argv[1:3]
NAME = 'payload.skvoz.test'


def answer(packet):
    assert 12 < len(packet) <= 4096
    ident, _, questions, _, _, _ = struct.unpack('!6H',packet[:12])
    assert questions == 1
    position, labels = 12, []
    while packet[position]:
        size=packet[position]
        assert size <= 63
        labels.append(packet[position+1:position+1+size].decode('ascii').lower())
        position += size+1
    end=position+5
    kind, cls=struct.unpack('!HH',packet[position+1:end])
    name='.'.join(labels)
    expected=name==NAME and cls==1 and kind in (1,28)
    body=socket.inet_pton(socket.AF_INET if kind==1 else socket.AF_INET6,IP4 if kind==1 else IP6) if expected else b''
    header=struct.pack('!6H',ident,0x8580 if expected else 0x8583,1,int(expected),0,0)
    response=header+packet[12:end]
    if expected:
        response += b'\xc0\x0c'+struct.pack('!HHIH',kind,1,60,len(body))+body
    print('DNS '+json.dumps({'name':name,'type':kind,'answer':expected}),flush=True)
    return response


def udp():
    sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0',53))
    while True:
        packet,peer=sock.recvfrom(4096)
        sock.sendto(answer(packet),peer)


def tcp():
    listener=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    listener.bind(('0.0.0.0',53))
    listener.listen(8)
    while True:
        peer,_=listener.accept()
        with peer:
            peer.settimeout(3)
            def exact(count):
                data=b''
                while len(data)<count:
                    part=peer.recv(count-len(data))
                    if not part: raise EOFError()
                    data+=part
                return data
            packet=exact(struct.unpack('!H',exact(2))[0])
            response=answer(packet)
            peer.sendall(struct.pack('!H',len(response))+response)


threading.Thread(target=udp,daemon=True).start()
print('DNS READY',flush=True)
tcp()
