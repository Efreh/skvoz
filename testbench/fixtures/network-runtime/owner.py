"""Ephemeral API1 owner; product runtime/helper are the unchanged executables."""
import array
import json
import os
import pwd
from pathlib import Path
import socket
import struct
import subprocess
import sys
import time
import traceback

HERE = Path('/fixture')
RUN = Path('/run/skvoz-joint')
RUNTIME = '/usr/local/bin/skvoz-network-runtime'
HELPER = '/usr/local/bin/skvoz-network-helper'
DROP = ['setpriv', '--reuid=10001', '--regid=10001', '--clear-groups', '--no-new-privs', '--bounding-set=-all', '--inh-caps=-all', '--ambient-caps=-all']
NARROW = ['setpriv', '--no-new-privs', '--bounding-set=-all,+net_admin', '--inh-caps=-all', '--ambient-caps=-all']


def private(path, value, uid=0):
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    if uid:
        os.chown(path, uid, uid)


class Channel:
    def __init__(self, sock):
        self.sock, self.id, self.events = sock, 0, []
        sock.settimeout(20)

    def read(self):
        fds = []
        def exact(n):
            data = b''
            while len(data) < n:
                part, controls, flags, _ = self.sock.recvmsg(n-len(data), socket.CMSG_SPACE(4))
                assert not flags & socket.MSG_CTRUNC
                for _, kind, body in controls:
                    assert kind == socket.SCM_RIGHTS
                    descriptors = array.array('i')
                    descriptors.frombytes(body)
                    fds.extend(descriptors)
                if not part:
                    raise EOFError('control closed')
                data += part
            return data
        size = struct.unpack('!I', exact(4))[0]
        assert 0 < size <= 32768
        value = json.loads(exact(size))
        assert value['fd_count'] == len(fds)
        return value, fds

    def call(self, op, args=None, fd=None):
        self.id += 1
        body = json.dumps({'v':1, 'id':self.id, 'op':op, 'args':args or {}, 'fd_count':int(fd is not None)}).encode()
        frame = struct.pack('!I', len(body)) + body
        if fd is None:
            self.sock.sendall(frame)
        else:
            rights = array.array('i', [fd])
            sent = self.sock.sendmsg([frame], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
            self.sock.sendall(frame[sent:])
        while True:
            result, fds = self.read()
            if 'event' in result:
                assert not fds and len(self.events) < 128
                self.events.append(result)
                continue
            assert result['id'] == self.id
            if result.get('error'):
                raise RuntimeError(op + ': ' + result['error'])
            return result['result'], fds

    def event(self, name):
        until = time.monotonic() + 25
        while time.monotonic() < until:
            for index, value in enumerate(self.events):
                if value['event'] == name:
                    return self.events.pop(index)['data']
            value, fds = self.read()
            assert not fds and 'event' in value
            if value['event'] != 'STATS':
                self.events.append(value)
        raise TimeoutError(name)


def child(argv, descriptors, log):
    output = open(RUN / log, 'ab', buffering=0)
    return subprocess.Popen(argv, pass_fds=tuple(descriptors), stdout=output, stderr=output)


def spawn_runtime(helper=None):
    left, right = socket.socketpair()
    right.setblocking(False)
    argv = [RUNTIME, '--config', str(RUN / 'profile' / 'runtime.json'), '--control-fd', str(right.fileno())]
    descriptors = [right.fileno()]
    if helper:
        argv += ['--helper-fd', str(helper.fileno())]
        descriptors.append(helper.fileno())
    proc = child((DROP if os.getuid() == 0 else []) + argv, descriptors, 'runtime.log')
    right.close()
    channel = Channel(left)
    channel.call('HELLO', {'api':1, 'network':2})
    while channel.event('RUNTIME_STATE')['state'] != 'ready':
        if proc.poll() is not None:
            raise RuntimeError('runtime exited')
    return proc, channel


def client_helper():
    ticks = Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19]
    check = subprocess.run(['pkcheck','--action-id','org.skvoz.network.manage','--process',f'{os.getpid()},{ticks},{os.getuid()}'],capture_output=True,text=True)
    print('POLKIT CHECK',check.returncode,check.stdout,check.stderr,flush=True)
    peer = socket.socket(socket.AF_UNIX)
    peer.connect('/run/skvoz-network-helper/10001/control.sock')
    channel = Channel(peer)
    channel.call('HELLO', {'api':1, 'network':2})
    recovered, _ = channel.call('RECOVER')
    return channel, recovered


def serve(role):
    helper_proc = None
    if role == 'server':
        left, right = socket.socketpair()
        left.setblocking(False)
        right.setblocking(False)
        helper_proc = child(NARROW + [HELPER, '--config', str(RUN / 'helper.json'), '--control-fd', str(right.fileno())], [right.fileno()], 'helper.log')
        right.close()
        proc, runtime = spawn_runtime(left)
        left.close()
        helper, recovered = None, None
    else:
        helper, recovered = client_helper()
        proc, runtime = spawn_runtime()
    handle = recovered.get('handle') if recovered else None
    listener = socket.socket(socket.AF_UNIX)
    command_path = RUN / 'owner' / 'command.sock'
    command_path.unlink(missing_ok=True)
    listener.bind(str(command_path))
    listener.listen(4)
    print('OWNER READY', json.dumps({'runtime_pid':proc.pid, 'helper_pid':helper_proc.pid if helper_proc else None, 'recovered':recovered}), flush=True)
    while True:
        connection, _ = listener.accept()
        with connection:
            request = json.loads(connection.recv(4096))
            try:
                op = request['op']
                if op == 'start':
                    result, _ = runtime.call('START_IP', {'families':request.get('families', [4,6]), 'max_mtu':request.get('max_mtu',1500), 'channels':1})
                    handle = result['handle']
                    configured = runtime.event('CONFIGURED')['config']
                    transport = [{'ip':request['broker'], 'port':4222}]
                    prepared, fds = helper.call('PREPARE_CLIENT', {'handle':handle, 'config':configured, 'transport_endpoints':transport})
                    assert len(fds) == 1
                    try:
                        runtime.call('ATTACH_IP', {'handle':handle, 'interface':prepared['interface'], 'mtu':prepared['mtu']}, fds[0])
                    finally:
                        os.close(fds[0])
                    helper.call('ACTIVATE_CLIENT', {'handle':handle})
                    runtime.call('LOCAL_READY', {'handle':handle})
                    assert runtime.event('ACTIVE')['handle'] == handle
                    result = {'handle':handle, 'config':configured}
                elif op == 'stop':
                    runtime.call('STOP_IP', {'handle':handle, 'reason':'user_stop'})
                    helper.call('RESTORE_CLIENT', {'handle':handle, 'reason':'user_stop'})
                    result = {'restored':handle}
                elif op == 'abort':
                    runtime.call('STOP_IP', {'handle':handle, 'reason':'shutdown'})
                    helper.call('ABORT_CLIENT', {'handle':handle})
                    result, _ = helper.call('RECOVER')
                elif op == 'restore':
                    result, _ = helper.call('RESTORE_CLIENT', {'handle':handle, 'reason':'user_stop'})
                elif op == 'status':
                    result, _ = runtime.call('STATUS')
                elif op == 'kill':
                    proc.kill()
                    proc.wait(timeout=5)
                    result = {'runtime_dead':True}
                elif op == 'owner_eof':
                    helper.sock.close()
                    runtime.sock.close()
                    connection.sendall(json.dumps({'ok':True, 'result':{'owner_closed':True}}).encode())
                    return
                elif op == 'shutdown':
                    result, _ = runtime.call('PREPARE_SHUTDOWN')
                    proc.wait(timeout=8)
                    assert proc.returncode == 0
                    if helper:
                        helper.sock.close()
                    if helper_proc:
                        helper_proc.wait(timeout=8)
                        assert helper_proc.returncode == 0
                    connection.sendall(json.dumps({'ok':True, 'result':result}).encode())
                    return
                else:
                    raise ValueError(op)
                connection.sendall(json.dumps({'ok':True, 'result':result}).encode())
            except Exception as error:
                traceback.print_exc()
                connection.sendall(json.dumps({'ok':False, 'error':str(error)}).encode())


def bootstrap(role):
    RUN.mkdir(exist_ok=True)
    RUN.chmod(0o755)
    (RUN/'owner').mkdir(mode=0o700, exist_ok=True)
    if role != 'server':
        os.chown(RUN/'owner', 10001, 10001)
    master = json.loads((HERE / 'master.json').read_text())
    (RUN/'profile').mkdir(mode=0o700, exist_ok=True)
    os.chown(RUN/'profile',0,0)
    if (RUN/'profile'/'runtime.json').exists(): os.chown(RUN/'profile'/'runtime.json',0,0,follow_symlinks=False)
    private(RUN/'profile'/'runtime.json', master['runtime'], 10001)
    os.chown(RUN/'profile', 10001, 10001)
    if (RUN/'runtime.log').exists(): os.chown(RUN/'runtime.log',0,0,follow_symlinks=False)
    (RUN / 'runtime.log').touch(mode=0o600)
    if role != 'server':
        os.chown(RUN / 'runtime.log', 10001, 10001)
    if role == 'server':
        Path(master['helper']['state_dir']).mkdir(mode=0o700, exist_ok=True)
        private(RUN / 'helper.json', master['helper'])
        serve(role)
    else:
        state = Path('/var/lib/skvoz-network-helper/10001')
        state.parent.mkdir(exist_ok=True)
        state.parent.chmod(0o711)
        state.mkdir(mode=0o700, exist_ok=True)
        private(RUN / 'helper.json', {'v':1,'role':'client','state_dir':str(state),'policy':None})
        Path('/run/dbus').mkdir(mode=0o755,exist_ok=True)
        if not Path('/run/dbus/system_bus_socket').exists():
            subprocess.Popen(['dbus-daemon', '--system', '--nofork'], stdout=open(RUN/'dbus.log','wb'), stderr=subprocess.STDOUT)
            time.sleep(.3)
            subprocess.Popen(['/usr/lib/polkit-1/polkitd', '--no-debug'], stdout=open(RUN/'polkit.log','wb'), stderr=subprocess.STDOUT)
            resolve = pwd.getpwnam('systemd-resolve')
            Path('/run/systemd/resolve').mkdir(mode=0o755,parents=True,exist_ok=True)
            os.chown('/run/systemd/resolve',resolve.pw_uid,resolve.pw_gid)
            subprocess.Popen(['setpriv',f'--reuid={resolve.pw_uid}',f'--regid={resolve.pw_gid}','--clear-groups','--no-new-privs','--bounding-set=-all,+net_raw,+net_bind_service,+setpcap','--inh-caps=+net_raw,+net_bind_service,+setpcap','--ambient-caps=+net_raw,+net_bind_service,+setpcap','/usr/lib/systemd/systemd-resolved'], stdout=open(RUN/'resolved.log','wb'), stderr=subprocess.STDOUT)
            time.sleep(1)
        root = Path('/run/skvoz-network-helper/10001')
        root.parent.mkdir(exist_ok=True)
        root.parent.chmod(0o711)
        root.mkdir(mode=0o711, exist_ok=True)
        endpoint = root / 'control.sock'
        endpoint.unlink(missing_ok=True)
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(endpoint))
        endpoint.chmod(0o600)
        os.chown(endpoint, 10001, 10001)
        listener.listen(1)
        pid = os.fork()
        if pid == 0:
            os.dup2(listener.fileno(), 3)
            os.set_inheritable(3, True)
            env = dict(os.environ, LISTEN_PID=str(os.getpid()), LISTEN_FDS='1', LISTEN_FDNAMES='control')
            log = os.open(RUN/'helper.log', os.O_CREAT|os.O_WRONLY|os.O_APPEND, 0o600)
            os.dup2(log, 1)
            os.dup2(log, 2)
            os.execvpe('setpriv', NARROW + [HELPER,'--config',str(RUN/'helper.json'),'--listen-fd','3'], env)
        (RUN/'helper.pid').write_text(str(pid))
        (RUN/'helper.pid').chmod(0o644)
        listener.close()
        time.sleep(.2)
        os.execvp('setpriv', DROP + ['python3', str(HERE/'owner.py'), 'client'])


if sys.argv[1] == 'command':
    peer = socket.socket(socket.AF_UNIX)
    peer.settimeout(35)
    peer.connect(str(RUN/'owner'/'command.sock'))
    peer.sendall(sys.argv[2].encode())
    output = peer.recv(32768)
    print(output.decode())
    sys.exit(0 if json.loads(output)['ok'] else 1)
elif sys.argv[1] == 'bootstrap':
    bootstrap(sys.argv[2])
else:
    serve(sys.argv[1])
