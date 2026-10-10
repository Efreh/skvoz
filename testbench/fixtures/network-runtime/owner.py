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
import threading
import traceback

HERE = Path('/fixture')
RUN = Path('/run/skvoz-joint')
RUNTIME = '/usr/local/bin/skvoz-network-runtime'
HELPER = '/usr/local/bin/skvoz-network-helper'
DROP = ['setpriv', '--reuid=10001', '--regid=10001', '--clear-groups', '--no-new-privs', '--bounding-set=-all', '--inh-caps=-all', '--ambient-caps=-all']
NARROW = ['setpriv', '--no-new-privs', '--bounding-set=-all,+net_admin', '--inh-caps=-all', '--ambient-caps=-all']


def as_uid(uid, operation):
    """Create private fixture files as their actual owner, without CHOWN."""
    if not uid:
        return operation()
    pid = os.fork()
    if pid == 0:
        try:
            os.setgroups([])
            os.setgid(pwd.getpwuid(uid).pw_gid)
            os.setuid(uid)
            operation()
            os._exit(0)
        except BaseException:
            traceback.print_exc()
            os._exit(1)
    _, status = os.waitpid(pid, 0)
    assert status == 0, 'scoped owner creation failed'


def owned_directory(path, uid, mode=0o700):
    if path.exists() and path.stat().st_uid != uid:
        # Package installation may leave this isolated runtime directory root-owned.
        # Recreate only an empty directory; never add CHOWN or override access.
        assert path.stat().st_uid == os.getuid() and not path.is_symlink()
        path.rmdir()
    parent = path.parent
    original = parent.stat().st_mode & 0o7777
    parent.chmod(0o733)
    try:
        as_uid(uid, lambda: path.mkdir(mode=mode, exist_ok=True))
        assert path.stat().st_uid == uid
    finally:
        parent.chmod(original)


def private(path, value, uid=0):
    def write():
        path.write_text(json.dumps(value))
        path.chmod(0o600)
    as_uid(uid, write)


def owned_listener(root, uid):
    """Prepare the systemd-equivalent listener before any helper admission."""
    endpoint = root / 'control.sock'
    endpoint.unlink(missing_ok=True)
    parent, child = socket.socketpair()
    # Only this isolated setup phase is writable by the fixture app. Restore
    # the root directory boundary before the product validates the listener.
    root.chmod(0o733)
    pid = os.fork()
    if pid == 0:
        try:
            parent.close()
            os.setgroups([]); os.setgid(uid); os.setuid(uid)
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(str(endpoint)); endpoint.chmod(0o600); listener.listen(1)
            child.sendmsg([b'L'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                   array.array('i', [listener.fileno()]))])
            os._exit(0)
        except BaseException:
            traceback.print_exc(); os._exit(1)
    child.close()
    try:
        marker, controls, flags, _ = parent.recvmsg(1, socket.CMSG_SPACE(4))
        _, status = os.waitpid(pid, 0)
        assert status == 0 and marker == b'L' and not flags & socket.MSG_CTRUNC
        assert len(controls) == 1 and controls[0][:2] == (socket.SOL_SOCKET, socket.SCM_RIGHTS)
        descriptors = array.array('i'); descriptors.frombytes(controls[0][2])
        assert len(descriptors) == 1
        return socket.socket(fileno=descriptors[0])
    finally:
        parent.close(); root.chmod(0o711)


class Channel:
    def __init__(self, sock):
        self.sock, self.id, self.events = sock, 0, []
        sock.settimeout(None)
        self.condition = threading.Condition()
        self.calls = threading.Lock()
        self.pending, self.reply, self.failure = False, None, None
        self.reader = threading.Thread(target=self.receive, daemon=True)
        self.reader.start()

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

    def receive(self):
        try:
            while True:
                value, fds = self.read()
                with self.condition:
                    if 'event' in value:
                        assert not fds
                        if value['event'] != 'STATS':
                            assert len(self.events) < 128
                            self.events.append(value)
                    else:
                        assert self.pending and value['id'] == self.id and self.reply is None
                        self.reply = (value, fds)
                    self.condition.notify_all()
        except BaseException as error:
            with self.condition:
                self.failure = error
                self.condition.notify_all()

    def healthy(self):
        if self.failure:
            raise self.failure

    def call(self, op, args=None, fd=None):
        with self.calls:
            with self.condition:
                self.healthy()
                self.id += 1
                self.pending = True
            body = json.dumps({'v':1, 'id':self.id, 'op':op, 'args':args or {}, 'fd_count':int(fd is not None)}).encode()
            frame = struct.pack('!I', len(body)) + body
            if fd is None:
                self.sock.sendall(frame)
            else:
                rights = array.array('i', [fd])
                sent = self.sock.sendmsg([frame], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
                self.sock.sendall(frame[sent:])
            until = time.monotonic() + 20
            with self.condition:
                while self.reply is None:
                    self.healthy()
                    remaining = until - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(op)
                    self.condition.wait(remaining)
                (result, fds), self.reply, self.pending = self.reply, None, False
            if result.get('error'):
                for descriptor in fds:
                    os.close(descriptor)
                raise RuntimeError(op + ': ' + result['error'])
            return result['result'], fds

    def event(self, name, handle=None):
        until = time.monotonic() + 25
        with self.condition:
            while True:
                index = 0
                while index < len(self.events):
                    value = self.events[index]
                    if value['event'] == name and (handle is None or value['data'].get('handle') == handle):
                        return self.events.pop(index)['data']
                    if value['event'] == 'CLOSED':
                        self.events.pop(index)
                        if handle is not None and value['data']['handle'] == handle:
                            raise RuntimeError(value['data'].get('error') or 'closed')
                        continue
                    index += 1
                self.healthy()
                remaining = until - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(name)
                self.condition.wait(remaining)


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
    channel.call('HELLO', {'api':1, 'network':5})
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
    channel.call('HELLO', {'api':1, 'network':5})
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
                    result, _ = runtime.call('START_IP', {'family_policy':request.get('family_policy', 'auto'), 'families':request.get('families', [4,6]), 'max_mtu':request.get('max_mtu',1500), 'channels':1})
                    handle = result['handle']
                    configured = runtime.event('CONFIGURED', handle)['config']
                    transport = [{'ip':request['broker'], 'port':4222}]
                    prepared, fds = helper.call('PREPARE_CLIENT', {'handle':handle, 'config':configured, 'transport_endpoints':transport})
                    assert len(fds) == 1
                    try:
                        runtime.call('ATTACH_IP', {'handle':handle, 'interface':prepared['interface'], 'mtu':prepared['mtu']}, fds[0])
                    finally:
                        os.close(fds[0])
                    helper.call('ACTIVATE_CLIENT', {'handle':handle})
                    runtime.call('LOCAL_READY', {'handle':handle})
                    assert runtime.event('ACTIVE', handle)['handle'] == handle
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
                elif op == 'diagnostics':
                    result, _ = runtime.call('STATUS')
                    result = {'status': result, 'events': runtime.events}
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
    owned_directory(RUN/'owner', 0 if role == 'server' else 10001)
    master = json.loads((HERE / 'master.json').read_text())
    owned_directory(RUN/'profile', 10001)
    private(RUN/'profile'/'runtime.json', master['runtime'], 10001)
    # Logs follow the same actual owner as the fixture host; no CHOWN needed.
    original = RUN.stat().st_mode & 0o7777
    RUN.chmod(0o733)
    try:
        as_uid(0 if role == 'server' else 10001, lambda: (RUN/'runtime.log').touch(mode=0o600))
    finally:
        RUN.chmod(original)
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
            Path('/run/systemd').mkdir(mode=0o755, exist_ok=True)
            directory = Path('/run/systemd/resolve')
            if directory.exists() and directory.stat().st_uid == 0:
                # Discard only the package-created stub in this disposable image;
                # real resolved recreates it under its scoped runtime owner.
                stub = directory/'stub-resolv.conf'
                if stub.exists():
                    assert set(directory.iterdir()) == {stub} and not stub.is_symlink()
                    assert stub.is_file() and stub.stat().st_uid == 0 and stub.stat().st_size <= 4096
                    stub.unlink()
            owned_directory(directory, resolve.pw_uid, 0o755)
            subprocess.Popen(['setpriv',f'--reuid={resolve.pw_uid}',f'--regid={resolve.pw_gid}','--clear-groups','--no-new-privs','--bounding-set=-all,+net_raw,+net_bind_service,+setpcap','--inh-caps=+net_raw,+net_bind_service,+setpcap','--ambient-caps=+net_raw,+net_bind_service,+setpcap','/usr/lib/systemd/systemd-resolved'], stdout=open(RUN/'resolved.log','wb'), stderr=subprocess.STDOUT)
            time.sleep(1)
        root = Path('/run/skvoz-network-helper/10001')
        root.parent.mkdir(exist_ok=True)
        root.parent.chmod(0o711)
        root.mkdir(mode=0o711, exist_ok=True)
        listener = owned_listener(root, 10001)
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
