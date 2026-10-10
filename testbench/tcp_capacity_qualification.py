"""Disposable independent-process capacity orchestration owned by run.py."""
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
from network_topology import users as topology_users
from fixture_resources import docker_limits


def qualify(root, settings, certificates, mode='capacity'):
    root = Path(root)
    token = secrets.token_hex(4)
    parent = root / 'temp'
    parent.mkdir(mode=0o700, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix='skvoz-tcp-capacity-'+token+'-', dir=parent))
    binary = settings.tcp_capacity_baseline_binary or root/'target/release/skvoz-network-runtime'
    name = 'skvoz-tcp-capacity-'+token
    try:
        certificates(directory)
        users = []
        for peer in range(max(settings.clients+1,4) if mode != 'capacity' else settings.clients+1):
            pub = ['tcp_capacity.join.*.0','tcp_capacity.lane.*.*.0.*.0.*'] if peer == 0 else [f'tcp_capacity.join.0.{peer}',f'tcp_capacity.lane.0.*.{peer%8}.*.{peer}.*']
            sub = ['tcp_capacity.join.0.*','tcp_capacity.lane.0.*.*.*.*.*'] if peer == 0 else [f'tcp_capacity.join.{peer}.*',f'tcp_capacity.lane.{peer}.*.*.*.*.*']
            users.append({'user':f'p{peer}', 'password':secrets.token_hex(24), 'permissions':{'publish':pub,'subscribe':sub}})
        if not settings.tcp_capacity_baseline_binary:
            users = topology_users('tcp_capacity', [user['password'] for user in users], secrets.token_hex(24))
        nats = {'port':4222, 'http_port':8222, 'max_payload':65588, 'max_pending':4194304,
            'tls':{'cert_file':'/work/server.pem','key_file':'/work/server.key','ca_file':'/work/ca.pem','handshake_first':False},
            'authorization':{'users':users}}
        (directory/'nats.json').write_text(json.dumps(nats))
        (directory/'nats.json').chmod(0o600)
        fixture = root/'network/tests/fixtures/client-startup.json'
        if settings.tcp_capacity_baseline_binary:
            # The caller supplies the baseline binary and its exact matching profile.
            fixture = settings.tcp_capacity_baseline_profile
        assert fixture and fixture.is_file() and binary.is_file()
        args = ['docker','run','--rm','--name',name,'--pull=never','--network','none','--user',f'{os.getuid()}:{os.getgid()}',
            '--cap-drop','ALL','--security-opt','no-new-privileges:true',
            *docker_limits(root),
            '--mount',f'type=bind,src={directory.resolve()},dst=/work',
            '--mount',f'type=bind,src={binary.resolve()},dst=/runtime,readonly',
            '--mount',f'type=bind,src={root.resolve()}/testbench,dst=/tests,readonly',
            '--mount',f'type=bind,src={fixture.resolve()},dst=/template.json,readonly',
            'skvoz-network:runtime-fixture','python3','/tests/tcp_capacity.py' if mode == 'capacity' else f'/tests/tcp_capacity_{mode}.py','--directory','/work','--binary','/runtime',
            '--template','/template.json']
        if mode == 'capacity':
            args += ['--clients',str(settings.clients),'--idle',str(settings.clients*settings.streams_per_client),
                '--active',str(settings.active_per_client),'--hold',str(settings.tcp_capacity_hold),'--cycles',str(settings.tcp_capacity_cycles)]
        if mode == 'setup':
            args[2:2] = ['--dns','127.0.0.1']
        if settings.tcp_capacity_baseline_binary:
            args.append('--baseline')
        if settings.tcp_capacity_benchmark:
            args += ['--bench','--idle','0','--active','0','--hold','0']
        subprocess.run(args, cwd=root, check=True)
    finally:
        subprocess.run(['docker','rm','--force',name], capture_output=True)
        if settings.report_directory:
            settings.report_directory.mkdir(parents=True, exist_ok=True)
            for path in directory.iterdir():
                if path.name in ('report.json','setup-report.json','overload-report.json','reduced-report.json') or path.name.startswith('runtime-') and path.suffix == '.log':
                    shutil.copy2(path, settings.report_directory / path.name)
        # Never export configurations, CA keys or role passwords with evidence.
        shutil.rmtree(directory)
