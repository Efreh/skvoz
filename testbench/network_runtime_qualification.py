"""Real isolated Linux runtime/helper/API1 IP qualification, owned by run.py."""
import io
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time


def qualify(root, settings, certificate_factory):
    ROOT = root
    HERE = ROOT/'testbench/fixtures/network-runtime'
    TOKEN = secrets.token_hex(4)
    PREFIX = 'skvoz-joint-'+TOKEN
    (ROOT/'temp').mkdir(mode=0o700,exist_ok=True)
    OUT = Path(tempfile.mkdtemp(prefix='skvoz-joint-'+TOKEN+'-',dir=ROOT/'temp'))
    containers, networks = [], []
    results = []
    number = int(TOKEN[:2], 16)
    U, E = f'198.18.{number}', f'198.19.{number}'
    V = f'2001:db8:{int(TOKEN[:4],16):x}:{int(TOKEN[4:],16):x}'
    U6 = '2001:db8:600:'+TOKEN[:4]+'::'
    POOL4, POOL6 = '10.203.0.0/24', '2001:db8:203::/64'
    roles = {}


    def run(*args, check=True, timeout=60):
        result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(str(args[:4])+': '+result.stdout+result.stderr)
        return result.stdout


    def exec_(role, *args, **kwargs):
        return run('docker','exec',roles[role],*args,**kwargs)


    def copy(role, source, destination):
        data = source.read_bytes()
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w') as tar:
            info = tarfile.TarInfo(Path(destination).name)
            info.uid = info.gid = 0
            info.mode = 0o600
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        subprocess.run(['docker','cp','-',roles[role]+':'+str(Path(destination).parent)], input=archive.getvalue(), check=True, capture_output=True)



    def api(role, op, **args):
        value = json.dumps({'op':op, **args})
        user = '0' if role == 'server' else '10001:10001'
        out = run('docker','exec','--user',user,roles[role], 'python3','/fixture/owner.py','command',value)
        result = json.loads(out)
        assert result['ok'], result
        return result['result']


    def owner_ready(role):
        log = exec_(role,'cat','/fixture/owner.log')
        if 'Traceback' in log: raise RuntimeError(log)
        return 'OWNER READY' in log


    def physical(role, address, expected):
        script="import socket,sys; s=socket.socket(socket.AF_INET6 if ':' in sys.argv[1] else socket.AF_INET); s.settimeout(1); s.setsockopt(socket.SOL_SOCKET,socket.SO_BINDTODEVICE,b'eth0\\0'); s.connect((sys.argv[1],9000)); s.sendall(b'physical'); assert s.recv(8)==b'physical'"
        result=subprocess.run(['docker','exec',roles[role],'python3','-c',script,address],capture_output=True,text=True,timeout=5)
        assert (result.returncode==0)==expected, result.stdout+result.stderr


    def wait(predicate, seconds=35):
        until = time.monotonic()+seconds
        while time.monotonic() < until:
            if predicate(): return
            time.sleep(.1)
        raise TimeoutError('fixture readiness')


    def private(path, value):
        path.write_text(json.dumps(value))
        path.chmod(0o600)


    def runtime(peer):
        value = json.loads((ROOT/'network/tests/fixtures/client-startup.json').read_text())
        value['network']['families'] = [4,6]
        core = value['core']
        core.update(url=f'tls://{U}.2:4222', tls_server_name='localhost', trust='managed_ca', ca_file='/fixture/ca.pem',
                    username=f'p{peer}', password=passwords[peer], namespace=f'skvoz.runtime.{TOKEN}.joint', peer_id=str(peer))
        if peer == 0:
            value['role']='server'
            value['network']['max_mtu']=1400
            core.update(membership='broker_authorized',allowed_peers=[],initiate=[])
            limits=value['network']['limits']
            limits.update(ip_sessions=128,core_streams=512,lease_identities=4096,core_receive_bytes=67108864,core_send_bytes=67108864,runtime_buffer_bytes=268435456,runtime_buffer_records=65536)
            value['server'] = {'ipv4':{'pool':POOL4,'egress':'nat44','interface':'eth1'},
                'ipv6':{'pool':POOL6,'egress':'routed','interface':'eth1'},
                'dns_servers':[E+'.30'], 'allow':[{'cidr':E+'.0/24','protocols':'any','ports':None},{'cidr':V+'::/64','protocols':'any','ports':None}],
                'deny':[], 'service_prefixes':[], 'lease_store':'/var/lib/skvoz-network/leases.json',
                'server_addresses':[U+'.20',E+'.20',V+'::20'],
                'management_endpoints':[{'address':U+'.20','protocol':6,'port':4222}]}
        return value


    def save_logs():
        for role in roles:
            copied = subprocess.run(['docker','cp',roles[role]+':/run/skvoz-joint','-'],capture_output=True)
            if copied.returncode == 0:
                with tarfile.open(fileobj=io.BytesIO(copied.stdout)) as tar:
                    for info in tar.getmembers():
                        if not info.isfile() or Path(info.name).suffix!='.log': continue
                        name = Path(info.name)
                        assert not name.is_absolute() and '..' not in name.parts
                        path = OUT/role/name
                        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        path.write_bytes(tar.extractfile(info).read())
                        path.chmod(0o600)

            if role in ('server','client'):
                for label,command in [('nft',['nft','-j','list','table','inet','skvoz_network']),('nft-terse',['nft','-j','-t','list','table','inet','skvoz_network']),('links',['ip','-d','-j','address','show']),('routes4',['ip','-N','-4','-j','route','show','table','all']),('routes6',['ip','-N','-6','-j','route','show','table','all'])]:
                    (OUT/(role+'-'+label+'.json')).write_text(exec_(role,*command,check=False))
            for name in ['owner.log','target.log','nats.log','dns.log']:
                output = run('docker','exec',roles[role],'cat','/fixture/'+name,check=False)
                if output: (OUT/(role+'-'+name)).write_text(output)
            (OUT/(role+'-container.log')).write_text(run('docker','logs',roles[role],check=False))


    passwords=[secrets.token_hex(24) for _ in range(3)]
    try:
        certificate_factory(OUT)
        users=[]
        ns=f'skvoz.runtime.{TOKEN}.joint'
        for peer,pw in enumerate(passwords):
            pub = [f'{ns}.join.*.0',f'{ns}.lane.*.*.0.*.0.*'] if peer==0 else [f'{ns}.join.0.{peer}',f'{ns}.lane.0.*.{peer%8}.*.{peer}.*']
            sub = [f'{ns}.join.0.*',f'{ns}.lane.0.*.*.*.*.*'] if peer==0 else [f'{ns}.join.{peer}.*',f'{ns}.lane.{peer}.*.*.*.*.*']
            users.append({'user':f'p{peer}','password':pw,'permissions':{'publish':pub,'subscribe':sub}})
        private(OUT/'nats.json',{'port':4222,'http_port':8222,'max_payload':65588,'max_pending':4194304,
            'tls':{'cert_file':'/fixture/server.pem','key_file':'/fixture/server.key','ca_file':'/fixture/ca.pem','handshake_first':False},
            'authorization':{'users':users}})
        for suffix,args in [('underlay',['--ipv6','--subnet',U+'.0/24','--subnet',U6+'/64']),('egress',['--ipv6','--subnet',E+'.0/24','--subnet',V+'::/64'])]:
            name=PREFIX+'-'+suffix
            run('docker','network','create','--internal','--label','skvoz.joint='+TOKEN,*args,name)
            networks.append(name)
        for role,addr in [('broker',U+'.2'),('server',U+'.20'),('client',U+'.10'),('target',E+'.30')]:
            name=PREFIX+'-'+role
            roles[role]=name
            net=networks[1] if role=='target' else networks[0]
            args=['docker','run','-d','--name',name,'--label','skvoz.joint='+TOKEN,'--pull=never','--network',net,'--ip',addr,
                  '--user','0','--cap-drop','ALL','--security-opt','no-new-privileges:true','--memory','1g','--cpus','2','--pids-limit','128']
            if role!='target':
                suffix={'broker':'2','server':'20','client':'10'}[role]
                args += ['--ip6',U6+suffix]
            if role in ('server','client'):
                for cap in ['NET_ADMIN','SETUID','SETGID','SETPCAP','CHOWN','NET_RAW','NET_BIND_SERVICE']:
                    args += ['--cap-add',cap]
                args += ['--device','/dev/net/tun','--sysctl','net.ipv4.ipfrag_high_thresh=4194304','--sysctl','net.ipv4.ipfrag_time=15',
                    '--sysctl','net.ipv6.ip6frag_high_thresh=4194304','--sysctl','net.ipv6.ip6frag_time=15']
            if role=='server':
                args += ['--sysctl','net.ipv4.ip_forward=1','--sysctl','net.ipv6.conf.all.forwarding=1',
                    '--sysctl','net.netfilter.nf_conntrack_frag6_timeout=15','--sysctl','net.netfilter.nf_conntrack_frag6_high_thresh=4194304']
            if role=='target':
                args += ['--ip6',V+'::30','--cap-add','NET_ADMIN','--cap-add','NET_RAW']
            args += ['skvoz-network:runtime-fixture']
            run(*args)
            containers.append(name)
            exec_(role,'mkdir','-p','/fixture')
            exec_(role,'chmod','0755','/fixture')
            if role in ('server','client'):
                copy(role,ROOT/'target/release/skvoz-network-helper','/usr/local/bin/skvoz-network-helper')
                exec_(role,'chmod','0555','/usr/local/bin/skvoz-network-helper')
                copy(role,ROOT/'target/release/skvoz-network-runtime','/usr/local/bin/skvoz-network-runtime')
                exec_(role,'chmod','0555','/usr/local/bin/skvoz-network-runtime')
            copy(role,HERE/'owner.py','/fixture/owner.py')
            exec_(role,'chmod','0444','/fixture/owner.py')
            copy(role,OUT/'ca.pem','/fixture/ca.pem')
            exec_(role,'chmod','0444','/fixture/ca.pem')
        run('docker','network','connect','--ip',E+'.20','--ip6',V+'::20',networks[1],roles['server'])
        exec_('server','ip','-6','route','add','default','via',V+'::1','dev','eth1')
        exec_('target','ip','-6','route','add',POOL6,'via',V+'::20')
        exec_('target','ip','route','add',U+'.0/24','via',E+'.20')
        exec_('target','ip','-6','route','add',U6+'/64','via',V+'::20')
        exec_('client','ip','route','add',E+'.0/24','via',U+'.20')
        exec_('client','ip','-6','route','add',V+'::/64','via',U6+'20')
        for name in ['server.pem','server.key','nats.json']:
            copy('broker',OUT/name,'/fixture/'+name)
            exec_('broker','chmod','0600','/fixture/'+name)
        run('docker','exec','-d',roles['broker'],'sh','-c','exec nats-server -c /fixture/nats.json >/fixture/nats.log 2>&1')
        wait(lambda:'Server is ready' in exec_('broker','cat','/fixture/nats.log'))
        copy('target',ROOT/'testbench/fixtures/network/traffic.py','/fixture/traffic.py')
        copy('client',ROOT/'testbench/fixtures/network/traffic.py','/fixture/traffic.py')
        exec_('client','chmod','0444','/fixture/traffic.py')
        run('docker','exec','-d',roles['target'],'sh','-c','exec python3 /fixture/traffic.py server >/fixture/target.log 2>&1')
        wait(lambda:'TARGET READY' in exec_('target','cat','/fixture/target.log'))
        copy('target',HERE/'dns.py','/fixture/dns.py')
        run('docker','exec','-d',roles['target'],'sh','-c',f'exec python3 /fixture/dns.py {E}.30 {V}::30 >/fixture/dns.log 2>&1')
        wait(lambda:'DNS READY' in exec_('target','cat','/fixture/dns.log'))
        for addr in [E+'.30',V+'::30']:
            physical('client',addr,True)
        results.append({'group':'physical-baseline-before-capture','status':'passed'})
        for role,peer in [('server',0),('client',1)]:
            config=runtime(peer)
            master={'runtime':config}
            if role=='server':
                master['helper']={'v':1,'role':'server','state_dir':'/var/lib/skvoz-network','policy':{'network':config['network'],'server':config['server']}}
            private(OUT/(role+'-master.json'),master)
            copy(role,OUT/(role+'-master.json'),'/fixture/master.json')
            exec_(role,'chmod','0600','/fixture/master.json')
            run('docker','exec','-d',roles[role],'sh','-c',f'exec python3 /fixture/owner.py bootstrap {role} >/fixture/owner.log 2>&1')
            wait(lambda role=role:owner_ready(role))
        for role in ['server','client']:
            ready=next(line[12:] for line in exec_(role,'cat','/fixture/owner.log').splitlines() if line.startswith('OWNER READY '))
            pids=json.loads(ready)
            for kind,pid in [('runtime',pids['runtime_pid']),('helper',pids['helper_pid'] or int(exec_(role,'cat','/run/skvoz-joint/helper.pid')))]:
                status=exec_(role,'cat',f'/proc/{pid}/status')
                fields=dict(line.split(':',1) for line in status.splitlines() if ':' in line)
                for cap in ['CapInh','CapPrm','CapEff','CapBnd','CapAmb']:
                    expected=4096 if kind=='helper' and cap in ['CapPrm','CapEff','CapBnd'] else 0
                    assert int(fields[cap].strip(),16)==expected,(kind,cap,fields[cap])
                assert fields['NoNewPrivs'].strip()=='1'
                uid='0' if kind=='helper' else '10001'
                assert all(value==uid for value in fields['Uid'].split())
        results.append({'group':'strict-runtime-helper-startup-UID-capabilities-NNP','status':'passed'})
        configured=api('client','start',broker=U+'.2',max_mtu=1280)
        print('ACTIVE', json.dumps(configured),flush=True)
        for role,expected in [('server',1400),('client',1280)]:
            links=json.loads(exec_(role,'ip','-j','address','show'))
            actual=next(link['mtu'] for link in links if link['ifname']=='skvoz0')
            assert actual==expected, f'{role} TUN MTU {actual} != negotiated {expected}'
        results.append({'group':'dual-stack-config-helper-attach-active','status':'passed'})
        for addr in [E+'.30',V+'::30']:
            physical('client',addr,False)
        results.append({'group':'no-physical-ipv4-ipv6-leak-while-active','status':'passed'})
        for kind,address in [('A',E+'.30'),('AAAA',V+'::30')]:
            answer=exec_('client','resolvectl','query','--cache=no','--type='+kind,'payload.skvoz.test')
            assert address in answer, answer
        results.append({'group':'real-resolved-A-AAAA-through-tun','status':'passed'})
        for address in [E+'.30',V+'::30']:
            output=exec_('client','ping','-n','-c','2','-W','2',address)
            assert '0% packet loss' in output, output
        results.append({'group':'kernel-icmp-echo-ipv4-ipv6-through-tun','status':'passed'})
        for addr in [E+'.30',V+'::30']:
            for kind,options in [('tcp',['--bytes','1048576']),('udp',['--bytes','64','--samples','50','--interval','.01']),('udp',['--bytes','4096','--samples','5']),('raw',[])]:
                if kind=='udp' and options[1]=='4096':
                    warmup=exec_('client','python3','/fixture/traffic.py',kind,addr,'--bytes','4096','--samples','5','--interval','.05')
                    print('FRAGMENT WARMUP',warmup,flush=True)
                    results.append({'group':'fragment-pmtu-warmup-'+addr,'status':'diagnostic','metrics':json.loads(warmup)})
                output=exec_('client','python3','/fixture/traffic.py',kind,addr,*options)
                print(output,flush=True)
                metrics=json.loads(output)
                if kind=='udp': assert metrics['lost']==0, metrics
                results.append({'group':kind+'-'+addr+'-'+str(metrics.get('bytes','')),'status':'passed','metrics':metrics})
        observed=exec_('target','cat','/fixture/target.log')
        pmtu=[json.loads(line[14:]) for line in observed.splitlines() if line.startswith('OBSERVED_PMTU ')]
        assert any(e['mtu']==1280 for e in pmtu), pmtu
        results.append({'group':'kernel-icmpv6-PTB-to-negotiated-MTU','status':'passed','events':pmtu})
        events=[json.loads(line[9:]) for line in observed.splitlines() if line.startswith('OBSERVED ')]
        assert any(e['family']==4 and e['source']==E+'.20' for e in events), events
        grant6=configured['config']['source_grants'][1].split('/')[0]
        assert any(e['family']==6 and e['source']==grant6 for e in events), events
        results.append({'group':'observed-nat44-and-native-routed-ipv6-source','status':'passed'})
        api('client','abort')
        for addr in [E+'.30',V+'::30']:
            physical('client',addr,False)
        assert 'skvoz0' not in exec_('client','ip','-j','address','show')
        assert 'skvoz_network' in exec_('client','nft','list','tables')
        results.append({'group':'accidental-stop-retains-guard-removes-tun-routes-dns','status':'passed'})
        stopped=api('client','restore')
        print('STOPPED',json.dumps(stopped),flush=True)
        assert 'skvoz_network' not in exec_('client','nft','list','tables')
        assert not any(str(r.get('protocol'))=='186' for family in ['-4','-6'] for r in json.loads(exec_('client','ip','-N',family,'-j','route','show','table','all')))
        for addr in [E+'.30',V+'::30']:
            physical('client',addr,True)
        results.append({'group':'explicit-stop-kernel-cleanup-restores-underlay','status':'passed'})
        configured=api('client','start',broker=U+'.2',max_mtu=1280)
        old_handle=configured['handle']
        old_grants=configured['config']['source_grants']
        started=time.monotonic()
        api('client','owner_eof')
        wait(lambda:'skvoz0' not in exec_('client','ip','-j','address','show'),seconds=3)
        elapsed=time.monotonic()-started
        for address in [E+'.30',V+'::30']:
            physical('client',address,False)
        assert 'skvoz_network' in exec_('client','nft','list','tables')
        results.append({'group':'owner-EOF-fail-closed-live-cleanup','status':'passed','seconds':elapsed})
        run('docker','exec','-d',roles['client'],'sh','-c','exec python3 /fixture/owner.py bootstrap client >/fixture/owner.log 2>&1')
        wait(lambda:owner_ready('client'))
        ready=next(line[12:] for line in exec_('client','cat','/fixture/owner.log').splitlines() if line.startswith('OWNER READY '))
        assert json.loads(ready)['recovered']=={'state':'guarded','handle':old_handle}
        api('client','restore')
        restored=api('client','start',broker=U+'.2',max_mtu=1280)
        assert restored['config']['source_grants']==old_grants
        assert restored['handle']!=old_handle
        output=exec_('client','python3','/fixture/traffic.py','tcp',E+'.30','--bytes','65536')
        results.append({'group':'authorized-owner-recovery-persistent-grants-new-session','status':'passed','metrics':json.loads(output)})
        crashed_handle=restored['handle']
        helper_pid=int(exec_('client','cat','/run/skvoz-joint/helper.pid'))
        exec_('client','kill','-KILL',str(helper_pid))
        api('client','owner_eof')
        for address in [E+'.30',V+'::30']:
            physical('client',address,False)
        run('docker','exec','-d',roles['client'],'sh','-c','exec python3 /fixture/owner.py bootstrap client >/fixture/owner.log 2>&1')
        wait(lambda:owner_ready('client'))
        ready=next(line[12:] for line in exec_('client','cat','/fixture/owner.log').splitlines() if line.startswith('OWNER READY '))
        assert json.loads(ready)['recovered']=={'state':'guarded','handle':crashed_handle}
        assert 'skvoz0' not in exec_('client','ip','-j','address','show')
        for address in [E+'.30',V+'::30']:
            physical('client',address,False)
        api('client','restore')
        recovered=api('client','start',broker=U+'.2',max_mtu=1280)
        assert recovered['config']['source_grants']==old_grants
        assert recovered['handle']!=crashed_handle
        output=exec_('client','python3','/fixture/traffic.py','tcp',V+'::30','--bytes','65536')
        results.append({'group':'helper-SIGKILL-journal-recovery-retains-guard-and-grants','status':'passed','metrics':json.loads(output)})
        api('client','stop')
        api('client','shutdown')
        api('server','shutdown')
        assert 'skvoz0' not in exec_('server','ip','-j','address','show')
        assert 'skvoz_network' not in exec_('server','nft','list','tables')
        results.append({'group':'server-shutdown-kernel-cleanup','status':'passed'})
    except Exception as error:
        results.append({'group':'failure','error':str(error)})
        raise
    finally:
        original_error = sys.exc_info()[1]
        cleanup_errors = []
        try:
            save_logs()
        except Exception as error:
            cleanup_errors.append(str(error))
        for name in reversed(containers):
            try:
                run('docker','rm','-f',name)
            except Exception as error:
                cleanup_errors.append(str(error))
        for name in reversed(networks):
            try:
                run('docker','network','rm',name)
            except Exception as error:
                cleanup_errors.append(str(error))
        (OUT/'report.json').write_text(json.dumps({'groups':results,'prefix':PREFIX, 'cleanup_errors':cleanup_errors},indent=2))
        for path in OUT.rglob('*'):
            if path.is_file(): path.chmod(0o600)
        if settings.report_directory:
            destination=settings.report_directory.resolve()
            destination.mkdir(mode=0o700,parents=True,exist_ok=True)
            destination.chmod(0o700)
            snapshot_names={f'{role}-{label}.json' for role in ('server','client') for label in ('nft','nft-terse','links','routes4','routes6')}
            for path in OUT.rglob('*'):
                if path.is_file() and (path.suffix=='.log' or path.name=='report.json' or path.name in snapshot_names):
                    relative=path.relative_to(OUT)
                    target=destination/relative
                    target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
                    shutil.copyfile(path,target)
                    target.chmod(0o600)
            print('EVIDENCE',destination,flush=True)
        shutil.rmtree(OUT)
        if cleanup_errors and original_error is None:
            raise RuntimeError('Fixture cleanup failed: '+ '; '.join(cleanup_errors))
