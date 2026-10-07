"""Ordinary server RSpec with consumers in a separate Docker cgroup."""
from pathlib import Path
import secrets
import subprocess


def qualify(root, settings):
    root = Path(root)
    token = secrets.token_hex(6)
    server, consumer, volume = ('skvoz-capacity-'+token+'-'+suffix for suffix in ('server','consumer','state'))
    def command(args, **kwargs):
        return subprocess.run(['docker',*args],cwd=root,check=True,**kwargs)
    try:
        command(['volume','create',volume],stdout=subprocess.DEVNULL)
        command(['run','--rm','--pull=never','--network','none','--user','0:0','--cap-drop','ALL','--cap-add','CHOWN',
            '--security-opt','no-new-privileges:true','--mount',f'type=volume,src={volume},dst=/work',
            '--entrypoint','ruby','skvoz-server:tcp-capacity-tests','-e',
            "File.chmod(0o700, '/work'); File.chown(10001, 10001, '/work')"])
        command(['run','--detach','--name',server,'--pull=never','--network','none','--user','10001:10001',
            '--cap-drop','ALL','--security-opt','no-new-privileges:true','--memory','1g','--cpus','2','--pids-limit','256',
            '--ulimit','nofile=8192:8192','--mount',f'type=volume,src={volume},dst=/work',
            '--env','SKVOZ_CAPACITY_DIRECTORY=/work','--env',f'SKVOZ_CAPACITY_HOLD={settings.tcp_capacity_hold}',
            'skvoz-server:tcp-capacity-tests','bundle','exec','rspec','spec/capacity_spec.rb'],stdout=subprocess.DEVNULL)
        command(['run','--rm','--name',consumer,'--pull=never','--network','container:'+server,'--user','10001:10001',
            '--cap-drop','ALL','--security-opt','no-new-privileges:true','--memory','512m','--cpus','2','--pids-limit','512',
            '--ulimit','nofile=8192:8192','--mount',f'type=volume,src={volume},dst=/work',
            '--mount',f'type=bind,src={root}/target/release/skvoz-network-runtime,dst=/runtime,readonly',
            '--mount',f'type=bind,src={root}/testbench,dst=/tests,readonly','skvoz-network:runtime-fixture',
            'python3','/tests/tcp_capacity_server.py','--directory','/work','--binary','/runtime','--cycles',str(settings.tcp_capacity_cycles)])
        result = command(['wait',server],capture_output=True,text=True)
        if result.stdout.strip() != '0':
            raise RuntimeError('server resource RSpec failed: '+result.stdout.strip())
    finally:
        # Let RSpec observe consumer completion and export its bounded log tail.
        try:
            subprocess.run(['docker','wait',server],check=False,capture_output=True,timeout=20)
        except subprocess.TimeoutExpired:
            print('Server diagnostic export wait expired after 20 seconds',flush=True)
        subprocess.run(['docker','logs',server],check=False)
        if settings.report_directory:
            settings.report_directory.mkdir(parents=True,exist_ok=True)
            for name in ('server-report.json','consumer-report.json','server.log',*(f'consumer-runtime-{peer}.log' for peer in range(16))):
                subprocess.run(['docker','cp',server+':/work/'+name,str(settings.report_directory/name)],check=False)
        subprocess.run(['docker','rm','--force',consumer,server],capture_output=True)
        subprocess.run(['docker','volume','rm',volume],capture_output=True)
