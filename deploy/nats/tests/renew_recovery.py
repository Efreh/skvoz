#!/usr/bin/env python3
"""Execute public renew.sh against exact simulated lego registration outcomes.

Only ACME/state recovery is tested here. Issuance, TLS apply and service control
are explicit seams; this is not a live-issuer or serving-certificate test.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

SCRIPT = Path(__file__).resolve().parents[1] / 'renew.sh'
EMAIL = 'entry@example.invalid'
SERVER = 'https://issuer.invalid/dir'
MOCK = '''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
root=Path(os.environ['SKVOZ_NATS_STATE']);server='https://issuer.invalid/dir';email='entry@example.invalid'
folder=root/'acme/accounts/issuer.invalid'/email;folder.mkdir(parents=True,exist_ok=True)
account=folder/'account.json';key=folder/(email+'.key')
with (root/'attempts').open('a') as out:out.write('attempt\\n')
mode=os.environ['UPSTREAM_RESULT']
if not account.exists():
 key.write_text('unregistered-private-key');key.chmod(0o600)
 account.write_text(json.dumps(dict(id=email,email=email,keyType='ec256',server=server,registration=None),indent='\\t'));account.chmod(0o600)
if mode=='transport':
 print('temporary issuer transport failure',file=sys.stderr);sys.exit(17)
if mode=='unknown':
 print('resolve account by key: urn:ietf:params:acme:error:accountDoesNotExist',file=sys.stderr);sys.exit(18)
record=json.loads(account.read_text());record['registration']={'status':'valid'}
account.write_text(json.dumps(record,indent='\\t'))
(root/'acme/certificates').mkdir(exist_ok=True)
(root/'acme/certificates/entry.invalid.crt').write_text('new-certificate')
(root/'acme/certificates/entry.invalid.key').write_text('new-certificate-key')
'''


def run(log_directory=None):
    facts = []
    with tempfile.TemporaryDirectory(prefix='skvoz-renew-recovery-') as directory:
        base = Path(directory)
        bindir = base / 'bin'
        bindir.mkdir()
        for name, body in {
            'lego': MOCK,
            'openssl': '#!/bin/sh\nexit 1\n',
            'systemctl': '#!/bin/sh\nexit 0\n',
        }.items():
            path = bindir / name
            path.write_text(body)
            path.chmod(0o700)
        shim = base / 'seams.sh'
        shim.write_text('function /usr/local/lib/skvoz-nats/tls.sh { printf "applied\\n" >> "$SKVOZ_NATS_STATE/applied"; }\n')
        env = dict(os.environ, PATH=str(bindir) + ':' + os.environ['PATH'], BASH_ENV=str(shim),
                   SKVOZ_ACME_EMAIL=EMAIL, SKVOZ_ACME_TERMS_AGREED='true', SKVOZ_ACME_DIRECTORY=SERVER)

        def state(name, registration=None, server=SERVER):
            root = base / name
            root.mkdir(mode=0o700)
            (root / 'address').write_text('entry.invalid\n')
            cert = root / 'tls/current/certificate.pem'
            cert.parent.mkdir(parents=True)
            cert.write_bytes(b'existing-certificate')
            folder = root / 'acme/accounts/issuer.invalid' / EMAIL
            folder.mkdir(parents=True)
            if registration is not None or server != SERVER:
                (folder / 'account.json').write_text(json.dumps(dict(id=EMAIL, email=EMAIL, keyType='ec256', server=server, registration=registration), indent='\t'))
                (folder / (EMAIL + '.key')).write_bytes(b'existing-private-key')
            return root, folder, cert

        def invoke(root, mode, name):
            current = dict(env, SKVOZ_NATS_STATE=str(root), UPSTREAM_RESULT=mode)
            result = subprocess.run(['bash', str(SCRIPT)], env=current, capture_output=True, timeout=5)
            if log_directory:
                path = log_directory / (name + '.log')
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, 'wb') as out:
                    out.write(result.stdout + result.stderr)
            return result

        root, folder, cert = state('fresh')
        first = invoke(root, 'transport', 'first-transport')
        assert first.returncode == 17
        account = folder / 'account.json'
        key = folder / (EMAIL + '.key')
        assert json.loads(account.read_text())['registration'] is None and key.exists()
        original = (account.read_bytes(), key.read_bytes(), cert.read_bytes())
        unknown = invoke(root, 'unknown', 'unknown-account')
        assert unknown.returncode == 18 and b'accountDoesNotExist' in unknown.stderr
        backups = list((root / 'acme').glob('unregistered-*'))
        assert len(backups) == 1 and (backups[0].stat().st_mode & 0o777) == 0o700
        assert (backups[0] / 'account.json').read_bytes() == original[0]
        assert (backups[0] / 'account.key').read_bytes() == original[1]
        assert not account.exists() and not key.exists() and cert.read_bytes() == original[2]
        success = invoke(root, 'success', 'next-success')
        assert success.returncode == 0 and (root / 'applied').read_text() == 'applied\n'
        assert json.loads(account.read_text())['registration']['status'] == 'valid'
        assert len((root / 'attempts').read_text().splitlines()) == 3
        facts.append({'case': 'first_failure_unknown_account_next_retry', 'exits': [17, 18, 0], 'attempts': 3,
                      'orphan_material_preserved': True, 'old_certificate_untouched_before_success': True})

        for name, registration, server in [
            ('registered', {'status': 'valid'}, SERVER),
            ('wrong-issuer', None, 'https://issuer.invalid/other-dir'),
        ]:
            root, folder, cert = state(name, registration, server)
            paths = [folder / 'account.json', folder / (EMAIL + '.key'), cert]
            before = [p.read_bytes() for p in paths]
            result = invoke(root, 'unknown', name)
            assert result.returncode == 18
            assert [p.read_bytes() for p in paths] == before
            assert not list((root / 'acme').glob('unregistered-*'))
            facts.append({'case': name, 'exit': 18, 'all_existing_material_preserved': True})

        root, folder, cert = state('ambiguous', {'status': 'valid'})
        account = folder / 'account.json'
        account.write_text(account.read_text().replace('"registration": {', '"unexpected": {\n\t"registration": null,\n\t"registered": {'))
        original = account.read_bytes()
        result = invoke(root, 'unknown', 'ambiguous')
        assert result.returncode == 18 and account.read_bytes() == original
        assert not list((root / 'acme').glob('unregistered-*'))
        facts.append({'case': 'ambiguous_nested_null', 'exit': 18, 'preserved': True})
    return {'status': 'passed', 'script_sha256': hashlib.sha256(SCRIPT.read_bytes()).hexdigest(),
            'facts': facts, 'scope': 'Actual public shell; simulated pinned upstream responses. No live issuer/TLS apply claim.',
            'resources': 'stdlib subprocesses only; temporary state and processes closed; no builds/containers/network'}


if __name__ == '__main__':
    logs = Path(sys.argv[1]) if len(sys.argv) == 2 else None
    if logs:
        logs.mkdir(mode=0o700, parents=True, exist_ok=True)
    print(json.dumps(run(logs), indent=2))
