#!/usr/bin/env python3
"""Check both packaged ELF ABIs and 16KiB APK alignment without executing them."""
import argparse
from pathlib import Path
import struct
import re
import subprocess
import zipfile


def certificate_sha256(value):
    if not re.fullmatch(r'[0-9a-fA-F]{64}', value):
        raise argparse.ArgumentTypeError('Certificate SHA-256 must be 64 hexadecimal characters without separators')
    return value.lower()


def verify_certificate_pin(output, expected):
    # SDK versions use different signer prefixes. Inspect every certificate,
    # including duplicate scheme reports, without accepting public-key digests.
    marker = 'certificate SHA-256 digest:'
    digests = [line.split(marker, 1)[1].strip() for line in output.splitlines() if marker in line]
    if not digests or any(not re.fullmatch(r'[0-9a-fA-F]{64}', value) for value in digests):
        raise SystemExit('APK signing certificate SHA-256 output missing or malformed')
    if {value.lower() for value in digests} != {expected}:
        raise SystemExit('APK signing certificate SHA-256 mismatch')


parser = argparse.ArgumentParser()
parser.add_argument('apk', type=Path)
parser.add_argument('--build-tools', type=Path, required=True)
parser.add_argument('--version')
parser.add_argument('--version-code', type=int)
parser.add_argument('--certificate-sha256', type=certificate_sha256)
arguments = parser.parse_args()
metadata = subprocess.check_output([str(arguments.build_tools / 'aapt'), 'dump', 'badging', str(arguments.apk)], text=True)
package = re.search(r"^package: name='([^']+)' versionCode='([^']+)' versionName='([^']+)'", metadata, re.MULTILINE)
minimum = re.search(r"^sdkVersion:'([^']+)'", metadata, re.MULTILINE)
target = re.search(r"^targetSdkVersion:'([^']+)'", metadata, re.MULTILINE)
if not package or package[1] != 'org.skvoz.android' or not minimum or minimum[1] != '31' or not target or target[1] != '37':
    raise SystemExit('Unexpected APK identity or SDK metadata')
if arguments.version and package[3] != arguments.version: raise SystemExit('APK versionName mismatch')
if arguments.version_code is not None and int(package[2]) != arguments.version_code: raise SystemExit('APK versionCode mismatch')
print(f'APK package={package[1]} versionName={package[3]} versionCode={package[2]} min31 target37')
with zipfile.ZipFile(arguments.apk) as archive:
    # AndroidX Graphics/Compose and DataStore include their own small native helpers.
    names = ('libskvoz_android.so', 'libandroidx.graphics.path.so', 'libdatastore_shared_counter.so')
    expected = {f'lib/{abi}/{name}': machine for abi, machine in [('arm64-v8a', 183), ('x86_64', 62)] for name in names}
    libraries = {name for name in archive.namelist() if name.endswith('.so')}
    if libraries != set(expected): raise SystemExit('Unexpected APK native libraries')
    for name, machine in expected.items():
        info = archive.getinfo(name)
        if info.compress_type != zipfile.ZIP_STORED: raise SystemExit('Native library must be uncompressed')
        data = archive.read(name)
        if data[:6] != b'\x7fELF\x02\x01' or struct.unpack_from('<H', data, 18)[0] != machine:
            raise SystemExit('Unexpected ELF architecture')
        start = struct.unpack_from('<Q', data, 32)[0]
        size, count = struct.unpack_from('<HH', data, 54)
        load = 0
        stack = False
        for index in range(count):
            kind, flags, offset, _, _, _, _, alignment = struct.unpack_from('<IIQQQQQQ', data, start + index * size)
            if kind == 1:
                load += 1
                if alignment < 16384 or offset % 16384 != struct.unpack_from('<Q', data, start + index * size + 16)[0] % 16384:
                    raise SystemExit('ELF load alignment below 16KiB')
            if kind == 0x6474e551:
                stack = True
                if flags & 1: raise SystemExit('Executable ELF stack')
        if not stack: raise SystemExit('ELF stack permission header missing')
        if not load: raise SystemExit('ELF has no load segments')
        if name.endswith('/libskvoz_android.so'):
            sections_start = struct.unpack_from('<Q', data, 40)[0]
            section_size, section_count = struct.unpack_from('<HH', data, 58)
            sections = [struct.unpack_from('<IIQQQQIIQQ', data, sections_start + index * section_size) for index in range(section_count)]
            exports = set()
            for section in sections:
                if section[1] != 11: continue  # SHT_DYNSYM
                strings = sections[section[6]]
                table = data[strings[4]:strings[4] + strings[5]]
                for offset in range(section[4], section[4] + section[5], section[9]):
                    symbol, info_byte, _, defined, _, _ = struct.unpack_from('<IBBHQQ', data, offset)
                    if defined and info_byte >> 4 in (1, 2):
                        symbol_name = table[symbol:table.find(b'\0', symbol)].decode('ascii')
                        if symbol_name.startswith('Java_'): exports.add(symbol_name)
            expected_exports = {'Java_org_skvoz_android_NativeBridge_' + method for method in ('cancellationToken', 'cancelEnrollment', 'enroll', 'liveHandles', 'start', 'request', 'poll', 'diagnostics', 'stop', 'tunName')}
            if exports != expected_exports: raise SystemExit('JNI exports differ from Android bridge contract')
            print(f'{name}: exact {len(exports)} JNI exports')
        print(f'{name}: ELF64 machine={machine} 16KiB load alignment, non-executable stack')
subprocess.run([str(arguments.build_tools / 'zipalign'), '-c', '-P', '16', '-v', '4', str(arguments.apk)], check=True)
signature = subprocess.check_output([str(arguments.build_tools / 'apksigner'), 'verify', '--verbose', '--print-certs', str(arguments.apk)], text=True)
print(signature, end='')
if arguments.certificate_sha256 is not None:
    verify_certificate_pin(signature, arguments.certificate_sha256)
    print('APK signing certificate SHA-256 matches expected certificate')
