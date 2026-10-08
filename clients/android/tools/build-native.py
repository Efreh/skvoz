#!/usr/bin/env python3
"""Build the same shared Rust runtime for both packaged Android ABIs."""
import os
from pathlib import Path
import shutil
import subprocess

component = Path(__file__).resolve().parents[1]
root = component.parents[1]
sdk = Path(os.environ.get('ANDROID_HOME', os.environ.get('ANDROID_SDK_ROOT', '')))
ndk = sdk / 'ndk' / '30.0.16248370' / 'toolchains' / 'llvm' / 'prebuilt' / 'linux-x86_64' / 'bin'
if not ndk.is_dir():
    raise SystemExit('Android NDK 30.0.16248370 is required')
target_dir = Path(os.environ.get('CARGO_TARGET_DIR', root / 'target'))
if not target_dir.is_absolute():
    target_dir = root / target_dir
output = component / 'app' / 'build' / 'generated' / 'jniLibs'
for abi, target, clang in [('arm64-v8a', 'aarch64-linux-android', 'aarch64-linux-android31-clang'), ('x86_64', 'x86_64-linux-android', 'x86_64-linux-android31-clang')]:
    env = os.environ.copy()
    env['CARGO_BUILD_JOBS'] = '1'
    env['CARGO_INCREMENTAL'] = '0'
    env['CC_' + target.replace('-', '_')] = str(ndk / clang)
    env['AR_' + target.replace('-', '_')] = str(ndk / 'llvm-ar')
    prefix = 'CARGO_TARGET_' + target.upper().replace('-', '_')
    env[prefix + '_LINKER'] = str(ndk / clang)
    flags = '-C link-arg=-Wl,-z,max-page-size=16384'
    if os.environ.get('SKVOZ_ANDROID_SYSROOT'):
        flags += ' --sysroot ' + os.environ['SKVOZ_ANDROID_SYSROOT']
    env[prefix + '_RUSTFLAGS'] = flags
    subprocess.run(['cargo', 'build', '--locked', '--release', '-p', 'skvoz-android-native', '--target', target], cwd=root, env=env, check=True)
    dest = output / abi
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(target_dir / target / 'release' / 'libskvoz_android.so', dest / 'libskvoz_android.so')
