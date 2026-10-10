"""Finite ordinary Docker fixture resources; no host configuration changes."""
import os
from pathlib import Path
import stat
import subprocess


def block_device(root):
    """Select the backing disk; an override handles layered filesystems."""
    device = os.environ.get('SKVOZ_TEST_BLOCK_DEVICE')
    if not device:
        source = subprocess.check_output(['findmnt', '-n', '-o', 'SOURCE', '--target', str(root)], text=True).strip()
        parent = subprocess.check_output(['lsblk', '-n', '-o', 'PKNAME', source], text=True).strip()
        device = '/dev/' + parent if parent else source
    assert stat.S_ISBLK(Path(device).stat().st_mode), 'fixture I/O cap requires a block device'
    return device


def docker_limits(root, memory='1g', pids=256, read_bps=31457280, write_bps=10485760):
    device = block_device(root)
    return ['--memory', memory, '--memory-swap', memory, '--cpus', '1',
            '--cpuset-cpus', '0', '--pids-limit', str(pids), '--ulimit', 'core=0',
            '--ulimit', 'nofile=8192:8192', '--device-read-bps', f'{device}:{read_bps}',
            '--device-write-bps', f'{device}:{write_bps}']
