"""
Failure handling of the era5_dl CLI, driven end to end through the real ``main`` with fake ``rclone`` and ``ncks``
executables on PATH.

Every case here was a silent loss in 0.2.1: the run exited 0 while a file was missing from the target. Success is now
judged by each tool's return code, any failed file makes the run exit 1, and every NOTICE / FAILED line names its step.

The fakes treat ``dl:<path>`` and ``ul:<path>`` as local paths. A fault is injected per step and per file with the
``FAKE_FAULTS`` environment variable: ``<step>:<substring of the key>:<mode>[;...]``. ``step`` is ``download``,
``clip``, ``upload`` or ``lsf`` (``lsf`` matches the listed directory), and ``mode`` is one of:
- ``rc0err``: do the work, write a notice to stderr, exit 0 (what rclone does after a retried transfer succeeds);
- ``rc1``: write nothing, exit 1;
- ``rc0nofile``: exit 0 without writing the file;
- ``partial``: list half the files, then exit 1 (lsf only).
"""
import os
import subprocess
import sys
import textwrap

import pytest

DAY = '2023021200_2023021223'
MONTH = '2023020100_2023022823'
PL = ['128_129_z.ll025sc', '128_133_q.ll025sc', '128_130_t.ll025sc', '128_131_u.ll025uv', '128_132_v.ll025uv']
SFC = ['128_235_skt', '128_039_swvl1', '128_040_swvl2', '128_041_swvl3', '128_042_swvl4', '128_139_stl1',
       '128_170_stl2', '128_183_stl3', '128_236_stl4', '128_167_2t', '128_168_2d', '128_165_10u', '128_166_10v',
       '128_033_rsn', '128_141_sd', '128_151_msl', '128_134_sp']
INV = ['128_129_z', '128_172_lsm']

FAKE_COMMON = r'''
import fnmatch, os, pathlib, shutil, sys

def fault(step, target):
    for spec in filter(None, os.environ.get('FAKE_FAULTS', '').split(';')):
        s, sub, mode = spec.split(':')
        if s == step and sub in target:
            return mode
    return None

def local(p):
    return pathlib.Path(p.split(':', 1)[1]) if p.startswith(('dl:', 'ul:')) else pathlib.Path(p)
'''

FAKE_RCLONE = FAKE_COMMON + r'''
args = sys.argv[1:]
cmd = args[0]
if cmd == 'config':
    sys.exit(0)
if cmd == 'lsf':
    root = local(args[1])
    incs = [args[i + 1] for i, a in enumerate(args) if a == '--include']
    names = sorted(str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()) if '-R' in args else \
        sorted(p.name for p in root.iterdir() if p.is_file()) if root.is_dir() else []
    names = [n for n in names if not incs or any(fnmatch.fnmatch(pathlib.Path(n).name, i) for i in incs)]
    mode = fault('lsf', args[1])
    if mode == 'rc1':
        sys.stderr.write('ERROR : listing failed\n')
        sys.exit(1)
    if mode == 'partial':
        print('\n'.join(names[: len(names) // 2]))
        sys.stderr.write('ERROR : listing interrupted\n')
        sys.exit(1)
    print('\n'.join(names))
    sys.exit(0)
if cmd == 'copy':
    src, dst = args[1], args[2]
    step = 'download' if src.startswith('dl:') else 'upload'
    mode = fault(step, src)
    if mode == 'rc1':
        sys.stderr.write('ERROR : Attempt 3/3 failed with 1 errors\n')
        sys.exit(1)
    if mode == 'rc0nofile':
        sys.exit(0)
    d = local(dst)
    d.mkdir(parents=True, exist_ok=True)
    shutil.copy(local(src), d / local(src).name)
    if mode == 'rc0err':
        sys.stderr.write('ERROR : Attempt 1/3 failed with 1 errors and: connection reset\n'
                         'ERROR : Attempt 2/3 succeeded\n')
    sys.exit(0)
sys.stderr.write(f'fake rclone: unhandled {args}\n')
sys.exit(2)
'''

FAKE_NCKS = FAKE_COMMON + r'''
src, dst = sys.argv[-2], sys.argv[-1]
mode = fault('clip', src)
if mode == 'rc1':
    pathlib.Path(dst).write_bytes(b'partial')
    sys.stderr.write('ncks: ERROR something\n')
    sys.exit(1)
shutil.copy(src, dst)
if mode == 'rc0err':
    sys.stderr.write('ncks: WARNING a harmless warning\n')
sys.exit(0)
'''


@pytest.fixture
def env(tmp_path):
    """A fake NCAR-layout source with one day's 24 files (the wrf preset minus sstk, ci), the fakes on PATH, a
    target dir and a parameters TOML."""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    for name, body in (('rclone', FAKE_RCLONE), ('ncks', FAKE_NCKS)):
        p = bin_dir / name
        p.write_text(f'#!{sys.executable}\n' + body)
        p.chmod(0o755)
    src = tmp_path / 'src'
    for code in PL:
        f = src / 'e5.oper.an.pl' / '202302' / f'e5.oper.an.pl.{code}.{DAY}.nc'
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(code.encode())
    for code in SFC:
        f = src / 'e5.oper.an.sfc' / '202302' / f'e5.oper.an.sfc.{code}.ll025sc.{MONTH}.nc'
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(code.encode())
    for code in INV:
        f = src / 'e5.oper.invariant' / '197901' / f'e5.oper.invariant.{code}.ll025sc.1979010100_1979010100.nc'
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(code.encode())
    dst = tmp_path / 'dst'
    dst.mkdir()
    params = tmp_path / 'params.toml'
    params.write_text(textwrap.dedent(f'''
        [source]
        type = "local"
        path = "{src}"
        [remote]
        type = "local"
        path = "{dst}"
        [bounds]
        min_lon = 160.0
        max_lon = 180.0
        min_lat = -50.0
        max_lat = -30.0
        [dates]
        start_date = "2023-02-12"
        end_date = "2023-02-12"
    '''))
    return {'tmp': tmp_path, 'bin': bin_dir, 'src': src, 'dst': dst, 'params': params}


def run(env, faults='', check_target=False):
    e = dict(os.environ, PATH=f'{env["bin"]}{os.pathsep}{os.environ["PATH"]}', FAKE_FAULTS=faults)
    cmd = [sys.executable, '-c', 'from era5_dl.era5_dl import app; app()', str(env['params']), '--preset', 'wrf',
           '--skip-vars', 'sstk,ci', '-n', '2', '-w', str(env['tmp'] / 'work'),
           '--check-target' if check_target else '--no-check-target']
    return subprocess.run(cmd, capture_output=True, text=True, env=e, timeout=300)


def stored(env):
    return sorted(p.name for p in env['dst'].rglob('*.nc'))


def test_healthy_run_stores_every_file(env):
    p = run(env)
    assert p.returncode == 0, p.stderr
    assert len(stored(env)) == 24 and 'NOTICE' not in p.stdout and 'FAILED' not in p.stderr


def test_download_stderr_on_success_keeps_the_file(env):
    """rclone recovered from a transient failure: exit 0 with stderr. 0.2.1 deleted the good file and exited 0."""
    p = run(env, 'download:128_133_q:rc0err')
    assert p.returncode == 0, p.stderr
    assert len(stored(env)) == 24
    assert 'NOTICE download e5.oper.an.pl/202302/e5.oper.an.pl.128_133_q' in p.stdout
    assert 'Attempt 2/3 succeeded' in p.stdout


def test_download_failure_exits_nonzero_and_names_the_key(env):
    p = run(env, 'download:128_132_v:rc1')
    assert p.returncode == 1
    assert 'FAILED download e5.oper.an.pl/202302/e5.oper.an.pl.128_132_v.ll025uv' in p.stderr
    assert '-- 1 of 24 files FAILED' in p.stderr
    assert len(stored(env)) == 23 and 'Completed!' not in p.stdout


def test_download_exit0_without_a_file_is_a_failure(env):
    """0.2.1 crashed in clip_file's unlink here; it is now a named FAILED line."""
    p = run(env, 'download:128_134_sp:rc0nofile')
    assert p.returncode == 1
    assert 'FAILED download e5.oper.an.sfc/202302/e5.oper.an.sfc.128_134_sp' in p.stderr
    assert 'the file is absent' in p.stderr


@pytest.mark.parametrize('mode', ['rc1', 'partial'])
def test_failed_month_listing_exits_nonzero(env, mode):
    """A failed listing dropped the whole product-month while 0.2.1 exited 0."""
    p = run(env, f'lsf:e5.oper.an.sfc/202302:{mode}')
    assert p.returncode != 0
    assert 'listing e5.oper.an.sfc/202302/ failed' in p.stderr
    assert stored(env) == []


def test_failed_invariant_listing_exits_nonzero(env):
    p = run(env, 'lsf:e5.oper.invariant:rc1')
    assert p.returncode != 0 and 'listing e5.oper.invariant/ failed' in p.stderr


def test_clip_warning_keeps_the_file(env):
    """ncks wrote a warning and exited 0. 0.2.1 never uploaded the file and exited 0."""
    p = run(env, 'clip:128_167_2t:rc0err')
    assert p.returncode == 0, p.stderr
    assert len(stored(env)) == 24
    assert 'NOTICE clip e5.oper.an.sfc/202302/e5.oper.an.sfc.128_167_2t' in p.stdout


def test_clip_failure_exits_nonzero_and_leaves_no_partial(env):
    p = run(env, 'clip:128_167_2t:rc1')
    assert p.returncode == 1 and 'FAILED clip e5.oper.an.sfc/202302/e5.oper.an.sfc.128_167_2t' in p.stderr
    assert not list((env['tmp'] / 'work' / 'clipped').glob('*'))


def test_upload_failure_exits_nonzero(env):
    p = run(env, 'upload:128_131_u:rc1')
    assert p.returncode == 1 and 'FAILED upload e5.oper.an.pl/202302/e5.oper.an.pl.128_131_u' in p.stderr
    assert len(stored(env)) == 23


def test_upload_notice_is_not_a_failure(env):
    """0.2.1 reported a delivered file as failed."""
    p = run(env, 'upload:128_131_u:rc0err')
    assert p.returncode == 0 and 'NOTICE upload' in p.stdout and len(stored(env)) == 24


def test_check_target_with_everything_present_downloads_nothing(env):
    """In 0.2.1 an empty `rclone check` output became the key '', i.e. a copy of the whole source root."""
    assert run(env).returncode == 0
    marker = env['src'] / 'not_era5.txt'          # a root-level file that only a whole-root copy would fetch
    marker.write_text('x')
    p = run(env, check_target=True)
    assert p.returncode == 0, p.stderr
    assert '-- 0 files will be downloaded' in p.stdout
    assert not list((env['tmp'] / 'work').rglob('not_era5.txt'))


def test_check_target_downloads_only_what_is_missing(env):
    assert run(env).returncode == 0
    gone = next(env['dst'].rglob('*128_134_sp*'))
    gone.unlink()
    p = run(env, check_target=True)
    assert p.returncode == 0, p.stderr
    assert '-- 1 files will be downloaded' in p.stdout and gone.exists()


def test_check_target_listing_failure_exits_nonzero(env):
    """A target that cannot be listed must not read as 'everything present' (0.2.2's first draft: exit 0, nothing
    delivered, `Completed!`)."""
    p = run(env, f'lsf:{env["dst"]}:rc1', check_target=True)
    assert p.returncode != 0 and 'listing the target' in p.stderr
    assert 'Completed!' not in p.stdout and stored(env) == []
