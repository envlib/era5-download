"""
Download ERA5 data from NCAR, clip to geographic bounds, and upload to remote storage.
"""
import json
import pathlib
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path
from typing import Optional

import concurrent.futures
import multiprocessing as mp
import pendulum
import typer
from typing_extensions import Annotated


######################################################
### Constants

# Load the full ERA5 variable catalog
_VAR_JSON_PATH = pathlib.Path(__file__).parent / 'era5_variables.json'
with open(_VAR_JSON_PATH) as _f:
    ALL_VARIABLES = json.load(_f)

# Reverse lookup: uppercase NetCDF variable name -> (product, file_code)
NC_VAR_LOOKUP = {}
for _product, _vars in ALL_VARIABLES.items():
    for _file_code, _meta in _vars.items():
        NC_VAR_LOOKUP[_meta['nc_var'].upper()] = (_product, _file_code)

# Presets: named sets of file codes per product
PRESETS = {
    'wrf': {
        'e5.oper.an.pl': ['128_129_z', '128_133_q', '128_130_t', '128_131_u', '128_132_v'],
        'e5.oper.an.sfc': [
            '128_034_sstk', '128_235_skt', '128_039_swvl1', '128_040_swvl2',
            '128_041_swvl3', '128_042_swvl4', '128_139_stl1', '128_170_stl2',
            '128_183_stl3', '128_236_stl4', '128_031_ci', '128_167_2t',
            '128_168_2d', '128_165_10u', '128_166_10v', '128_033_rsn',
            '128_141_sd', '128_151_msl', '128_134_sp',
        ],
        'e5.oper.invariant': ['128_129_z', '128_172_lsm'],
    },
}

# WRF variable name -> ERA5 file code (for reference/documentation)
WRF_NAME_MAP = {
    'GEOPT': '128_129_z', 'SPECHUMD': '128_133_q', 'TT_pl': '128_130_t',
    'UU_pl': '128_131_u', 'VV_pl': '128_132_v',
    'SST': '128_034_sstk', 'SKINTEMP': '128_235_skt',
    'SM000007': '128_039_swvl1', 'SM007028': '128_040_swvl2',
    'SM028100': '128_041_swvl3', 'SM100289': '128_042_swvl4',
    'ST000007': '128_139_stl1', 'ST007028': '128_170_stl2',
    'ST028100': '128_183_stl3', 'ST100289': '128_236_stl4',
    'SEAICE': '128_031_ci', 'TT_sfc': '128_167_2t',
    'DEWPT': '128_168_2d', 'UU_sfc': '128_165_10u', 'VV_sfc': '128_166_10v',
    'SNOW_DEN': '128_033_rsn', 'SNOW_EC': '128_141_sd',
    'PMSL': '128_151_msl', 'PSFC': '128_134_sp',
    'SOILGEO': '128_129_z', 'LANDSEA': '128_172_lsm',
}

clip_str_format = 'ncks -O -4 -L 3 -d latitude,{min_lat:.1f},{max_lat:.1f} -d longitude,{min_lon:.1f},{max_lon:.1f} {dl_file_path} {clip_file_path}'


def resolve_variables(preset_name, variables_str, skip_vars_str=None):
    """
    Resolve preset and/or variable names into a dict of {product: {file_code: metadata}}.

    skip_vars_str, if given, is a comma-separated list of NetCDF variable names
    to exclude. It is applied after preset/variables are resolved and composes
    cleanly with both.
    """
    selected = {}  # {product: set of file_codes}

    if preset_name:
        name = preset_name.lower()
        if name == 'all':
            selected = {product: set(vars_.keys()) for product, vars_ in ALL_VARIABLES.items()}
        elif name not in PRESETS:
            print(f'Error: Unknown preset "{preset_name}". Available: {", ".join(PRESETS.keys())}, all', file=sys.stderr)
            raise typer.Exit(code=1)
        else:
            for product, codes in PRESETS[name].items():
                selected.setdefault(product, set()).update(codes)

    if variables_str:
        for var_name in variables_str.split(','):
            var_name = var_name.strip()
            if not var_name:
                continue
            key = var_name.upper()
            if key not in NC_VAR_LOOKUP:
                print(f'Error: Unknown variable "{var_name}". Use NetCDF variable names (case insensitive).', file=sys.stderr)
                raise typer.Exit(code=1)
            product, file_code = NC_VAR_LOOKUP[key]
            selected.setdefault(product, set()).update([file_code])

    if skip_vars_str:
        for var_name in skip_vars_str.split(','):
            var_name = var_name.strip()
            if not var_name:
                continue
            key = var_name.upper()
            if key not in NC_VAR_LOOKUP:
                print(f'Error: Unknown --skip-vars variable "{var_name}". Use NetCDF variable names (case insensitive).', file=sys.stderr)
                raise typer.Exit(code=1)
            product, file_code = NC_VAR_LOOKUP[key]
            if product in selected:
                selected[product].discard(file_code)
                if not selected[product]:
                    del selected[product]

    if not selected:
        print('Error: No variables selected. Use --preset (e.g. "wrf") and/or --variables (e.g. "sp,var_2t").', file=sys.stderr)
        raise typer.Exit(code=1)

    # Build filtered dict matching ALL_VARIABLES structure
    result = {}
    for product, codes in selected.items():
        result[product] = {code: ALL_VARIABLES[product][code] for code in codes}

    return result


######################################################
### Functions


def create_rclone_config(name, config_path, config_dict):
    """
    Create an rclone config entry via the rclone CLI.
    """
    type_ = config_dict['type']
    config_list = [f'{k}={v}' for k, v in config_dict.items() if k != 'type']
    config_str = ' '.join(config_list)
    config_path = config_path.joinpath('rclone.config')
    cmd_str = f'rclone config create {name} {type_} {config_str} --config={config_path} --non-interactive'
    cmd_list = shlex.split(cmd_str)
    subprocess.run(cmd_list, capture_output=True, text=True, check=True)

    return config_path


def date_range_months(start_date, end_date):
    """
    Return list of YYYYMM strings covering the date range.
    """
    months = []
    current = pendulum.date(start_date.year, start_date.month, 1)
    end = pendulum.date(end_date.year, end_date.month, 1)
    while current <= end:
        months.append(current.format('YYYYMM'))
        current = current.add(months=1)
    return months


def parse_stdout_files(stdout, start_date, end_date, base_path):
    """
    Parse rclone lsf output and filter files by date range.
    """
    src_files = set()
    for key_name in stdout.strip('\n').split('\n'):
        if not key_name:
            continue
        dates_str = key_name.split('.')[-2].split('_')
        file_start_date = pendulum.from_format(dates_str[0], 'YYYYMMDDHH').date()
        file_end_date = pendulum.from_format(dates_str[1], 'YYYYMMDDHH').date()
        if (file_end_date >= start_date) and (file_start_date <= end_date):
            src_files.add(base_path + key_name)

    return src_files


def _list_month(product, month, names_str, config_path, src_path, start_date, end_date):
    """
    List files for a single product/month directory and filter by date range.
    """
    base_path = f'{product}/{month}/'
    cmd_str = f'rclone lsf dl:{src_path}/{base_path} --config={config_path} --files-only --include {names_str} --fast-list'
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
    return parse_stdout_files(p.stdout, start_date, end_date, base_path)


def _list_invariant(names_str, config_path, src_path):
    """
    List invariant (time-independent) files.
    """
    product = 'e5.oper.invariant'
    base_path = f'{product}/'
    cmd_str = f'rclone lsf dl:{src_path}/{base_path} -R --config={config_path} --files-only --include {names_str} --fast-list'
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
    src_files = set()
    for key_name in p.stdout.strip('\n').split('\n'):
        if key_name:
            src_files.add(base_path + key_name)
    return src_files


def query_source(config_path, start_date, end_date, src_path, era5_vars):
    """
    Query the NCAR ERA5 source for files matching the date range.
    Queries only the YYYYMM directories that overlap the date range,
    and runs all listings in parallel.

    era5_vars: dict of {product: {file_code: metadata}} from resolve_variables().
    """
    months = date_range_months(start_date, end_date)
    print(f'-- Querying {len(months)} monthly directories across products...')

    t1 = time.monotonic()

    src_files = set()
    futures = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for product, vars_dict in era5_vars.items():
            names_set = set('*.' + file_code + '.*' for file_code in vars_dict)
            names_str = ' --include '.join(names_set)

            if 'invariant' in product:
                f = executor.submit(_list_invariant, names_str, config_path, src_path)
                futures[f] = product
            else:
                for month in months:
                    f = executor.submit(_list_month, product, month, names_str, config_path, src_path, start_date, end_date)
                    futures[f] = f'{product}/{month}'

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            src_files.update(result)

    t2 = time.monotonic()
    secs = round(t2 - t1)
    print(f'-- Source query took {secs} secs, found {len(src_files)} files.')

    return src_files


def download_file(key, dl_path, config_path, src_path):
    """
    Download a single file from the source via rclone.
    """
    src_str = f'dl:{src_path}/{key}'
    cmd_str = f'rclone copy {src_str} {dl_path} --config={config_path}'
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
    if p.stderr != '':
        file_name = key.split('/')[-1]
        dl_file_path = dl_path.joinpath(file_name)
        if dl_file_path.exists():
            dl_file_path.unlink()
        return p.stderr
    else:
        return True


def clip_file(key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat):
    """
    Clip a downloaded NetCDF file to the specified geographic bounds using ncks.
    """
    file_name = key.split('/')[-1]

    dl_file_path = dl_path.joinpath(file_name)
    clip_file_path = clip_path.joinpath(file_name)
    cmd_str = clip_str_format.format(min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon, dl_file_path=dl_file_path, clip_file_path=clip_file_path)
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
    dl_file_path.unlink()
    if p.stderr != '':
        return p.stderr
    else:
        return True


def upload_file(key, clip_path, config_path, ul_path):
    """
    Upload a clipped file to the remote destination via rclone.
    """
    file_name = key.split('/')[-1]
    clip_file_path = clip_path.joinpath(file_name)

    base_key = key.rstrip(file_name)

    dst_str = f'ul:{ul_path}/{base_key}'
    cmd_str = f'rclone copy {clip_file_path} {dst_str} --config={config_path} --no-check-dest'
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
    clip_file_path.unlink()
    if p.stderr != '':
        return p.stderr
    else:
        return True


def marshall(key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat, config_path, ul_path, src_path):
    """
    Orchestrate the download → clip → upload pipeline for a single file.
    """
    msg = download_file(key, dl_path, config_path, src_path)
    if not isinstance(msg, str):
        msg = clip_file(key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat)

        if not isinstance(msg, str):
            msg = upload_file(key, clip_path, config_path, ul_path)

    return msg


######################################################
### CLI

app = typer.Typer()


@app.command()
def main(
    parameters_path: Annotated[Path, typer.Argument(
        help='Path to the parameters TOML configuration file',
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
    )],
    n_tasks: Annotated[Optional[int], typer.Option(
        '--n-tasks', '-n',
        help='Number of parallel download workers (overrides TOML n_tasks)',
    )] = None,
    work_dir: Annotated[Optional[Path], typer.Option(
        '--work-dir', '-w',
        help='Working directory for intermediate files (overrides TOML work_dir). Uses a temp directory if not set.',
    )] = None,
    check_target: Annotated[Optional[bool], typer.Option(
        '--check-target/--no-check-target',
        help='Check remote target for existing files before downloading',
    )] = None,
    start_date: Annotated[Optional[str], typer.Option(
        '--start-date', '-s',
        help='Start date in YYYY-MM-DD format (overrides TOML dates.start_date)',
    )] = None,
    end_date: Annotated[Optional[str], typer.Option(
        '--end-date', '-e',
        help='End date in YYYY-MM-DD format (overrides TOML dates.end_date)',
    )] = None,
    min_lon: Annotated[Optional[float], typer.Option(
        '--min-lon',
        help='Minimum longitude for clipping (overrides TOML bounds.min_lon)',
    )] = None,
    max_lon: Annotated[Optional[float], typer.Option(
        '--max-lon',
        help='Maximum longitude for clipping (overrides TOML bounds.max_lon)',
    )] = None,
    min_lat: Annotated[Optional[float], typer.Option(
        '--min-lat',
        help='Minimum latitude for clipping (overrides TOML bounds.min_lat)',
    )] = None,
    max_lat: Annotated[Optional[float], typer.Option(
        '--max-lat',
        help='Maximum latitude for clipping (overrides TOML bounds.max_lat)',
    )] = None,
    preset: Annotated[Optional[str], typer.Option(
        '--preset', '-p',
        help='Variable preset to use (e.g. "wrf", "all"). Combine with --variables to add extra variables.',
    )] = None,
    variables: Annotated[Optional[str], typer.Option(
        '--variables', '-v',
        help='Comma-separated NetCDF variable names to download (case insensitive, e.g. "sp,var_2t,z").',
    )] = None,
    skip_vars: Annotated[Optional[str], typer.Option(
        '--skip-vars',
        help='Comma-separated NetCDF variable names to exclude from download (case insensitive). Composes with --preset and --variables.',
    )] = None,
    list_only: Annotated[bool, typer.Option(
        '--list-only', '-l',
        help='Only query and list available source files, then exit. Does not require a [remote] config.',
    )] = False,
):
    """Download ERA5 data from NCAR, clip to geographic bounds, and upload to remote storage."""

    # Check for ncks (not needed for list-only mode)
    if not list_only and shutil.which('ncks') is None:
        print('Error: ncks (NCO) is not installed or not on PATH. Install NCO and try again.', file=sys.stderr)
        raise typer.Exit(code=1)

    # Load parameters TOML
    with open(parameters_path, 'rb') as f:
        params = tomllib.load(f)

    # Apply CLI overrides
    if n_tasks is not None:
        params['n_tasks'] = n_tasks
    if work_dir is not None:
        params['work_dir'] = str(work_dir)
    if check_target is not None:
        params['check_target'] = check_target
    if start_date is not None:
        params.setdefault('dates', {})['start_date'] = start_date
    if end_date is not None:
        params.setdefault('dates', {})['end_date'] = end_date
    if min_lon is not None:
        params.setdefault('bounds', {})['min_lon'] = min_lon
    if max_lon is not None:
        params.setdefault('bounds', {})['max_lon'] = max_lon
    if min_lat is not None:
        params.setdefault('bounds', {})['min_lat'] = min_lat
    if max_lat is not None:
        params.setdefault('bounds', {})['max_lat'] = max_lat
    if preset is not None:
        params['preset'] = preset
    if variables is not None:
        params['variables'] = variables
    if skip_vars is not None:
        params['skip_vars'] = skip_vars

    # Resolve variables
    era5_vars = resolve_variables(params.get('preset'), params.get('variables'), params.get('skip_vars'))

    # Sentry setup
    if 'sentry' in params:
        import sentry_sdk

        sentry = params['sentry']

        if sentry['dsn'] != '':
            sentry_sdk.init(
                dsn=sentry['dsn'],
                send_default_pii=True,
            )

        if sentry['tags']:
            sentry_sdk.set_tags(sentry['tags'])

    # Source config (default: NCAR ERA5 public bucket)
    source = params.get('source', {
        'type': 's3',
        'provider': 'AWS',
        'env_auth': 'false',
        'region': 'us-west-2',
        'path': 'nsf-ncar-era5/',
    })
    src_path = pathlib.Path(source.pop('path'))

    # Bounds
    bounds = params['bounds']
    bound_min_lon = bounds['min_lon']
    bound_max_lon = bounds['max_lon']
    bound_min_lat = bounds['min_lat']
    bound_max_lat = bounds['max_lat']

    # Check target flag
    do_check_target = params.get('check_target', True)

    # Dates
    dates = params['dates']

    try:
        parsed_start_date = pendulum.parse(dates['start_date']).date()
    except Exception:
        parsed_start_date = pendulum.date(1940, 1, 1)

    try:
        parsed_end_date = pendulum.parse(dates['end_date']).date()
    except Exception:
        parsed_end_date = pendulum.today().date()

    if parsed_end_date < parsed_start_date:
        print('Error: start_date is after the end_date.', file=sys.stderr)
        raise typer.Exit(code=1)

    # Work directory: use provided path or a temp dir that auto-cleans
    use_temp = 'work_dir' not in params
    temp_dir_ctx = tempfile.TemporaryDirectory(prefix='era5_dl_') if use_temp else None

    if use_temp:
        data_path = pathlib.Path(temp_dir_ctx.name)
    else:
        data_path = pathlib.Path(params['work_dir'])
        data_path.mkdir(parents=True, exist_ok=True)

    try:
        print(f'-- dates to be downloaded are from {parsed_start_date} to {parsed_end_date}')

        config_path = create_rclone_config('dl', data_path, source)

        print('-- Determine the files that need to be downloaded.')

        src_files = query_source(config_path, parsed_start_date, parsed_end_date, src_path, era5_vars)

        if list_only or 'remote' not in params:
            src_files_sorted = sorted(src_files, key=lambda key: key[-24:-3], reverse=True)
            for f in src_files_sorted:
                print(f)
            print(f'\n-- {len(src_files_sorted)} files found.')
            return

        # Remote config
        remote = params['remote']
        ul_path = pathlib.Path(remote.pop('path'))
        _ = create_rclone_config('ul', data_path, remote)

        dst_str = f'ul:{ul_path}'

        if do_check_target:
            print('-- Checking files in target.')

            stdin = '\n'.join(src_files)
            src_str = f'dl:{src_path}/'
            cmd_str = f'rclone check {src_str} {dst_str} --missing-on-dst - --files-from-raw - --config={config_path} --fast-list'
            cmd_list = shlex.split(cmd_str)
            p = subprocess.run(cmd_list, input=stdin, capture_output=True, text=True, check=False)

            src_files_new = [key for key in p.stdout.strip('\n').split('\n')]
        else:
            src_files_new = list(src_files)

        src_files_new.sort(key=lambda key: key[-24:-3], reverse=True)

        print(f'-- {len(src_files_new)} files will be downloaded...')

        dl_path = data_path / 'download'
        clip_path = data_path / 'clipped'
        dl_path.mkdir(parents=True, exist_ok=True)
        clip_path.mkdir(parents=True, exist_ok=True)

        worker_count = params.get('n_tasks', 8)

        with concurrent.futures.ProcessPoolExecutor(max_workers=worker_count, mp_context=mp.get_context('spawn')) as executor:
            futures = {}
            for key in src_files_new:
                f1 = executor.submit(marshall, key, dl_path, clip_path, bound_min_lon, bound_max_lon, bound_min_lat, bound_max_lat, config_path, ul_path, src_path)
                futures[f1] = key

            counter = 0
            for future in concurrent.futures.as_completed(futures):
                key = futures[future]
                msg = future.result()
                counter += 1
                if isinstance(msg, str):
                    print(f'{key} failed: {msg}')
                else:
                    if counter % 100 == 0 or counter == 1:
                        print(f'{counter}')
                        print(key)

        print('Completed!')
    finally:
        if temp_dir_ctx is not None:
            temp_dir_ctx.cleanup()
