#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 28 15:49:37 2025

@author: mike
"""
import os
import tomllib
import pathlib
import concurrent.futures
import multiprocessing as mp
import subprocess
import shlex
import pendulum
import sentry_sdk
# from rclone_python import rclone

######################################################
### Parameters

base_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

with open(base_path.joinpath("parameters.toml"), "rb") as f:
    params = tomllib.load(f)


## Sentry
sentry = params['sentry']

if sentry['dsn'] != '':
    sentry_sdk.init(
        dsn=sentry['dsn'],
        # Add data like request headers and IP for users,
        # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
        send_default_pii=True,
    )

if sentry['tags']:
    sentry_sdk.set_tags(sentry['tags'])


## Inputs
source = params['source']
src_path = pathlib.Path(source.pop('path'))

data_path = pathlib.Path('/data')
# data_path = pathlib.Path('/home/mike/data/ncar/tests')

if 'download_path' in params:
    dl_path = pathlib.Path(params['download_path'])
    data_path = dl_path.parent
else:
    dl_path = pathlib.Path('/data/download')

if 'clipped_path' in params:
    clip_path = pathlib.Path(params['clipped_path'])
else:
    clip_path = pathlib.Path('/data/clipped')

dl_path.mkdir(parents=True, exist_ok=True)
clip_path.mkdir(parents=True, exist_ok=True)

bounds = params['bounds']
min_lon = bounds['min_lon']
max_lon = bounds['max_lon']
min_lat = bounds['min_lat']
max_lat = bounds['max_lat']

dates = params['dates']

try:
    start_date = pendulum.parse(dates['start_date']).date()
except:
    start_date = pendulum.date(1940, 1, 1)

try:
    end_date = pendulum.parse(dates['end_date']).date()
except:
    end_date = pendulum.today().date()

if end_date < start_date:
    raise ValueError('start_date is after the end_date.')

ncar_era5_pl_names = {
    'GEOPT': '128_129_z',
    'SPECHUMD': '128_133_q',
    'TT': '128_130_t',
    'UU': '128_131_u',
    'VV': '128_132_v',
    }

ncar_era5_sfc_names = {
    'SST': '128_034_sstk',
    'SKINTEMP': '128_235_skt',
    'SM000007': '128_039_swvl1',
    'SM007028': '128_040_swvl2',
    'SM028100': '128_041_swvl3',
    'SM100289': '128_042_swvl4',
    'ST000007': '128_139_stl1',
    'ST007028': '128_170_stl2',
    'ST028100': '128_183_stl3',
    'ST100289': '128_236_stl4',
    'SEAICE': '128_031_ci',
    'TT': '128_167_2t',
    'DEWPT': '128_168_2d',
    'UU': '128_165_10u',
    'VV': '128_166_10v',
    'SNOW_DEN': '128_033_rsn',
    'SNOW_EC': '128_141_sd',
    'PMSL': '128_151_msl',
    'PSFC': '128_134_sp',
    }

ncar_era5_invariant_names = {
    'SOILGEO': '128_129_z',
    'LANDSEA': '128_172_lsm',
    }

ncar_era5_names = {
    'e5.oper.an.sfc': ncar_era5_sfc_names,
    'e5.oper.an.pl': ncar_era5_pl_names,
    'e5.oper.invariant': ncar_era5_invariant_names,
    }

clip_str_format = 'ncks -O -4 -L 3 -d latitude,{min_lat:.1f},{max_lat:.1f} -d longitude,{min_lon:.1f},{max_lon:.1f} {dl_file_path} {clip_file_path}'

remote = params['remote']

# aws_config = {
#     'type': 's3',
#     'provider': 'AWS',
#     'env_auth': 'false',
#     'region': 'us-west-2',
#     }

######################################################
### functions


def create_rclone_config(name, config_path, config_dict):
    """

    """
    type_ = config_dict['type']
    config_list = [f'{k}={v}' for k, v in config_dict.items() if k != 'type']
    config_str = ' '.join(config_list)
    config_path = config_path.joinpath('rclone.config')
    cmd_str = f'rclone config create {name} {type_} {config_str} --config={config_path} --non-interactive'
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, capture_output=True, text=True, check=True)

    return config_path


def parse_stdout_files(stdout, start_date, end_date, base_path):
    """

    """
    src_files = set()
    for key_name in stdout.strip('\n').split('\n'):
        dates_str = key_name.split('.')[-2].split('_')
        file_start_date = pendulum.from_format(dates_str[0], 'YYYYMMDDHH').date()
        file_end_date = pendulum.from_format(dates_str[1], 'YYYYMMDDHH').date()
        if (file_end_date >= start_date) and (file_start_date <= end_date):
            src_files.add(base_path + key_name)

    return src_files


def query_source(config_path, start_date, end_date):
    """

    """
    src_files = set()
    for product, names in ncar_era5_names.items():
        names_set = set('*.' + v + '.*' for v in names.values())
        names_str = ' --include '.join(names_set)
        print(product)

        t1 = pendulum.now()

        if 'pl' in product and False: # Takes too long for many years
            interval = pendulum.interval(start_date, end_date)
            for month in interval.range('months'):
                date_str = month.format('YYYYMM')
                print(month)

                base_path = f'{product}/{date_str}/'

                cmd_str = f'rclone lsf dl:{src_path}/{base_path} -R --config={config_path} --files-only --include {names_str} --fast-list'
                cmd_list = shlex.split(cmd_str)
                p = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
                src_files.update(parse_stdout_files(p.stdout, start_date, end_date, base_path))
        else:
            base_path = f'{product}/'

            cmd_str = f'rclone lsf dl:{src_path}/{base_path} -R --config={config_path} --files-only --include {names_str} --fast-list'
            cmd_list = shlex.split(cmd_str)
            p = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
            if 'invariant' in product:
                for key_name in p.stdout.strip('\n').split('\n'):
                    src_files.add(base_path + key_name)
            else:
                src_files.update(parse_stdout_files(p.stdout, start_date, end_date, base_path))

        t2 = pendulum.now()

        print(round((t2 - t1).total_seconds()))

    return src_files


def download_file(key, dl_path, config_path):
    """

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


def marshall(key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat, config_path, ul_path):
    """

    """
    msg = download_file(key, dl_path, config_path)
    if not isinstance(msg, str):
        msg = clip_file(key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat)

        if not isinstance(msg, str):
            msg = upload_file(key, clip_path, config_path, ul_path)

    return msg


######################################################
### Get data



if __name__ == '__main__':

    print(f'-- dates to be downloaded are from {start_date} to {end_date}')

    # s3 = boto3.client('s3', 'us-west-2', config=Config(max_pool_connections=source['n_tasks'], retries={'mode': 'adaptive', 'max_attempts': 3}, read_timeout=120, signature_version=UNSIGNED))

    # src_session = s3func.S3Session('', '', src_bucket)
    # src_session.client = s3

    config_path = create_rclone_config('dl', data_path, source)

    ul_path = pathlib.Path(remote.pop('path'))
    _ = create_rclone_config('ul', data_path, remote)

    dst_str = f'ul:{ul_path}'

    print('-- Determine the files that need to be downloaded.')

    src_files = query_source(config_path, start_date, end_date)
    stdin = '\n'.join(src_files)
    src_str = f'dl:{src_path}/'
    cmd_str = f'rclone check {src_str} {dst_str} --missing-on-dst - --files-from-raw - --config={config_path} --fast-list'
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, input=stdin, capture_output=True, text=True, check=False)

    # print(p.stdout)
    # print(p.stderr)
    src_files_new = [key for key in p.stdout.strip('\n').split('\n')]
    src_files_new.sort(key=lambda key: key[-24:-3], reverse=True)

    print(f'-- {len(src_files_new)} files will be downloaded...')

    with concurrent.futures.ProcessPoolExecutor(max_workers=params['n_tasks'], mp_context=mp.get_context("spawn")) as executor:
        futures = {}
        for key in src_files_new:
            # f1 = executor.submit(marshall, key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat, config_path, ul_path)
            f1 = executor.submit(download_file, key, dl_path, config_path)
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























































