#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 28 15:49:37 2025

@author: mike
"""
import os
import tomllib
import pathlib
import s3func
import boto3
import botocore
from botocore import UNSIGNED
from botocore.config import Config
import shutil
import concurrent.futures
import multiprocessing as mp
import urllib3
import subprocess
import shlex
import pendulum
# from rclone_python import rclone

######################################################
### Parameters

base_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

with open(base_path.joinpath("parameters.toml"), "rb") as f:
    params = tomllib.load(f)

source = params['source']
remote = params['remote']

data_path = pathlib.Path('/data')

if 'download_path' in source:
    dl_path = pathlib.Path(source['download_path'])
else:
    dl_path = pathlib.Path('/data/download')

if 'clipped_path' in source:
    clip_path = pathlib.Path(source['clipped_path'])
else:
    clip_path = pathlib.Path('/data/clipped')

dl_path.mkdir(parents=True, exist_ok=True)
clip_path.mkdir(parents=True, exist_ok=True)

src_bucket = source['bucket']

bounds = source['bounds']
min_lon = bounds['min_lon']
max_lon = bounds['max_lon']
min_lat = bounds['min_lat']
max_lat = bounds['max_lat']

try:
    start_date = pendulum.parse(source['start_date']).date()
except:
    start_date = pendulum.date(1940, 1, 1)

try:
    end_date = pendulum.parse(source['end_date']).date()
except:
    end_date = pendulum.today().date()

if end_date < start_date:
    raise ValueError('start_date is after the end_date.')

ncar_era5_pl_names = {
    # 'GEOPT': '128_129_z',
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
    'e5.oper.an.sfc/': ncar_era5_sfc_names,
    'e5.oper.an.pl/': ncar_era5_pl_names,
    'e5.oper.invariant/': ncar_era5_invariant_names,
    }

clip_str_format = 'ncks -O -4 -L 3 -d latitude,{min_lat:.1f},{max_lat:.1f} -d longitude,{min_lon:.1f},{max_lon:.1f} {dl_file_path} {clip_file_path}'


######################################################
### functions


def create_rclone_config(name, data_path, access_key_id=None, access_key=None, endpoint_url=None, download_url=None):
    """

    """
    config_dict = {}
    if isinstance(access_key_id, str):
        if isinstance(endpoint_url, str):
            if isinstance(download_url, str) or 'backblazeb2' in endpoint_url:
                type_ = 'b2'
                config_dict['account'] = access_key_id
                config_dict['key'] = access_key
                config_dict['hard_delete'] = 'true'
                if isinstance(download_url, str):
                    config_dict['download_url'] = download_url
            else:
                type_ = 's3'
                if 'mega' in endpoint_url:
                    provider = 'Mega'
                else:
                    provider = 'Other'
                config_dict['provider'] = provider
                config_dict['access_key_id'] = access_key_id
                config_dict['secret_access_key'] = access_key
                config_dict['endpoint'] = endpoint_url
        else:
            type_ = 's3'
            config_dict['provider'] = 'AWS'
            config_dict['access_key_id'] = access_key_id
            config_dict['secret_access_key'] = access_key
    else:
        type_ = 's3'
        config_dict['provider'] = 'AWS'
        config_dict['env_auth'] = 'false'
        config_dict['region'] = 'us-west-2'

    config_list = [f'{k}={v}' for k, v in config_dict.items()]
    config_str = ' '.join(config_list)
    config_path = data_path.joinpath('rclone.config')
    cmd_str = f'rclone config create {name} {type_} {config_str} --config={config_path} --non-interactive --obscure'
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, capture_output=True, text=True, check=True)

    return config_path


def download_file(key, dl_path, config_path):
    """

    """
    src_str = f's3_dl:{src_bucket}/{key}'
    cmd_str = f'rclone copy {src_str} {str(dl_path)} --config={config_path}'
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


def upload_file(key, clip_path, config_path, dst_bucket, dst_base_path):
    """

    """
    file_name = key.split('/')[-1]
    clip_file_path = clip_path.joinpath(file_name)

    base_key = key.rstrip(file_name)

    dst_str = f's3_ul:{dst_bucket}/{dst_base_path}{base_key}'
    cmd_str = f'rclone copy {str(clip_file_path)} {dst_str} --config={config_path}'
    cmd_list = shlex.split(cmd_str)
    p = subprocess.run(cmd_list, capture_output=True, text=True, check=False)
    clip_file_path.unlink()
    if p.stderr != '':
        return p.stderr
    else:
        return True


def marshall(key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat, config_path, dst_bucket, dst_base_path):
    """

    """
    msg = download_file(key, dl_path, config_path)
    if not isinstance(msg, str):
        msg = clip_file(key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat)

        if not isinstance(msg, str) and dst_bucket:
            msg = upload_file(key, clip_path, config_path, dst_bucket, dst_base_path)

    return msg


######################################################
### Get data



if __name__ == '__main__':

    print(f'-- dates to be downloaded are from {start_date} to {end_date}')

    s3 = boto3.client('s3', 'us-west-2', config=Config(max_pool_connections=source['n_tasks'], retries={'mode': 'adaptive', 'max_attempts': 3}, read_timeout=120, signature_version=UNSIGNED))

    src_session = s3func.S3Session('', '', src_bucket)
    src_session.client = s3

    config_path = create_rclone_config('s3_dl', data_path)

    print('-- Determine the files that need to be downloaded.')

    try:
        if 'endpoint_url' in remote:
            endpoint_url = remote['endpoint_url']
        else:
            endpoint_url = None

        dst_session = s3func.S3Session(remote['access_key_id'], remote['access_key'], remote['bucket'], endpoint_url, stream=False)
        dst_bucket = remote['bucket']
        dst_base_path = remote['path']
    
        dst_obj_resp = dst_session.list_objects(remote['path'])
        dst_files = set(obj['key'].split('/')[-1] for obj in dst_obj_resp.iter_objects() if obj['key'].endswith('.nc'))

        _ = create_rclone_config('s3_ul', data_path, remote['access_key_id'], remote['access_key'], endpoint_url)

    except:
        dst_files = set()
        for file_path in clip_path.rglob('*'):
            if file_path.is_file():
                if file_path.name.endswith('.nc'):
                    dst_files.add(file_path.name)

        dst_bucket = False
        dst_base_path = False

    src_files = set()
    src_size = 0
    for path, names in ncar_era5_names.items():
        names_set = set(names.values())
        src_obj_resp = src_session.list_objects(path)
        for obj in src_obj_resp.iter_objects():
            key = obj['key']
            key_name = key.split('/')[-1]
            for name in names_set:
                if name in key:
                    if key_name not in dst_files:
                        dates_str = key_name.split('.')[-2].split('_')
                        file_start_date = pendulum.from_format(dates_str[0], 'YYYYMMDDHH').date()
                        file_end_date = pendulum.from_format(dates_str[1], 'YYYYMMDDHH').date()
                        if (file_end_date >= start_date) and (file_start_date <= end_date):
                            src_files.add(key)
                            src_size += obj['content_length'] * 0.000001
                    break

            # if src_files:
            #     break

    src_files_new = list(src_files)
    src_files_new.sort(key=lambda key: key[-24:-3], reverse=True)

    print(f'-- {int(src_size):,} MBs and {len(src_files_new)} files will be downloaded...')

    with concurrent.futures.ProcessPoolExecutor(max_workers=source['n_tasks'], mp_context=mp.get_context("spawn")) as executor:
        futures = {}
        for key in src_files_new:
            f1 = executor.submit(marshall, key, dl_path, clip_path, min_lon, max_lon, min_lat, max_lat, config_path, dst_bucket, dst_base_path)
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























































