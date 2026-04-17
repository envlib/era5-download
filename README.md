# era5-s3-dl

CLI tool for downloading ERA5 reanalysis data from NCAR's AWS S3 archive, clipping files to geographic bounds, and uploading the results to a remote destination. As of this writing, the total size of the ERA5 global files is 236 TB -- you will definitely want to clip to your region.

For each file, the tool: downloads from the NCAR source via rclone, clips to lat/lon bounds via ncks (converting to NetCDF-4 with compression), and uploads the clipped file to a configured remote destination.

## Prerequisites

- **Python >= 3.10**
- **ncks** (part of [NCO](https://nco.sourceforge.net/)) must be installed separately for NetCDF clipping

## Installation

```bash
pipx install era5_s3_dl
```

With optional Sentry error tracking (used in Docker deployment):
```bash
pipx install era5_s3_dl[sentry]
```

## Docker

The Docker image includes ncks and installs the tool with the Sentry extra. Modify `parameters.toml` and `docker-compose.yml`, then run:

```bash
docker-compose up -d
docker-compose logs -f
```

The `docker-compose.yml` mounts your `parameters.toml` into the container and maps an output volume:
```yaml
volumes:
    - "./parameters.toml:/parameters.toml"
    - "./output:/data/output"
```

## Configuration

Copy `parameters_example.toml` to `parameters.toml` and edit it. The TOML file is the primary configuration mechanism -- all settings live here, and select values can be overridden via CLI options.

### TOML sections

**Root-level settings:**
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `n_tasks` | int | `8` | Number of parallel download workers |
| `check_target` | bool | `true` | Check remote for existing files before downloading (skips duplicates) |
| `work_dir` | string | (temp dir) | Working directory for intermediate files. Creates `download/` and `clipped/` subdirs within it. If not set, uses a temporary directory that is cleaned up on completion. |
| `preset` | string | | Variable preset (e.g. `"wrf"`, `"all"`). Required unless `variables` is set. |
| `variables` | string | | Comma-separated NetCDF variable names to download (case insensitive). Combined with `preset` if both set. |

**`[dates]`** -- Date range for files to download:
```toml
[dates]
start_date = "2020-01-01"
end_date = "2024-12-31"
```

**`[bounds]`** -- Geographic clipping bounds (longitude/latitude):
```toml
[bounds]
min_lon = 145
max_lon = 195
min_lat = -60
max_lat = -20
```

**`[source]`** -- rclone config for the NCAR ERA5 source bucket. The default points to the public `nsf-ncar-era5` bucket on AWS:
```toml
[source]
type = 's3'
provider = 'AWS'
env_auth = 'false'
region = 'us-west-2'
path = 'nsf-ncar-era5/'
```

**`[remote]`** -- rclone config for the upload destination. Can be S3 or a local path. Optional for `--list-only` mode:
```toml
# S3 example:
[remote]
type = 's3'
provider = 'Mega'
endpoint = 'https://s3.example.com'
access_key_id = ''
secret_access_key = ''
path = '/data/ncar/era5/'

# Local example (use with Docker volume mount):
[remote]
type = 'local'
path = '/data/output'
```

**`[sentry]`** (optional) -- Sentry error tracking. Requires the `sentry` extra to be installed:
```toml
[sentry]
dsn = ''
tags = {}
```

## Variable Selection

You must specify which ERA5 variables to download using `--preset` and/or `--variables`.

**Presets** select a predefined group of variables:
- `wrf` -- the 26 variables required for WRF model initialization
- `all` -- all 92 available ERA5 variables

**Variables** are specified by their NetCDF variable name (case insensitive). The full catalog is in `era5_dl/era5_variables.json`. Examples: `SP`, `VAR_2T`, `Z`, `CAPE`, `U`, `V`.

When both `--preset` and `--variables` are given, the variables are combined (union).

```bash
# Download WRF variables only
era5_dl parameters.toml --preset wrf

# Download just surface pressure and 2m temperature
era5_dl parameters.toml --variables sp,var_2t

# Download WRF variables plus CAPE
era5_dl parameters.toml --preset wrf --variables cape

# List all available files without downloading
era5_dl parameters.toml --preset all --list-only
```

## Usage

Basic invocation with a parameters file:
```bash
era5_dl parameters.toml --preset wrf
```

Override specific TOML values via CLI options:
```bash
era5_dl parameters.toml --preset wrf -s 2024-01-01 -e 2024-12-31 -n 4
era5_dl parameters.toml --preset wrf --min-lon 160 --max-lon 180 --no-check-target
```

List matching files without downloading (does not require `[remote]` config):
```bash
era5_dl parameters.toml --preset wrf --list-only
```

See all options:
```bash
era5_dl --help
```

## CLI Options

All options are optional and override the corresponding value in the parameters TOML file.

| Option | Short | Type | Overrides |
|--------|-------|------|-----------|
| `--n-tasks` | `-n` | int | `n_tasks` |
| `--work-dir` | `-w` | path | `work_dir` |
| `--check-target` / `--no-check-target` | | bool | `check_target` |
| `--start-date` | `-s` | text | `dates.start_date` |
| `--end-date` | `-e` | text | `dates.end_date` |
| `--min-lon` | | float | `bounds.min_lon` |
| `--max-lon` | | float | `bounds.max_lon` |
| `--min-lat` | | float | `bounds.min_lat` |
| `--max-lat` | | float | `bounds.max_lat` |
| `--preset` | `-p` | text | `preset` |
| `--variables` | `-v` | text | `variables` |
| `--list-only` | `-l` | flag | Query and list files only, skip download |


