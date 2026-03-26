# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CLI tool that downloads ERA5 climate reanalysis data from NCAR's AWS S3 bucket, clips NetCDF files to geographic bounds using `ncks`, and uploads results to a remote destination via `rclone`. Supports configurable variable selection via presets (e.g. `wrf`) or individual NetCDF variable names.

## Commands

```bash
uv sync                        # Install dependencies
uv run era5_dl --help          # Show CLI help
uv run era5_dl params.toml --preset wrf --list-only  # List WRF files without downloading
uv run era5_dl params.toml --preset wrf -s 2024-01-01 -e 2024-12-31  # Download with overrides
uv run pytest                  # Run tests
uv run ruff check .            # Lint
uv run black .                 # Format
```

Docker deployment:
```bash
docker-compose up -d
docker-compose logs -f
```

## Architecture

Single-package CLI (`era5_dl/era5_dl.py`) using Typer:

1. **Variable catalog** (`era5_dl/era5_variables.json`): All 92 ERA5 variables from the NCAR bucket with file codes, NetCDF names, long names, and units. Loaded at module level into `ALL_VARIABLES`. A reverse lookup (`NC_VAR_LOOKUP`) maps uppercase NetCDF names to `(product, file_code)`.
2. **Presets** (`PRESETS` dict): Named sets of file codes per product type. Currently just `wrf`. `WRF_NAME_MAP` preserves the WRF→ERA5 name mapping for reference.
3. **`resolve_variables()`**: Takes preset name and/or comma-separated variable names, resolves to a filtered `{product: {file_code: metadata}}` dict. Variable names are case insensitive.
4. **`query_source()`**: Queries only the YYYYMM directories within the date range (not recursive full-tree listing). Runs all rclone lsf calls in parallel via `ThreadPoolExecutor`.
5. **`marshall()`**: Per-file pipeline: `download_file()` → `clip_file()` → `upload_file()`. Runs in parallel via `ProcessPoolExecutor` (spawn context).
6. **CLI (`main()`)**: Loads `parameters.toml`, applies CLI overrides, resolves variables, then orchestrates query and download.

Configuration is primarily via `parameters.toml` (copy from `parameters_example.toml`). CLI options override specific TOML values. Source/remote/sentry config stays TOML-only. `parameters.toml` is gitignored because it may contain credentials.

`--list-only` mode queries the source and prints matching files without downloading. Does not require `[remote]` config or `ncks`.

## External Dependencies

- **rclone** -- installed via `rclone-bin` PyPI package; used for cloud storage operations (list, copy, check)
- **ncks** (from NCO) -- must be installed separately; used for NetCDF spatial subsetting with deflate compression (`-4 -L 3`)
- **sentry_sdk** -- optional extra (`pip install ".[sentry]"`); used in Docker deployment for error tracking

## ERA5 Data Structure

Source bucket: `nsf-ncar-era5/` on AWS `us-west-2`. Three product types organized by `{product}/{YYYYMM}/` directories:
- `e5.oper.an.pl` -- 16 pressure level variables (daily files)
- `e5.oper.an.sfc` -- 62 surface variables (monthly files)
- `e5.oper.invariant` -- 14 time-invariant fields

File naming encodes date ranges as `YYYYMMDDHH_YYYYMMDDHH` in the second-to-last dot-separated segment. Variable metadata sourced from the NCAR THREDDS server.
