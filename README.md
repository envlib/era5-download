# era5-download
Docker image for downloading ERA5 data from NCAR and clipping the files to the lat/lon boundaries defined in the parameters.toml.
As of this writing, the total size of the ERA5 global files is 236 TB. You will definitely want to clip the size to your region.

## Usage
Modify the parameters_example.toml and the docker-compose.yml. Then run:
```
docker-compose up -d
```
```
docker-compose logs -f
```