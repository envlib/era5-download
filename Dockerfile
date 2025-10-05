FROM mullenkamp/wrf-base-debian:1.0

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --no-create-home appuser

RUN mkdir /data
RUN mkdir /data/download
RUN mkdir /data/clipped
RUN mkdir /data/output
RUN chown -R 1000:1000 /data
RUN chmod -R 777 /data

COPY download_era5.py ./
RUN chown 1000:1000 ./download_era5.py
RUN chmod -R 777 ./download_era5.py

USER appuser

CMD ["python", "-u", "download_era5.py"]
