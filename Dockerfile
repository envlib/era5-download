FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl unzip nco ncl-ncarg wget nano bash && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV PYTHONUNBUFFERED=True

RUN curl https://rclone.org/install.sh | bash

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --no-create-home appuser

RUN mkdir /data
RUN mkdir /data/download
RUN mkdir /data/clipped
RUN mkdir /data/output
RUN chown -R 1000:1000 /data

COPY download_era5.py ./
RUN chown 1000:1000 ./download_era5.py

USER appuser

CMD ["python", "-u", "download_era5.py"]
