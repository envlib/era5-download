FROM mullenkamp/wrf-base-debian:1.0

COPY . /app
WORKDIR /app
RUN uv pip install --no-cache-dir ".[sentry]"

RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --no-create-home appuser

RUN mkdir -p /data/download /data/clipped /data/output && \
    chown -R 1000:1000 /data && \
    chmod -R 777 /data

USER appuser

ENV PYTHONUNBUFFERED=1

CMD ["era5_dl", "/parameters.toml"]
