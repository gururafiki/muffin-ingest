# ONE image for all three Dagster services (code location, daemon, webserver). They differ only by
# `command`, so a single image means one build, one pull and one thing to keep on an arm64 node
# whose root filesystem is the scarce resource.
FROM python:3.13-slim

# `openbb-core` and `edgartools` both pull scientific wheels; keep the layer that installs them
# separate from the source so an edit to our code does not re-resolve the world.
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/opt/dagster/home \
    PROMETHEUS_MULTIPROC_DIR=/tmp/prom

RUN useradd --create-home --uid 10001 muffin \
 && mkdir -p /opt/dagster/home /tmp/prom \
 && chown -R muffin:muffin /opt/dagster /tmp/prom

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . && rm -rf /root/.cache

USER muffin
EXPOSE 4000 9102
# Overridden per service in the stack; this is the code location, which is the one that must exist.
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "muffin_ingest_dagster.definitions"]
