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

# BUILD OPENBB'S EXTENSION MAP AT IMAGE-BUILD TIME, AS ROOT.
#
# `import openbb` reconciles the installed extensions against a static map it keeps INSIDE
# site-packages, and rebuilds when they differ — writing `openbb/.build.lock` and the generated
# package. The container runs as uid 10001 against a root-owned site-packages, so at runtime that
# rebuild raises
#
#     PermissionError: [Errno 13] Permission denied: '.../openbb/.build.lock'
#
# on EVERY call. Which is not how it presented: the exception surfaced as a failed batch, the
# isolation pass then failed every symbol individually, and the asset reported `empty: 50` — fifty
# securities that had answered nothing, when the truth was that we had never asked. Nothing was
# marked, because `mark_absent` refuses without an isolated attempt and a healthy control, so the
# damage was bounded to a wasted run. It was found by driving the asset against production, and by
# no test.
#
# Building here means the map is complete before the image is ever run, so the runtime user only
# ever READS it. The alternative — making site-packages writable — hands a network-facing worker
# write access to its own code.
RUN python -c "import openbb" && rm -rf /root/.cache

USER muffin
EXPOSE 4000 9102
# Overridden per service in the stack; this is the code location, which is the one that must exist.
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "muffin_ingest_dagster.definitions"]
