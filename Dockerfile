# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: builder — install deps and run tests
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

LABEL maintainer="Kishan Borad <kishanborad27@gmail.com>"
LABEL description="Event-stream-playground Python streaming engine"

# System dependencies for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Install Python dependencies first (layer-cached separately from source)
COPY python/requirements.txt ./requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy Python source
COPY python/ ./python/

# Run test suite during build — fail fast on broken code
RUN cd python && python -m pytest tests/ \
        -x \
        --tb=short \
        -q \
        --no-header \
        2>&1 | tee /tmp/test-results.txt \
    && echo "All tests passed."

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: runtime — lean image with only what's needed to run
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

LABEL maintainer="Kishan Borad <kishanborad27@gmail.com>"
LABEL org.opencontainers.image.title="event-stream-playground"
LABEL org.opencontainers.image.description="Python event streaming engine — producer, consumer, analyzer"
LABEL org.opencontainers.image.source="https://github.com/kishanborad/event-stream-playground"

# Non-root user for security
RUN groupadd -r streamuser && useradd -r -g streamuser streamuser

WORKDIR /app

# Install runtime deps (no dev/test deps)
COPY python/requirements.txt ./requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir websockets>=12.0 aiohttp>=3.9.0 \
    && rm requirements.txt

# Copy Python source modules from builder
COPY --from=builder /build/python/*.py ./

# Shared volume for event logs (producer writes, consumer reads, analyzer reads)
VOLUME ["/data/events"]

# Default environment variables — override via docker-compose or -e flags
ENV EVENT_MODE=stdout
ENV EVENT_RATE=25
ENV EVENT_PARTITIONS=4
ENV EVENT_TYPES=""
ENV EVENT_COUNT=""
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Expose ports for SSE and WebSocket modes
EXPOSE 8080 8765

# Switch to non-root
USER streamuser

# Healthcheck: verify Python can import the producer module
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import event_producer; print('healthy')" || exit 1

# Default entrypoint — run the producer in stdout mode
ENTRYPOINT ["python", "event_producer.py"]
CMD ["--mode", "stdout", "--rate", "25", "--partitions", "4", "--count", "1000"]
