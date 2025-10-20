# Multi-stage build for minimal production image
FROM rust:1.75-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/sombra

# Copy manifests
COPY Cargo.toml Cargo.lock build.rs ./
COPY napi.toml ./

# Copy source code
COPY src ./src
COPY benches ./benches
COPY examples ./examples
COPY tests ./tests

# Build release binary
RUN cargo build --release --bin sombra-inspect --bin sombra-repair

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r sombra && useradd -r -g sombra sombra

# Create data directory
RUN mkdir -p /data/sombra && chown -R sombra:sombra /data/sombra

# Copy binaries from builder
COPY --from=builder /usr/src/sombra/target/release/sombra-inspect /usr/local/bin/
COPY --from=builder /usr/src/sombra/target/release/sombra-repair /usr/local/bin/

# Set working directory
WORKDIR /data/sombra

# Switch to non-root user
USER sombra

# Health check (requires application to implement)
# HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
#   CMD /usr/local/bin/sombra-inspect info /data/sombra/graph.db || exit 1

# Volume for database persistence
VOLUME ["/data/sombra"]

# Default command
CMD ["/bin/bash"]

# Labels
LABEL org.opencontainers.image.title="Sombra Graph Database"
LABEL org.opencontainers.image.description="Production-ready embedded graph database"
LABEL org.opencontainers.image.version="0.2.0"
LABEL org.opencontainers.image.vendor="Sombra"
LABEL org.opencontainers.image.url="https://github.com/maskdotdev/sombra"
LABEL org.opencontainers.image.source="https://github.com/maskdotdev/sombra"
LABEL org.opencontainers.image.licenses="MIT"
