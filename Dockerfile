# Important: This file is provided for demonstration purposes and may NOT be suitable for production use.
# The maintainers of electrs are not deeply familiar with Docker, so you should DYOR.
# If you are not familiar with Docker either it's probably be safer to NOT use it.

FROM debian:bookworm-slim AS base
RUN apt-get update -qqy
RUN apt-get install -qqy librocksdb-dev curl

### Electrum Rust Server ###
FROM base AS electrs-build
# Debian's apt cargo (bookworm: 1.63) cannot parse edition-2024 dependency
# manifests - e.g. the locked zeroize 1.9.0 dies with "failed to parse the
# `edition` key" (exit 101) - and the registry keeps drifting forward. Use
# the rolling stable toolchain instead, matching the
# dtolnay/rust-toolchain@stable matrix job in .github/workflows/rust.yml.
RUN apt-get install -qqy ca-certificates clang cmake
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH=/root/.cargo/bin:$PATH

# Install electrs
WORKDIR /build/electrs
COPY . .
ENV ROCKSDB_INCLUDE_DIR=/usr/include
ENV ROCKSDB_LIB_DIR=/usr/lib
RUN cargo install --locked --path .

FROM base AS result
# Copy the binaries
COPY --from=electrs-build /root/.cargo/bin/electrs /usr/bin/electrs

WORKDIR /
