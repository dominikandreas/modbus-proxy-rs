FROM mcr.microsoft.com/devcontainers/rust:latest

# [Optional] Install additional packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    musl-tools \
    gcc-x86-64-linux-gnu

# [Optional] Install rust targets
RUN rustup target add x86_64-unknown-linux-musl
