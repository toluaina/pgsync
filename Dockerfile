FROM python:3.12-slim AS build

ARG WORKDIR=/code
ARG EXAMPLE_NAME=airbnb
ENV EXAMPLE_NAME=$EXAMPLE_NAME

# Create and switch to workdir
RUN mkdir -p "$WORKDIR"
WORKDIR "$WORKDIR"

# Install git (required for pip install from git+ URL)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY ./examples/ ./examples
COPY ./docker/wait-for-it.sh ./wait-for-it.sh
COPY ./docker/runserver.sh ./runserver.sh

# Install pgsync from GitHub
RUN pip install --no-cache-dir git+https://github.com/toluaina/pgsync.git

# Make scripts executable
RUN chmod +x wait-for-it.sh runserver.sh
