FROM python:3.12.12-slim

ARG EXAMPLE_NAME=airbnb
ENV EXAMPLE_NAME=${EXAMPLE_NAME}

WORKDIR /code

# Install dependencies separately so this layer is reused until they change.
COPY requirements/base.txt ./requirements/base.txt
RUN pip install --no-cache-dir -r requirements/base.txt

# Install the checked-out project rather than an unpinned remote revision.
COPY pgsync/ ./pgsync/
COPY README.rst README.md LICENSE setup.cfg setup.py ./
COPY bin/bootstrap bin/parallel_sync bin/pgsync ./bin/
RUN pip install --no-cache-dir --no-deps .

# Example and Compose runtime files change more often than dependencies.
COPY examples/ ./examples/
COPY --chmod=755 docker/wait-for-it.sh docker/runserver.sh ./
