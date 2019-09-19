FROM python:3.7-alpine

ENV PYTHONUNBUFFERED 1

ENV PYTHONDONTWRITEBYTECODE 1

RUN apk add --no-cache \
    build-base \
    git \
    libffi-dev \
    openssh-client \
    openssl-dev \
    python-dev 

# Required for building psycopg2-binary: https://github.com/psycopg/psycopg2/issues/684
RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev

# Install requirements
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && pip install --upgrade -r /requirements.txt \
    && rm -r /root/.cache

ARG WORKDIR=/code

RUN mkdir $WORKDIR

ADD ./.env.sample $WORKDIR/.env
ADD ./codecov.yml $WORKDIR/codecov.yml
ADD ./bin/ $WORKDIR/bin
ADD ./pgsync/ $WORKDIR/pgsync
ADD ./tests/ $WORKDIR/tests
ADD ./examples/ $WORKDIR/examples
ADD ./supervisor/ $WORKDIR/supervisor
ADD ./requirements.in $WORKDIR/requirements.in
ADD ./requirements.txt $WORKDIR/requirements.txt

WORKDIR $WORKDIR

ENV PYTHONPATH=$WORKDIR/pgsync
ENV PATH=$PATH:$WORKDIR/bin
ENV SCHEMA=$WORKDIR/examples/airbnb/schema.json
ENV LOG_LEVEL=debug

COPY supervisor/supervisord.conf /etc/supervisor/supervisord.conf
COPY supervisor/pgsync.conf /etc/supervisor/conf.d/
ENTRYPOINT ["/bin/sh", "supervisor/supervisord_entrypoint.sh"]
CMD ["-c", "/etc/supervisor/supervisord.conf"]
