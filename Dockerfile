FROM python:3.9
ARG WORKDIR=/code
RUN mkdir $WORKDIR
ADD ./examples/ $WORKDIR/examples
WORKDIR $WORKDIR
RUN pip install git+https://github.com/toluaina/pgsync.git
COPY ./docker/wait-for-it.sh wait-for-it.sh
ARG EXAMPLE_NAME=airbnb
ENV EXAMPLE_NAME=$EXAMPLE_NAME
COPY ./docker/runserver.sh runserver.sh
RUN chmod +x wait-for-it.sh
RUN chmod +x runserver.sh
