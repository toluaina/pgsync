FROM python:3.8
ARG WORKDIR=/code
RUN mkdir ${WORKDIR}
ADD ./examples/ ${WORKDIR}/examples
ADD ./plugins/ ${WORKDIR}/plugins
ENV PYTHONPATH="${PYTHONPATH}:${WORKDIR}/plugins"
WORKDIR $WORKDIR
ADD . ${WORKDIR}
RUN pip install ${WORKDIR}
COPY ./docker/wait-for-it.sh wait-for-it.sh
COPY ./docker/runserver.sh runserver.sh
RUN chmod +x wait-for-it.sh
RUN chmod +x runserver.sh
