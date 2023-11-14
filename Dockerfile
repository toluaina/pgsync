FROM debian:stable-slim
RUN apt update && apt -y upgrade
RUN apt -y install python3-pip python3.11 git
RUN rm /usr/lib/python3.11/EXTERNALLY-MANAGED
ARG WORKDIR=/code
RUN mkdir $WORKDIR
ADD ./examples/ $WORKDIR/examples
WORKDIR $WORKDIR
RUN pip install git+https://github.com/toluaina/pgsync.git
COPY ./docker/wait-for-it.sh wait-for-it.sh
COPY ./docker/runserver.sh runserver.sh
COPY ./docker/runserver_transaction.sh runserver_transaction.sh
COPY ./docker/runserver_balance_history.sh runserver_balance_history.sh
COPY ./docker/runserver_combined.sh runserver_combined.sh

RUN chmod +x wait-for-it.sh
RUN chmod +x runserver.sh
RUN chmod +x runserver_transaction.sh
RUN chmod +x runserver_balance_history.sh
RUN chmod +x runserver_combined.sh