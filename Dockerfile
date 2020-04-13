FROM python:3.7
ARG WORKDIR=/code
RUN mkdir $WORKDIR
ADD ./examples/ $WORKDIR/examples
ADD ./supervisor/ $WORKDIR/supervisor
WORKDIR $WORKDIR
RUN pip install pgsync
COPY supervisor/supervisord.conf /etc/supervisor/supervisord.conf
COPY supervisor/pgsync.conf /etc/supervisor/conf.d/
ENTRYPOINT ["/bin/sh", "supervisor/supervisord_entrypoint.sh"]
CMD ["-c", "/etc/supervisor/supervisord.conf"]
