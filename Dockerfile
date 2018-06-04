FROM dv01/jdk8

MAINTAINER dv01 <dean@dv01.co>

ENV INSTALL_OPTS="-y --no-install-recommends"
ENV SPARK_HOME /usr/local/spark
ENV PATH $PATH:$SPARK_HOME/bin
ENV TMP /tmpsparksrc
ENV MASTER_HOSTNAME master

RUN echo "deb http://cran.rstudio.com/bin/linux/ubuntu xenial/" | tee -a /etc/apt/sources.list \
  && gpg --keyserver keyserver.ubuntu.com --recv-key E084DAB9 \
  && gpg -a --export E084DAB9 | apt-key add - \
  && apt-get update \
  && apt-get install $INSTALL_OPTS \
    r-base \
    r-base-dev \
    maven \
  && rm -rf /var/lib/apt/lists/* \
  && mkdir -p $TMP

WORKDIR $TMP

COPY . $TMP

RUN ./dev/make-distribution.sh --name dv01 -Psparkr -Phadoop-2.7 -Phive -Phive-thriftserver \
  && mkdir -p $SPARK_HOME \
  && cp -R dist/* $SPARK_HOME/ \
  && rm -rf $TMP \
  && mkdir -p /spark/scratch \
  && chmod -R 777 /spark

WORKDIR $SPARK_HOME

VOLUME ["/spark"]

# 8080 -> master ui, 7077 -> spark-submit, 6066 -> spark-submit-rest 8888 -> worker port, 8081 -> worker ui
EXPOSE 8080 7077 6066 8888 8081

CMD ["/bin/sh", "-c", "ls"]
# CMD ["/bin/sh", "-c", "SPARK_PUBLIC_DNS=${SPARK_PUBLIC_DNS:-$(hostname)} ./sbin/start-slave.sh spark://$MASTER_HOSTNAME:7077 -d /spark/scratch && tail -f /usr/local/spark/logs/spark--*"]
