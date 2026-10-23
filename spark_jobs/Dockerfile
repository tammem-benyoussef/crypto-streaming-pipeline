FROM spark:4.2.0-scala2.13-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir "psycopg[binary]==3.3.6"
WORKDIR /app
COPY spark_jobs ./spark_jobs
RUN mkdir -p /checkpoints /home/spark \
    && mkdir -p /home/spark/.ivy2 \
    && chown -R spark:spark /app /checkpoints /home/spark

USER spark
ENV HOME=/home/spark

RUN /opt/spark/bin/spark-submit \
      --conf spark.jars.ivy=/home/spark/.ivy2 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
      --class org.apache.spark.examples.SparkPi \
      "$(ls /opt/spark/examples/jars/spark-examples*.jar | head -1)" 1

ENV PYTHONPATH=/app PYTHONUNBUFFERED=1

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
            "--conf", "spark.jars.ivy=/home/spark/.ivy2", \
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0", \
            "/app/spark_jobs/bars_job.py"]