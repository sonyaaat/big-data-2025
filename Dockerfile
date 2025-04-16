ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.2.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}
COPY . .

ENV PYSPARK_SUBMIT_ARGS="--driver-memory 8g --executor-memory 8g pyspark-shell"
ENV SPARK_DRIVER_MEMORY=8g
ENV SPARK_EXECUTOR_MEMORY=8g
ENV JAVA_OPTS="-XX:+UseG1GC -Xmx8g"

RUN chmod +x /run_scripts.sh
CMD ["/run_scripts.sh"]