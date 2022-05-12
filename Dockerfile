FROM quay.io/ascendio/pyspark:production-io.ascend-spark_2.12-3.1.0-SNAPSHOT

RUN pip install jupyterlab

EXPOSE 8888

RUN mkdir -p /app/notebooks
VOLUME ["/app/notebooks"]

WORKDIR /app/notebooks
CMD ["/home/ascend/.local/bin/jupyter-lab", "--ip=0.0.0.0"]