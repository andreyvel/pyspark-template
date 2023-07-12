# https://hub.docker.com/r/apache/spark-py/tags
# docker pull apache/spark-py:v3.2.3

# map current dir as /opt/spark/work-dir inside docker container
docker run -it -v ./:/opt/spark/work-dir apache/spark-py:v3.2.3 bash
# run script inside container, for example ./udf_example1.sh
