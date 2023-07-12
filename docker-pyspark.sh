# https://hub.docker.com/r/apache/spark-py/tags
# docker pull apache/spark-py:v3.2.3

docker run -it apache/spark-py:v3.2.3 /opt/spark/bin/pyspark
# run python code inside docker container
# spark.range(1000 * 1000 * 1000).count()
