# https://hub.docker.com/r/apache/spark-py/tags
# docker pull apache/spark:v3.2.3

docker run -it apache/spark:v3.2.3 /opt/spark/bin/spark-shell
# run scala code inside docker container
# spark.range(1000 * 1000 * 1000).count()
