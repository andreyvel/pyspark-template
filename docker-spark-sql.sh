# https://hub.docker.com/r/apache/spark-py/tags
# docker pull apache/spark:v3.2.3

docker run -it apache/spark:v3.2.3 /opt/spark/bin/spark-sql
# run SQL code inside docker container
# select 42 * 42 as col1, current_date() as dt;
