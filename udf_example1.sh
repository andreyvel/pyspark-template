# run ./docker-bash.sh and run this script inside docker container

/opt/spark/bin/spark-submit \
  --master local[2] \
  --deploy-mode client \
  --driver-memory 1G \
  --executor-memory 1G \
  --executor-cores 1 \
  ./udf_example1.py
