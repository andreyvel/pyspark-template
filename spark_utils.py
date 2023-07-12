import os
from pyspark.sql import SparkSession

def get_spark_session():
    # setup compatible java version for pyspark
    os.environ["JAVA_HOME"] = "/opt/jdk-11.0.2"
    spark = SparkSession.builder.getOrCreate()

    # print spark default config
    print("=" * 80)
    print(f"spark.version={spark.version}")
    for pair in spark.sparkContext.getConf().getAll():
        print(f"{pair[0]}={str(pair[1])}")
    print("=" * 80)

    return spark
