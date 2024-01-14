from delta.tables import DeltaTable
from pyspark.sql import SparkSession

def main():

    source_bucket = "wba"

    spark = SparkSession.builder \
        .appName("CSV File to Delta Lake Table") \
        .enableHiveSupport() \
        .getOrCreate()

    input_path = f"s3a://{source_bucket}/test-data/people-100.csv"
    delta_path = f"s3a://{source_bucket}/delta/wba/tables/"

    spark.sql("DROP SCHEMA IF EXISTS wba CASCADE")

    spark.sql("CREATE DATABASE IF NOT EXISTS wba")
    spark.sql("USE wba")

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df.show()

    df.write.format("delta").option("delta.columnMapping.mode", "name")\
        .option("path", f'{delta_path}/test_table')\
        .saveAsTable("wba.test_table")

    dt = DeltaTable.forName(spark, "wba.test_table")

    dt.toDF().show()


if __name__ == "__main__":
    main()