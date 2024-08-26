import sys
from pyspark.sql import SparkSession

def load_tables_df(spark, data_file, dbname, tablename1):
    # Load the CSV file with the pipe delimiter
    rulesdf = spark.read \
        .option("header", "true") \
        .option("delimiter", "|") \
        .csv(data_file)

    # Save the DataFrame as a Hive table
    rulesdf.write.mode('overwrite').saveAsTable(f"{dbname}.{tablename1}")

# Create Spark session with Hive enabled
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("DataWash1") \
        .config("spark.sql.DataWashImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    dbname = sys.argv[1]

    # Create the database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")

    # Create the gx_rules table with the specified schema if it doesn't exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dbname}.gx_rules (
            RULE_ID INT,
            RULE_CREATION_DATE TIMESTAMP,
            RULE_GROUP INT,
            RULE_NAME STRING,
            RULE_DESCRIPTION STRING,
            RULE_TYPE STRING,
            MODULE STRING,
            DATABASE STRING,
            SCHEMA STRING,
            `TABLE` STRING,
            `COLUMN` STRING,
            PARAMETERS STRING,
            ENABLE INT,
            SCHEDULE STRING
        )
    """)

    # Load the data from the CSV file into the gx_rules table
    load_tables_df(spark, 'hdfs:///tmp/gx_rules.csv', dbname, "gx_rules")
