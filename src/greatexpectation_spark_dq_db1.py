import great_expectations as gx
from pyspark.sql import SparkSession
from great_expectations.checkpoint import Checkpoint
import sys
import datetime
import os
import json
from pyspark import Row
from pyspark.sql.functions import to_timestamp

# Initialize Great Expectations context
context = gx.get_context()

# Define data source
datasource = context.sources.add_or_update_spark("my_spark_datasource")
name = "my_df_asset"
data_asset = datasource.add_dataframe_asset(name=name)

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("hive_datasource") \
    .config("spark.sql.DataWashImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Define date and timestamp for file naming
date = datetime.datetime.now().strftime("%Y%m%d")
timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

# Define input parameters
dbname = "dqtest"

# Define schema for tableDF
#schema = StructType([StructField("tableName", StringType(), True)])

# Get list of tables
tableDF = spark.sql(f"SHOW TABLES IN {dbname}").select("tableName").cache()
tableDF.show()

# Collect table names
table_names = [row["tableName"] for row in tableDF.collect()]

# Define tables to process

for table_name in table_names:
        try:
            # Load data for the table
            sampleDF = spark.sql(f"SELECT * FROM {dbname}.{table_name}")
            sampleDF.show()

            # Create batch request for Great Expectations
            batch_request = data_asset.build_batch_request(dataframe=sampleDF)

            # Define and update expectation suite
            expectation_suite_name = f"analyse_{table_name}"
            context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

            # Create validator
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name,
            )
            if table_name in ["employee_monthly_salary_new", "employee_monthly_salary"]:
                validator.expect_column_values_to_not_be_null(column="name")
                validator.expect_column_values_to_be_between(column="age", min_value=18, max_value=32)
                validator.expect_column_values_to_not_be_null(column="date_of_birth")
                validator.expect_column_values_to_be_unique(column="join_date")
            if table_name == 'customer_churn':
                validator.expect_column_values_to_be_unique(column="internetservice")
            # Save expectation suite
            validator.save_expectation_suite(discard_failed_expectations=False)

            # Define checkpoint
            my_checkpoint_name = "spark_checkpoint"
            checkpoint = Checkpoint(
                name=my_checkpoint_name,
                run_name_template=f"{timestamp}-{table_name}",
                data_context=context,
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name,
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                    {
                        "name": "update_data_docs",
                        "action": {"class_name": "UpdateDataDocsAction"}
                    },
                ],
            )

            # Add or update checkpoint
            context.add_or_update_checkpoint(checkpoint=checkpoint)

            # Run checkpoint
            checkpoint_result = checkpoint.run()
            PATH = f"great_expectation/{date}"
            if not os.path.exists(PATH):
                os.makedirs(PATH)

            # Serialize checkpoint result
            checkpoint_result_serializable = checkpoint_result.to_json_dict()

            # Write validation result to file
            with open(f"{PATH}/{table_name}_validation_check_{timestamp}.json", 'w') as f:
                json.dump(checkpoint_result_serializable, f, indent=4)

            # Read the file
            with open(f"{PATH}/{table_name}_validation_check_{timestamp}.json", 'r') as file:
                data = json.load(file)

            # Extract dynamic key from "run_results"
            run_results = data["run_results"]
            dynamic_key = list(run_results.keys())[0]
            validation_results = run_results[dynamic_key]["validation_result"]["results"]
            run_time = data["run_id"]["run_time"]

            # Extract required fields
            rows = []
            for result in validation_results:
                success = result["success"]
                expectation_type = result["expectation_config"]["expectation_type"]
                element_count = result["result"]["element_count"]
                column_name = result["expectation_config"]["kwargs"]["column"]
                unexpected_count = result["result"]["unexpected_count"]
                unexpected_percent = result["result"].get("unexpected_percent", 0.0)

                rows.append(Row(run_time=run_time, success=success, expectation_type=expectation_type,
                                element_count=element_count, column_name=column_name,
                                unexpected_count=unexpected_count, unexpected_percent=unexpected_percent,
                                tablename=dbname + '.' + table_name))

            # Create DataFrame
            df = spark.createDataFrame(rows)
            df = df.withColumn("run_time", to_timestamp(df["run_time"]))
            df.show()

            # Write DataFrame to Hive table
            df.write.mode('append').format("hive").saveAsTable("datawash.greatexpectation_result")

        except Exception as e:
            print(f"An error occurred processing table {table_name}: {e}", file=sys.stderr)

# Stop SparkSession
spark.stop()
