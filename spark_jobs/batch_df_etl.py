from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col, avg, sum as spark_sum
from pyspark.sql import DataFrame
import json
import logging
from utils.logger import setup_logging

logger = setup_logging(__name__)

# Variable for correcting fuel type abbreviations
CORRECTED_ABBREVIATIONS = ({"battery storage" : "bats",
                            "solar battery" : "sb", 
                            "unknown energy" : "ue"})

COLUMN_RENAME_MAP = {
    "respondent-name" : "respondent_name",
    "type-name" : "type_name",
    "value-units" : "value_units"
}

def build_session(appName : str ="Electricity-Analytics-Pipeline"):

    try:
    # Create a SparkSession
        spark = SparkSession.builder \
            .appName(appName) \
            .master("local[*]") \
            .getOrCreate()
        
        return spark
    
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def clean_df(df: DataFrame, rename_map: dict) -> DataFrame:
    columns_renamed_df = df.select([col(c).alias(rename_map.get(c, c)) for c in df.columns])
    return columns_renamed_df

def save_df(df, path_name):

    output_path = f"/opt/airflow/data/transformed/{path_name}"

    try:
        df.write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(output_path)
        logger.info(f"Saved DataFrame to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write DataFrame to {output_path}")
        raise
        

def main():

    path = "/opt/airflow/data/raw/*.json"

    try:
        # Create a spark session
        spark = build_session()

        electricity_df = spark.read.json(path)

        # Rename the columns
        electricity_df = clean_df(electricity_df, COLUMN_RENAME_MAP)

        # Cache the main dataframe for repeated use
        electricity_df_cached = electricity_df.persist(StorageLevel.MEMORY_AND_DISK)

        # Display the main dataframe
        electricity_df_cached.select("*").show()

        sum_by_type = electricity_df_cached.groupBy("type_name").agg(
            spark_sum(col("value")).alias("total_megawatthours")
        ).sort("total_megawatthours", ascending=False)
        sum_by_type.show()

        save_df(sum_by_type, "production_by_fuel_type")

        print(f"Number of records: {electricity_df_cached.count()}")

        total_by_respondent = electricity_df_cached.groupBy("respondent_name").agg(
            spark_sum(col("value")).alias("total_megawatthours")
        ).sort("respondent_name")
        total_by_respondent.show()

        save_df(total_by_respondent, "production_by_respondent_name")

        avg_hourly_by_type = electricity_df_cached.groupBy("fueltype", "type_name").agg(
            avg(col("value")).alias("average_hourly_production")
        ).sort("average_hourly_production", ascending=False)
        avg_hourly_by_type.show()

        save_df(avg_hourly_by_type, "average_hourly_production_by_fuel_type")

        avg_hourly_by_respondent = electricity_df_cached.groupBy("respondent_name").agg(
            avg(col("value")).alias("average_hourly_production")
        ).sort("respondent_name")
        avg_hourly_by_respondent.show()

        save_df(avg_hourly_by_respondent, "average_hourly_production_by_respondent")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        if spark:
            # Clean up
            spark.stop()


if __name__ == "__main__":
    main()