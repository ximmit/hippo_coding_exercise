"""
This module provides analytical capabilities to process pharmacy claims data using Apache Spark.
It includes functions to recommend top pharmacy chains based on average drug prices and to determine
the most common quantities prescribed for each drug.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from constants import (
    CLAIMS_DIR,
    PHARMACY_DIR,
    CLAIMS_SCHEMA,
    PHARMACY_SCHEMA,
    TASK_3_OUTPUT,
    TASK_4_OUTPUT,
)


def recommend_top_chains(spark, claims_data_path, pharmacy_data_path, output_path):
    """Recommend the top two pharmacy chains with the lowest average drug prices per NDC."""
    df_claims = spark.read.option("mode", "DROPMALFORMED").json(
        claims_data_path, schema=CLAIMS_SCHEMA
    )
    df_pharmacy = spark.read.option("mode", "DROPMALFORMED").csv(
        pharmacy_data_path, schema=PHARMACY_SCHEMA, header=True
    )

    # Join claims data with pharmacy data on NPI
    df_joined = df_claims.join(df_pharmacy, "npi")

    # Calculate average price per NDC per chain
    df_avg_price = df_joined.groupBy("ndc", "chain").agg(
        F.avg("price").alias("avg_price")
    )

    # Window specification to rank chains by avg_price per drug
    window_spec = Window.partitionBy("ndc").orderBy(F.col("avg_price").asc())

    # Rank chains by average price and filter for top 2
    df_ranked = df_avg_price.withColumn("rank", F.rank().over(window_spec)).filter(
        F.col("rank") <= 2
    )

    # Transform into desired JSON structure by grouping and collecting structures
    df_result = df_ranked.groupBy("ndc").agg(
        F.collect_list(
            F.struct(F.col("chain").alias("name"), F.col("avg_price"))
        ).alias("chain")
    )

    # Write the output to JSON
    df_result.coalesce(1).write.mode("overwrite").json(output_path)


def calculate_most_common_quantity(spark, claims_data_path, output_path):
    """ "
    Calculate and output the five most common quantities prescribed for each drug.
    """

    # Read claims data
    df_claims = spark.read.option("mode", "DROPMALFORMED").json(
        claims_data_path, schema=CLAIMS_SCHEMA
    )

    # Compute the most common quantities per NDC
    df_counts = df_claims.groupBy("ndc", "quantity").count()
    # Use a window function to rank quantities by occurrence
    window_spec = Window.partitionBy("ndc").orderBy(F.desc("count"))
    df_ranked = df_counts.withColumn("rank", F.rank().over(window_spec))
    # Select the most frequent quantities per drug
    df_most_common = (
        df_ranked.filter(F.col("rank") <= 5)
        .groupBy("ndc")
        .agg(F.collect_list("quantity").alias("most_prescribed_quantity"))
    )

    # Write the output to JSON
    df_most_common.coalesce(1).write.mode("overwrite").json(output_path)


if __name__ == "__main__":
    spark_s = SparkSession.builder.appName("Task 3 and 4 calculation").getOrCreate()
    recommend_top_chains(spark_s, CLAIMS_DIR, PHARMACY_DIR, TASK_3_OUTPUT)
    calculate_most_common_quantity(spark_s, CLAIMS_DIR, TASK_4_OUTPUT)
