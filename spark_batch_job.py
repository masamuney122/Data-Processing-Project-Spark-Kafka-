#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


OUTPUT_PATH = "hdfs://namenode:9000/project/batch_results_parquet/"


def main():
    spark = (
        SparkSession.builder
        .appName("TwitterAirlineSentimentBatch")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Hive table: default.tweets_raw_csv
    df = spark.table("default.tweets_raw_csv").select(
        F.col("airline").alias("airline"),
        F.lower(F.col("airline_sentiment")).alias("sentiment")
    )

    # Basit temizlik: airline null/empty ise ele
    df = df.filter(F.col("airline").isNotNull() & (F.length(F.trim(F.col("airline"))) > 0))

    agg = (
        df.groupBy("airline")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("total_tweets"),
            F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).cast("bigint").alias("positive_count"),
            F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).cast("bigint").alias("negative_count"),
            F.sum(F.when(F.col("sentiment") == "neutral", 1).otherwise(0)).cast("bigint").alias("neutral_count"),
        )
        .withColumn(
            "negative_ratio",
            F.when(F.col("total_tweets") > 0, F.col("negative_count") / F.col("total_tweets")).otherwise(F.lit(0.0))
        )
        .orderBy(F.desc("negative_ratio"), F.desc("total_tweets"))
    )

    # Output'u temiz yaz (external table aynı LOCATION'u görüyor)
    (
        agg.coalesce(1)  # rapor/screenshot için tek parquet dosyası daha rahat (isteğe bağlı)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    # Log için hızlı kontrol
    print("=== Top 10 airlines by negative_ratio ===")
    agg.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
