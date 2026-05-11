from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_replace,
    length,
    when,
    lit,
    count,
    avg,
    min as spark_min,
    max as spark_max,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


RAW_PATH = "src/data/raw/snackshack_reviews.jsonl"
PROCESSED_PATH = "src/data/processed/spark_cleaned_reviews"
REJECTED_PATH = "src/data/rejected/spark_rejected_reviews"
METRICS_PATH = "src/data/metrics/spark_metrics"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SnackShackTrainingDataPipeline")
        .master("local[*]")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("text", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("rating", IntegerType(), True),
    ])

    raw_df = spark.read.schema(schema).json(RAW_PATH)

    email_pattern = r"\b[\w\.-]+@[\w\.-]+\.\w+\b"
    phone_pattern = r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"

    normalized_df = (
        raw_df
        .withColumn("text_normalized", lower(trim(col("text"))))
        .withColumn("text_normalized", regexp_replace(col("text_normalized"), email_pattern, "[EMAIL]"))
        .withColumn("text_normalized", regexp_replace(col("text_normalized"), phone_pattern, "[PHONE]"))
        .withColumn("text_length", length(col("text_normalized")))
        .withColumn(
            "had_pii",
            col("text").rlike(email_pattern) | col("text").rlike(phone_pattern)
        )
    )

    deduped_df = normalized_df.dropDuplicates(["text_normalized"])

    quality_df = (
        deduped_df
        .withColumn("valid_rating", col("rating").isin([1, 2, 3, 4, 5]))
        .withColumn("valid_length", (col("text_length") >= 10) & (col("text_length") <= 500))
        .withColumn(
            "probably_english",
            col("text_normalized").rlike(r"\b(the|was|and|but|service|food|great|worst)\b")
        )
        .withColumn(
            "quality_score",
            lit(1.0)
            - when(col("text_length") < 20, lit(0.3)).otherwise(lit(0.0))
            - when(col("had_pii"), lit(0.2)).otherwise(lit(0.0))
            - when(col("rating").isNull(), lit(0.2)).otherwise(lit(0.0))
            - when(col("text_normalized").contains("asdf"), lit(0.5)).otherwise(lit(0.0))
        )
        .withColumn(
            "quality_score",
            when(col("quality_score") < 0, lit(0.0)).otherwise(col("quality_score"))
        )
        .withColumn("dataset_version", lit("2026_05_10"))
    )

    accepted_df = (
        quality_df
        .filter(col("valid_rating") & col("valid_length") & col("probably_english"))
        .select(
            "id",
            "source",
            col("text_normalized").alias("text"),
            "rating",
            "text_length",
            "quality_score",
            "dataset_version",
        )
    )

    rejected_df = (
        quality_df
        .filter(~(col("valid_rating") & col("valid_length") & col("probably_english")))
    )

    metrics_df = quality_df.agg(
        count("*").alias("post_dedup_count"),
        avg("quality_score").alias("avg_quality_score"),
        spark_min("quality_score").alias("min_quality_score"),
        spark_max("quality_score").alias("max_quality_score"),
    )

    accepted_df.write.mode("overwrite").parquet(PROCESSED_PATH)
    rejected_df.write.mode("overwrite").parquet(REJECTED_PATH)
    metrics_df.write.mode("overwrite").parquet(METRICS_PATH)

    print("Accepted records:")
    accepted_df.show(truncate=False)

    print("Metrics:")
    metrics_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()