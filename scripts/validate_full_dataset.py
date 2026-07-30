"""Validate full-scale Amazon review and metadata inputs with PySpark."""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


DEFAULT_REVIEWS_INPUT = (
    "/home/jovyan/work/data/raw/full/reviews/*.jsonl"
)
DEFAULT_METADATA_INPUT = (
    "/home/jovyan/work/data/raw/full/metadata/*.jsonl"
)
CORRUPT_COLUMN = "_corrupt_record"

REVIEW_SCHEMA = StructType([
    StructField("parent_asin", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("helpful_vote", LongType(), True),
    StructField("verified_purchase", BooleanType(), True),
    StructField(CORRUPT_COLUMN, StringType(), True),
])

METADATA_SCHEMA = StructType([
    StructField("parent_asin", StringType(), True),
    StructField("title", StringType(), True),
    StructField("main_category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("average_rating", DoubleType(), True),
    StructField("rating_number", LongType(), True),
    StructField("store", StringType(), True),
    StructField(CORRUPT_COLUMN, StringType(), True),
])


def sample_fraction(value):
    """Parse a sampling fraction greater than zero and at most one."""
    parsed_value = float(value)
    if not 0 < parsed_value <= 1:
        raise argparse.ArgumentTypeError(
            "sample fraction must be greater than 0 and at most 1"
        )
    return parsed_value


def positive_integer(value):
    """Parse a positive integer."""
    parsed_value = int(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed_value


def parse_args(argv=None):
    """Parse validation paths and Spark tuning options."""
    parser = argparse.ArgumentParser(
        description=(
            "Validate Amazon review and metadata JSONL inputs before a "
            "full-scale Spark run."
        )
    )
    parser.add_argument(
        "--reviews-input",
        default=DEFAULT_REVIEWS_INPUT,
        help=(
            "Review JSONL path or glob. Supports local paths and s3:// URIs. "
            f"Default: {DEFAULT_REVIEWS_INPUT}"
        ),
    )
    parser.add_argument(
        "--metadata-input",
        default=DEFAULT_METADATA_INPUT,
        help=(
            "Metadata JSONL path or glob. Supports local paths and s3:// "
            f"URIs. Default: {DEFAULT_METADATA_INPUT}"
        ),
    )
    parser.add_argument(
        "--sample-fraction",
        type=sample_fraction,
        default=0.001,
        help=(
            "Deterministic fraction of valid reviews used for join coverage. "
            "Default: 0.001 (0.1%%)"
        ),
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=675,
        help="Random seed for deterministic review sampling. Default: 675",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=positive_integer,
        default=128,
        help="Spark shuffle partition count. Default: 128",
    )
    return parser.parse_args(argv)


def read_json_with_schema(spark, path, schema):
    """Read JSON permissively and retain malformed input in a named column."""
    return (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(schema)
        .json(path)
        .withColumn("source_file", F.input_file_name())
    )


def show_review_validation(reviews_df):
    """Display counts and critical-field checks for every review file."""
    print("\n=== REVIEW FILE VALIDATION ===")
    (
        reviews_df
        .groupBy("source_file")
        .agg(
            F.count("*").alias("row_count"),
            F.sum(F.col(CORRUPT_COLUMN).isNotNull().cast("long"))
            .alias("corrupt_rows"),
            F.sum(F.col("parent_asin").isNull().cast("long"))
            .alias("null_parent_asin"),
            F.sum(F.col("rating").isNull().cast("long"))
            .alias("null_rating"),
            F.sum(F.col("text").isNull().cast("long"))
            .alias("null_text"),
            F.sum(F.col("timestamp").isNull().cast("long"))
            .alias("null_timestamp"),
            F.sum(F.col("helpful_vote").isNull().cast("long"))
            .alias("null_helpful_vote"),
            F.sum(F.col("verified_purchase").isNull().cast("long"))
            .alias("null_verified_purchase"),
        )
        .orderBy("source_file")
        .show(truncate=False)
    )


def show_metadata_validation(metadata_df):
    """Display counts and join-key checks for every metadata file."""
    print("\n=== METADATA FILE VALIDATION ===")
    (
        metadata_df
        .groupBy("source_file")
        .agg(
            F.count("*").alias("row_count"),
            F.sum(F.col(CORRUPT_COLUMN).isNotNull().cast("long"))
            .alias("corrupt_rows"),
            F.sum(F.col("parent_asin").isNull().cast("long"))
            .alias("null_parent_asin"),
            F.countDistinct("parent_asin").alias("distinct_parent_asin"),
        )
        .orderBy("source_file")
        .show(truncate=False)
    )


def show_metadata_duplicate_keys(valid_metadata_df):
    """Display duplicate-key counts within and across metadata files."""
    metadata_key_counts_df = (
        valid_metadata_df
        .groupBy("parent_asin")
        .agg(
            F.count("*").alias("metadata_rows"),
            F.countDistinct("source_file").alias("source_file_count"),
        )
    )

    print("\n=== DUPLICATE METADATA KEY VALIDATION ===")
    (
        metadata_key_counts_df
        .agg(
            F.sum((F.col("metadata_rows") > 1).cast("long"))
            .alias("duplicate_parent_asin_keys"),
            F.coalesce(
                F.sum(
                    F.when(
                        F.col("metadata_rows") > 1,
                        F.col("metadata_rows") - 1,
                    ).otherwise(0)
                ),
                F.lit(0),
            ).alias("extra_metadata_rows"),
            F.coalesce(
                F.sum(
                    (
                        (F.col("metadata_rows") > 1)
                        & (F.col("source_file_count") > 1)
                    ).cast("long")
                ),
                F.lit(0),
            ).alias("keys_shared_across_categories"),
            F.coalesce(
                F.max("metadata_rows"),
                F.lit(0),
            ).alias("maximum_rows_for_one_key"),
        )
        .show(truncate=False)
    )


def show_sample_join_coverage(
    reviews_df,
    valid_metadata_df,
    fraction,
    seed,
):
    """Display deterministic sampled review-to-metadata join coverage."""
    metadata_keys_df = valid_metadata_df.select("parent_asin").distinct()
    review_sample_df = (
        reviews_df
        .filter(
            F.col(CORRUPT_COLUMN).isNull()
            & F.col("parent_asin").isNotNull()
        )
        .sample(withReplacement=False, fraction=fraction, seed=seed)
        .select("parent_asin", "source_file")
    )

    print("\n=== DETERMINISTIC REVIEW SAMPLE JOIN COVERAGE ===")
    (
        review_sample_df
        .join(
            metadata_keys_df.withColumn("metadata_match", F.lit(1)),
            on="parent_asin",
            how="left",
        )
        .groupBy("source_file")
        .agg(
            F.count("*").alias("sampled_reviews"),
            F.sum(F.col("metadata_match").isNotNull().cast("long"))
            .alias("matched_reviews"),
        )
        .withColumn(
            "join_coverage_percent",
            F.round(
                F.col("matched_reviews")
                / F.col("sampled_reviews")
                * 100,
                3,
            ),
        )
        .orderBy("source_file")
        .show(truncate=False)
    )


def main():
    """Run all full-scale input validation checks."""
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("Amazon Reviews Full Data Validation")
        .config(
            "spark.sql.shuffle.partitions",
            str(args.shuffle_partitions),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        reviews_df = read_json_with_schema(
            spark,
            args.reviews_input,
            REVIEW_SCHEMA,
        )
        metadata_df = read_json_with_schema(
            spark,
            args.metadata_input,
            METADATA_SCHEMA,
        )

        show_review_validation(reviews_df)
        show_metadata_validation(metadata_df)

        valid_metadata_df = metadata_df.filter(
            F.col(CORRUPT_COLUMN).isNull()
            & F.col("parent_asin").isNotNull()
        )
        show_metadata_duplicate_keys(valid_metadata_df)
        show_sample_join_coverage(
            reviews_df,
            valid_metadata_df,
            args.sample_fraction,
            args.sample_seed,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
