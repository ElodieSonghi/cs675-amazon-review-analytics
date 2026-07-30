import argparse
import os
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_REVIEWS_PATH = "/home/jovyan/work/data/raw/All_Beauty.jsonl"
DEFAULT_METADATA_PATH = "/home/jovyan/work/data/raw/meta_All_Beauty.jsonl"
DEFAULT_PROCESSED_OUTPUT_BASE = "/home/jovyan/work/data/processed"
DEFAULT_RESULTS_OUTPUT_BASE = "/home/jovyan/work/results"

REQUIRED_REVIEW_COLUMNS = {
    "parent_asin",
    "asin",
    "user_id",
    "rating",
    "title",
    "text",
    "timestamp",
    "helpful_vote",
    "verified_purchase"
}
REQUIRED_METADATA_COLUMNS = {
    "parent_asin",
    "title",
    "main_category",
    "price",
    "average_rating",
    "rating_number",
    "store"
}


def parse_args(argv=None):
    """Parse input and output paths supplied on the command line."""
    parser = argparse.ArgumentParser(
        description=(
            "Clean, join, and analyze Amazon review and product metadata "
            "with PySpark."
        )
    )
    parser.add_argument(
        "--reviews-input",
        default=DEFAULT_REVIEWS_PATH,
        help=(
            "Review JSONL input path. Supports local paths and s3:// URIs. "
            f"Default: {DEFAULT_REVIEWS_PATH}"
        )
    )
    parser.add_argument(
        "--metadata-input",
        default=DEFAULT_METADATA_PATH,
        help=(
            "Product metadata JSONL input path. Supports local paths and "
            f"s3:// URIs. Default: {DEFAULT_METADATA_PATH}"
        )
    )
    parser.add_argument(
        "--processed-output-base",
        default=DEFAULT_PROCESSED_OUTPUT_BASE,
        help=(
            "Base directory or s3:// prefix for processed Parquet outputs. "
            f"Default: {DEFAULT_PROCESSED_OUTPUT_BASE}"
        )
    )
    parser.add_argument(
        "--results-output-base",
        default=DEFAULT_RESULTS_OUTPUT_BASE,
        help=(
            "Base directory or s3:// prefix for analytical CSV outputs. "
            f"Default: {DEFAULT_RESULTS_OUTPUT_BASE}"
        )
    )
    args = parser.parse_args(argv)

    try:
        validate_path_arguments(args)
    except ValueError as error:
        parser.error(str(error))

    return args


def join_path(base_path, child_path):
    """Join a child name to a local path or URI without changing its scheme."""
    return f"{base_path.rstrip('/')}/{child_path}"


def normalize_path(path):
    """Normalize a local path or supported URI for safety comparisons."""
    stripped_path = path.strip()
    if not stripped_path:
        raise ValueError("Paths cannot be empty.")

    parsed_path = urlparse(stripped_path)
    if parsed_path.scheme:
        scheme = parsed_path.scheme.lower()
        if scheme not in {"file", "s3"}:
            raise ValueError(
                f"Unsupported path scheme '{parsed_path.scheme}' in "
                f"'{stripped_path}'. Use a local path, file:// URI, or "
                "s3:// URI."
            )
        if scheme == "s3" and not parsed_path.netloc:
            raise ValueError(
                f"S3 path '{stripped_path}' must include a bucket name."
            )
        if parsed_path.query or parsed_path.fragment:
            raise ValueError(
                f"Path '{stripped_path}' cannot contain a query or fragment."
            )
        normalized_uri = (
            f"{scheme}://{parsed_path.netloc}{parsed_path.path}"
        )
        return normalized_uri.rstrip("/")

    absolute_path = os.path.abspath(stripped_path)
    if absolute_path == os.path.sep:
        return absolute_path
    return absolute_path.rstrip("/")


def paths_overlap(first_path, second_path):
    """Return whether two normalized paths are equal or nested."""
    first_normalized = normalize_path(first_path)
    second_normalized = normalize_path(second_path)

    return (
        first_normalized == second_normalized
        or first_normalized.startswith(f"{second_normalized}/")
        or second_normalized.startswith(f"{first_normalized}/")
    )


def validate_path_arguments(args):
    """Reject unsupported or unsafe combinations of input/output paths."""
    named_paths = {
        "--reviews-input": args.reviews_input,
        "--metadata-input": args.metadata_input,
        "--processed-output-base": args.processed_output_base,
        "--results-output-base": args.results_output_base
    }

    for option_name, path in named_paths.items():
        try:
            normalize_path(path)
        except ValueError as error:
            raise ValueError(f"{option_name}: {error}") from error

    if paths_overlap(
        args.processed_output_base,
        args.results_output_base
    ):
        raise ValueError(
            "--processed-output-base and --results-output-base must be "
            "different, non-overlapping locations because Spark overwrites "
            "output directories."
        )

    output_paths = [
        join_path(args.processed_output_base, "reviews_clean.parquet"),
        join_path(args.processed_output_base, "metadata_clean.parquet"),
        join_path(args.processed_output_base, "joined_reviews.parquet"),
        join_path(args.results_output_base, "verified_analysis"),
        join_path(args.results_output_base, "length_analysis"),
        join_path(args.results_output_base, "popularity_analysis"),
        join_path(args.results_output_base, "polarization_analysis")
    ]
    for input_option in ("reviews_input", "metadata_input"):
        input_path = getattr(args, input_option)
        for output_path in output_paths:
            if paths_overlap(input_path, output_path):
                raise ValueError(
                    f"--{input_option.replace('_', '-')} cannot overlap "
                    f"output location '{output_path}'."
                )


def validate_required_columns(dataframe, required_columns, dataset_name):
    """Raise a helpful error when an input dataset has missing columns."""
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        missing_list = ", ".join(missing_columns)
        raise ValueError(
            f"{dataset_name} input is missing required columns: "
            f"{missing_list}."
        )


def read_json_input(spark, path, dataset_name):
    """Read JSON input and add context to Spark's low-level error message."""
    try:
        return spark.read.json(path)
    except Exception as error:
        raise RuntimeError(
            f"Unable to read {dataset_name} input from '{path}'. Check that "
            "the path exists and that Spark has permission to read it."
        ) from error


def create_spark_session() -> SparkSession:
    """Create and return the Spark session."""
    return (
        SparkSession.builder
        .appName("Amazon Review Analytics")
        .getOrCreate()
    )



def preprocess_reviews(reviews_df):
    """Clean and engineer review-level features."""

    reviews_clean_df = reviews_df.select(
        "parent_asin",
        "asin",
        "user_id",
        "rating",
        "title",
        "text",
        "timestamp",
        "helpful_vote",
        "verified_purchase"
    )

    reviews_clean_df = reviews_clean_df.dropDuplicates()

    reviews_clean_df = (
        reviews_clean_df
        .withColumn(
            "review_timestamp",
            F.to_timestamp(
                F.from_unixtime(F.col("timestamp") / 1000)
            )
        )
        .withColumn(
            "review_date",
            F.to_date("review_timestamp")
        )
        .withColumn(
            "review_year",
            F.year("review_timestamp")
        )
        .withColumn(
            "review_month",
            F.month("review_timestamp")
        )
        .withColumn(
            "review_text_length",
            F.length("text")
        )
        .withColumn(
            "received_helpful_vote",
            F.when(F.col("helpful_vote") > 0, 1).otherwise(0)
        )
        .withColumn(
            "log_helpful_vote",
            F.log1p("helpful_vote")
        )
        .withColumn(
            "helpful_vote_capped",
            F.least(F.col("helpful_vote"), F.lit(13))
        )
        .withColumn(
            "verified_purchase_encoded",
            F.when(F.col("verified_purchase"), 1).otherwise(0)
        )
        .withColumn(
            "text_length_bin",
            F.when(F.col("review_text_length") < 50, "Under 50")
             .when(F.col("review_text_length") < 150, "50-149")
             .when(F.col("review_text_length") < 300, "150-299")
             .when(F.col("review_text_length") < 600, "300-599")
             .otherwise("600+")
        )
    )

    length_stats = reviews_clean_df.select(
        F.avg("review_text_length").alias("mean_length"),
        F.stddev("review_text_length").alias("std_length")
    ).first()

    mean_length = length_stats["mean_length"]
    std_length = length_stats["std_length"]

    reviews_clean_df = reviews_clean_df.withColumn(
        "review_text_length_z",
        (
            F.col("review_text_length") - F.lit(mean_length)
        ) / F.lit(std_length)
    )

    reviews_clean_df = reviews_clean_df.withColumn(
        "review_age_days",
        F.datediff(
            F.lit("2023-09-09"),
            F.col("review_date")
        )
    )

    reviews_clean_df = reviews_clean_df.withColumn(
        "helpful_votes_per_year",
        F.when(
            F.col("review_age_days") > 0,
            F.col("helpful_vote") /
            (F.col("review_age_days") / 365.25)
        ).otherwise(0)
    )

    return reviews_clean_df



def preprocess_metadata(metadata_df):
    """Clean and prepare product metadata."""

    metadata_clean_df = metadata_df.select(
        "parent_asin",
        "title",
        "main_category",
        "price",
        "average_rating",
        "rating_number",
        "store"
    )

    metadata_clean_df = (
        metadata_clean_df
        .withColumn(
            "price_missing",
            F.col("price").isNull()
        )
        .withColumn(
            "store_missing",
            F.col("store").isNull()
        )
        .withColumn(
            "store_imputed",
            F.coalesce(
                F.col("store"),
                F.lit("Unknown")
            )
        )
    )

    return metadata_clean_df



def join_reviews_with_metadata(reviews_clean_df, metadata_clean_df):
    """Join review records with product metadata."""

    metadata_for_join_df = metadata_clean_df.select(
        "parent_asin",
        F.col("title").alias("product_title"),
        "main_category",
        "price",
        "price_missing",
        "average_rating",
        "rating_number",
        F.col("store_imputed").alias("store"),
        "store_missing"
    )

    joined_df = reviews_clean_df.join(
        metadata_for_join_df,
        on="parent_asin",
        how="inner"
    )

    joined_df = joined_df.withColumn(
        "product_popularity_bin",
        F.when(F.col("rating_number") < 10, "Under 10 ratings")
         .when(F.col("rating_number") < 50, "10-49 ratings")
         .when(F.col("rating_number") < 200, "50-199 ratings")
         .when(F.col("rating_number") < 1000, "200-999 ratings")
         .otherwise("1000+ ratings")
    )

    return metadata_for_join_df, joined_df




def run_analyses(joined_df):
    """Run the final analytical queries and return the result DataFrames."""

    recent_reviews_df = joined_df.filter(
        (F.col("review_year") >= 2018) &
        (F.col("review_year") <= 2022)
    )

    verified_analysis_df = joined_df.groupBy(
        "verified_purchase"
    ).agg(
        F.count("*").alias("review_count"),
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.round(
            F.avg("review_text_length"),
            2
        ).alias("avg_text_length"),
        F.round(
            F.avg("helpful_vote"),
            3
        ).alias("avg_helpful_vote"),
        F.round(
            F.avg("received_helpful_vote") * 100,
            2
        ).alias("percent_with_helpful_vote")
    )

    length_analysis_df = recent_reviews_df.groupBy(
        "text_length_bin"
    ).agg(
        F.count("*").alias("review_count"),
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.round(
            F.avg("received_helpful_vote") * 100,
            2
        ).alias("percent_with_helpful_vote"),
        F.round(
            F.avg("helpful_votes_per_year"),
            3
        ).alias("avg_helpful_votes_per_year")
    )

    popularity_analysis_df = joined_df.groupBy(
        "product_popularity_bin"
    ).agg(
        F.count("*").alias("review_count"),
        F.round(
            F.avg("rating"),
            3
        ).alias("avg_review_rating"),
        F.round(
            F.avg("review_text_length"),
            2
        ).alias("avg_text_length"),
        F.round(
            F.avg("received_helpful_vote") * 100,
            2
        ).alias("percent_with_helpful_vote"),
        F.round(
            F.avg("helpful_votes_per_year"),
            3
        ).alias("avg_helpful_votes_per_year")
    )

    product_rating_stats_df = joined_df.groupBy(
        "parent_asin",
        "product_popularity_bin"
    ).agg(
        F.count("*").alias("review_count"),
        F.avg("rating").alias("avg_rating"),
        F.stddev("rating").alias("rating_stddev"),
        F.avg(
            F.when(
                F.col("rating").isin(1.0, 5.0),
                1
            ).otherwise(0)
        ).alias("extreme_rating_share")
    )

    polarization_analysis_df = product_rating_stats_df.groupBy(
        "product_popularity_bin"
    ).agg(
        F.count("*").alias("product_count"),
        F.round(
            F.avg("rating_stddev"),
            3
        ).alias("avg_rating_stddev"),
        F.round(
            F.avg("extreme_rating_share") * 100,
            2
        ).alias("avg_extreme_rating_percent")
    )

    print("\n1. Verified vs. unverified reviews")
    verified_analysis_df.show(truncate=False)

    print("\n2. Review length vs. helpfulness")
    length_analysis_df.show(truncate=False)

    print("\n3. Product popularity vs. review outcomes")
    popularity_analysis_df.show(truncate=False)

    print("\n4. Product popularity vs. rating polarization")
    polarization_analysis_df.show(truncate=False)

    return (
        verified_analysis_df,
        length_analysis_df,
        popularity_analysis_df,
        polarization_analysis_df
    )




def main() -> None:
    args = parse_args()
    spark = create_spark_session()

    spark.conf.set("spark.sql.caseSensitive", "true")

    reviews_path = args.reviews_input
    metadata_path = args.metadata_input

    reviews_output_path = join_path(
        args.processed_output_base,
        "reviews_clean.parquet"
    )
    metadata_output_path = join_path(
        args.processed_output_base,
        "metadata_clean.parquet"
    )
    joined_output_path = join_path(
        args.processed_output_base,
        "joined_reviews.parquet"
    )
    verified_results_path = join_path(
        args.results_output_base,
        "verified_analysis"
    )
    length_results_path = join_path(
        args.results_output_base,
        "length_analysis"
    )
    popularity_results_path = join_path(
        args.results_output_base,
        "popularity_analysis"
    )
    polarization_results_path = join_path(
        args.results_output_base,
        "polarization_analysis"
    )

    reviews_df = read_json_input(spark, reviews_path, "reviews")
    metadata_df = read_json_input(spark, metadata_path, "metadata")

    validate_required_columns(
        reviews_df,
        REQUIRED_REVIEW_COLUMNS,
        "Reviews"
    )
    validate_required_columns(
        metadata_df,
        REQUIRED_METADATA_COLUMNS,
        "Metadata"
    )

    print("Raw reviews:", reviews_df.count())
    print("Raw metadata rows:", metadata_df.count())

    reviews_clean_df = preprocess_reviews(reviews_df)
    metadata_clean_df = preprocess_metadata(metadata_df)

    metadata_for_join_df, joined_df = join_reviews_with_metadata(
        reviews_clean_df,
        metadata_clean_df
    )

    print("Clean reviews:", reviews_clean_df.count())
    print("Clean metadata rows:", metadata_clean_df.count())
    print("Joined rows:", joined_df.count())
    
    (
        verified_analysis_df,
        length_analysis_df,
        popularity_analysis_df,
        polarization_analysis_df
    ) = run_analyses(joined_df)
    
    reviews_clean_df.write \
        .mode("overwrite") \
        .parquet(reviews_output_path)

    metadata_for_join_df.write \
        .mode("overwrite") \
        .parquet(metadata_output_path)

    joined_df.write \
        .mode("overwrite") \
        .parquet(joined_output_path)

    verified_analysis_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(verified_results_path)

    length_analysis_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(length_results_path)

    popularity_analysis_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(popularity_results_path)

    polarization_analysis_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(polarization_results_path)

    print("Parquet and CSV outputs written successfully.")

    spark.stop()



if __name__ == "__main__":
    main()
