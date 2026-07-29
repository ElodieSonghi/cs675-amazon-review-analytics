from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    spark = create_spark_session()

    spark.conf.set("spark.sql.caseSensitive", "true")

    reviews_path = "/home/jovyan/work/data/raw/All_Beauty.jsonl"
    metadata_path = "/home/jovyan/work/data/raw/meta_All_Beauty.jsonl"

    reviews_output_path = (
        "/home/jovyan/work/data/processed/reviews_clean.parquet"
    )
    metadata_output_path = (
        "/home/jovyan/work/data/processed/metadata_clean.parquet"
    )
    joined_output_path = (
        "/home/jovyan/work/data/processed/joined_reviews.parquet"
    )
    verified_results_path = (
        "/home/jovyan/work/results/verified_analysis"
    )
    length_results_path = (
        "/home/jovyan/work/results/length_analysis"
    )
    popularity_results_path = (
        "/home/jovyan/work/results/popularity_analysis"
    )
    polarization_results_path = (
        "/home/jovyan/work/results/polarization_analysis"
    )

    reviews_df = spark.read.json(reviews_path)
    metadata_df = spark.read.json(metadata_path)

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