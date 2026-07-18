# Amazon Product Review Analytics at Cloud Scale

## Project Goal

This project will use Apache Spark to analyze large-scale Amazon product
review data and product metadata.

The project will examine how product category, price, verified-purchase
status, and time relate to ratings, review helpfulness, and review volume.

## Datasets

1. Amazon Reviews 2023 review records
2. Amazon Reviews 2023 product metadata

## Join Key

The two datasets will be joined using:

reviews.parent_asin = metadata.parent_asin

## Research Questions

1. How do product category and price relate to review ratings and helpful votes?
2. Do verified-purchase reviews differ from unverified reviews?
3. How do review volume and average ratings change over time?
4. Are highly reviewed products more likely to have polarized ratings?

## Local Development Plan

A smaller Amazon product category will be used for testing and development.

The local dataset will contain approximately 1 to 5 million reviews.

## Cloud-Scale Plan

The AWS version will process at least 100 million review records across
multiple product categories.

## Technologies

- Python
- PySpark
- Parquet
- GitHub
- Amazon S3
- AWS EMR or AWS Glue

## Planned Preprocessing

- Remove duplicate records
- Validate rating values
- Parse review timestamps
- Handle missing product prices
- Process verified-purchase status
- Calculate review-text length
- Create product price ranges
- Detect extreme helpful-vote values
- Convert source data to Parquet

## Planned Performance Testing

- JSON versus Parquet
- Unpartitioned versus partitioned data
- Filtering before versus after joins
- Default join versus optimized join
- Repartitioning by parent_asin
- Analysis of data skew
