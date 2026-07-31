# Amazon Product Review Analytics at Cloud Scale

## Project Goal

I used Apache Spark to analyze Amazon product reviews and product metadata,
first locally and then with AWS EMR Serverless.

I focused on verified purchases, review length, product popularity, review
helpfulness, and rating polarization.

## Datasets

1. Amazon Reviews 2023 review records
2. Amazon Reviews 2023 product metadata

## Join Key

I joined the two datasets using:

reviews.parent_asin = metadata.parent_asin

## Research Questions

1. Do verified-purchase reviews differ from unverified reviews?
2. How does review length relate to ratings and helpfulness?
3. How does product popularity relate to review outcomes?
4. Are highly reviewed products more likely to have polarized ratings?

## Local Development

I developed and tested the pipeline with the smaller `All_Beauty` category,
which has 701,528 raw reviews.

## Cloud-Scale Execution

For the AWS run, I used 133,443,290 reviews from two categories and 10,954,065
metadata rows. After removing duplicates, Spark joined all 132,084,185 clean
reviews to metadata.

## Technologies

- Python
- PySpark
- Parquet
- GitHub
- Amazon S3
- Amazon EMR Serverless

## Implemented Preprocessing

- Remove duplicate records
- Parse review timestamps
- Process verified-purchase status
- Calculate review-text length
- Cap extreme helpful-vote values
- Standardize review text length
- Create missing-value indicators
- Create product-popularity bins
- Convert source data to Parquet

## Final Run

The final run used EMR Serverless and finished in 64 minutes 34 seconds. It:

- exceeded the 100-million-row requirement;
- joined reviews and metadata using `parent_asin`;
- wrote three processed Parquet datasets;
- wrote four analytical CSV result tables;
- automatically stopped the EMR application after completion.

Detailed evidence and reproduction instructions are in
[`cloud-plan.md`](cloud-plan.md).
