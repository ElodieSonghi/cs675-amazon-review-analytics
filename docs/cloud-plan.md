# AWS Cloud Run

## Goal

My goal was to run the existing PySpark pipeline on AWS without changing its
analysis logic. The assignment required at least 100 million review rows and a
second dataset that could be joined to the reviews.

I used these Amazon Reviews 2023 categories:

- `Clothing_Shoes_and_Jewelry`
- `Home_and_Kitchen`
- the matching metadata file for each category

Together, the two review files contain 133,443,290 rows. The two metadata files
contain 10,954,065 rows. Reviews and metadata are joined with `parent_asin`.

## AWS Setup

I used this flow:

```text
Amazon review JSONL + product metadata JSONL
                     |
                     v
                Private S3 bucket
                     |
                     v
             EMR Serverless (Spark)
                     |
              +------+------+
              |             |
              v             v
       Processed Parquet   CSV results and logs
```

The S3 bucket used the following folders:

```text
s3://<bucket-name>/
    code/
    data/raw/reviews/
    data/raw/metadata/
    data/processed/
    results/
    logs/
```

I kept S3 Block Public Access enabled. The bucket does not need to be public
for grading because the code, documentation, and small final CSV files are in
GitHub. I also used a separate EMR runtime role instead of putting AWS keys in
the Python script.

The real bucket name, account number, role ARN, and credentials are not stored
in this repository.

## Checks Before Running on AWS

I validated the full files locally before uploading them. The validation found:

- 133,443,290 review rows;
- 10,954,065 metadata rows;
- no malformed rows;
- no missing `parent_asin` values;
- no duplicate metadata keys;
- 100% join coverage for a deterministic sample of 133,683 reviews.

The full validation procedure is in
[`data-validation.md`](data-validation.md).

I kept the same analysis decisions used by the local pipeline. In particular,
the helpful-vote cap stayed at 13 and the review-age reference date stayed at
`2023-09-09`. The verified-purchase analysis uses all available years. I
considered restricting it to 2018-2022 to reduce time-period differences, but
I did not want to change the analysis while moving it to AWS.

## EMR Serverless Settings

The successful run used:

- Region: `us-east-1`
- EMR release: `emr-7.13.0`
- Spark version: 3.5.6
- application limit: 16 vCPU, 64 GB memory, and 500 GB disk
- driver: 4 vCPU and 14 GB memory
- executor: 4 vCPU and 14 GB memory
- dynamic allocation: 1 to 3 executors, starting with 3
- no pre-initialized workers
- automatic start enabled
- automatic stop after five idle minutes
- EMR dynamic-allocation optimization enabled
- EMR Serverless shuffle storage enabled

I chose EMR Serverless because this project already ran as a batch
`spark-submit` job. I did not need to maintain an EC2 cluster, and the
application stopped automatically when it was idle.

## Job Arguments

The same Python script works locally and on S3 because its paths are command-line
arguments. The cloud job used the following pattern:

```text
--reviews-input
s3://<bucket-name>/data/raw/reviews/
--metadata-input
s3://<bucket-name>/data/raw/metadata/
--processed-output-base
s3://<bucket-name>/data/processed/
--results-output-base
s3://<bucket-name>/results/full/
```

The Spark sizing options were:

```text
--conf spark.driver.cores=4
--conf spark.driver.memory=14g
--conf spark.driver.maxResultSize=2g
--conf spark.executor.cores=4
--conf spark.executor.memory=14g
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.initialExecutors=3
--conf spark.dynamicAllocation.minExecutors=1
--conf spark.dynamicAllocation.maxExecutors=3
```

## Smoke Test

Before paying for the full run, I submitted a small S3 smoke test. It used four
reviews and two metadata rows. All four reviews joined successfully, and Spark
wrote all expected Parquet and CSV folders. This confirmed that the IAM role,
S3 paths, command-line arguments, and pipeline worked together.

## First Full Attempt and Retry

The first full attempt read, cleaned, joined, and analyzed the data, but it
failed while writing `joined_reviews.parquet`. Spark reported:

```text
Total size of serialized results ... is bigger than
spark.driver.maxResultSize (1024.0 MiB)
```

The task results were only slightly larger than Spark's default 1 GB driver
limit. I did not change the cleaning or analysis code. I increased
`spark.driver.maxResultSize` to `2g` and submitted the same job again.

The retry succeeded:

- job name: `cs675-full-133m-retry-2g`
- job ID: `00g7kd3h6vnf7g0b`
- start: July 31, 2026 at 01:40:17 UTC
- end: July 31, 2026 at 02:44:52 UTC
- runtime: 3,874 seconds (64 minutes 34 seconds)
- final state: `SUCCESS`

The application stopped automatically five minutes after it became idle.

## Final Row Counts

The final Spark log showed:

| Metric | Count |
| --- | ---: |
| Raw reviews | 133,443,290 |
| Raw metadata rows | 10,954,065 |
| Clean reviews | 132,084,185 |
| Clean metadata rows | 10,954,065 |
| Joined reviews | 132,084,185 |

The clean-review count and joined-review count are equal. This means every
review remaining after deduplication found matching product metadata.

I also checked the `_SUCCESS` marker for each output:

- `data/processed/reviews_clean.parquet/`
- `data/processed/metadata_clean.parquet/`
- `data/processed/joined_reviews.parquet/`
- `results/full/verified_analysis/`
- `results/full/length_analysis/`
- `results/full/popularity_analysis/`
- `results/full/polarization_analysis/`

The four small result tables are saved under `results/cloud_full/`. The two
review-based tables both add up to 132,084,185 reviews. The polarization table
covers 10,951,300 products.

## Results I Noticed

- Verified purchases made up 123,817,015 reviews, or 93.74% of the joined
  data. Their average rating was 4.184, compared with 4.089 for unverified
  reviews.
- Unverified reviews were longer on average: 332.31 characters compared with
  151.88 for verified reviews.
- In the 2018-2022 length analysis, 60.29% of reviews with at least 600
  characters had a helpful vote. Only 8.82% of reviews under 50 characters had
  one.
- Products with at least 1,000 ratings accounted for 62,982,170 reviews and
  had an average rating of 4.202.
- Average rating variation increased with popularity. The mean rating standard
  deviation was 0.881 for products with fewer than 10 ratings and 1.141 for
  products with at least 1,000 ratings.

## Cost

AWS reported 17.144 vCPU-hours, 68.578 GB-memory-hours, and 85.722
GB-storage-hours for the successful retry. Using the public `us-east-1` vCPU
and memory rates, I estimate the EMR compute cost at about $1.30 before
credits. This is an estimate, not the final bill, and it does not include the
smaller S3 storage and request charges.

Current prices are available on the
[EMR pricing page](https://aws.amazon.com/emr/pricing/).

## How to Reproduce the Cloud Run

1. Download the two review categories and their matching metadata.
2. Run `scripts/validate_full_dataset.py` locally.
3. Create a private S3 bucket and an EMR Serverless runtime role.
4. Upload `src/amazon_reviews_pipeline.py` to `code/`.
5. Upload the review and metadata JSONL files to their S3 raw folders.
6. Create an EMR Serverless Spark application with the settings above.
7. Run a small S3 smoke test.
8. Submit the full job with the four path arguments and Spark settings above.
9. Check the row counts, logs, `_SUCCESS` files, and CSV contents.
10. Confirm that the EMR application stops after the job.

Raw JSONL files, Parquet outputs, logs, credentials, and environment files must
stay outside Git.
