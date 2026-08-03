# Amazon Review Analytics with PySpark

## Project Overview

This project analyzes Amazon Reviews 2023 data using PySpark.

The local solution cleans review and product metadata, engineers analytical features, joins the two datasets, runs four aggregate analyses, and saves processed data and result tables.

## Current Status

The local pipeline, validation script, and smoke tests are complete. I also ran
the full pipeline on AWS EMR Serverless with more than 100 million reviews. The
cloud run finished successfully and the small result tables are included in
this repository.

## Local Dataset

The current local prototype uses the Amazon Reviews 2023 `All_Beauty` category.

The source data is published by the McAuley Lab:

- [Amazon Reviews 2023 dataset page](https://amazon-reviews-2023.github.io/main.html)
- [Amazon Reviews 2023 on Hugging Face](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023)

The raw files are not included in Git because they are large. Download the
review and matching metadata files from the dataset source and place them in
the local paths shown below.

### Input files

- `data/raw/All_Beauty.jsonl`
- `data/raw/meta_All_Beauty.jsonl`

### Row counts

- Raw reviews: 701,528
- Clean reviews after exact duplicate removal: 694,253
- Product metadata rows: 112,590
- Joined review rows: 694,253

The review and metadata datasets are joined using `parent_asin`.

## Preprocessing

The pipeline performs the following operations:

- removes exact duplicate review records
- converts millisecond timestamps into date and time fields
- creates review year and month fields
- calculates review text length
- encodes verified purchase status
- creates review length bins
- applies log transformation to helpful-vote counts
- caps helpful-vote outliers at the 99th percentile value of 13
- standardizes review text length using a z-score
- calculates review age and helpful votes per year
- creates missing-value indicators for product price and store
- imputes missing store values as `Unknown`
- creates product popularity bins using total rating count

Price is not imputed because approximately 84% of product price values are missing.

## Analyses

The project includes four main analyses:

1. Verified versus unverified reviews
2. Review length versus helpfulness
3. Product popularity versus review outcomes
4. Product popularity versus rating polarization

## Repository Structure

```text
data/
    raw/
    processed/
docs/
    cloud-plan.md
    data-validation.md
    project-scope.md
notebooks/
    01_data_inspection.ipynb
results/
    cloud_full/
scripts/
    run_local_smoke_test.sh
    validate_full_dataset.py
src/
    amazon_reviews_pipeline.py
.gitignore
README.md
```

## Local Environment

### Prerequisites

- Docker Desktop
- Git
- approximately 2 GB of free disk space for the local `All_Beauty` prototype
- substantially more local or S3 storage for the two full-scale categories

The project was tested with Spark 3.5.0 in this Docker image:

```text
jupyter/pyspark-notebook@sha256:58377aaa152b741e244f201679f96d909a024ea337088cc276b0ee32ab3f076f
```

From the repository root on macOS, create and start the tested container with:

```bash
docker run --name cs675-spark \
    -p 8888:8888 \
    -v "$(pwd):/home/jovyan/work" \
    jupyter/pyspark-notebook@sha256:58377aaa152b741e244f201679f96d909a024ea337088cc276b0ee32ab3f076f
```

If the container already exists but is stopped, restart it with:

```bash
docker start -a cs675-spark
```

The local project folder is mounted into the container at:

```text
/home/jovyan/work
```

## Running the Pipeline

Start the Docker container and open a terminal inside JupyterLab.
Then run:

```bash
cd /home/jovyan/work
spark-submit src/amazon_reviews_pipeline.py
```

A successful run ends with:

```text
Parquet and CSV outputs written successfully.
```

## Command-Line Paths

The default command above still uses the existing local input and output
locations. To use different local paths or S3 locations, pass all four path
options:

```bash
spark-submit src/amazon_reviews_pipeline.py \
    --reviews-input /path/to/reviews.jsonl \
    --metadata-input /path/to/metadata.jsonl \
    --processed-output-base /path/to/processed \
    --results-output-base /path/to/results
```

The same options accept `s3://bucket/prefix` paths when Spark is running in an
AWS environment with permission to access that bucket.

## Local Smoke Test

From a terminal inside the running Docker container, run:

```bash
bash scripts/run_local_smoke_test.sh
```

The script creates three tiny review records and one matching metadata record
under `/tmp`, runs the complete Spark pipeline, verifies all seven expected
output folders, and deletes the temporary files when it finishes. It does not
write test data or generated outputs into the repository.

## Full-Scale Data Validation

Before uploading the 100M+ datasets to S3, run:

```bash
spark-submit scripts/validate_full_dataset.py
```

The validator checks complete row counts, malformed records, critical null
values, metadata-key uniqueness, and deterministic sampled join coverage.
Full-scale validation results and reproduction instructions are documented in
`docs/data-validation.md`.

## Outputs

Processed Parquet datasets are saved to:

```text
data/processed/reviews_clean.parquet
data/processed/metadata_clean.parquet
data/processed/joined_reviews.parquet
```

Analysis results are saved as CSV output folders:

```text
results/verified_analysis
results/length_analysis
results/popularity_analysis
results/polarization_analysis
```

Each Spark CSV output folder contains a file beginning with part-00000 and a _SUCCESS marker.

I copied the four small result tables from the AWS run to
`results/cloud_full/`. The raw JSONL files and generated Parquet files are too
large for Git, so they are still excluded.

## Cloud-Scale Run

For the cloud run, I stored the data in S3 and ran the PySpark script with EMR
Serverless. I used two review categories and their matching metadata:

- `Clothing_Shoes_and_Jewelry`
- `Home_and_Kitchen`

Spark reported these counts:

- raw reviews: 133,443,290
- clean reviews after deduplication: 132,084,185
- metadata rows: 10,954,065
- joined reviews: 132,084,185

The final run took 3,874 seconds, or 64 minutes 34 seconds, on EMR 7.13.0. It
wrote three Parquet datasets and four CSV result tables to S3. Based on the
reported vCPU and memory use, I estimate the EMR compute cost at about $1.30
before credits. This does not include the much smaller S3 charges.

The AWS setup, retry, and commands are documented in
[`docs/cloud-plan.md`](docs/cloud-plan.md).

The AWS resources were configured manually in the AWS console rather than
with Terraform or CloudFormation. The cloud guide records the S3 layout, EMR
Serverless settings, Spark arguments, validation steps, and reproduction
procedure. Account numbers, bucket names, role ARNs, and credentials are
intentionally omitted.

## Notebook and Script Roles

The notebook is used for exploratory analysis, data validation, and preprocessing decisions.
The Python script is the reproducible end-to-end pipeline used for both local
execution and the completed AWS cloud deployment.

## Assignment Requirements

- The project uses Spark locally and on AWS.
- The full run processed 133,443,290 raw review rows.
- Reviews were joined to product metadata with `parent_asin`.
- Parquet and CSV outputs were written to S3.
- The repository includes the local tests, validation method, AWS settings,
  runtime, results, and cost estimate.
