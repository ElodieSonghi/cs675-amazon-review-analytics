# Amazon Review Analytics with PySpark

## Project Overview

This project analyzes Amazon Reviews 2023 data using PySpark.

The local solution cleans review and product metadata, engineers analytical features, joins the two datasets, runs four aggregate analyses, and saves processed data and result tables.

## Current Status

- Step 00: Local Spark solution completed
- Step 01: Cloud deployment and 100M+ row run not yet completed

## Local Dataset

The current local prototype uses the Amazon Reviews 2023 `All_Beauty` category.

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


data/
    raw/
    processed/
docs/
notebooks/
    01_data_inspection.ipynb
results/
src/
    amazon_reviews_pipeline.py
README.md

Local Environment
The project runs inside a Docker container using the Jupyter PySpark image.
The local project folder is mounted into the container at:

/home/jovyan/work
Running the Pipeline
Start the Docker container and open a terminal inside JupyterLab.
Then run:

cd /home/jovyan/work
spark-submit src/amazon_reviews_pipeline.py
A successful run ends with:
Parquet and CSV outputs written successfully.

Command-Line Paths
The default command above still uses the existing local input and output
locations. To use different local paths or S3 locations, pass all four path
options:

spark-submit src/amazon_reviews_pipeline.py \
    --reviews-input /path/to/reviews.jsonl \
    --metadata-input /path/to/metadata.jsonl \
    --processed-output-base /path/to/processed \
    --results-output-base /path/to/results

The same options accept `s3://bucket/prefix` paths when Spark is running in an
AWS environment with permission to access that bucket.

Local Smoke Test
From a terminal inside the running Docker container, run:

bash scripts/run_local_smoke_test.sh

The script creates three tiny review records and one matching metadata record
under `/tmp`, runs the complete Spark pipeline, verifies all seven expected
output folders, and deletes the temporary files when it finishes. It does not
write test data or generated outputs into the repository.

Full-Scale Data Validation
Before uploading the 100M+ datasets to S3, run:

spark-submit scripts/validate_full_dataset.py

The validator checks complete row counts, malformed records, critical null
values, metadata-key uniqueness, and deterministic sampled join coverage.
Full-scale validation results and reproduction instructions are documented in
`docs/data-validation.md`.

Outputs
Processed Parquet datasets are saved to:
data/processed/reviews_clean.parquet
data/processed/metadata_clean.parquet
data/processed/joined_reviews.parquet
Analysis results are saved as CSV output folders:
results/verified_analysis
results/length_analysis
results/popularity_analysis
results/polarization_analysis
Each Spark CSV output folder contains a file beginning with part-00000 and a _SUCCESS marker.
Notebook and Script Roles
The notebook is used for exploratory analysis, data validation, and preprocessing decisions.
The Python script is the reproducible end-to-end pipeline used for local execution and future cloud deployment.

Step 01 Plan
The next phase will:
store the full-scale datasets in Amazon S3
process at least 100 million rows
use at least two joinable datasets
run the Spark solution on cloud infrastructure
save cloud outputs to S3
document runtime, configuration, cost, and results
make the full workflow reproducible from this repository

