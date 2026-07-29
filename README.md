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



