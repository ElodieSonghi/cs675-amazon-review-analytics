# Full-Scale Data Validation

## Purpose

This validation confirms that the selected Amazon Reviews 2023 inputs satisfy
the project's scale and joinability requirements before they are uploaded to
Amazon S3 or processed with EMR Serverless.

The validation was run locally with Apache Spark 3.5.0 on July 30, 2026. Raw
datasets and generated Spark files remain excluded from Git.

## Selected Inputs

Review datasets:

- `Clothing_Shoes_and_Jewelry.jsonl`
- `Home_and_Kitchen.jsonl`

Matching product metadata:

- `meta_Clothing_Shoes_and_Jewelry.jsonl`
- `meta_Home_and_Kitchen.jsonl`

The datasets use `parent_asin` as the review-to-product join key.

## Validation Method

The reusable PySpark validator is:

```text
scripts/validate_full_dataset.py
```

It performs the following checks:

1. reads review and metadata JSONL with explicit schemas;
2. retains malformed input in `_corrupt_record`;
3. counts rows separately for every source file;
4. counts null values in critical review fields;
5. counts null metadata join keys;
6. compares metadata row counts with distinct `parent_asin` counts;
7. detects duplicate metadata keys within and across category files;
8. takes a deterministic 0.1% review sample using seed `675`;
9. measures sample review-to-metadata join coverage by source file.

The sample is used only for the join-coverage check. The row, malformed-record,
null-field, distinct-key, and duplicate-key checks scan the complete datasets.

## Results

### Review validation

| Dataset | Rows | Malformed rows | Null `parent_asin` | Null rating | Null text | Null timestamp | Null helpful vote | Null verified status |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Clothing, Shoes and Jewelry | 66,033,346 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Home and Kitchen | 67,409,944 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| **Total** | **133,443,290** | **0** | **0** | **0** | **0** | **0** | **0** | **0** |

The combined review input exceeds the assignment requirement of 100 million
rows by 33,443,290 rows.

### Metadata validation

| Dataset | Rows | Distinct `parent_asin` | Malformed rows | Null `parent_asin` |
|---|---:|---:|---:|---:|
| Clothing, Shoes and Jewelry metadata | 7,218,481 | 7,218,481 | 0 | 0 |
| Home and Kitchen metadata | 3,735,584 | 3,735,584 | 0 | 0 |
| **Total** | **10,954,065** | **10,954,065** | **0** | **0** |

Combined metadata-key checks:

| Check | Result |
|---|---:|
| Duplicate `parent_asin` keys | 0 |
| Extra metadata rows caused by duplicates | 0 |
| Keys shared across the two category files | 0 |
| Maximum metadata rows for one key | 1 |

Because every combined metadata key is unique, the metadata side of the join
will not multiply review rows.

### Deterministic sample join coverage

| Review dataset | Sampled reviews | Matched reviews | Coverage |
|---|---:|---:|---:|
| Clothing, Shoes and Jewelry | 65,943 | 65,943 | 100.0% |
| Home and Kitchen | 67,740 | 67,740 | 100.0% |
| **Total** | **133,683** | **133,683** | **100.0%** |

This result provides strong evidence that the selected review files have
matching product metadata. The full cloud job must still record its exact
joined-review count as the final join-coverage evidence.

## Reproduction

From a terminal inside the local Docker container:

```bash
cd /home/jovyan/work

spark-submit scripts/validate_full_dataset.py \
    --reviews-input "data/raw/full/reviews/*.jsonl" \
    --metadata-input "data/raw/full/metadata/*.jsonl" \
    --sample-fraction 0.001 \
    --sample-seed 675 \
    --shuffle-partitions 128
```

The defaults point to these same local paths, so the shorter equivalent is:

```bash
spark-submit scripts/validate_full_dataset.py
```

After upload, the validator can run against S3 by supplying prefixes or globs:

```bash
spark-submit scripts/validate_full_dataset.py \
    --reviews-input "s3://<bucket-name>/data/raw/reviews/*.jsonl" \
    --metadata-input "s3://<bucket-name>/data/raw/metadata/*.jsonl"
```

Bucket names, AWS account identifiers, role ARNs, and credentials must not be
hard-coded in the repository.

## Cloud Result

The selected inputs passed the local structural, scale, metadata-key, and
sample-join checks. No metadata deduplication rule is required for these two
files.

The AWS run later confirmed the following:

- exact clean-review count after deduplication: 132,084,185;
- exact joined-review count: 132,084,185;
- full cleaned-review join retention: 100%;
- all three processed Parquet outputs completed;
- all four analytical result tables completed;
- I recorded the Spark settings, runtime, logs, resource use, estimated cost,
  and automatic shutdown.

See [`cloud-plan.md`](cloud-plan.md) for the full execution record.

The validation does not change analytical choices. In particular, the
verified-purchase analysis still uses all years, the helpful-vote cap remains
13, and the review-age reference date remains `2023-09-09`.
