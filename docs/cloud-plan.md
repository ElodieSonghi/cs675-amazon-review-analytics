# AWS Cloud-Scale Plan

## Status and Guardrail

The local argument refactor and temporary-data smoke test have passed. This
document is a deployment plan only: no AWS resources have been created.
Provisioning should begin only after the remaining full-scale data checks in
this document are resolved.

Never place AWS access keys, session tokens, account IDs, role ARNs, bucket
names containing private information, or `.env` files in Git. Use an AWS CLI
profile or short-lived AWS credentials outside the repository.

## Recommended Architecture

Use Amazon S3 for storage and Amazon EMR Serverless for the required batch
PySpark run:

```text
Amazon Reviews 2023 review JSONL
        + matching product metadata JSONL
                        |
                        v
S3 data/raw/reviews/ and data/raw/metadata/
                        |
                        v
EMR Serverless Spark application
                        |
             +----------+----------+
             |                     |
             v                     v
S3 data/processed/          S3 results/ and logs/
             |
             v
Optional Athena SQL validation over processed Parquet
```

EMR Serverless is the preferred compute service because this project already
has a batch-style `spark-submit` application. It accepts a PySpark entry point
from S3, passes command-line arguments to it, supports S3 job logs, and permits
explicit driver, executor, and scaling limits. See the official
[EMR Serverless Spark job documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html).

Athena for Apache Spark is a valid alternative, but it is oriented around
interactive sessions and notebooks. Its driver is billed for the entire
session as well as worker calculations. AWS's current pricing example uses
$0.35 per DPU-hour. For this one-off reproducible batch job, EMR Serverless
provides a closer match and avoids paying for an accidentally idle notebook
session. Athena SQL can still be useful after the run for small validation
queries. See [Athena pricing](https://aws.amazon.com/athena/pricing/).

This recommendation is based on workload fit, not a guaranteed fixed price.
EMR Serverless charges for worker vCPU, memory, and configured storage while
workers are active, with a one-minute minimum. Actual regional prices must be
checked in the [official EMR pricing page](https://aws.amazon.com/emr/pricing/)
or AWS Pricing Calculator immediately before deployment.

## Full-Scale Dataset

The official Amazon Reviews 2023 release contains 571.54 million reviews and
matching item metadata. The project page also confirms that `parent_asin` is
the intended review-to-metadata join key. See the
[Amazon Reviews 2023 dataset documentation](https://amazon-reviews-2023.github.io/).

Recommended initial selection:

- `Clothing_Shoes_and_Jewelry`: approximately 66.0 million reviews
- `Home_and_Kitchen`: approximately 67.4 million reviews
- the matching metadata file for each category
- expected raw review total: approximately 133.4 million rows

This provides a comfortable margin above the 100-million-row requirement
without processing all 571.54 million reviews. Record the exact raw count
reported by Spark; the published category counts are planning estimates, not
the final evidence for the assignment.

The source downloads are commonly compressed JSONL files. A small number of
large gzip objects gives Spark too little input parallelism because a gzip
stream cannot normally be divided among many Spark tasks. Before the paid run,
decompress and divide each review dataset into multiple reasonably sized JSONL
objects, then upload those pieces under the review prefix. Do the same for very
large metadata files if necessary. Keep these raw pieces outside Git.

## S3 Layout

Use one private bucket in the same AWS Region as EMR Serverless:

```text
s3://<bucket-name>/
    code/
        amazon_reviews_pipeline.py
    data/
        raw/
            reviews/
            metadata/
        processed/
    results/
    logs/
```

Keep all four S3 Block Public Access settings enabled. AWS recommends this for
private data workloads; see the
[S3 Block Public Access documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html).
Use the bucket's default server-side encryption and add a lifecycle rule to
expire temporary logs or raw staging objects after the course retention period.

## IAM Design

Create a dedicated EMR Serverless job runtime role with least-privilege access:

- list the project bucket;
- read `code/` and `data/raw/`;
- read, write, and delete only under `data/processed/` and `results/` because
  the pipeline uses overwrite mode;
- write under `logs/`;
- no access to unrelated buckets;
- no credentials embedded in the script or command.

The person submitting the job also needs permission to pass only this runtime
role to EMR Serverless. Do not commit the role ARN; represent it as
`<execution-role-arn>` in documentation and example files.

## Pre-Deployment Gates

Complete these checks before creating the paid full-scale job:

1. Confirm exact review and metadata schemas for both selected categories.
2. Count duplicate `parent_asin` values across the combined metadata files.
   The current pipeline does not deduplicate metadata, so duplicate keys could
   multiply review rows during the join. If duplicates exist, agree on a
   deterministic rule and test that change locally before cloud deployment.
3. Decide whether the helpful-vote cap of 13 should remain a fixed local-study
   value or be recomputed from the full dataset.
4. Decide whether the fixed review-age reference date of `2023-09-09` remains
   appropriate.
5. Keep verified-purchase analysis unchanged for the first reproducibility
   run. It currently uses all years. The 2018-2022 restriction should be a
   separately approved analytical change, not mixed into deployment work.
6. Confirm sufficient join coverage with a sample from every category.
7. Confirm that no raw data, Parquet output, log, AWS CLI profile, or secret is
   staged in Git.

## Low-Cost Execution Sequence

1. Create the private S3 bucket, runtime role, and one EMR Serverless Spark
   application in a single Region.
2. Upload the reviewed pipeline script to `code/`.
3. Upload split review JSONL objects and matching metadata to their raw
   prefixes.
4. Run a small S3 smoke test first, writing to isolated `smoke/` prefixes.
5. Stop and inspect its logs, row counts, schemas, join count, and outputs.
6. Submit the full 133.4-million-row run only after the S3 smoke test passes.
7. Download or query only the small result CSVs for inspection.
8. Stop the application and delete resources that are not needed for grading.

Do not configure pre-initialized capacity for this project. AWS charges for
those workers while they wait. Leave automatic start enabled and reduce the
idle auto-stop period from its default 15 minutes after verifying that the
chosen EMR release supports the desired setting. See
[EMR Serverless application behavior](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-behavior.html)
and [pre-initialized capacity guidance](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html).

Enable Spark dynamic allocation with a conservative maximum executor count and
EMR Serverless dynamic allocation optimization. AWS states that this
optimization can better reuse workers across stages and lower cost. Start with
moderate workers, inspect the S3 smoke-test Spark UI and logs, and increase the
maximum only if the job is demonstrably constrained. Exact worker sizing should
be recorded with the final runtime rather than guessed in advance.

## Full Job Arguments

The EMR Serverless job should pass these entry-point arguments:

```text
--reviews-input
s3://<bucket-name>/data/raw/reviews/
--metadata-input
s3://<bucket-name>/data/raw/metadata/
--processed-output-base
s3://<bucket-name>/data/processed/
--results-output-base
s3://<bucket-name>/results/
```

Configure S3 monitoring logs at:

```text
s3://<bucket-name>/logs/
```

Do not paste credentials into the entry-point arguments. EMR Serverless reads
and writes S3 through the job runtime role.

## Required Evidence and Validation

Capture the following for the final report:

- source category names and official published counts;
- Spark's exact raw review count, proving at least 100 million rows;
- raw metadata count;
- exact clean-review count after duplicate removal;
- metadata count used for the join;
- joined-review count and join coverage percentage;
- selected EMR release and Spark version;
- driver, executor, dynamic allocation, and maximum-capacity settings;
- job ID, start time, end time, and runtime;
- successful S3 output paths;
- row counts or screenshots for all four final analyses;
- Spark UI evidence for stages, tasks, shuffle, and any skew;
- cost estimate before the job and actual AWS cost after billing appears;
- cleanup actions and any intentionally retained resources.

For optional Athena validation, query processed Parquet rather than raw JSON so
that validation scans less data. Athena does not replace the required Spark
processing step.

## Reproducibility Order

The final documented workflow should be reproducible in this order:

1. obtain the two review categories and their matching metadata;
2. split and upload raw files to the documented S3 prefixes;
3. upload the exact committed pipeline version;
4. create or select the documented EMR Serverless application;
5. run the S3 smoke test;
6. run the full job with the four path arguments;
7. validate counts and outputs;
8. record runtime and cost;
9. stop compute and apply the documented cleanup policy.
