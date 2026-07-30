#!/usr/bin/env bash

set -euo pipefail

project_root="${1:-/home/jovyan/work}"
smoke_root="$(mktemp -d /tmp/cs675-smoke-test.XXXXXX)"

cleanup() {
    rm -rf "${smoke_root:?}"
}
trap cleanup EXIT

reviews_path="${smoke_root}/reviews.jsonl"
metadata_path="${smoke_root}/metadata.jsonl"
processed_path="${smoke_root}/processed"
results_path="${smoke_root}/results"

printf '%s\n' \
    '{"parent_asin":"P1","asin":"A1","user_id":"U1","rating":5.0,"title":"Great","text":"Works well.","timestamp":1609459200000,"helpful_vote":2,"verified_purchase":true}' \
    '{"parent_asin":"P1","asin":"A2","user_id":"U2","rating":1.0,"title":"Poor","text":"This product did not work for me.","timestamp":1640995200000,"helpful_vote":0,"verified_purchase":false}' \
    '{"parent_asin":"P1","asin":"A2","user_id":"U2","rating":1.0,"title":"Poor","text":"This product did not work for me.","timestamp":1640995200000,"helpful_vote":0,"verified_purchase":false}' \
    > "${reviews_path}"

printf '%s\n' \
    '{"parent_asin":"P1","title":"Sample Product","main_category":"Test","price":9.99,"average_rating":3.0,"rating_number":2,"store":"Sample Store"}' \
    > "${metadata_path}"

echo "Running the pipeline with temporary test data at ${smoke_root}"

spark-submit "${project_root}/src/amazon_reviews_pipeline.py" \
    --reviews-input "${reviews_path}" \
    --metadata-input "${metadata_path}" \
    --processed-output-base "${processed_path}" \
    --results-output-base "${results_path}"

expected_paths=(
    "${processed_path}/reviews_clean.parquet/_SUCCESS"
    "${processed_path}/metadata_clean.parquet/_SUCCESS"
    "${processed_path}/joined_reviews.parquet/_SUCCESS"
    "${results_path}/verified_analysis/_SUCCESS"
    "${results_path}/length_analysis/_SUCCESS"
    "${results_path}/popularity_analysis/_SUCCESS"
    "${results_path}/polarization_analysis/_SUCCESS"
)

for expected_path in "${expected_paths[@]}"; do
    if [[ ! -f "${expected_path}" ]]; then
        echo "Smoke test failed: expected output not found: ${expected_path}" >&2
        exit 1
    fi
done

echo "Smoke test passed: all expected Parquet and CSV outputs were created."
