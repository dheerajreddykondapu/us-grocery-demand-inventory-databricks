# Debugging Notes

## AWS IAM S3 Access Error

Lambda initially failed with `AccessDenied` on `s3:PutObject`. The Lambda execution role was updated with S3 object write, multipart upload, and CloudWatch log permissions.

## Lambda Memory Limit

Lambda memory usage was close to the 1024 MB limit during ingestion. Memory and ephemeral storage can be increased if needed for future runs.

## Unity Catalog Metadata Issue

`input_file_name()` failed in Unity Catalog. The Bronze ingestion notebook was updated to use `_metadata.file_path` instead.

## Malformed Product Record

One product row contained malformed numeric key data. Silver processing uses `try_cast()` and routes invalid records to `grocery_silver.quarantine_malformed_products`.

## Databricks AI Runtime

Running `ai_classify()` for all 49K products was slow. The final design uses a hybrid approach: Databricks AI Functions classify 1,000 products, and rule-based fallback logic covers the remaining products.
