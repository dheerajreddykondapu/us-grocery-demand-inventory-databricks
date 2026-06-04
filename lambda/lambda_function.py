import os
import json
import zipfile
import urllib.request
import base64
import boto3
from pathlib import Path
from datetime import datetime, timezone

s3 = boto3.client("s3")

KAGGLE_DATASET_SLUG = os.environ.get(
    "KAGGLE_DATASET_SLUG",
    "yasserh/instacart-online-grocery-basket-analysis-dataset"
)

KAGGLE_DOWNLOAD_URL = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_DATASET_SLUG}"

FILE_TO_S3_PREFIX = {
    "orders.csv": "raw/instacart/orders/",
    "order_products__prior.csv": "raw/instacart/order_products_prior/",
    "order_products__train.csv": "raw/instacart/order_products_train/",
    "products.csv": "raw/instacart/products/",
    "aisles.csv": "raw/instacart/aisles/",
    "departments.csv": "raw/instacart/departments/"
}


def get_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def download_kaggle_dataset(username: str, key: str, output_zip_path: str) -> None:
    credentials = f"{username}:{key}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    request = urllib.request.Request(KAGGLE_DOWNLOAD_URL)
    request.add_header("Authorization", f"Basic {encoded_credentials}")

    with urllib.request.urlopen(request, timeout=180) as response:
        if response.status != 200:
            raise RuntimeError(f"Kaggle download failed with status code {response.status}")

        with open(output_zip_path, "wb") as output_file:
            output_file.write(response.read())


def find_file_recursively(base_dir: Path, file_name: str):
    matches = list(base_dir.rglob(file_name))
    if not matches:
        return None
    return matches[0]


def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    s3.upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=key,
        ExtraArgs={
            "ContentType": "text/csv",
            "Metadata": {
                "source": "instacart_market_basket",
                "ingestion_layer": "raw",
                "ingested_by": "aws_lambda",
                "ingestion_timestamp_utc": datetime.now(timezone.utc).isoformat()
            }
        }
    )


def lambda_handler(event, context):
    bucket_name = get_env("S3_BUCKET_NAME")
    kaggle_username = get_env("KAGGLE_USERNAME")
    kaggle_key = get_env("KAGGLE_KEY")

    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    tmp_dir = Path("/tmp/instacart_ingestion")
    extract_dir = tmp_dir / "extracted"

    tmp_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    zip_path = str(tmp_dir / "instacart_dataset.zip")

    uploaded_files = []
    skipped_files = []

    try:
        print("Starting Instacart raw ingestion.")
        print(f"Dataset slug: {KAGGLE_DATASET_SLUG}")
        print(f"Target bucket: {bucket_name}")

        print("Downloading dataset from Kaggle...")
        download_kaggle_dataset(kaggle_username, kaggle_key, zip_path)
        print("Download completed.")

        print("Extracting ZIP file...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        print("Extraction completed.")

        print("Extracted files:")
        for path in extract_dir.rglob("*"):
            if path.is_file():
                print(str(path))

        for file_name, s3_prefix in FILE_TO_S3_PREFIX.items():
            local_file = find_file_recursively(extract_dir, file_name)

            if local_file is None:
                skipped_files.append(file_name)
                print(f"Skipped missing file: {file_name}")
                continue

            s3_key = f"{s3_prefix}ingestion_date={ingestion_date}/{file_name}"
            print(f"Uploading {file_name} to s3://{bucket_name}/{s3_key}")

            upload_to_s3(local_path=str(local_file), bucket=bucket_name, key=s3_key)

            uploaded_files.append({
                "file_name": file_name,
                "s3_path": f"s3://{bucket_name}/{s3_key}"
            })

        result = {
            "status": "success",
            "message": "Instacart raw ingestion completed successfully.",
            "dataset": KAGGLE_DATASET_SLUG,
            "run_timestamp": run_ts,
            "uploaded_file_count": len(uploaded_files),
            "skipped_file_count": len(skipped_files),
            "uploaded_files": uploaded_files,
            "skipped_files": skipped_files
        }

        print(json.dumps(result, indent=2))
        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        error_result = {
            "status": "failed",
            "message": str(e),
            "dataset": KAGGLE_DATASET_SLUG,
            "run_timestamp": run_ts
        }
        print(json.dumps(error_result, indent=2))
        raise
