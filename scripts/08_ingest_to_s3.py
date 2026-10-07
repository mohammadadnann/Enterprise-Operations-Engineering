import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3


BUCKET_NAME = "enterprise-operations-engineering-755642981182-eu-west-2-an"
REGION = "eu-west-2"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "data" / "messy"
CONFIG_PATH = PROJECT_ROOT / "config" / "source_config.json"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


def calculate_hash(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def get_source_files(source_folder, settings):
    if "files" in settings:
        return [
            source_folder / file_name
            for file_name in settings["files"]
        ]

    allowed_formats = {
        f".{file_format.lower()}"
        for file_format in settings.get("formats", [])
    }

    excluded = set(settings.get("exclude", []))

    return [
        file_path
        for file_path in source_folder.rglob("*")
        if file_path.is_file()
        and file_path.suffix.lower() in allowed_formats
        and file_path.name not in excluded
    ]


def object_is_unchanged(s3, s3_key, file_hash):
    try:
        response = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )

        return response.get("Metadata", {}).get("sha256") == file_hash

    except s3.exceptions.ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def upload_file(s3, file_path, domain, ingestion_date):
    file_hash = calculate_hash(file_path)

    s3_key = (
        f"raw/{domain}/"
        f"ingestion_date={ingestion_date}/"
        f"{file_path.name}"
    )

    if object_is_unchanged(s3, s3_key, file_hash):
        return s3_key, False

    s3.upload_file(
        str(file_path),
        BUCKET_NAME,
        s3_key,
        ExtraArgs={
            "Metadata": {
                "sha256": file_hash
            }
        },
    )

    return s3_key, True


def main():
    config = load_config()
    s3 = boto3.client("s3", region_name=REGION)

    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    uploaded_files = 0
    skipped_files = 0

    print("\nStarting raw S3 ingestion\n")

    for domain, settings in config.items():

        if settings["source_type"] == "postgresql":
            print("Skipping manufacturing: PostgreSQL ingestion handled separately")
            continue

        source_folder = SOURCE_ROOT / settings["source_folder"]
        source_files = get_source_files(source_folder, settings)

        print(f"Processing {domain}")

        for file_path in source_files:
            if not file_path.exists():
                print(f"  Missing: {file_path}")
                continue

            s3_key, uploaded = upload_file(
                s3,
                file_path,
                domain,
                ingestion_date,
            )

            if uploaded:
                uploaded_files += 1
                print(f"  Uploaded: {s3_key}")
            else:
                skipped_files += 1
                print(f"  Unchanged: {s3_key}")

    print("\nRaw file ingestion completed")
    print(f"Files uploaded: {uploaded_files}")
    print(f"Files skipped: {skipped_files}")
    print(f"Ingestion date: {ingestion_date}")


if __name__ == "__main__":
    main()