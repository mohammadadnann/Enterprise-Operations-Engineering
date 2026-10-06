from datetime import datetime, timezone
from pathlib import Path

import boto3


BUCKET_NAME = "enterprise-operations-engineering-755642981182-eu-west-2-an"
REGION = "eu-west-2"

project_root = Path(__file__).resolve().parent.parent
source_root = project_root / "data" / "messy"


source_systems = {
    "sales_api": "sales",
    "procurement_sftp": "procurement",
    "inventory_s3": "inventory",
    "manufacturing_db": "manufacturing",
    "finance_s3": "finance",
}


def upload_file(s3, file_path, source_folder, target_folder, ingestion_date):
    relative_path = file_path.relative_to(source_folder)

    s3_key = (
        f"raw/{target_folder}/"
        f"ingestion_date={ingestion_date}/"
        f"{relative_path}"
    )

    s3.upload_file(
        str(file_path),
        BUCKET_NAME,
        s3_key,
    )

    return s3_key


def main():
    s3 = boto3.client("s3", region_name=REGION)

    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    uploaded_files = 0

    print("\nStarting raw S3 ingestion\n")

    for source_name, target_name in source_systems.items():
        source_folder = source_root / source_name

        print(f"Processing {source_name}")

        for file_path in source_folder.rglob("*"):
            if not file_path.is_file():
                continue

            s3_key = upload_file(
                s3,
                file_path,
                source_folder,
                target_name,
                ingestion_date,
            )

            uploaded_files += 1

            print(f"  Uploaded: {s3_key}")

    print("\nRaw ingestion completed")
    print(f"Files uploaded: {uploaded_files}")
    print(f"Ingestion date: {ingestion_date}")


if __name__ == "__main__":
    main()