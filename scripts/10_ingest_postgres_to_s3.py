import hashlib
from datetime import datetime, timezone
from io import BytesIO

import boto3
import psycopg


BUCKET_NAME = "enterprise-operations-engineering-755642981182-eu-west-2-an"
REGION = "eu-west-2"
DATABASE = "enterprise_operations"

TABLES = [
    "WorkOrder",
    "WorkOrderOperation",
    "MaterialIssue",
    "MaterialIssueLine",
    "ProductionCompletion",
]


def extract_table(conn, table_name):
    buffer = BytesIO()

    with conn.cursor() as cursor:
        with cursor.copy(
            f'COPY "{table_name}" TO STDOUT WITH (FORMAT CSV, HEADER TRUE)'
        ) as copy:
            for data in copy:
                buffer.write(data)

    return buffer.getvalue()


def calculate_hash(data):
    return hashlib.sha256(data).hexdigest()


def object_is_unchanged(s3, s3_key, data_hash):
    try:
        response = s3.head_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )

        return response.get("Metadata", {}).get("sha256") == data_hash

    except s3.exceptions.ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def main():
    s3 = boto3.client("s3", region_name=REGION)
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    uploaded_tables = 0
    skipped_tables = 0

    print("\nStarting PostgreSQL manufacturing ingestion\n")

    with psycopg.connect(f"dbname={DATABASE}") as conn:

        for table_name in TABLES:
            data = extract_table(conn, table_name)
            data_hash = calculate_hash(data)

            s3_key = (
                f"raw/manufacturing/"
                f"ingestion_date={ingestion_date}/"
                f"{table_name}.csv"
            )

            if object_is_unchanged(s3, s3_key, data_hash):
                skipped_tables += 1
                print(f"  Unchanged: {table_name}")
                continue

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=data,
                Metadata={
                    "sha256": data_hash
                },
            )

            uploaded_tables += 1
            print(
                f"  Uploaded: {table_name} "
                f"({len(data):,} bytes)"
            )

    print("\nPostgreSQL manufacturing ingestion completed")
    print(f"Tables uploaded: {uploaded_tables}")
    print(f"Tables skipped: {skipped_tables}")
    print(f"Ingestion date: {ingestion_date}")


if __name__ == "__main__":
    main()