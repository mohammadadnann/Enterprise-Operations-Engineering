import json
import shutil
from pathlib import Path

import pandas as pd


# Set project paths
project_root = Path(__file__).resolve().parent.parent
clean_path = project_root / "data" / "simulated"
messy_path = project_root / "data" / "messy"

messy_path.mkdir(parents=True, exist_ok=True)


def copy_clean_sources():
    for source_folder in clean_path.iterdir():
        if source_folder.is_dir():
            destination = messy_path / source_folder.name

            if destination.exists():
                shutil.rmtree(destination)

            shutil.copytree(source_folder, destination)


def inject_procurement_duplicates():
    file_path = messy_path / "procurement_sftp" / "PurchaseOrder.csv"

    data = pd.read_csv(file_path)

    duplicate_rows = data.sample(
        n=100,
        random_state=42,
    )

    data = pd.concat(
        [data, duplicate_rows],
        ignore_index=True,
    )

    data.to_csv(file_path, index=False)

    return len(duplicate_rows)


def inject_malformed_json():
    file_path = messy_path / "sales_api" / "SalesOrder.json"

    with open(file_path, "r", encoding="utf-8") as file:
        records = json.load(file)

    malformed_records = [
        '{"SalesOrderID": "BROKEN_001", "CustomerID": ',
        '{"SalesOrderID": "BROKEN_002", "OrderDate": ',
        '{"SalesOrderID": "BROKEN_003", "Status": ',
    ]

    output_file = messy_path / "sales_api" / "SalesOrder_malformed.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(records, file)

        for record in malformed_records:
            file.write("\n")
            file.write(record)

    return len(malformed_records)


def create_late_arriving_file():
    source_file = messy_path / "procurement_sftp" / "GoodsReceiptLine.csv"

    data = pd.read_csv(source_file)

    late_records = data.sample(
        n=50,
        random_state=42,
    )

    late_folder = messy_path / "procurement_sftp" / "late_arrivals"
    late_folder.mkdir(exist_ok=True)

    output_file = late_folder / "GoodsReceiptLine_late.csv"

    late_records.to_csv(output_file, index=False)

    return len(late_records)


def inject_schema_drift():
    source_file = messy_path / "finance_s3" / "SalesInvoice.csv"

    data = pd.read_csv(source_file)

    drift_records = data.tail(500).copy()

    drift_records = drift_records.rename(
        columns={"GrandTotal": "InvoiceTotal"}
    )

    output_file = messy_path / "finance_s3" / "SalesInvoice_schema_drift.csv"

    drift_records.to_csv(output_file, index=False)

    return len(drift_records)


def save_manifest(results):
    manifest_path = messy_path / "data_quality_manifest.csv"

    manifest = pd.DataFrame(results)

    manifest.to_csv(manifest_path, index=False)


def main():
    print("\nCreating messy source environment\n")

    copy_clean_sources()

    duplicate_count = inject_procurement_duplicates()
    malformed_count = inject_malformed_json()
    late_count = create_late_arriving_file()
    drift_count = inject_schema_drift()

    results = [
        {
            "issue": "duplicate_records",
            "source": "procurement_sftp",
            "file": "PurchaseOrder.csv",
            "records_affected": duplicate_count,
        },
        {
            "issue": "malformed_json",
            "source": "sales_api",
            "file": "SalesOrder_malformed.json",
            "records_affected": malformed_count,
        },
        {
            "issue": "late_arriving_data",
            "source": "procurement_sftp",
            "file": "GoodsReceiptLine_late.csv",
            "records_affected": late_count,
        },
        {
            "issue": "schema_drift",
            "source": "finance_s3",
            "file": "SalesInvoice_schema_drift.csv",
            "records_affected": drift_count,
        },
    ]

    save_manifest(results)

    print(f"Duplicate procurement records: {duplicate_count}")
    print(f"Malformed JSON records: {malformed_count}")
    print(f"Late arriving records: {late_count}")
    print(f"Schema drift records: {drift_count}")

    print("\nData quality manifest created")
    print("Messy source environment created successfully")


if __name__ == "__main__":
    main()