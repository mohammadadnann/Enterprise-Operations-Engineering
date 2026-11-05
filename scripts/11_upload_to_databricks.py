"""Upload the simulated source files to the Databricks Unity Catalog volume.

Each domain folder in data/messy maps to a matching folder in the volume.
I uploaded only the specific files each domain needs, since some source
folders hold both a clean and a messy version of the same table.
"""

import subprocess

# CLI profile and paths for the new Databricks Free Edition account
PROFILE = "Adnan"
VOLUME_BASE = "dbfs:/Volumes/workspace/default/enterprise_raw"
LOCAL_BASE = "data/messy"

# listing every file to upload, as (local subfolder, filename, target domain folder)
# this is the single source of truth for what goes where
files_to_upload = [
    ("sales_api", "Customer.json", "sales"),
    ("sales_api", "SalesOrder.json", "sales"),
    ("sales_api", "SalesOrderLine.json", "sales"),
    ("sales_api", "Shipment.json", "sales"),
    ("sales_api", "ShipmentLine.json", "sales"),

    ("procurement_sftp", "Supplier.xlsx", "procurement"),
    ("procurement_sftp", "PurchaseOrder.csv", "procurement"),
    ("procurement_sftp", "PurchaseOrderLine.csv", "procurement"),
    ("procurement_sftp", "GoodsReceipt.xlsx", "procurement"),
    ("procurement_sftp", "GoodsReceiptLine.csv", "procurement"),
    ("procurement_sftp/late_arrivals", "GoodsReceiptLine_late.csv", "procurement/late_arrivals"),

    ("inventory_s3", "Item.parquet", "inventory"),
    ("inventory_s3", "InventoryPolicy.parquet", "inventory"),
    ("inventory_s3", "DemandForecast.parquet", "inventory"),

    ("manufacturing_db", "WorkOrder.csv", "manufacturing"),
    ("manufacturing_db", "WorkOrderOperation.csv", "manufacturing"),
    ("manufacturing_db", "MaterialIssue.csv", "manufacturing"),
    ("manufacturing_db", "MaterialIssueLine.csv", "manufacturing"),
    ("manufacturing_db", "ProductionCompletion.csv", "manufacturing"),

    ("finance_s3", "SalesInvoice.csv", "finance"),
    ("finance_s3", "SalesInvoiceLine.parquet", "finance"),
    ("finance_s3", "CashReceipt.csv", "finance"),
    ("finance_s3", "PurchaseInvoice.csv", "finance"),
    ("finance_s3", "PurchaseInvoiceLine.parquet", "finance"),
    ("finance_s3", "DisbursementPayment.csv", "finance"),
    ("finance_s3", "GLEntry.parquet", "finance"),
    ("finance_s3", "SalesInvoice_schema_drift.csv", "finance"),
]


def upload_file(local_subfolder, filename, target_folder):
    """Uploading a single file to its matching folder in the volume, 
    overwriting if it already exists."""
    local_path = f"{LOCAL_BASE}/{local_subfolder}/{filename}"
    remote_path = f"{VOLUME_BASE}/{target_folder}/{filename}"

    result = subprocess.run(
        [
            "databricks", "fs", "cp", local_path, remote_path,
            "--overwrite", "--profile", PROFILE,
        ],
        capture_output=True, text=True,
    )

    if result.returncode == 0:
        print(f"Uploaded {filename} to {target_folder}")
    else:
        print(f"Failed {filename}: {result.stderr.strip()}")


if __name__ == "__main__":
    # looping through every file in the list and uploading it one by one
    for local_subfolder, filename, target_folder in files_to_upload:
        upload_file(local_subfolder, filename, target_folder)

    print(f"\nFinished. {len(files_to_upload)} files attempted.")