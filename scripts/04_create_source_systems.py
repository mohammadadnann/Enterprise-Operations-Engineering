import sqlite3
from pathlib import Path

import pandas as pd


# Set project paths
project_root = Path(__file__).resolve().parent.parent
database_path = project_root / "data" / "source" / "CharlesRiver.sqlite"
output_path = project_root / "data" / "simulated"


# Define the five source systems
source_systems = {
    "sales_api": [
        "Customer",
        "SalesOrder",
        "SalesOrderLine",
        "Shipment",
        "ShipmentLine",
    ],
    "procurement_sftp": [
        "Supplier",
        "PurchaseOrder",
        "PurchaseOrderLine",
        "GoodsReceipt",
        "GoodsReceiptLine",
    ],
    "inventory_s3": [
        "Item",
        "InventoryPolicy",
        "DemandForecast",
    ],
    "manufacturing_db": [
        "WorkOrder",
        "WorkOrderOperation",
        "MaterialIssue",
        "MaterialIssueLine",
        "ProductionCompletion",
    ],
    "finance_s3": [
        "SalesInvoice",
        "SalesInvoiceLine",
        "CashReceipt",
        "PurchaseInvoice",
        "PurchaseInvoiceLine",
        "DisbursementPayment",
        "GLEntry",
    ],
}


def read_table(connection, table_name):
    query = f'SELECT * FROM "{table_name}"'
    return pd.read_sql_query(query, connection)


def save_csv(data, folder, table_name):
    file_path = folder / f"{table_name}.csv"
    data.to_csv(file_path, index=False)


def main():
    connection = sqlite3.connect(database_path)

    total_rows = 0

    for system_name, tables in source_systems.items():
        system_path = output_path / system_name
        system_path.mkdir(parents=True, exist_ok=True)

        print(f"\nCreating {system_name}")

        for table in tables:
            data = read_table(connection, table)

            save_csv(data, system_path, table)

            total_rows += len(data)

            print(f"{table}: {len(data):,} rows")

    connection.close()

    print("\nSource systems created successfully")
    print(f"Total rows exported: {total_rows:,}")


if __name__ == "__main__":
    main()