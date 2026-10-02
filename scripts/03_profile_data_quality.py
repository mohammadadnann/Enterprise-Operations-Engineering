import sqlite3
from pathlib import Path

import pandas as pd


# Set the database path
project_root = Path(__file__).resolve().parent.parent
database_path = project_root / "data" / "source" / "CharlesRiver.sqlite"


tables = [
    "Customer",
    "SalesOrder",
    "SalesOrderLine",
    "Shipment",
    "ShipmentLine",
    "Supplier",
    "PurchaseOrder",
    "PurchaseOrderLine",
    "GoodsReceipt",
    "GoodsReceiptLine",
    "Item",
    "InventoryPolicy",
    "DemandForecast",
    "WorkOrder",
    "WorkOrderOperation",
    "MaterialIssue",
    "MaterialIssueLine",
    "ProductionCompletion",
    "SalesInvoice",
    "SalesInvoiceLine",
    "CashReceipt",
    "PurchaseInvoice",
    "PurchaseInvoiceLine",
    "DisbursementPayment",
    "GLEntry",
]


def get_table_info(connection, table_name):
    query = f'PRAGMA table_info("{table_name}")'
    return pd.read_sql_query(query, connection)


def profile_table(connection, table_name):
    data = pd.read_sql_query(f'SELECT * FROM "{table_name}"', connection)
    table_info = get_table_info(connection, table_name)

    primary_keys = table_info.loc[table_info["pk"] > 0, "name"].tolist()

    duplicate_keys = 0

    if primary_keys:
        duplicate_keys = data.duplicated(subset=primary_keys).sum()

    null_values = int(data.isnull().sum().sum())

    return {
        "table_name": table_name,
        "row_count": len(data),
        "column_count": len(data.columns),
        "primary_key": ", ".join(primary_keys),
        "duplicate_keys": int(duplicate_keys),
        "null_values": null_values,
    }


def main():
    connection = sqlite3.connect(database_path)

    results = []

    for table in tables:
        result = profile_table(connection, table)
        results.append(result)

    connection.close()

    profile = pd.DataFrame(results)

    print("\nSOURCE DATA QUALITY BASELINE\n")
    print(profile.to_string(index=False))

    print("\nSUMMARY")
    print(f"Tables checked: {len(profile)}")
    print(f"Rows checked: {profile['row_count'].sum():,}")
    print(f"Duplicate primary keys: {profile['duplicate_keys'].sum():,}")
    print(f"Null values: {profile['null_values'].sum():,}")


if __name__ == "__main__":
    main()