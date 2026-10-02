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
    "Item",
    "InventoryPolicy",
    "DemandForecast",
    "Supplier",
    "PurchaseOrder",
    "PurchaseOrderLine",
    "GoodsReceipt",
    "GoodsReceiptLine",
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


def get_columns(connection, table_name):
    query = f'PRAGMA table_info("{table_name}")'
    columns = pd.read_sql_query(query, connection)

    return columns[["name", "type", "pk"]]


def main():
    connection = sqlite3.connect(database_path)

    for table in tables:
        columns = get_columns(connection, table)

        print(f"\n{'=' * 60}")
        print(table)
        print("=" * 60)

        print(columns.to_string(index=False))

    connection.close()


if __name__ == "__main__":
    main()