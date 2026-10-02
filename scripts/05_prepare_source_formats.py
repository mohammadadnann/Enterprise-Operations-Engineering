from pathlib import Path

import pandas as pd


# Set project paths
project_root = Path(__file__).resolve().parent.parent
source_path = project_root / "data" / "simulated"


def convert_sales_to_json():
    folder = source_path / "sales_api"

    for file_path in folder.glob("*.csv"):
        data = pd.read_csv(file_path)

        output_file = file_path.with_suffix(".json")

        data.to_json(
            output_file,
            orient="records",
            indent=2,
        )

        print(f"Sales JSON: {output_file.name}")


def convert_procurement_files():
    folder = source_path / "procurement_sftp"

    excel_tables = {
        "Supplier",
        "GoodsReceipt",
    }

    for file_path in folder.glob("*.csv"):
        if file_path.stem in excel_tables:
            data = pd.read_csv(file_path)

            output_file = file_path.with_suffix(".xlsx")
            data.to_excel(output_file, index=False)

            print(f"Procurement Excel: {output_file.name}")
        else:
            print(f"Procurement CSV: {file_path.name}")


def convert_inventory_to_parquet():
    folder = source_path / "inventory_s3"

    for file_path in folder.glob("*.csv"):
        data = pd.read_csv(file_path)

        output_file = file_path.with_suffix(".parquet")
        data.to_parquet(output_file, index=False)

        print(f"Inventory Parquet: {output_file.name}")


def convert_finance_to_parquet():
    folder = source_path / "finance_s3"

    parquet_tables = {
        "GLEntry",
        "SalesInvoiceLine",
        "PurchaseInvoiceLine",
    }

    for file_path in folder.glob("*.csv"):
        if file_path.stem in parquet_tables:
            data = pd.read_csv(file_path)

            output_file = file_path.with_suffix(".parquet")
            data.to_parquet(output_file, index=False)

            print(f"Finance Parquet: {output_file.name}")
        else:
            print(f"Finance CSV: {file_path.name}")


def main():
    print("\nPreparing source formats\n")

    convert_sales_to_json()
    convert_procurement_files()
    convert_inventory_to_parquet()
    convert_finance_to_parquet()

    print("\nSource formats prepared successfully")


if __name__ == "__main__":
    main()