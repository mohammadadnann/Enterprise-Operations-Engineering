from pathlib import Path

import pandas as pd
import psycopg


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_ROOT / "data" / "messy" / "manufacturing_db"

DATABASE = "enterprise_operations"

TABLES = [
    "WorkOrder",
    "WorkOrderOperation",
    "MaterialIssue",
    "MaterialIssueLine",
    "ProductionCompletion",
]


def load_table(conn, table_name):
    file_path = SOURCE_DIR / f"{table_name}.csv"
    df = pd.read_csv(file_path)

    columns = ", ".join(f'"{column}" TEXT' for column in df.columns)

    with conn.cursor() as cursor:
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cursor.execute(f'CREATE TABLE "{table_name}" ({columns})')

        with cursor.copy(
            f'COPY "{table_name}" FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
        ) as copy:
            with open(file_path, "r", encoding="utf-8") as file:
                while data := file.read(1024 * 1024):
                    copy.write(data)

    print(f"{table_name}: {len(df):,} rows loaded")


def main():
    with psycopg.connect(f"dbname={DATABASE}") as conn:
        for table_name in TABLES:
            load_table(conn, table_name)

    print("Manufacturing PostgreSQL source created successfully.")


if __name__ == "__main__":
    main()