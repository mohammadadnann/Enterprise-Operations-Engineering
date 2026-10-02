import sqlite3
from pathlib import Path

import pandas as pd


# Set the database path
project_root = Path(__file__).resolve().parent.parent
database_path = project_root / "data" / "source" / "CharlesRiver.sqlite"


def get_tables(connection):
    query = """
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
    ORDER BY name
    """

    tables = pd.read_sql_query(query, connection)
    return tables["name"].tolist()


def get_row_count(connection, table_name):
    query = f'SELECT COUNT(*) AS row_count FROM "{table_name}"'
    result = pd.read_sql_query(query, connection)
    return int(result.loc[0, "row_count"])


def main():
    connection = sqlite3.connect(database_path)

    tables = get_tables(connection)

    results = []

    for table in tables:
        row_count = get_row_count(connection, table)

        results.append(
            {
                "table_name": table,
                "row_count": row_count,
            }
        )

    connection.close()

    profile = pd.DataFrame(results)
    profile = profile.sort_values("row_count", ascending=False)

    print(f"\nDatabase: {database_path.name}")
    print(f"Tables: {len(profile)}")
    print(f"Total rows: {profile['row_count'].sum():,}\n")

    print(profile.to_string(index=False))


if __name__ == "__main__":
    main()