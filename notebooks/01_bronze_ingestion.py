# Databricks notebook source
# installing openpyxl so pandas can read Excel files
%pip install openpyxl
dbutils.library.restartPython()

# COMMAND ----------

# importing libraries and setting the base path to the volume
import json
import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

RAW_BASE = "/Volumes/workspace/default/enterprise_raw"

# COMMAND ----------

# creating a schema to hold the bronze tables
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

# COMMAND ----------

# reading a file based on its format and returning a dataframe
import pandas as pd

def read_source_file(path, file_format):
    if file_format == "json":
        return spark.read.option("multiLine", True).json(path)
    elif file_format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    elif file_format == "parquet":
        return spark.read.parquet(path)
    elif file_format == "xlsx":
        pandas_df = pd.read_excel(path)
        return spark.createDataFrame(pandas_df)
    else:
        raise ValueError(f"Unsupported format: {file_format}")

# COMMAND ----------

# computing a stable batch id from the file's path, size and modified time
import hashlib

def compute_batch_id(path):
    stat = os.stat(path)
    raw = f"{path}_{stat.st_size}_{stat.st_mtime}"
    return hashlib.md5(raw.encode()).hexdigest()

# COMMAND ----------

# checking if this batch is already loaded so rerunning the notebook does not duplicate it
def batch_already_loaded(table_name, batch_id):
    try:
        existing = spark.table(table_name).where(F.col("_batch_id") == batch_id).limit(1)
        return existing.count() > 0
    except AnalysisException:
        return False

# COMMAND ----------

# reading one file, tagging it with batch metadata, and appending it to its bronze table
def ingest_to_bronze(domain, file_name, file_format, table_name):
    path = f"{RAW_BASE}/{domain}/{file_name}"
    batch_id = compute_batch_id(path)
    full_table = f"workspace.bronze.{table_name}"

    if batch_already_loaded(full_table, batch_id):
        print(f"Skipped {file_name}, batch already loaded")
        return

    df = read_source_file(path, file_format)
    df = df.withColumn("_source_file", F.lit(file_name)) \
           .withColumn("_batch_id", F.lit(batch_id)) \
           .withColumn("_ingested_at", F.current_timestamp())

    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table)
    print(f"Loaded {file_name} into {table_name}")

# COMMAND ----------

# listing every source file and its target bronze table for the first batch
bronze_sources = [
    ("sales", "Customer.json", "json", "customer"),
    ("sales", "SalesOrder.json", "json", "salesorder"),
    ("sales", "SalesOrderLine.json", "json", "salesorderline"),
    ("sales", "Shipment.json", "json", "shipment"),
    ("sales", "ShipmentLine.json", "json", "shipmentline"),

    ("procurement", "Supplier.xlsx", "xlsx", "supplier"),
    ("procurement", "PurchaseOrder.csv", "csv", "purchaseorder"),
    ("procurement", "PurchaseOrderLine.csv", "csv", "purchaseorderline"),
    ("procurement", "GoodsReceipt.xlsx", "xlsx", "goodsreceipt"),
    ("procurement", "GoodsReceiptLine.csv", "csv", "goodsreceiptline"),

    ("inventory", "Item.parquet", "parquet", "item"),
    ("inventory", "InventoryPolicy.parquet", "parquet", "inventorypolicy"),
    ("inventory", "DemandForecast.parquet", "parquet", "demandforecast"),

    ("manufacturing", "WorkOrder.csv", "csv", "workorder"),
    ("manufacturing", "WorkOrderOperation.csv", "csv", "workorderoperation"),
    ("manufacturing", "MaterialIssue.csv", "csv", "materialissue"),
    ("manufacturing", "MaterialIssueLine.csv", "csv", "materialissueline"),
    ("manufacturing", "ProductionCompletion.csv", "csv", "productioncompletion"),

    ("finance", "SalesInvoice.csv", "csv", "salesinvoice"),
    ("finance", "SalesInvoiceLine.parquet", "parquet", "salesinvoiceline"),
    ("finance", "CashReceipt.csv", "csv", "cashreceipt"),
    ("finance", "PurchaseInvoice.csv", "csv", "purchaseinvoice"),
    ("finance", "PurchaseInvoiceLine.parquet", "parquet", "purchaseinvoiceline"),
    ("finance", "DisbursementPayment.csv", "csv", "disbursementpayment"),
    ("finance", "GLEntry.parquet", "parquet", "glentry"),
]

# COMMAND ----------

# running the first batch load for every source
for domain, file_name, file_format, table_name in bronze_sources:
    ingest_to_bronze(domain, file_name, file_format, table_name)

# COMMAND ----------

# loading the late arriving file as a second batch into the real goodsreceiptline table
ingest_to_bronze("procurement/late_arrivals", "GoodsReceiptLine_late.csv", "csv", "goodsreceiptline")

# COMMAND ----------

# loading the schema drift file as a second batch into the real salesinvoice table
ingest_to_bronze("finance", "SalesInvoice_schema_drift.csv", "csv", "salesinvoice")

# COMMAND ----------

# splitting the malformed sales order file into valid and broken lines
malformed_path = f"{RAW_BASE}/sales/SalesOrder_malformed.json"

good_records = []
bad_records = []

with open(malformed_path, "r") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            good_records.append(json.loads(line))
        except json.JSONDecodeError:
            bad_records.append(line)

# the one valid line is itself a list of records, so flattening it out
flat_good_records = []
for record in good_records:
    if isinstance(record, list):
        flat_good_records.extend(record)
    else:
        flat_good_records.append(record)

print(f"Good records: {len(flat_good_records)}, bad lines: {len(bad_records)}")

# COMMAND ----------

# loading valid records into salesorder and broken lines into a quarantine table
malformed_batch_id = compute_batch_id(malformed_path)

if flat_good_records:
    good_df = spark.createDataFrame(flat_good_records)
    good_df = good_df.withColumn("_source_file", F.lit("SalesOrder_malformed.json")) \
                      .withColumn("_batch_id", F.lit(malformed_batch_id)) \
                      .withColumn("_ingested_at", F.current_timestamp())
    good_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("workspace.bronze.salesorder")

if bad_records:
    bad_df = spark.createDataFrame([(line,) for line in bad_records], ["raw_line"])
    bad_df = bad_df.withColumn("_source_file", F.lit("SalesOrder_malformed.json")) \
                    .withColumn("_batch_id", F.lit(malformed_batch_id)) \
                    .withColumn("_ingested_at", F.current_timestamp())
    bad_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("workspace.bronze.quarantine_salesorder")

print(f"Loaded {len(flat_good_records)} good records, quarantined {len(bad_records)} bad lines")

# COMMAND ----------

# checking the purchaseorder table's columns
display(spark.table("workspace.bronze.purchaseorder").limit(5))

# COMMAND ----------

# finding duplicate purchase orders using a window function
display(spark.sql("""
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY PurchaseOrderID ORDER BY _ingested_at) AS rn
        FROM workspace.bronze.purchaseorder
    )
    SELECT * FROM ranked WHERE rn > 1
"""))

# COMMAND ----------

# checking the salesinvoice table's columns
display(spark.table("workspace.bronze.salesinvoice").limit(5))

# COMMAND ----------

# counting rows that came in with the drifted column name instead of the original one
display(spark.sql("""
    SELECT
        SUM(CASE WHEN InvoiceTotal IS NOT NULL THEN 1 ELSE 0 END) AS drifted_rows,
        SUM(CASE WHEN GrandTotal IS NOT NULL THEN 1 ELSE 0 END) AS original_rows
    FROM workspace.bronze.salesinvoice
"""))

# COMMAND ----------

# checking how many rows came from the late arriving file
display(spark.sql("""
    SELECT _source_file, COUNT(*) AS row_count
    FROM workspace.bronze.goodsreceiptline
    GROUP BY _source_file
"""))

# COMMAND ----------

# counting rows in every bronze table to reconcile against the expected totals
tables = spark.sql("SHOW TABLES IN workspace.bronze").collect()

for row in tables:
    table_name = row["tableName"]
    count = spark.table(f"workspace.bronze.{table_name}").count()
    print(f"{table_name}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC