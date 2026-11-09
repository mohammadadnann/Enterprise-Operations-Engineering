# Databricks notebook source
# printing the schema of every bronze table so I can write real cleaning logic, not guesses
tables = spark.sql("SHOW TABLES IN workspace.bronze").collect()

for row in tables:
    table_name = row["tableName"]
    print(f"\n--- {table_name} ---")
    spark.table(f"workspace.bronze.{table_name}").printSchema()

# COMMAND ----------

# importing libraries and creating the silver schema
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# COMMAND ----------

# mapping each bronze table to its primary key, used to find and remove duplicates
primary_keys = {
    "cashreceipt": "CashReceiptID",
    "customer": "CustomerID",
    "demandforecast": "DemandForecastID",
    "disbursementpayment": "DisbursementID",
    "glentry": "GLEntryID",
    "goodsreceipt": "GoodsReceiptID",
    "goodsreceiptline": "GoodsReceiptLineID",
    "inventorypolicy": "InventoryPolicyID",
    "item": "ItemID",
    "materialissue": "MaterialIssueID",
    "materialissueline": "MaterialIssueLineID",
    "productioncompletion": "ProductionCompletionID",
    "purchaseinvoice": "PurchaseInvoiceID",
    "purchaseinvoiceline": "PILineID",
    "purchaseorder": "PurchaseOrderID",
    "purchaseorderline": "POLineID",
    "salesinvoice": "SalesInvoiceID",
    "salesinvoiceline": "SalesInvoiceLineID",
    "salesorder": "SalesOrderID",
    "salesorderline": "SalesOrderLineID",
    "shipment": "ShipmentID",
    "shipmentline": "ShipmentLineID",
    "supplier": "SupplierID",
    "workorder": "WorkOrderID",
    "workorderoperation": "WorkOrderOperationID",
}

# COMMAND ----------

# listing date columns that came in as strings, so I can cast them properly
date_columns = {
    "demandforecast": ["ForecastWeekStartDate", "ForecastWeekEndDate", "ApprovedDate"],
    "goodsreceipt": ["ReceiptDate"],
    "glentry": ["PostingDate", "CreatedDate"],
    "inventorypolicy": ["EffectiveStartDate", "EffectiveEndDate"],
    "item": ["LaunchDate"],
    "salesorder": ["OrderDate", "RequestedDeliveryDate"],
    "shipment": ["DeliveryDate", "ShipmentDate"],
}

# COMMAND ----------

# fixing the salesinvoice schema drift by merging the two total columns into one
def fix_salesinvoice(df):
    return df.withColumn("GrandTotal", F.coalesce(F.col("GrandTotal"), F.col("InvoiceTotal"))) \
              .drop("InvoiceTotal")

# flagging goodsreceiptline rows that came from the late arriving file
def flag_goodsreceiptline(df):
    return df.withColumn("is_late_arrival", F.col("_source_file") == F.lit("GoodsReceiptLine_late.csv"))

special_transforms = {
    "salesinvoice": fix_salesinvoice,
    "goodsreceiptline": flag_goodsreceiptline,
}

# COMMAND ----------

# cleaning one bronze table: applying its special fix if it has one, casting its date columns, and removing duplicates
def clean_to_silver(table_name):
    df = spark.table(f"workspace.bronze.{table_name}")

    if table_name in special_transforms:
        df = special_transforms[table_name](df)

    for col_name in date_columns.get(table_name, []):
        df = df.withColumn(col_name, F.to_date(F.col(col_name)))

    primary_key = primary_keys[table_name]
    window = F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"]).Window
        .partitionBy(primary_key).orderBy(F.col("_ingested_at").desc())
    )
    before_count = df.count()
    df = df.withColumn("_rn", window).where(F.col("_rn") == 1).drop("_rn")
    after_count = df.count()

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"workspace.silver.{table_name}")

    print(f"{table_name}: {before_count} -> {after_count} rows ({before_count - after_count} duplicates removed)")

# COMMAND ----------

# importing Window for the duplicate removal logic
from pyspark.sql.window import Window

# checking whether duplicate rows are true copies or actually conflicting data, and saving what gets dropped for review
def audit_duplicates(df, table_name, primary_key):
    window = Window.partitionBy(primary_key).orderBy(F.col("_ingested_at").desc())
    ranked = df.withColumn("_rn", F.row_number().over(window))
    duplicates = ranked.where(F.col("_rn") > 1).drop("_rn")

    if duplicates.count() > 0:
        duplicates.write.format("delta").mode("append").option("mergeSchema", "true") \
            .saveAsTable(f"workspace.silver.dropped_duplicates_{table_name}")

    return duplicates.count()

# COMMAND ----------

# cleaning one bronze table: applying its special fix, casting dates, auditing and removing duplicates
def clean_to_silver(table_name):
    df = spark.table(f"workspace.bronze.{table_name}")

    if table_name in special_transforms:
        df = special_transforms[table_name](df)

    for col_name in date_columns.get(table_name, []):
        df = df.withColumn(col_name, F.to_date(F.col(col_name)))

    primary_key = primary_keys[table_name]
    before_count = df.count()
    dropped_count = audit_duplicates(df, table_name, primary_key)

    window = Window.partitionBy(primary_key).orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")
    after_count = df.count()

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"workspace.silver.{table_name}")

    print(f"{table_name}: {before_count} -> {after_count} rows ({dropped_count} duplicates removed and saved for review)")

# COMMAND ----------

# listing every table to clean, taken from the primary key mapping so quarantine_salesorder is left out automatically
tables_to_process = list(primary_keys.keys())

# COMMAND ----------

# running the cleaning function for every bronze table
for table_name in tables_to_process:
    clean_to_silver(table_name)

# COMMAND ----------

# checking for negative values in key monetary columns, which should never happen
amount_columns = {
    "purchaseorder": "OrderTotal",
    "purchaseorderline": "LineTotal",
    "purchaseinvoice": "GrandTotal",
    "purchaseinvoiceline": "LineTotal",
    "salesorder": "OrderTotal",
    "salesorderline": "LineTotal",
    "salesinvoice": "GrandTotal",
    "salesinvoiceline": "LineTotal",
    "cashreceipt": "Amount",
    "disbursementpayment": "Amount",
}

for table_name, amount_col in amount_columns.items():
    negative_count = spark.table(f"workspace.silver.{table_name}").where(F.col(amount_col) < 0).count()
    print(f"{table_name}.{amount_col}: {negative_count} negative values")

# COMMAND ----------

# checking that key foreign keys actually exist in their parent tables, instead of assuming they do
foreign_keys = [
    ("purchaseorder", "SupplierID", "supplier", "SupplierID"),
    ("salesorder", "CustomerID", "customer", "CustomerID"),
    ("purchaseorderline", "ItemID", "item", "ItemID"),
    ("salesorderline", "ItemID", "item", "ItemID"),
    ("goodsreceipt", "PurchaseOrderID", "purchaseorder", "PurchaseOrderID"),
    ("salesinvoice", "SalesOrderID", "salesorder", "SalesOrderID"),
]

for child_table, child_col, parent_table, parent_col in foreign_keys:
    child_df = spark.table(f"workspace.silver.{child_table}")
    parent_df = spark.table(f"workspace.silver.{parent_table}")
    orphan_count = child_df.join(parent_df, child_df[child_col] == parent_df[parent_col], "left_anti").count()
    print(f"{child_table}.{child_col} -> {parent_table}.{parent_col}: {orphan_count} orphan rows")

# COMMAND ----------

# reconciling every silver table against what was actually dropped, using the audit tables as the source of truth
silver_tables = [row["tableName"] for row in spark.sql("SHOW TABLES IN workspace.silver").collect()]

for table_name in sorted(silver_tables):
    if table_name.startswith("dropped_duplicates_"):
        continue
    silver_count = spark.table(f"workspace.silver.{table_name}").count()
    dropped_table = f"dropped_duplicates_{table_name}"
    dropped_count = spark.table(f"workspace.silver.{dropped_table}").count() if dropped_table in silver_tables else 0
    print(f"{table_name}: {silver_count} rows kept, {dropped_count} rows dropped (audited)")

# COMMAND ----------

# checking that silver kept the late arriving corrected version, not the original, for every duplicate key
dropped_ids = spark.table("workspace.silver.dropped_duplicates_goodsreceiptline").select("GoodsReceiptLineID")
kept_rows = spark.table("workspace.silver.goodsreceiptline").join(dropped_ids, "GoodsReceiptLineID", "inner")
display(kept_rows.groupBy("_source_file").count())

# COMMAND ----------

# comparing dropped salesorder duplicates against the kept row for the same key to confirm they are true identical copies
business_cols = ["CustomerID", "OrderNumber", "OrderDate", "OrderTotal", "Status", "SalesRepEmployeeID", "CostCenterID", "FreightTerms", "RequestedDeliveryDate"]

dropped = spark.table("workspace.silver.dropped_duplicates_salesorder").select("SalesOrderID", *business_cols).alias("dropped")
kept = spark.table("workspace.silver.salesorder").select("SalesOrderID", *business_cols).alias("kept")
compared = dropped.join(kept, "SalesOrderID", "inner")

mismatch_conditions = [F.col(f"dropped.{c}") != F.col(f"kept.{c}") for c in business_cols]
any_mismatch = mismatch_conditions[0]
for cond in mismatch_conditions[1:]:
    any_mismatch = any_mismatch | cond

mismatched_rows = compared.where(any_mismatch).count()
total_compared = compared.count()
print(f"{total_compared} duplicate pairs compared, {mismatched_rows} have different business content")

# COMMAND ----------

# counting exactly how many tables were processed into silver
print(f"{len(tables_to_process)} tables processed into silver")