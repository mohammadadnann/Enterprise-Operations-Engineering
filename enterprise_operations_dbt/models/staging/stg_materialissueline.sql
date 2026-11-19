select
    "MaterialIssueLineID" as material_issue_line_id,
    "MaterialIssueID" as material_issue_id,
    "BOMLineID" as bom_line_id,
    "LineNumber" as line_number,
    "ItemID" as item_id,
    "QuantityIssued" as quantity_issued,
    "ExtendedStandardCost" as extended_standard_cost,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'materialissueline') }}
