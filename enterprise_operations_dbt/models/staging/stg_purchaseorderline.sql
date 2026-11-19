select
    "POLineID" as po_line_id,
    "PurchaseOrderID" as purchase_order_id,
    "RequisitionID" as requisition_id,
    "LineNumber" as line_number,
    "ItemID" as item_id,
    "Quantity" as quantity,
    "UnitCost" as unit_cost,
    "LineTotal" as line_total,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'purchaseorderline') }}
