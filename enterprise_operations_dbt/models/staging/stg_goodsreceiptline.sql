select
    "GoodsReceiptLineID" as goods_receipt_line_id,
    "GoodsReceiptID" as goods_receipt_id,
    "POLineID" as po_line_id,
    "LineNumber" as line_number,
    "ItemID" as item_id,
    "QuantityReceived" as quantity_received,
    "ExtendedStandardCost" as extended_standard_cost,
    "is_late_arrival" as is_late_arrival,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'goodsreceiptline') }}
