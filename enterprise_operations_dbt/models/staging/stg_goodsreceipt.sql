select
    "GoodsReceiptID" as goods_receipt_id,
    "ReceiptNumber" as receipt_number,
    "ReceiptDate" as receipt_date,
    "PurchaseOrderID" as purchase_order_id,
    "WarehouseID" as warehouse_id,
    "ReceivedByEmployeeID" as received_by_employee_id,
    "Status" as receipt_status,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'goodsreceipt') }}
