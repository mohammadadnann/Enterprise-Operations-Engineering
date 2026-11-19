select
    "PurchaseOrderID" as purchase_order_id,
    "PONumber" as po_number,
    "OrderDate" as order_date,
    "SupplierID" as supplier_id,
    "RequisitionID" as requisition_id,
    "ExpectedDeliveryDate" as expected_delivery_date,
    "Status" as po_status,
    "CreatedByEmployeeID" as created_by_employee_id,
    "ApprovedByEmployeeID" as approved_by_employee_id,
    "OrderTotal" as order_total,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'purchaseorder') }}
