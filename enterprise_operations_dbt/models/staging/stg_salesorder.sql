select
    "SalesOrderID" as sales_order_id,
    "OrderNumber" as order_number,
    "CustomerID" as customer_id,
    "OrderDate" as order_date,
    "RequestedDeliveryDate" as requested_delivery_date,
    "OrderTotal" as order_total,
    "Status" as order_status,
    "SalesRepEmployeeID" as sales_rep_employee_id,
    "CostCenterID" as cost_center_id,
    "FreightTerms" as freight_terms,
    "Notes" as notes,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'salesorder') }}
