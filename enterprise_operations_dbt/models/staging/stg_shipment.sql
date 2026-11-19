select
    "ShipmentID" as shipment_id,
    "ShipmentNumber" as shipment_number,
    "SalesOrderID" as sales_order_id,
    "ShipmentDate" as shipment_date,
    "DeliveryDate" as delivery_date,
    "Status" as shipment_status,
    "WarehouseID" as warehouse_id,
    "ShippedBy" as shipped_by,
    "TrackingNumber" as tracking_number,
    "FreightCost" as freight_cost,
    "BillableFreightAmount" as billable_freight_amount,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'shipment') }}
