select
    "ShipmentLineID" as shipment_line_id,
    "ShipmentID" as shipment_id,
    "SalesOrderLineID" as sales_order_line_id,
    "LineNumber" as line_number,
    "ItemID" as item_id,
    "QuantityShipped" as quantity_shipped,
    "ExtendedStandardCost" as extended_standard_cost,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'shipmentline') }}
