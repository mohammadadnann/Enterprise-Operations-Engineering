select
    "WorkOrderID" as work_order_id,
    "WorkOrderNumber" as work_order_number,
    "ItemID" as item_id,
    "BOMID" as bom_id,
    "RoutingID" as routing_id,
    "WarehouseID" as warehouse_id,
    "PlannedQuantity" as planned_quantity,
    "ReleasedDate" as released_date,
    "DueDate" as due_date,
    "CompletedDate" as completed_date,
    "ClosedDate" as closed_date,
    "Status" as work_order_status,
    "CostCenterID" as cost_center_id,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'workorder') }}
