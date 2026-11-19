select
    "InventoryPolicyID" as inventory_policy_id,
    "ItemID" as item_id,
    "WarehouseID" as warehouse_id,
    "PlanningGroup" as planning_group,
    "PolicyType" as policy_type,
    "SafetyStockQuantity" as safety_stock_quantity,
    "ReorderPointQuantity" as reorder_point_quantity,
    "ReorderQuantity" as reorder_quantity,
    "TargetDaysSupply" as target_days_supply,
    "PlanningLeadTimeDays" as planning_lead_time_days,
    "EffectiveStartDate" as effective_start_date,
    "EffectiveEndDate" as effective_end_date,
    "IsActive" as is_active,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'inventorypolicy') }}
