select
    "ItemID" as item_id,
    "ItemCode" as item_code,
    "ItemName" as item_name,
    "ItemGroup" as item_group,
    "ItemType" as item_type,
    "StandardCost" as standard_cost,
    "ListPrice" as list_price,
    "UnitOfMeasure" as unit_of_measure,
    "SupplyMode" as supply_mode,
    "ProductionLeadTimeDays" as production_lead_time_days,
    "LifecycleStatus" as lifecycle_status,
    "LaunchDate" as launch_date,
    "IsActive" as is_active,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'item') }}
