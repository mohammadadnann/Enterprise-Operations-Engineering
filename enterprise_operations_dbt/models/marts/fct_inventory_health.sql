-- one row per item, comparing supply in (received plus produced) against demand out
-- (consumed into production plus shipped to customers). net_inventory_movement is a net flow
-- across this dataset's time horizon, not a true stock-on-hand balance: there is no opening
-- balance, scrap or warehouse transfer tracking in this dataset
with items as (
    select * from {{ ref('stg_item') }}
),

policies as (
    select * from {{ ref('stg_inventorypolicy') }}
),

issue_lines as (
    select * from {{ ref('stg_materialissueline') }}
),

work_orders as (
    select * from {{ ref('stg_workorder') }}
),

receipt_lines as (
    select * from {{ ref('stg_goodsreceiptline') }}
),

shipment_lines as (
    select * from {{ ref('stg_shipmentline') }}
),

-- summing active policy settings across warehouses per item, since a policy is set per item and warehouse
policy_summary as (
    select
        item_id,
        sum(safety_stock_quantity) as total_safety_stock_quantity,
        sum(reorder_point_quantity) as total_reorder_point_quantity,
        sum(reorder_quantity) as total_reorder_quantity,
        avg(target_days_supply) as avg_target_days_supply,
        count(distinct warehouse_id) as warehouse_count
    from policies
    where is_active = 1
    group by item_id
),

received_summary as (
    select
        item_id,
        sum(quantity_received) as total_quantity_received
    from receipt_lines
    group by item_id
),

-- counting only work orders that have actually completed, using planned quantity as the completed quantity
produced_summary as (
    select
        item_id,
        sum(planned_quantity) as total_quantity_produced
    from work_orders
    where completed_date is not null
    group by item_id
),

issued_summary as (
    select
        item_id,
        sum(quantity_issued) as total_quantity_issued
    from issue_lines
    group by item_id
),

shipped_summary as (
    select
        item_id,
        sum(quantity_shipped) as total_quantity_shipped
    from shipment_lines
    group by item_id
)

select
    i.item_id,
    i.item_code,
    i.item_name,
    i.item_group,
    i.item_type,
    i.supply_mode,
    i.standard_cost,
    i.list_price,
    i.production_lead_time_days,
    i.lifecycle_status,
    i.is_active,
    ps.total_safety_stock_quantity,
    ps.total_reorder_point_quantity,
    ps.total_reorder_quantity,
    ps.avg_target_days_supply,
    ps.warehouse_count,
    coalesce(rs.total_quantity_received, 0) as total_quantity_received,
    coalesce(prs.total_quantity_produced, 0) as total_quantity_produced,
    coalesce(iss.total_quantity_issued, 0) as total_quantity_issued,
    coalesce(shs.total_quantity_shipped, 0) as total_quantity_shipped,
    coalesce(rs.total_quantity_received, 0) + coalesce(prs.total_quantity_produced, 0)
        - coalesce(iss.total_quantity_issued, 0) - coalesce(shs.total_quantity_shipped, 0) as net_inventory_movement
from items i
left join policy_summary ps on i.item_id = ps.item_id
left join received_summary rs on i.item_id = rs.item_id
left join produced_summary prs on i.item_id = prs.item_id
left join issued_summary iss on i.item_id = iss.item_id
left join shipped_summary shs on i.item_id = shs.item_id
