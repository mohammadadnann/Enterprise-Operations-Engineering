-- one row per sales order, measuring fulfillment completeness and delivery timeliness
with orders as (
    select * from {{ ref('stg_salesorder') }}
),

order_lines as (
    select * from {{ ref('stg_salesorderline') }}
),

shipments as (
    select * from {{ ref('stg_shipment') }}
),

shipment_lines as (
    select * from {{ ref('stg_shipmentline') }}
),

-- summing quantity ordered per sales order
ordered_quantity as (
    select
        sales_order_id,
        sum(quantity) as total_quantity_ordered
    from order_lines
    group by sales_order_id
),

-- summing quantity actually shipped per sales order, going through shipment lines back to order lines
shipped_quantity as (
    select
        ol.sales_order_id,
        sum(sl.quantity_shipped) as total_quantity_shipped
    from shipment_lines sl
    inner join order_lines ol
        on sl.sales_order_line_id = ol.sales_order_line_id
    group by ol.sales_order_id
),

-- summarising shipment level dates and freight cost per sales order, since one order can have multiple shipments
shipment_summary as (
    select
        sales_order_id,
        min(shipment_date) as first_shipment_date,
        max(delivery_date) as last_delivery_date,
        sum(freight_cost) as total_freight_cost,
        count(distinct shipment_id) as shipment_count
    from shipments
    group by sales_order_id
)

select
    o.sales_order_id,
    o.order_number,
    o.customer_id,
    o.order_date,
    o.requested_delivery_date,
    o.order_status,
    o.order_total,
    coalesce(oq.total_quantity_ordered, 0) as total_quantity_ordered,
    coalesce(sq.total_quantity_shipped, 0) as total_quantity_shipped,
    case
        when coalesce(oq.total_quantity_ordered, 0) = 0 then null
        else coalesce(sq.total_quantity_shipped, 0) / oq.total_quantity_ordered
    end as fulfillment_rate,
    ss.shipment_count,
    ss.first_shipment_date,
    ss.last_delivery_date,
    ss.total_freight_cost,
    datediff('day', o.order_date, ss.first_shipment_date) as days_to_first_shipment,
    datediff('day', o.requested_delivery_date, ss.last_delivery_date) as days_late,
    case
        when ss.last_delivery_date is null then null
        when ss.last_delivery_date <= o.requested_delivery_date then true
        else false
    end as delivered_on_time
from orders o
left join ordered_quantity oq on o.sales_order_id = oq.sales_order_id
left join shipped_quantity sq on o.sales_order_id = sq.sales_order_id
left join shipment_summary ss on o.sales_order_id = ss.sales_order_id
