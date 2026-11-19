-- one row per purchase order, measuring receipt completeness and delivery timeliness by supplier
with purchase_orders as (
    select * from {{ ref('stg_purchaseorder') }}
),

po_lines as (
    select * from {{ ref('stg_purchaseorderline') }}
),

suppliers as (
    select * from {{ ref('stg_supplier') }}
),

goods_receipts as (
    select * from {{ ref('stg_goodsreceipt') }}
),

receipt_lines as (
    select * from {{ ref('stg_goodsreceiptline') }}
),

-- summing quantity ordered per purchase order
ordered_quantity as (
    select
        purchase_order_id,
        sum(quantity) as total_quantity_ordered
    from po_lines
    group by purchase_order_id
),

-- summing quantity actually received per purchase order, going through receipt lines back to po lines
received_quantity as (
    select
        pol.purchase_order_id,
        sum(rl.quantity_received) as total_quantity_received,
        count(case when rl.is_late_arrival then 1 end) as late_arrival_line_count,
        count(*) as total_receipt_lines
    from receipt_lines rl
    inner join po_lines pol
        on rl.po_line_id = pol.po_line_id
    group by pol.purchase_order_id
),

-- summarising receipt level dates per purchase order, since one PO can have multiple receipts
receipt_summary as (
    select
        purchase_order_id,
        min(receipt_date) as first_receipt_date,
        max(receipt_date) as last_receipt_date,
        count(distinct goods_receipt_id) as receipt_count
    from goods_receipts
    group by purchase_order_id
)

select
    po.purchase_order_id,
    po.po_number,
    po.supplier_id,
    s.supplier_name,
    s.supplier_category,
    s.supplier_risk_rating,
    po.order_date,
    po.expected_delivery_date,
    po.po_status,
    po.order_total,
    coalesce(oq.total_quantity_ordered, 0) as total_quantity_ordered,
    coalesce(rq.total_quantity_received, 0) as total_quantity_received,
    case
        when coalesce(oq.total_quantity_ordered, 0) = 0 then null
        else coalesce(rq.total_quantity_received, 0) / oq.total_quantity_ordered
    end as receipt_fulfillment_rate,
    coalesce(rq.late_arrival_line_count, 0) as late_arrival_line_count,
    coalesce(rq.total_receipt_lines, 0) as total_receipt_lines,
    rs.receipt_count,
    rs.first_receipt_date,
    rs.last_receipt_date,
    datediff('day', po.order_date, rs.first_receipt_date) as days_to_first_receipt,
    datediff('day', po.expected_delivery_date, rs.last_receipt_date) as days_late,
    case
        when rs.last_receipt_date is null then null
        when rs.last_receipt_date <= po.expected_delivery_date then true
        else false
    end as received_on_time
from purchase_orders po
left join suppliers s on po.supplier_id = s.supplier_id
left join ordered_quantity oq on po.purchase_order_id = oq.purchase_order_id
left join received_quantity rq on po.purchase_order_id = rq.purchase_order_id
left join receipt_summary rs on po.purchase_order_id = rs.purchase_order_id
