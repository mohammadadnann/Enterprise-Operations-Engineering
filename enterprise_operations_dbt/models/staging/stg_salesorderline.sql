select
    "SalesOrderLineID" as sales_order_line_id,
    "SalesOrderID" as sales_order_id,
    "LineNumber" as line_number,
    "ItemID" as item_id,
    "Quantity" as quantity,
    "UnitPrice" as unit_price,
    "BaseListPrice" as base_list_price,
    "Discount" as discount,
    "LineTotal" as line_total,
    "PricingMethod" as pricing_method,
    "PriceListLineID" as price_list_line_id,
    "PriceOverrideApprovalID" as price_override_approval_id,
    "PromotionID" as promotion_id,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'salesorderline') }}
