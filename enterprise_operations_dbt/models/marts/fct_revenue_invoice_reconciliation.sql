-- one row per sales invoice, measuring only cash that was explicitly reconciled against that invoice
-- note: most cash receipts in this dataset (92.8 percent of receipt records) carry no invoice
-- reference at all, so outstanding_invoice_balance here is not the customer's true unpaid balance,
-- only the gap left after directly matched payments. see fct_customer_cash_position for the full picture
with invoices as (
    select * from {{ ref('stg_salesinvoice') }}
),

cash_receipts as (
    select * from {{ ref('stg_cashreceipt') }}
),

cash_summary as (
    select
        sales_invoice_id,
        sum(amount) as directly_reconciled_cash,
        count(distinct cash_receipt_id) as reconciled_receipt_count,
        min(receipt_date) as first_reconciled_payment_date,
        max(receipt_date) as last_reconciled_payment_date
    from cash_receipts
    where sales_invoice_id is not null
    group by sales_invoice_id
)

select
    i.sales_invoice_id,
    i.invoice_number,
    i.invoice_date,
    i.due_date,
    i.sales_order_id,
    i.customer_id,
    i.sub_total,
    i.freight_amount,
    i.tax_amount,
    i.grand_total,
    i.invoice_status,
    coalesce(cs.directly_reconciled_cash, 0) as directly_reconciled_cash,
    i.grand_total - coalesce(cs.directly_reconciled_cash, 0) as outstanding_invoice_balance,
    case
        when coalesce(cs.directly_reconciled_cash, 0) = 0 then 'unreconciled'
        when coalesce(cs.directly_reconciled_cash, 0) >= i.grand_total then 'fully_reconciled'
        else 'partially_reconciled'
    end as invoice_payment_status,
    cs.reconciled_receipt_count,
    cs.first_reconciled_payment_date,
    cs.last_reconciled_payment_date,
    datediff('day', i.invoice_date, cs.last_reconciled_payment_date) as days_to_reconciled_payment,
    case
        when cs.last_reconciled_payment_date is null then null
        when cs.last_reconciled_payment_date <= i.due_date then true
        else false
    end as reconciled_payment_on_time
from invoices i
left join cash_summary cs on i.sales_invoice_id = cs.sales_invoice_id
