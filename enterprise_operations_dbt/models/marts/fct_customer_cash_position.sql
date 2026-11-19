-- one row per customer, comparing total invoiced against total cash received (matched or not)
-- net_invoice_cash_gap is a simple cash gap, not a true receivables balance: it does not account
-- for credit notes, opening balances, refunds or payment timing differences across periods
with invoices as (
    select * from {{ ref('stg_salesinvoice') }}
),

cash_receipts as (
    select * from {{ ref('stg_cashreceipt') }}
),

invoice_totals as (
    select
        customer_id,
        sum(grand_total) as total_invoiced,
        count(distinct sales_invoice_id) as invoice_count
    from invoices
    group by customer_id
),

cash_totals as (
    select
        customer_id,
        sum(amount) as total_cash_received,
        sum(case when sales_invoice_id is not null then amount else 0 end) as directly_reconciled_cash,
        sum(case when sales_invoice_id is null then amount else 0 end) as unallocated_cash,
        count(distinct cash_receipt_id) as receipt_count
    from cash_receipts
    group by customer_id
)

select
    coalesce(it.customer_id, ct.customer_id) as customer_id,
    coalesce(it.total_invoiced, 0) as total_invoiced,
    coalesce(it.invoice_count, 0) as invoice_count,
    coalesce(ct.total_cash_received, 0) as total_cash_received,
    coalesce(ct.directly_reconciled_cash, 0) as directly_reconciled_cash,
    coalesce(ct.unallocated_cash, 0) as unallocated_cash,
    coalesce(ct.receipt_count, 0) as receipt_count,
    coalesce(it.total_invoiced, 0) - coalesce(ct.total_cash_received, 0) as net_invoice_cash_gap
from invoice_totals it
full outer join cash_totals ct on it.customer_id = ct.customer_id
