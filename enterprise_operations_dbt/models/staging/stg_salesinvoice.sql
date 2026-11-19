select
    "SalesInvoiceID" as sales_invoice_id,
    "InvoiceNumber" as invoice_number,
    "InvoiceDate" as invoice_date,
    "DueDate" as due_date,
    "SalesOrderID" as sales_order_id,
    "CustomerID" as customer_id,
    "SubTotal" as sub_total,
    "FreightAmount" as freight_amount,
    "TaxAmount" as tax_amount,
    "GrandTotal" as grand_total,
    "Status" as invoice_status,
    "PaymentDate" as payment_date,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'salesinvoice') }}
