select
    "CashReceiptID" as cash_receipt_id,
    "ReceiptNumber" as receipt_number,
    "ReceiptDate" as receipt_date,
    "CustomerID" as customer_id,
    "SalesInvoiceID" as sales_invoice_id,
    "Amount" as amount,
    "PaymentMethod" as payment_method,
    "ReferenceNumber" as reference_number,
    "DepositDate" as deposit_date,
    "RecordedByEmployeeID" as recorded_by_employee_id,
    "_source_file" as source_file,
    "_batch_id" as batch_id,
    "_ingested_at" as ingested_at
from {{ source('silver', 'cashreceipt') }}
