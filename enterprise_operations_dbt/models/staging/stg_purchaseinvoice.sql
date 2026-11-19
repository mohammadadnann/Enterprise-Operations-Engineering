select * from {{ source('silver', 'purchaseinvoice') }}
