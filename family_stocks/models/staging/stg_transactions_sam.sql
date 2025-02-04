with clean_columns as (

select 
    "Datum" as date, 
    "Product" as product,
    "ISIN" as isin,
    "Beurs" as exchange,
    "Uitvoeringsplaats" as execution_place,
    "Aantal" as quantity,
    "Koers" as stock_price,
    i as currency,
    "Lokale waarde" as local_value, 
    "Waarde" as value,
    "Wisselkoers" as exchange_rate,
    "Transactiekosten en/of" as transaction_costs,
    "Totaal" as total_value,
    "Order ID" as order_id
from 
    {{ ref('transactions_sam') }}

),

standardized_dates as (
    select 
        *,
        case 
            when date ~ '^\d{4}-\d{2}-\d{2}$' then date
            else to_char(to_date(date, 'dd-MM-yyyy'), 'yyyy-MM-dd')  -- Bring dd-MM-yyyy to yyyy-MM-dd
        end as standardized_date
    from clean_columns
)

select 
    to_date(standardized_date, 'yyyy-MM-dd') as date,
    product,
    isin,
    exchange,
    execution_place,
    quantity,
    stock_price,
    currency,
    cast(abs(local_value) as float) as local_value,
    cast(abs(value) as float) as value,
    exchange_rate,
    cast(abs(transaction_costs) as float) as transaction_costs,
    cast(abs(total_value) as float) as total_value,
    order_id,
    case when value < 0 then 'aankoop' else 'verkoop' end as transaction_type
from 
    standardized_dates