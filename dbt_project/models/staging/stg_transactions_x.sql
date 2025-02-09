with clean_columns as (

    select
        "Datum" as _date,
        "Product" as product,
        "ISIN" as isin,
        "Beurs" as exchange,
        "Uitvoeringsplaats" as execution_place,
        "Aantal" as quantity,
        "Koers" as stock_price,
        i as currency,
        "Lokale waarde" as local_value,
        "Waarde" as _value,
        "Wisselkoers" as exchange_rate,
        "Transactiekosten en/of" as transaction_costs,
        "Totaal" as total_value,
        "Order ID" as order_id
    from
        {{ ref('transactions_x') }}

),

standardized_dates as (
    select
        *,
        case
            when _date ~ '^\d{4}-\d{2}-\d{2}$' then _date
            -- Bring dd-MM-yyyy to yyyy-MM-dd
            else to_char(to_date(_date, 'dd-MM-yyyy'), 'yyyy-MM-dd')
        end as standardized_date
    from clean_columns
)

select
    product,
    isin,
    exchange,
    execution_place,
    quantity,
    stock_price,
    currency,
    cast(abs(local_value) as float) as local_value,
    cast(abs(_value) as float) as _value,
    exchange_rate,
    cast(abs(transaction_costs) as float) as transaction_costs,
    order_id,
    'x' as stock_owner,
    to_date(standardized_date, 'yyyy-MM-dd') as _date,
    case
        when total_value is null then 0 else
            cast(abs(total_value) as float)
    end as total_value,
    case when _value < 0 then 'aankoop' else 'verkoop' end as transaction_type
from
    standardized_dates
