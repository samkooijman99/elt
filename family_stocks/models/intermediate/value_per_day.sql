-- -- models/value_per_day.sql

with joining_dates as (

select 
    dd.*,
    ts.date as stock_date,
    ts.product,
    ts.isin as isin,
    sm.symbol as symbol,
    ts.total_value,
    ts.quantity,
    ts.stock_price,
    ts.transaction_costs


from {{ ref('stg_dim_date')}} dd

left join {{ ref ('stg_transactions_sam')}} ts

on dd.date = ts.date

left join {{ ref ('stock_mappings')}} sm

on ts.isin = sm.isin)

select * from joining_dates