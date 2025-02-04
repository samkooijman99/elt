-- -- models/value_per_day.sql

with joining_dates_personal_stocks as (

select 
    dd.*,
    -- ts.date as stock_date,
    -- ts.product,
    -- ts.isin as isin,
    sm.symbol as symbol,
    sm.isin as isin,
    sdh.close as close
    -- ts.total_value,
    -- ts.quantity,
    -- ts.stock_price,
    -- ts.transaction_costs


from {{ ref('stg_dim_date')}} dd

left join {{ ref('stg_stock_data_hist')}} sdh

on dd.date = sdh.date

left join {{ ref ('stock_mappings')}} sm

on sdh.symbol = sm.symbol),

-- left join {{ ref ('stg_transactions_sam')}} ts

-- on dd.date = ts.date and sdh.isin = ts.isin)


group_transactions as (
    select 
        date,
        product,
        isin, 
        sum(total_value) as total_value,
        sum(quantity) as quantity,
        avg(stock_price) as stock_price,
        sum(transaction_costs) as transaction_costs
    from
        {{ ref ('stg_transactions_sam')}}
    group by
        date,
        product,
        isin

)

select 
    jdps.*,
    gt.product,
    gt.total_value,
    gt.quantity,
    gt.stock_price,
    gt.transaction_costs

from 
    joining_dates_personal_stocks jdps

left join group_transactions gt on jdps.date = gt.date and jdps.isin = gt.isin


where jdps.date > '2020-01-01'


-- joining_yfinance as (
-- select
--     gt.date,
--     gt.stock_date,
--     gt.product,
--     gt.isin,
--     case when gt.symbol is null then sdh.symbol else gt.symbol end as symbol,
--     gt.total_value,
--     gt.quantity,
--     gt.stock_price,
--     gt.transaction_costs,
--     sdh.close 

-- from group_transactions gt
-- left join {{ ref ('stg_stock_data_hist')}} sdh
-- on 
--     (gt.symbol = sdh.symbol and gt.date = sdh.date) 
--     or (gt.date = sdh.date)
-- )

-- select * from joining_yfinance