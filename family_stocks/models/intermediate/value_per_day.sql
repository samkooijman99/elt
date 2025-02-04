-- -- models/value_per_day.sql

with 
stock_mappings as (select * from {{ ref ('stock_mappings')}} ),

transactions_sam as (select * from {{ ref ('stg_transactions_sam')}} where isin in (select isin from stock_mappings)),
 
joining_dates_personal_stocks as (

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

left join transactions_sam ts

on dd.date = ts.date

left join stock_mappings sm

on ts.isin = sm.isin),

group_transactions as (
    select 
        date,
        stock_date,
        product,
        isin, 
        symbol,
        sum(total_value) as total_value,
        sum(quantity) as quantity,
        avg(stock_price) as stock_price,
        sum(transaction_costs) as transaction_costs
    from
        joining_dates_personal_stocks
    
    group by
        date,
        stock_date,
        product,
        isin, 
        symbol
    

),

joining_yfinance as (
    select
        gt.date,
        gt.stock_date,
        gt.product,
        gt.isin,
        case when gt.symbol is null then sdh.symbol else gt.symbol end as symbol,
        gt.total_value,
        gt.quantity,
        gt.stock_price,
        gt.transaction_costs,
        sdh.close 
    from group_transactions gt
    left join {{ ref ('stg_stock_data_hist')}} sdh
        on case 
            when gt.symbol is not null then (gt.symbol = sdh.symbol and gt.date = sdh.date)
            else (gt.date = sdh.date)
           end
)

select * from joining_yfinance where date > '2020-01-01'