with 
stock_mappings as (select * from {{ ref ('stock_mappings')}} ),

transactions_sam as (select * from {{ ref ('combining_portfolios')}} where isin in (select isin from stock_mappings)),

unique_owners as (select distinct owner from {{ ref ('combining_portfolios')}}),



-- Create rows for every owner and symbol, so that we can easily join later on
joining_owners_mappings as (
    select 
        dd.*,
        uo.owner,
        sm.symbol,
        sm.isin
    from 
        {{ ref('stg_dim_date')}} dd
    cross join unique_owners uo

    cross join stock_mappings sm

    where dd.date > '2020-01-01'

    order by dd.date, uo.owner asc
),

 
joining_dates_personal_stocks as (
select 
    jow.*,
    ts.date as stock_date,
    ts.product,
    ts.total_value,
    ts.quantity,
    ts.stock_price,
    ts.transaction_costs
from joining_owners_mappings jow

left join transactions_sam ts
on 
    jow.date = ts.date 
    and jow.owner = ts.owner
    and jow.isin = ts.isin
),


group_transactions as (
    select 
        date,
        stock_date,
        product,
        isin, 
        symbol,
        owner,
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
        symbol,
        owner

),

joining_yfinance as (
    select
        gt.date,
        gt.stock_date,
        gt.product,
        gt.isin,
        gt.owner,
        case when gt.symbol is null then sdh.symbol else gt.symbol end as symbol,
        case when gt.total_value is null then 0 else gt.total_value end as total_value,
        case when gt.quantity is null then 0 else gt.quantity end as quantity,
        gt.stock_price,
        gt.transaction_costs,
        sdh.close,
        sdh.dividends
    from group_transactions gt
    left join {{ ref ('stg_stock_data_hist')}} sdh
    on gt.symbol = sdh.symbol and gt.date = sdh.date
)

select * from joining_yfinance
order by date asc