with
stock_mappings as (
    select * from {{ ref('stock_mappings') }}
),

transactions as (
    select t.*
    from {{ ref('combining_portfolios') }} as t
    where t.isin in (
        select sm.isin
        from stock_mappings as sm
    )
),

unique_stock_owners as (
    select distinct stock_owner
    from {{ ref('combining_portfolios') }}
),

joining_stock_owners_mappings as (
    select
        dd.*,
        uo.stock_owner,
        sm.symbol,
        sm.isin
    from {{ ref('stg_dim_date') }} as dd
    cross join unique_stock_owners as uo
    cross join stock_mappings as sm
    where dd._date > '2020-01-01'
),

joining_dates_personal_stocks as (
    select
        jow.*,   
        t._date as stock_date,
        t.product,
        t.total_value,
        t.quantity,
        t.stock_price,
        t.transaction_costs
    from joining_stock_owners_mappings as jow
    left join transactions as t
        on
            jow._date = t._date
            and jow.stock_owner = t.stock_owner
            and jow.isin = t.isin
),

group_transactions as (
    select
        jdps._date,
        jdps.stock_date,
        jdps.product,
        jdps.isin,
        jdps.symbol,
        jdps.stock_owner,
        sum(jdps.total_value) as total_value,
        sum(jdps.quantity) as quantity,
        avg(jdps.stock_price) as stock_price,
        sum(jdps.transaction_costs) as transaction_costs
    from joining_dates_personal_stocks as jdps
    group by
        jdps._date,
        jdps.stock_date,
        jdps.product,
        jdps.isin,
        jdps.symbol,
        jdps.stock_owner
),

joining_yfinance as (
    select
        gt._date,
        gt.stock_date,
        gt.product,
        gt.isin,
        gt.stock_owner,
        gt.stock_price,
        gt.transaction_costs,
        sdh.stock_close,
        sdh.dividends,
        coalesce(gt.symbol, sdh.symbol) as symbol,
        coalesce(gt.total_value, 0) as total_value,
        coalesce(gt.quantity, 0) as quantity
    from group_transactions as gt
    left join {{ ref('stg_stock_data_hist') }} as sdh
        on
            gt.symbol = sdh.symbol
            and gt._date = sdh._date
)

select jf.*
from joining_yfinance as jf
order by jf._date
