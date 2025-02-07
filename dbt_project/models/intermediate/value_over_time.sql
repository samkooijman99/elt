with sum_some_shit as (
    select 
        date,
        symbol,
        owner,
        sum(total_value) over (partition by symbol, owner order by date asc) as running_costs,
        sum(quantity) over (partition by symbol, owner order by date asc) as current_quantity,
        dividends,
        close
    from 
        {{ ref ('yfinance_x_stocks')}}
    order by date asc
),

calculate_dividends as (
    select *,
    dividends * current_quantity as dividend_payout,
    sum(dividends * current_quantity) over (partition by symbol, owner order by date asc) as running_dividend_payouts
    from sum_some_shit
),

calculate_current_value as (
    select 
        *, 
        close * current_quantity as current_value
    from calculate_dividends
    where running_costs > 0
)

select *,
current_value - (running_costs - running_dividend_payouts) as current_profit
from calculate_current_value
