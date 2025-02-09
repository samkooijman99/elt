with sum_some_shit as (
    select
        _date,
        symbol,
        stock_owner,
        dividends,
        stock_close,
        sum(total_value)
            over (
                partition by symbol, stock_owner
                order by _date asc
            )
        as running_costs,
        sum(quantity)
            over (
                partition by symbol, stock_owner
                order by _date asc
            )
        as current_quantity
    from
        {{ ref ('yfinance_x_stocks') }}
),

calculate_dividends as (
    select
        *,
        dividends * current_quantity as dividend_payout,
        sum(dividends * current_quantity)
            over (
                partition by symbol, stock_owner
                order by _date asc
            )
        as running_dividend_payouts
    from sum_some_shit
),

calculate_current_value as (
    select
        *,
        stock_close * current_quantity as current_value
    from calculate_dividends
    where running_costs > 0
)

select
    _date,
    symbol,
    stock_owner,
    current_quantity
    as dividends,
    stock_close,
    dividend_payout,
    running_dividend_payouts,
    current_value,
    running_costs - running_dividend_payouts as running_costs,
    current_value - (running_costs - running_dividend_payouts) as current_profit
from
    calculate_current_value
