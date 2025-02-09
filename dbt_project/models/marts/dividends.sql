with value_over_time as (
    select * from {{ ref ('value_over_time') }}
),

selection as (
    select
        _date,
        dividend_payout,
        running_dividend_payouts,
        symbol,
        stock_owner
    from
        value_over_time
    where dividend_payout > 0
)

select
    *,
    (
        (
            dividend_payout - lag(dividend_payout) over (
                partition by symbol, stock_owner
                order by _date asc
            )
        )
        / lag(dividend_payout)
            over (
                partition by symbol, stock_owner
                order by _date asc
            )
    ) * 100 as dividend_change_pct,
    (
        dividend_payout - lag(dividend_payout) over (
            partition by symbol, stock_owner
            order by _date asc
        )
    ) as dividend_change_abs

from
    selection
