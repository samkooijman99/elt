with value_over_time as (
    select * from {{ ref ('value_over_time')}}
),

selection as (select
    date,
    dividend_payout,
    running_dividend_payouts,
    symbol,
    owner
from
    value_over_time
where dividend_payout > 0
)

select
    *,
    ((dividend_payout - lag(dividend_payout) over (partition by symbol, owner order by date asc)) 
    / lag(dividend_payout) over (partition by symbol, owner order by date asc)) * 100 as dividend_change_pct,
    (dividend_payout - lag(dividend_payout) over (partition by symbol, owner order by date asc)) as dividend_change_abs

from
    selection