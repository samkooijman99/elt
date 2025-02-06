WITH sum_some_shit as (
    select 
        date,
        symbol,
        sum(total_value) over (partition by symbol order by date asc) as current_costs,
        sum(quantity) over (partition by symbol order by date asc) as current_quantity,
        close
    from 
        {{ ref ('yfinance_x_stocks')}}
    order by date asc
),

calculate_current_value as (
    select *, 
        close * current_quantity as current_value
    from sum_some_shit
    where current_costs > 0
),

correct_dates as (
    select date 
    from calculate_current_value 
    group by date
    having count(*) = 2
)

select *, current_value - current_costs as current_profit
from calculate_current_value
where date in (select date from correct_dates)