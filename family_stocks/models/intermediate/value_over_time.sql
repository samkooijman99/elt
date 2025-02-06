
with sum_some_shit as (select 
    date,
    symbol,
    sum(total_value) over (partition by symbol order by date asc) as current_costs,
    sum(quantity) over (partition by symbol order by date asc) as current_quantity,
    close
from 
    {{ ref ('yfinance_x_stocks')}}

order by date asc)

select *, close * current_quantity as current_value
from sum_some_shit