select 
    sd.date,
    sd.open,
    ts.value
    
from 
    {{ ref ('stg_stock_data_hist')}} sd

left join 
    {{ ref ('stg_transactions_sam')}} as ts

on 
    sd.date = ts.date