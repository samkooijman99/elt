select
    * 
from
    {{ source ('yfinance_data', 'stock_data_hist') }}