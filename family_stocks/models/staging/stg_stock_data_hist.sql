select
    cast("Date" as date) as date,
    "Open" as open,
    "High" as high, 
    "Low" as low,
    "Close" as close,
    "Volume" as volume,
    "Stock_Splits" as stock_splits,
    "Capital_Gains" as capital_gains,
    "Symbol" as symbol
from
    {{ source ('yfinance_data', 'stock_data_hist') }}
