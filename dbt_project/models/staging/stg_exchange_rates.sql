select 
    "Date" as date,
    "usd_per_eur" as usd_per_eur,
    "eur_per_usd" as eur_per_usd
from
    {{ source ('general_dimensions', 'exchange_rates')}}