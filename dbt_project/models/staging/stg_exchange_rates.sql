select
    "Date" as _date,
    usd_per_eur,
    eur_per_usd
from
    {{ source ('general_dimensions', 'exchange_rates') }}
