select date as _date
from
    {{ source ('general_dimensions', 'dim_date') }}
