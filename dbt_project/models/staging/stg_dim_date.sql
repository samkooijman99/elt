select 
    "date" as date
from
    {{ source ('general_dimensions', 'dim_date')}}