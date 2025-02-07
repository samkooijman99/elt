with combining as (
    select * from {{ ref ('stg_transactions_x')}}
union all 
    select * from {{ ref('stg_transactions_y')}}

)

select * from combining order by date asc