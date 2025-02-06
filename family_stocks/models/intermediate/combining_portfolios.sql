with combining as (
    select * from {{ ref ('stg_transactions_sam')}}
union all 
    select * from {{ ref('stg_transactions_parents')}}

)

select * from combining order by date asc