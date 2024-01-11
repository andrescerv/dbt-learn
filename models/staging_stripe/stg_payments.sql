with

payments as (

    select *

    -- from dbt-learn-warehouse.dbt_acervantes.payments
    from {{ source('stripe', 'payments') }}

)

select *
from payments