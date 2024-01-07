with

payments as (

    select 
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        amount / 100 as amount,
        created
    from dbt-learn-warehouse.dbt_acervantes.payments
    where status <> 'fail'

)

select *
from payments