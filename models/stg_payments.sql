with

payments as (

    select 
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        amount,
        created
    from dbt-learn-warehouse.dbt_acervantes.payments

)

select *
from payments