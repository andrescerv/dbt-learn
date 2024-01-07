with

payments as (

    select 
        id,
        orderid,
        paymentmethod,
        status,
        amount,
        created
    from dbt-learn-warehouse.dbt_acervantes.payments

)

select *
from payments