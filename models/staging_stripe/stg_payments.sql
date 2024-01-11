with
payments as (

    select
        id,
        orderid,
        paymentmethod,
        status,
        amount,
        created
    from {{ source('stripe', 'payments') }}

)

select *
from payments