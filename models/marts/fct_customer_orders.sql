with

    -- import CTEs 

    orders AS (

        select * from {{ ref('stg_orders') }}

    ),

    customers as (

        select * from {{ ref('stg_customers') }}

    ),

    payments as (

        select * from {{ ref('stg_payments') }}

    ),

    -- logical CTEs

    payments_ii as (

        select
            order_id,
            max(payment_created_at) as payment_finalized_date,
            sum(payment_amount) as total_amount_paid

        from payments

        group by 1

    ),

    paid_orders as (

        select
            orders.order_id,
            orders.customer_id,
            orders.valid_order_date as order_placed_at,
            orders.order_status,
            payments_ii.total_amount_paid,
            payments_ii.payment_finalized_date,
            customers.customer_first_name,
            customers.customer_last_name

        from orders

        left join payments_ii on orders.order_id = payments_ii.order_id
        left join customers on orders.customer_id = customers.customer_id

    )

select
    p.order_id,
    p.customer_id,
    p.order_placed_at,
    p.order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    p.customer_first_name,
    p.customer_last_name,

    -- transaction sequence
    row_number() over (order by p.order_id) as transaction_seq,

    -- customer transaction sequence
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,

    -- new vs returning user
    case 
        when first_value(p.order_placed_at) over (
            partition by p.customer_id
            order by p.order_placed_at
        ) = p.order_placed_at then 'new'
        else 'return'
    end as nvsr,

    -- CLV
    sum(p.total_amount_paid) over(partition by p.customer_id order by p.order_id) as customer_lifetime_value,

    -- first day of sale
    first_value(p.order_placed_at) over (
        partition by p.customer_id
        order by p.order_placed_at
    ) as fdos, 

from paid_orders as p
order by order_id
  
