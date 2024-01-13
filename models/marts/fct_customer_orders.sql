with

    -- import CTEs 

    orders AS (

        select * from {{ ref('stg_orders') }}

    ),

    customers as (

        select * from {{ ref('stg_customers') }}

    ),

    payments as (

        select * from {{ ref("stg_payments") }}

    ),

    -- logical CTEs

    payments_ii as (

        select
            order_id,
            max(created) as payment_finalized_date,
            sum(amount) as total_amount_paid

        from payments

        group by 1

    ),

    paid_orders as (

        select
            orders.order_id,
            orders.customer_id,
            orders.order_date as order_placed_at,
            orders.order_status,
            payments_ii.total_amount_paid,
            payments_ii.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name

        from orders

        left join payments_ii on orders.order_id = payments_ii.order_id
        left join customers on orders.customer_id = customers.customer_id

    ),

    first_order as (

        select
            customers.customer_id,
            min(orders.order_date) as first_order_date,

        from customers
        left join orders on orders.customer_id = customers.customer_id

        group by 1

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
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
    if(first_order.first_order_date = p.order_placed_at, 'new', 'return') as nvsr, -- is_new_order,
    sum(p.total_amount_paid) over(partition by p.customer_id order by p.order_id) as customer_lifetime_value,
    first_order.first_order_date AS fdos,

from paid_orders as p
left join first_order using (customer_id)
order by order_id
