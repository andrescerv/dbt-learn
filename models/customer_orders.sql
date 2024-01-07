with
    orders AS (
        select
            order_id,
            customer_id,
            order_date,
            order_status
        from {{ ref('stg_orders') }}
    ),

    customers as (
        select 
            customer_id,
            first_name,
            last_name,
        from {{ ref('stg_customers') }}
    ),

    payments as (
        select
            order_id,
            max(created) as payment_finalized_date,
            sum(amount) as total_amount
        from {{ ref("stg_payments") }}
        group by 1
    ),

    paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            orders.order_date as order_placed_at,
            orders.order_status,
            payments.total_amount,
            payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders
        left join payments on orders.order_id = payments.order_id
        left join customers on orders.customer_id = customers.customer_id
    ),

    clv as (
        select 
            p.order_id,
            sum(t2.total_amount) as customer_lifetime_value
        from paid_orders as p
        left join paid_orders as t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
        group by 1
        order by p.order_id
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
    p.total_amount,
    p.payment_finalized_date,
    p.customer_first_name,
    p.customer_last_name,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
    if(first_order.first_order_date = p.order_placed_at, 1, 0) as is_new_order,
    first_order.first_order_date,
    clv.customer_lifetime_value,

from paid_orders as p
left join first_order using (customer_id)
left outer join clv on clv.order_id = p.order_id
order by order_id
