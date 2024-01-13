-- audit helper that compares queries

{% set legacy_query %}
  select 
    order_id,
    customer_id,
    order_placed_at,
    order_status,
    total_amount_paid,
    payment_finalized_date,
    customer_first_name,
    customer_last_name,
    transaction_seq,
    customer_sales_seq,
    nvsr,
    customer_lifetime_value,
    fdos,
  from {{ ref('leg_customer_orders') }}
{% endset %}

{% set refactored_query %}
  select 
    order_id,
    customer_id,
    order_placed_at,
    order_status,
    total_amount_paid,
    payment_finalized_date,
    customer_first_name,
    customer_last_name,
    transaction_seq,
    customer_sales_seq,
    nvsr,
    customer_lifetime_value,
    fdos,
  from {{ ref('fct_customer_orders') }}
{% endset %}

{{ audit_helper.compare_queries(
    a_query = legacy_query,
    b_query = refactored_query,
    primary_key = "order_id"
) }}