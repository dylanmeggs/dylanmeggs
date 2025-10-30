with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
drivers as (
    select * from {{ ref('stg_drivers') }}
),
businesses as (
    select * from {{ ref('stg_businesses') }}
)

select
    o.order_number,
    c.first_name || ' ' || c.last_name as customer_name,
    d.first_name || ' ' || d.last_name as driver_name,
    b.business_name,
    o.order_amount,
    o.order_date
from orders o
left join customers c on o.customer_id = c.customer_id
left join drivers d on o.driver_id = d.driver_id
left join businesses b on o.business_id = b.business_id