select
    customer_id,
    sum(order_amount) as lifetime_value,
    count(order_number) as total_orders
from {{ ref('orders_enriched') }}
group by customer_id
order by lifetime_value desc