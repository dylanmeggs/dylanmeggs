select
    order_date,
    count(order_number) as total_orders,
    sum(order_amount) as total_revenue
from {{ ref('orders_enriched') }}
group by order_date
order by order_date