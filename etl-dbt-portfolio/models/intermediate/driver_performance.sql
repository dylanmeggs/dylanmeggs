select
    driver_id,
    count(order_number) as total_deliveries,
    sum(order_amount) as total_revenue
from {{ ref('stg_orders') }}
group by driver_id