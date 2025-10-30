with raw as (
    select *
    from {{ ref('orders') }}
)

select
    order_number,
    customer_id,
    driver_id,
    business_id,
    order_amount::numeric as order_amount,
    order_date::date as order_date
from raw