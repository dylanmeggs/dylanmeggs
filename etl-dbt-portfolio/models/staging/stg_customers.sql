-- dbt model for staging customers
with raw as (
    select *
    from {{ ref('customers') }}
)

select
    customer_id,
    {{ string_clean("first_name") }} as first_name,
    {{ string_clean("last_name") }} as last_name,
    email,
    signup_date::date as signup_date
from raw