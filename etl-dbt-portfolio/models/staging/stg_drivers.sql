with raw as (
    select *
    from {{ ref('drivers') }}
)

select
    driver_id,
    {{ string_clean("first_name") }} as first_name,
    {{ string_clean("last_name") }} as last_name,
    vehicle_type,
    signup_date::date as signup_date
from raw

