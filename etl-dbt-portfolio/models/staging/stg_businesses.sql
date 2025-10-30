with raw as (
    select *
    from {{ ref('businesses') }}
)

select
    business_id,
    {{ string_clean("business_name") }} as business_name,
    {{ string_clean("business_type") }} as business_type
from raw

