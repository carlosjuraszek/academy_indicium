with staging_shipper as (
    select *
    from {{ ref('stg_shipper') }}
)

select *
from staging_shipper