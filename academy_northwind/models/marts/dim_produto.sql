with staging_produtos as (
    select *
    from {{ ref('stg_products') }}
)

select *
from staging_produtos