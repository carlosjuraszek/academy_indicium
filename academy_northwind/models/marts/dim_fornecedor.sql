with staging_fornecedor as (
    select *
    from {{ ref('stg_supplier') }}
)

select *
from staging_fornecedor