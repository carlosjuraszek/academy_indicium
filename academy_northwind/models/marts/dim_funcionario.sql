with staging_funcionario as (
    select *
    from {{ ref('stg_employees') }}
)

select *
from staging_funcionario