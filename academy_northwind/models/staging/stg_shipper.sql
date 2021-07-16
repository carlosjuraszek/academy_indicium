with fonte_de_dados as (
    select
    row_number() over (order by shipper_id) as sk_transportador --chave auto-incremental
    , shipper_id as id_transportador
    , company_name as nome_transportador
    , phone as telefone
    from {{ source('northwind_etl', 'shippers')}}
)

select *
from fonte_de_dados