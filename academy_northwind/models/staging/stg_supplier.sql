with dados_supplier as (
    select
    row_number() over (order by supplier_id) as sk_fornecedor --chave auto-incremental
    , supplier_id as id_fornecedor
    , company_name as nome_fornecedor
    , contact_name as contato_fornecedor
    , contact_title as contato_titulo
    , phone as telefone
    , address as endereco_fornecedor
    , postal_code as cep_fornecedor
    , city as cidade_fornecedor
    , country as pais_fornecedor
    from {{ source('northwind_etl', 'suppliers')}}
)

select *
from dados_supplier