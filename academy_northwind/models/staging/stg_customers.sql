with dados_fonte as (

    select
    row_number() over (order by customer_id) as sk_cliente, --chave auto-incremental
    customer_id as id_cliente,
    contact_name as nome_contato,
    phone as telefone,
    company_name as empresa,
    city as cidade,
    country as pais,
    postal_code as cep,
    address,
    country
    from {{ source('northwind_etl', 'customers')}}

)

select *
from dados_fonte