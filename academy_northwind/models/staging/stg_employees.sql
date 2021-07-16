with dados_funcionarios as (
    select
    row_number() over (order by employee_id) as sk_funcionario --chave auto-incremental
    , employee_id as id_funcionario
    , concat(first_name, ' ', last_name) as nome_funcionario
    , country as pais_funcionario
    , city as cidade_funcionario
    , home_phone as telefone
    , address as endereco_funcionario
    from {{ source('northwind_etl', 'employees')}}
)

select *
from dados_funcionarios