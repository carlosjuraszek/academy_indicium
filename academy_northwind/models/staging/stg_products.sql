with source_products as (
    select
    row_number() over (order by product_id) as sk_produto --chave auto-incremental
    , product_id as id_produto
    , product_name as nome_produto
    , units_in_stock as unidades_em_estoque
    , category_id as id_categoria
    , supplier_id as id_fornecedor
    , unit_price as preco_unidade
    , quantity_per_unit as medida_unidade
    from {{ source('northwind_etl', 'products') }}
)

select *
from source_products