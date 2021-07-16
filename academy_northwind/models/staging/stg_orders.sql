with pedidos as (

    select
    order_id as id_do_pedido
    , employee_id as id_funcionario
    , order_date as data_pedido
    , customer_id as id_cliente
    , ship_region regiao_entrega
    , shipped_date as data_expedicao
    , ship_country as pais_entrega
    , ship_name as nome_entrega
    , ship_postal_code as cep_entrega
    , ship_city as cidade_entrega
    , freight as frete
    , ship_via as id_transportador
    , ship_address as endereco_entrega
    , required_date as data_prevista
    from {{ source('northwind_etl', 'orders')}}

)

, pedido_item as (
    select
    order_id as id_pedido
    , quantity as quantidade
    , unit_price as preco_unidade
    , discount as desconto
    , product_id as id_produto
    -- order_id as id_pedido
    -- , sum(quantity) as quantidade_total
    -- , sum(unit_price * (1 - discount) * quantity)  as valor_faturado
    -- , count(*) as quantidade_itens
    from {{ source('northwind_etl', 'order_details')}}
    -- group by order_id
)

, dados_juntados as (
    select
    id_pedido
    , pedidos.id_funcionario
    , pedidos.id_cliente
    , pedidos.regiao_entrega
    , pedidos.data_pedido
    , pedidos.data_expedicao
    , pedidos.pais_entrega
    , pedidos.nome_entrega
    , pedidos.cep_entrega
    , pedidos.cidade_entrega
    , pedidos.frete
    , pedidos.id_transportador
    , pedidos.endereco_entrega
    , pedidos.data_prevista
    , quantidade
    , preco_unidade
    , desconto
    , id_produto
    from pedido_item
    left join pedidos on pedido_item.id_pedido = pedidos.id_do_pedido
)

select *
from dados_juntados