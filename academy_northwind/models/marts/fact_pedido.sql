with orders as (
    select *
    from {{ ref('stg_orders') }}
)

, clientes as (
    select *
    from {{ ref('dim_clientes') }}
)

, transportador as (
    select *
    from {{ ref('dim_transportador') }}
)

, funcionarios as (
    select *
    from {{ ref('dim_funcionario')}}
)

, produtos as (
    select *
    from {{ ref('dim_produto') }}
)

, fornecedores as (
    select *
    from {{ ref('dim_fornecedor') }}
)

, juntar_chaves as (
    select
    id_pedido
    , clientes.sk_cliente --chave auto-incremental
    , funcionarios.sk_funcionario --chave auto-incremental
    , transportador.sk_transportador --chave auto-incremental
    , produtos.sk_produto --chave auto-incremental
    , fornecedores.sk_fornecedor --chave auto-incremental
    , quantidade
    , orders.preco_unidade
    , desconto
    , data_pedido
    , regiao_entrega
    , data_expedicao
    , pais_entrega
    , nome_entrega
    , cep_entrega
    , cidade_entrega
    , frete
    , endereco_entrega
    , data_prevista
    from orders
    left join clientes on orders.id_cliente = clientes.id_cliente
    left join transportador on orders.id_transportador = transportador.id_transportador
    left join funcionarios on orders.id_funcionario = funcionarios.id_funcionario
    left join produtos on orders.id_produto = produtos.id_produto
    left join fornecedores on produtos.id_fornecedor = fornecedores.id_fornecedor
)

select *
from juntar_chaves