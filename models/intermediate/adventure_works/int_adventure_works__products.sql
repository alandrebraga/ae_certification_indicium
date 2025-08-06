with
    products as (
        select * from {{ ref('stg_adventure_works__products') }}
    )

    , product_hierarchy as (
        select * from {{ ref('stg_adventure_works__products_hierarchy') }}
    )

    , joined_table as (
        select 
            products.product_id
            , products.product_name
            , products.standart_cost
            , products.selling_price
            , products.product_hierarchy_id
            , product_hierarchy.product_category_name
            , product_hierarchy.product_subcategory_name
        from products
        left join product_hierarchy on product_hierarchy.product_hierarchy_id = products.product_hierarchy_id 
    )

    select *
    from joined_table