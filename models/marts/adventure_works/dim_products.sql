with
    source as (
        select * from {{ ref("int_adventure_works__products") }}
    )

    select 
            product_sk
            , product_id
            , product_name
            , standart_cost
            , selling_price
            , product_hierarchy_id
            , product_category_name
            , product_subcategory_name
    from source