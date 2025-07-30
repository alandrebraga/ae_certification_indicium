with
    product_category as (
        select * from {{ ref("base_adventure_works__category") }}
    )

    , product_subcategory as (
        select * from {{ ref("base_adventure_works__subcategory") }}
    )

    , product_hierarchy as (
        select 
            product_subcategory.product_subcategory_id as product_hierarchy_id
            , product_category.product_category_name
            , product_subcategory.product_subcategory_name
        from product_subcategory
        left join product_category on product_category.product_category_id = product_subcategory.product_category_id
    )

    select *
    from product_hierarchy