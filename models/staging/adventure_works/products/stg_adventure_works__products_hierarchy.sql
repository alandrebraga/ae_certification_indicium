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

    , not_informed_line as (
        select 
            '-1' as product_hierarchy_id,
            'Unknown Category' as product_category_name,
            'Unknown Subcategory' as product_subcategory_name
    )

    , union_table as (
        select * from product_hierarchy
        union all
        select * from not_informed_line
    )

    select *
    from union_table