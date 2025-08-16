with 
    source as (

        select * from {{ source('adventure_works', 'product') }}

    ),

    renamed as (

        select
            cast(productid as string) as product_id
            ,cast(name as string) as product_name
            ,cast(standardcost as float) as standart_cost
            ,cast(listprice as float) selling_price
            ,coalesce(productline, 'Not Informed') as product_line
            ,coalesce(style, 'Not Informed') as product_style
            ,cast(coalesce(productsubcategoryid, -1) as string) as product_hierarchy_id
        from source

    )

    select * from renamed
