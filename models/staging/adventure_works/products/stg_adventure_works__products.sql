with 

source as (

    select * from {{ source('adventure_works', 'product') }}

),

renamed as (

    select
        productid as product_id
        ,name as product_name
        ,standardcost as standart_cost
        ,listprice as selling_price
        ,productline as product_line
        ,style as product_style
        ,productsubcategoryid as product_hierarchy_id
    from source

)

select * from renamed
