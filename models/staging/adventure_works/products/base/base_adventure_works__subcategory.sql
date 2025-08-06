with 

source as (

    select * from {{ source('adventure_works', 'ProductSubcategory') }}

),

renamed as (

    select
        cast(productsubcategoryid as string) as product_subcategory_id
        ,cast(productcategoryid as string) as product_category_id
        ,cast(name as string) as product_subcategory_name
    from source

)

select * from renamed
