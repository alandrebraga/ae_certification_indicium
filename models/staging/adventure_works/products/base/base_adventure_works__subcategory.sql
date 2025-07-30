with 

source as (

    select * from {{ source('adventure_works', 'ProductSubcategory') }}

),

renamed as (

    select
        productsubcategoryid as product_subcategory_id
        ,productcategoryid as product_category_id
        ,name as product_subcategory_name
    from source

)

select * from renamed
