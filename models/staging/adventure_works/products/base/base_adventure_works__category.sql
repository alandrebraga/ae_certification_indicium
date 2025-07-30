with 

source as (

    select * from {{ source('adventure_works', 'ProductCategory') }}

),

renamed as (

    select
        productcategoryid as product_category_id
        ,name as product_category_name
    from source

)

select * from renamed
