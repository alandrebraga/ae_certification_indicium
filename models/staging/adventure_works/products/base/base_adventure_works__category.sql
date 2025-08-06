with 

source as (

    select * from {{ source('adventure_works', 'ProductCategory') }}

),

renamed as (

    select
        cast(productcategoryid as string) as product_category_id
        ,cast(name as string) as product_category_name
    from source

)

select * from renamed
