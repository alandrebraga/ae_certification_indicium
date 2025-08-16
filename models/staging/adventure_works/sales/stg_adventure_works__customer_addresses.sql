with
    source as (
        select * from {{ source('adventure_works', 'customerterritory') }}
    )

    , renamed as (
        select 
            cast(customerid as string) as customer_id
            , cast(territoryid as string) as territory_id
        from source
    )

    select * 
    from renamed
