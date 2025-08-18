with
    source as (
        select * from {{ source('adventure_works', 'address') }}
    )

    , renamed as (
        select 
            cast(addressid as string) as address_id
            , cast(addressline1 as string) as address_name
            , cast(city as string) as city
            , cast(stateprovinceid as string) as state_province_id
            , cast(postalcode as string) as postal_code
        from source
    )

    select * 
    from renamed
