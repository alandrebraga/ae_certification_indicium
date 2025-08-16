with
    source as (
        select * from {{ source('adventure_works', 'countryregion') }}
    )

    , renamed as (
        select 
            cast(countryregioncode as string) as country_region_code
            , cast(name as string) as country_region_name
        from source
    )

    select * 
    from renamed
