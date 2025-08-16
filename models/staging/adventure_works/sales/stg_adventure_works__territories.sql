with
    source as (
        select * from {{ source('adventure_works', 'salesterritory') }}
    )

    , renamed as (
        select 
            cast(territoryid as string) as territory_id
            , cast(name as string) as territory_name
            , cast(countryregioncode as string) as country_region_code
            , cast("group" as string) as territory_group
        from source
    )

    select * 
    from renamed
