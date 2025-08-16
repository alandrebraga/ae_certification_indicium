with
    source as (
        select * from {{ source('adventure_works', 'stateprovince') }}
    )

    , renamed as (
        select 
            cast(stateprovinceid as string) as state_province_id
            , cast(stateprovincecode as string) as state_province_code
            , cast(name as string) as state_province_name
            , cast(countryregioncode as string) as country_region_code
            , cast(isonlystateprovinceflag as boolean) as is_only_state_province
        from source
    )

    select * 
    from renamed
