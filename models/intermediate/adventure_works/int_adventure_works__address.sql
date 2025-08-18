with
    address as (
        select * from {{ ref("stg_adventure_works__addresses") }}
    )

    , state_province as (
        select * from {{ ref("stg_adventure_works__state_provinces") }}
    )

    , country_region as (
        select * from {{ ref("stg_adventure_works__country_regions") }}
    )

    , joined_data as (
        select 
            {{ dbt_utils.generate_surrogate_key(['address.address_id']) }} as address_sk
            , address.address_id
            , address.address_name
            , address.city
            , state_province.state_province_name
            , country_region.country_region_name
        from address
        left join state_province on state_province.state_province_id = address.state_province_id
        left join country_region on country_region.country_region_code = state_province.country_region_code
    )

    select *
    from joined_data