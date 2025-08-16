with
    customers as (
        select * from {{ ref('stg_adventure_works__customers') }}
    )

    , person as (
        select * from {{ ref('stg_adventure_works__person') }}
    )

    , addresses as (
        select * from {{ ref('stg_adventure_works__addresses') }}
    )

    , state_provinces as (
        select * from {{ ref('stg_adventure_works__state_provinces') }}
    )

    , country_regions as (
        select * from {{ ref('stg_adventure_works__country_regions') }}
    )

    , territories as (
        select * from {{ ref('stg_adventure_works__territories') }}
    )

    , joined_table as (
        select 
            {{ dbt_utils.generate_surrogate_key(['customers.customer_id']) }} as customer_sk
            , customers.customer_id
            , customers.store_id
            , customers.territory_id
            , person.person_name as customer_name
            , person.person_type
            , territories.territory_name
            , territories.territory_group
            , state_provinces.state_province_name as state
            , country_regions.country_region_name as country
        from customers
        left join person on person.person_id = customers.person_id
        left join territories on territories.territory_id = customers.territory_id
        left join state_provinces on state_provinces.country_region_code = territories.country_region_code
        left join country_regions on country_regions.country_region_code = territories.country_region_code
    )

    select * 
    from joined_table
