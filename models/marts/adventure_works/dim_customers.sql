{{
    config(
        materialized = "table"
        , description = "Customer dimension table containing customer information, location, and territory details"
    )
}}

with
    source as (
        select * from {{ ref("int_adventure_works__customers") }}
    )

    select 
        customer_sk
        , customer_id
        , store_id
        , territory_id
        , customer_name
        , person_type
        , territory_name
        , territory_group
        , state
        , country
    from source
