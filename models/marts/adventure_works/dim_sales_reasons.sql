{{
    config(
        materialized = "table"
        , description = "Sales reasons dimension table containing sales reason information and categorization"
    )
}}

with
    source as (
        select * from {{ ref("stg_adventure_works__sales_reasons") }}
    )

    select 
        sales_reason_id
        , sales_reason_name
        , reason_type
    from source
