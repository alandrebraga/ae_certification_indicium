{{
    config(
        materialized = "table"
        , description = "Sales reasons dimension table containing sales reason information and categorization"
    )
}}

with
    source as (
        select * from {{ ref("int_adventure_works__sales_reasons") }}
    )

    select 
        sales_reason_sk
        , sales_reason_id
        , sales_reason_names
    from source
