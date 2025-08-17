with
    source as (
        select * from {{ ref("int_adventure_works__salespeople") }}
    )

    select 
        salesperson_sk
        , business_entity_id
        , territory_id
        , sales_quota
        , bonus
        , commission_pct
        , sales_ytd
        , sales_last_year
        , salesperson_name
        , job_title
        , hire_date
        , territory_name
        , territory_group
    from source
