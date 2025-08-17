with
    source as (
        select * from {{ ref("int_adventure_works__customers") }}
    )

    select 
        customer_sk
        , customer_id
        , territory_name
        , country
        , customer_name
        , person_type
    from source
