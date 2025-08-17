with
    source as (
        select * from {{ ref("stg_adventure_works__credit_cards") }}
    )

    select 
        credit_card_id
        , card_type
        , card_number
        , exp_month
        , exp_year
    from source
