with
    source as (
        select * from {{ source('adventure_works', 'creditcard') }}
    )

    , renamed as (
        select 
            cast(creditcardid as string) as credit_card_id
            , cast(cardtype as string) as card_type
            , cast(cardnumber as string) as card_number
            , cast(expmonth as integer) as exp_month
            , cast(expyear as integer) as exp_year
        from source
    )

    select * 
    from renamed
