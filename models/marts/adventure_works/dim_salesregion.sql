with
    source as (
        select * from {{ ref('int_adventure_works__address') }}
    )

    select *
    from source