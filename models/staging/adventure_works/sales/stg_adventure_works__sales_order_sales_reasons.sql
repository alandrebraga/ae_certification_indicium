with
    source as (
        select * from {{ source('adventure_works', 'salesorderheadersalesreason') }}
    )

    , renamed as (
        select 
            cast(salesorderid as string) as sales_order_id
            , cast(salesreasonid as string) as sales_reason_id
        from source
    )

    select * 
    from renamed
