with
    source as (
        select * from {{ source('adventure_works', 'salesreason') }}
    )

    , renamed as (
        select 
            cast(salesreasonid as string) as sales_reason_id
            , cast(name as string) as sales_reason_name
            , cast(reasontype as string) as reason_type
        from source
    )

    select * 
    from renamed
