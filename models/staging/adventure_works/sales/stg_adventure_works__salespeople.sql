with
    source as (
        select * from {{ source('adventure_works', 'salesperson') }}
    )

    , renamed as (
        select 
            cast(businessentityid as string) as business_entity_id
            , cast(territoryid as string) as territory_id
            , cast(salesquota as float) as sales_quota
            , cast(bonus as float) as bonus
            , cast(commissionpct as float) as commission_pct
            , cast(salesytd as float) as sales_ytd
            , cast(saleslastyear as float) as sales_last_year
        from source
    )

    select * 
    from renamed
