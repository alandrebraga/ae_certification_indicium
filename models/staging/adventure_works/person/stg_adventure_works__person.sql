with
    source as (
        select * from {{ source('adventure_works', 'person') }}
    )

    , renamed as (
        select 
            BusinessEntityID as person_id
            , PersonType as person_type
            , concat_ws(' ', coalesce(FirstName, '')
                            , coalesce(MiddleName, '')
                            , coalesce(LastName, '')
                        ) as person_name
        from source
    )

    select * 
    from renamed
