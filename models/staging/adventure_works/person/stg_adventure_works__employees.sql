with
    source as (
        select * from {{ source('adventure_works', 'employee') }}
    )

    , renamed as (
        select 
            cast(businessentityid as string) as business_entity_id
            , cast(nationalidnumber as string) as national_id_number
            , cast(loginid as string) as login_id
            , cast(jobtitle as string) as job_title
            , cast(birthdate as date) as birth_date
            , cast(maritalstatus as string) as marital_status
            , cast(gender as string) as gender
            , cast(hiredate as date) as hire_date
            , cast(salariedflag as boolean) as is_salaried
            , cast(vacationhours as integer) as vacation_hours
            , cast(sickleavehours as integer) as sick_leave_hours
            , cast(currentflag as boolean) as is_current
        from source
    )

    select * 
    from renamed
