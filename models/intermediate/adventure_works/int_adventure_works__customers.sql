with
    customers as (
        select * from {{ ref('stg_adventure_works__customers') }}
    )
    
    , person as (
        select * from {{ ref("stg_adventure_works__person")}}
    )

    , territores as (
        select * from {{ ref("stg_adventure_works__territories")}}
    )

    , joined_table as (
        select 
            {{ dbt_utils.generate_surrogate_key(['customers.customer_id']) }} as customer_sk
            , customers.customer_id
            , territores.territory_name
            , territores.country_region_code as country
            , person.person_name as customer_name
            , person.person_type as person_type
        from customers
        left join person on person.person_id = customers.person_id
        left join territores on territores.territory_id =  customers.territory_id
    )

    select * 
    from joined_table
