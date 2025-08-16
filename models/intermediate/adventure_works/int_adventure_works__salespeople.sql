with
    salespeople as (
        select * from {{ ref('stg_adventure_works__salespeople') }}
    )

    , employees as (
        select * from {{ ref('stg_adventure_works__employees') }}
    )

    , person as (
        select * from {{ ref('stg_adventure_works__person') }}
    )

    , territories as (
        select * from {{ ref('stg_adventure_works__territories') }}
    )

    , joined_table as (
        select 
            {{ dbt_utils.generate_surrogate_key(['salespeople.business_entity_id']) }} as salesperson_sk
            , salespeople.business_entity_id
            , salespeople.territory_id
            , salespeople.sales_quota
            , salespeople.bonus
            , salespeople.commission_pct
            , salespeople.sales_ytd
            , salespeople.sales_last_year
            , person.person_name as salesperson_name
            , employees.job_title
            , employees.hire_date
            , territories.territory_name
            , territories.territory_group
        from salespeople
        left join employees on employees.business_entity_id = salespeople.business_entity_id
        left join person on person.person_id = salespeople.business_entity_id
        left join territories on territories.territory_id = salespeople.territory_id
    )

    select * 
    from joined_table
