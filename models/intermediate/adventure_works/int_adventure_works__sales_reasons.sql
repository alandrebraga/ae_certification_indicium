with
    sales_reasons as (
        select * from {{ ref("stg_adventure_works__sales_reasons") }}
    )

    , sales_order_sales_reasons as (
        select * from {{ ref("stg_adventure_works__sales_order_sales_reasons") }}
    )

    , joined_data as (
        select 
            sales_order_sales_reasons.sales_order_id
            , listagg(distinct sales_reasons.sales_reason_id, ',') within group (order by sales_reasons.sales_reason_id) as sales_reason_id
            , listagg(distinct sales_reasons.sales_reason_name, ',') within group (order by sales_reasons.sales_reason_name) as sales_reason_names
        from sales_order_sales_reasons
        inner join sales_reasons on sales_reasons.sales_reason_id = sales_order_sales_reasons.sales_reason_id
        group by all
    )

    select 
        {{ dbt_utils.generate_surrogate_key(['joined_data.sales_reason_id']) }} as sales_reason_sk
        , sales_reason_id
        , sales_reason_names
    from joined_data
    group by all