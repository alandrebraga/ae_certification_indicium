{{
    config(
        materialized = "table"
        , description = "Sales fact table containing all sales transactions with metrics for business analysis"
    )
}}

with
    source as (
        select * from {{ ref("int_adventure_works__sales_orders") }}
    )

    select 
        sales_order_detail_sk
        , customer_fk
        , sales_reason_fk
        , product_fk
        , address_fk
        , sales_order_id
        , order_date
        , order_status
        , card_type
        , order_year
        , order_month
        , order_day
        , case 
            when order_status = 1 then 'In Process'
            when order_status = 2 then 'Approved'
            when order_status = 3 then 'Backordered'
            when order_status = 4 then 'Rejected'
            when order_status = 5 then 'Shipped'
            when order_status = 6 then 'Cancelled'
            else 'Unknown'
          end as order_status_description
        , sum(order_qty) as order_qty
        , sum(unit_price) as unit_price
        , sum(line_total) as line_total
        , sum(gross_amount) as gross_amount 
        , sum(net_amount) as net_amount
    from source
    group by all
