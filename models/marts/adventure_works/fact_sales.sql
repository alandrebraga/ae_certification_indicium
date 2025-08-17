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
        , sales_order_id
        , sales_order_detail_id
        , order_date
        , due_date
        , ship_date
        , order_status
        , subtotal
        , tax_amount
        , freight
        , total_due
        , is_online_order
        , order_qty
        , unit_price
        , unit_price_discount
        , line_total
        , credit_card_id
        , card_type
        , gross_amount
        , net_amount
        , order_year
        , order_month
        , order_day
        , case 
            when unit_price_discount > 0 then true 
            else false 
          end as has_discount
        , (gross_amount - net_amount) as total_discount_amount
        , case 
            when order_status = 1 then 'In Process'
            when order_status = 2 then 'Approved'
            when order_status = 3 then 'Backordered'
            when order_status = 4 then 'Rejected'
            when order_status = 5 then 'Shipped'
            when order_status = 6 then 'Cancelled'
            else 'Unknown'
          end as order_status_description
    from source
