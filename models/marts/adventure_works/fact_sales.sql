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
        , cast(sum(order_qty) as integer) as order_qty
        , cast(sum(unit_price) as float) as unit_price
        , cast(sum(line_total) as float) as line_total
        , cast(sum(gross_amount) as float) as gross_amount 
        , cast(sum(net_amount) as float) as net_amount
    from source
    group by all
