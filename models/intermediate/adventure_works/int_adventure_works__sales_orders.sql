with
    sales_orders as (
        select * from {{ ref('stg_adventure_works__sales_orders') }}
    )

    , sales_order_details as (
        select * from {{ ref('stg_adventure_works__sales_order_details') }}
    )

    , customers as (
        select * from {{ ref('int_adventure_works__customers') }}
    )

    , credit_cards as (
        select * from {{ ref('stg_adventure_works__credit_cards') }}
    )

    , sales_reasons as (
        select * from {{ ref('stg_adventure_works__sales_reasons') }}
    )

    , sales_order_sales_reasons as (
        select * from {{ ref('stg_adventure_works__sales_order_sales_reasons') }}
    )

    , int_sales_reason as (
        select * from {{ ref('int_adventure_works__sales_reasons') }}
    )

    , products as (
        select * from {{ ref('int_adventure_works__products') }}
    )

    , joined_reason as (
        select 
            sales_orders.*
            , listagg(distinct sales_reasons.sales_reason_id, ',') within group (order by sales_reasons.sales_reason_id) as sales_reason_id
        from sales_orders
        left join sales_order_sales_reasons on sales_order_sales_reasons.sales_order_id = sales_orders.sales_order_id
        left join sales_reasons on sales_reasons.sales_reason_id = sales_order_sales_reasons.sales_reason_id
        group by all
    )

    , joined_table as (
        select 
            {{ dbt_utils.generate_surrogate_key(['joined_reason.sales_order_id', 'sales_order_details.sales_order_detail_id', 'customers.customer_sk', 'products.product_sk']) }} as sales_order_detail_sk
            , {{ dbt_utils.generate_surrogate_key(['joined_reason.sales_reason_id']) }} as sales_reason_fk
            , customers.customer_sk as customer_fk
            , products.product_sk as product_fk
            , joined_reason.sales_order_id
            , sales_order_details.sales_order_detail_id
            , joined_reason.order_date
            , joined_reason.due_date
            , joined_reason.ship_date
            , joined_reason.order_status
            , joined_reason.subtotal
            , joined_reason.tax_amount
            , joined_reason.freight
            , joined_reason.total_due
            , joined_reason.is_online_order
            , sales_order_details.order_qty
            , sales_order_details.unit_price
            , sales_order_details.unit_price_discount
            , sales_order_details.line_total
            , credit_cards.credit_card_id
            , credit_cards.card_type
            , sum((sales_order_details.unit_price * sales_order_details.order_qty)) as gross_amount
            , sum((sales_order_details.unit_price * sales_order_details.order_qty * (1 - sales_order_details.unit_price_discount))) as net_amount
            , extract(year from joined_reason.order_date) as order_year
            , extract(month from joined_reason.order_date) as order_month
            , extract(day from joined_reason.order_date) as order_day
        from joined_reason
        inner join sales_order_details on sales_order_details.sales_order_id = joined_reason.sales_order_id
        left join customers on customers.customer_id = joined_reason.customer_id
        left join credit_cards on credit_cards.credit_card_id = joined_reason.credit_card_id
        left join products on products.product_id = sales_order_details.product_id
        group by all
    )

    select * 
    from joined_table
