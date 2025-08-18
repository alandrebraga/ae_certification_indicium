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

    , address as (
        select * from {{ ref('int_adventure_works__address') }}
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

    , not_informed_reason_id as (
        select 
            * exclude(sales_reason_id)
            , case 
                when sales_reason_id = '' then '-1' else sales_reason_id
              end as sales_reason_id
        from joined_reason
    )

    , joined_table as (
        select 
            {{ dbt_utils.generate_surrogate_key(['not_informed_reason_id.sales_order_id', 'sales_order_details.sales_order_detail_id', 'customers.customer_sk', 'products.product_sk']) }} as sales_order_detail_sk
            , {{ dbt_utils.generate_surrogate_key(['not_informed_reason_id.sales_reason_id']) }} as sales_reason_fk
            , customers.customer_sk as customer_fk
            , products.product_sk as product_fk
            , address.address_sk as address_fk
            , not_informed_reason_id.sales_order_id
            , sales_order_details.sales_order_detail_id
            , not_informed_reason_id.order_date
            , not_informed_reason_id.due_date
            , not_informed_reason_id.ship_date
            , not_informed_reason_id.order_status
            , not_informed_reason_id.subtotal
            , not_informed_reason_id.tax_amount
            , not_informed_reason_id.freight
            , not_informed_reason_id.total_due
            , not_informed_reason_id.is_online_order
            , not_informed_reason_id.address_id
            , sales_order_details.order_qty
            , sales_order_details.unit_price
            , sales_order_details.unit_price_discount
            , sales_order_details.line_total
            , coalesce(credit_cards.card_type, 'Not Informed') as card_type
            , sum((sales_order_details.unit_price * sales_order_details.order_qty)) as gross_amount
            , sum((sales_order_details.unit_price * sales_order_details.order_qty * (1 - sales_order_details.unit_price_discount))) as net_amount
            , extract(year from not_informed_reason_id.order_date) as order_year
            , extract(month from not_informed_reason_id.order_date) as order_month
            , extract(day from not_informed_reason_id.order_date) as order_day
        from not_informed_reason_id
        inner join sales_order_details on sales_order_details.sales_order_id = not_informed_reason_id.sales_order_id
        left join customers on customers.customer_id = not_informed_reason_id.customer_id
        left join credit_cards on credit_cards.credit_card_id = not_informed_reason_id.credit_card_id
        left join products on products.product_id = sales_order_details.product_id
        left join address on address.address_id = not_informed_reason_id.address_id
        group by all
    )

    select *
    from joined_table
