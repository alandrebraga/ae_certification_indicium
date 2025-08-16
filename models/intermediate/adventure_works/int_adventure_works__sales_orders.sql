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

    , salespeople as (
        select * from {{ ref('int_adventure_works__salespeople') }}
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

    , products as (
        select * from {{ ref('int_adventure_works__products') }}
    )

    , joined_table as (
        select 
            {{ dbt_utils.generate_surrogate_key(['sales_orders.sales_order_id', 'sales_order_details.sales_order_detail_id']) }} as sales_order_detail_sk
            , sales_orders.sales_order_id
            , sales_order_details.sales_order_detail_id
            , sales_orders.order_date
            , sales_orders.due_date
            , sales_orders.ship_date
            , sales_orders.order_status
            , sales_orders.subtotal
            , sales_orders.tax_amount
            , sales_orders.freight
            , sales_orders.total_due
            , sales_orders.is_online_order
            , sales_order_details.order_qty
            , sales_order_details.unit_price
            , sales_order_details.unit_price_discount
            , sales_order_details.line_total
            , customers.customer_sk
            , customers.customer_id
            , customers.customer_name
            , customers.state
            , customers.country
            , customers.territory_name
            , salespeople.salesperson_sk
            , salespeople.salesperson_name
            , credit_cards.credit_card_id
            , credit_cards.card_type
            , products.product_sk
            , products.product_id
            , products.product_name
            , products.product_category_name
            , products.product_subcategory_name
            , sales_reasons.sales_reason_id
            , sales_reasons.sales_reason_name
            , sales_reasons.reason_type
            -- Calculated fields for business metrics
            , (sales_order_details.unit_price * sales_order_details.order_qty) as gross_amount
            , (sales_order_details.unit_price * sales_order_details.order_qty * (1 - sales_order_details.unit_price_discount)) as net_amount
            , extract(year from sales_orders.order_date) as order_year
            , extract(month from sales_orders.order_date) as order_month
            , extract(day from sales_orders.order_date) as order_day
        from sales_orders
        inner join sales_order_details on sales_order_details.sales_order_id = sales_orders.sales_order_id
        left join customers on customers.customer_id = sales_orders.customer_id
        left join salespeople on salespeople.business_entity_id = sales_orders.salesperson_id
        left join credit_cards on credit_cards.credit_card_id = sales_orders.credit_card_id
        left join products on products.product_id = sales_order_details.product_id
        left join sales_order_sales_reasons on sales_order_sales_reasons.sales_order_id = sales_orders.sales_order_id
        left join sales_reasons on sales_reasons.sales_reason_id = sales_order_sales_reasons.sales_reason_id
    )

    select * 
    from joined_table
