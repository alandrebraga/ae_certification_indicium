/*
Question 1: Qual o número de pedidos, quantidade comprada, valor total negociado por produto, 
tipo de cartão, motivo de venda, data de venda, cliente, status, cidade, estado e país?

This analysis provides comprehensive sales metrics broken down by all requested dimensions.
*/

with
    sales_facts as (
        select * from {{ ref('fact_sales') }}
    )

    , sales_metrics as (
        select 
            -- Product dimensions
            product_name
            , product_category_name
            , product_subcategory_name
            -- Credit card dimension
            , coalesce(card_type, 'No Credit Card') as card_type
            -- Sales reason dimension
            , coalesce(sales_reason_name, 'No Reason Specified') as sales_reason_name
            -- Date dimensions
            , order_date
            , order_year
            , order_month
            -- Customer dimension
            , customer_name
            -- Status dimension
            , order_status_description
            -- Location dimensions
            , state
            , country
            -- Metrics
            , count(distinct sales_order_id) as number_of_orders
            , sum(order_qty) as total_quantity_purchased
            , sum(gross_amount) as total_gross_amount
            , sum(net_amount) as total_net_amount
            , avg(gross_amount) as average_order_value
        from sales_facts
        group by 
            product_name
            , product_category_name
            , product_subcategory_name
            , card_type
            , sales_reason_name
            , order_date
            , order_year
            , order_month
            , customer_name
            , order_status_description
            , state
            , country
    )

    select 
        product_name
        , product_category_name
        , product_subcategory_name
        , card_type
        , sales_reason_name
        , order_date
        , order_year
        , order_month
        , customer_name
        , order_status_description
        , state
        , country
        , number_of_orders
        , total_quantity_purchased
        , total_gross_amount
        , total_net_amount
        , average_order_value
        , round(total_gross_amount, 2) as total_gross_amount_formatted
        , round(total_net_amount, 2) as total_net_amount_formatted
        , round(average_order_value, 2) as average_order_value_formatted
    from sales_metrics
    order by 
        total_gross_amount desc
        , number_of_orders desc
