/*
Question 3: Quais os 10 melhores clientes por valor total negociado filtrado por produto, 
tipo de cartão, motivo de venda, data de venda, status, cidade, estado e país?

This analysis identifies the top 10 customers by total negotiated value, 
with the ability to filter by various dimensions.
*/

with
    sales_facts as (
        select * from {{ ref('fact_sales') }}
    )

    , customer_performance as (
        select 
            -- Customer dimension
            customer_id
            , customer_name
            , state
            , country
            , territory_name
            -- Product dimensions
            , product_name
            , product_category_name
            , product_subcategory_name
            -- Credit card dimension
            , coalesce(card_type, 'No Credit Card') as card_type
            -- Sales reason dimension
            , coalesce(sales_reason_name, 'No Reason Specified') as sales_reason_name
            -- Date dimensions
            , order_year
            , order_month
            -- Status dimension
            , order_status_description
            -- Metrics
            , count(distinct sales_order_id) as number_of_orders
            , sum(order_qty) as total_quantity_purchased
            , sum(gross_amount) as total_gross_amount
            , sum(net_amount) as total_net_amount
            , avg(gross_amount) as average_order_value
        from sales_facts
        group by 
            customer_id
            , customer_name
            , state
            , country
            , territory_name
            , product_name
            , product_category_name
            , product_subcategory_name
            , card_type
            , sales_reason_name
            , order_year
            , order_month
            , order_status_description
    )

    , ranked_customers as (
        select 
            *
            , row_number() over (
                partition by product_name, card_type, sales_reason_name, order_year, order_month, 
                             order_status_description, state, country
                order by total_net_amount desc
            ) as customer_rank
        from customer_performance
    )

    , top_10_customers as (
        select 
            customer_id
            , customer_name
            , state
            , country
            , territory_name
            , product_name
            , product_category_name
            , product_subcategory_name
            , card_type
            , sales_reason_name
            , order_year
            , order_month
            , order_status_description
            , number_of_orders
            , total_quantity_purchased
            , total_gross_amount
            , total_net_amount
            , average_order_value
            , customer_rank
        from ranked_customers
        where customer_rank <= 10
    )

    select 
        customer_id
        , customer_name
        , state
        , country
        , territory_name
        , product_name
        , product_category_name
        , product_subcategory_name
        , card_type
        , sales_reason_name
        , order_year
        , order_month
        , order_status_description
        , number_of_orders
        , total_quantity_purchased
        , total_gross_amount
        , total_net_amount
        , average_order_value
        , customer_rank
        , round(total_gross_amount, 2) as total_gross_amount_formatted
        , round(total_net_amount, 2) as total_net_amount_formatted
        , round(average_order_value, 2) as average_order_value_formatted
    from top_10_customers
    order by 
        product_name
        , card_type
        , sales_reason_name
        , order_year desc
        , order_month
        , order_status_description
        , state
        , country
        , customer_rank
