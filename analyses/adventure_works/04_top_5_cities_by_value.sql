/*
Question 4: Quais as 5 melhores cidades em valor total negociado por produto, 
tipo de cartão, motivo de venda, data de venda, cliente, status, cidade, estado e país?

This analysis identifies the top 5 cities by total negotiated value, 
with the ability to filter by various dimensions.
*/

with
    sales_facts as (
        select * from {{ ref('fact_sales') }}
    )

    , city_performance as (
        select 
            -- Location dimensions
            state
            , country
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
            -- Customer dimension
            , customer_name
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
            state
            , country
            , product_name
            , product_category_name
            , product_subcategory_name
            , card_type
            , sales_reason_name
            , order_year
            , order_month
            , customer_name
            , order_status_description
    )

    , ranked_cities as (
        select 
            *
            , row_number() over (
                partition by product_name, card_type, sales_reason_name, order_year, order_month, 
                             customer_name, order_status_description, country
                order by total_net_amount desc
            ) as city_rank
        from city_performance
    )

    , top_5_cities as (
        select 
            state
            , country
            , product_name
            , product_category_name
            , product_subcategory_name
            , card_type
            , sales_reason_name
            , order_year
            , order_month
            , customer_name
            , order_status_description
            , number_of_orders
            , total_quantity_purchased
            , total_gross_amount
            , total_net_amount
            , average_order_value
            , city_rank
        from ranked_cities
        where city_rank <= 5
    )

    select 
        state
        , country
        , product_name
        , product_category_name
        , product_subcategory_name
        , card_type
        , sales_reason_name
        , order_year
        , order_month
        , customer_name
        , order_status_description
        , number_of_orders
        , total_quantity_purchased
        , total_gross_amount
        , total_net_amount
        , average_order_value
        , city_rank
        , round(total_gross_amount, 2) as total_gross_amount_formatted
        , round(total_net_amount, 2) as total_net_amount_formatted
        , round(average_order_value, 2) as average_order_value_formatted
    from top_5_cities
    order by 
        product_name
        , card_type
        , sales_reason_name
        , order_year desc
        , order_month
        , customer_name
        , order_status_description
        , country
        , city_rank
