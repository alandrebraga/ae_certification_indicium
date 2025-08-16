/*
Custom test to validate Carlos's requirement:
"vendas brutas no ano de 2011 foram de $12.646.112,16"

This test ensures that the total gross sales for 2011 matches the expected value.
*/

with
    sales_facts as (
        select * from {{ ref('fact_sales') }}
    )

    , sales_2011 as (
        select 
            sum(gross_amount) as total_gross_sales_2011
        from sales_facts
        where order_year = 2011
    )

    , validation as (
        select 
            total_gross_sales_2011
            , 12646112.16 as expected_gross_sales
            , abs(total_gross_sales_2011 - 12646112.16) as difference
            , case 
                when abs(total_gross_sales_2011 - 12646112.16) < 0.01 
                then 'PASS' 
                else 'FAIL' 
              end as test_result
        from sales_2011
    )

    select 
        total_gross_sales_2011
        , expected_gross_sales
        , difference
        , test_result
        , case 
            when test_result = 'FAIL' 
            then concat('2011 gross sales validation failed. Expected: $', 
                       cast(expected_gross_sales as string), 
                       ', Actual: $', 
                       cast(round(total_gross_sales_2011, 2) as string),
                       ', Difference: $', 
                       cast(round(difference, 2) as string))
            else '2011 gross sales validation passed successfully.'
          end as test_message
    from validation
