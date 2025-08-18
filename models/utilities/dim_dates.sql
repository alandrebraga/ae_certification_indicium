{{
    config(
        materialized = "table"
    )
}}

{{ generate_dynamic_date_dimension() }}