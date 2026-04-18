-- models/stg_sales.sql

WITH base AS (

    SELECT
        sale_id,
        sale_date,
        customer_id,
        product,
        quantity,
        unit_price,
        total_amount,
        region

    FROM {{ source('redshift_source', 'sales') }}

),

enriched AS (

    SELECT
        sale_id,
        sale_date,
        customer_id,
        product,
        quantity,
        unit_price,
        total_amount,
        region,
        quantity * unit_price AS total_amount_check,
        TO_CHAR(sale_date,'Day') AS day_of_week,
    CASE
        WHEN unit_price < 20 THEN 'Low'
        WHEN unit_price BETWEEN 20 AND 50 THEN 'Medium'
        ELSE 'High'
    END AS price_category
    FROM base

)

SELECT * FROM enriched