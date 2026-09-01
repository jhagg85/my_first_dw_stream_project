{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH silver_clean AS (
    SELECT * FROM {{ ref('silver_transactions') }}
),

currency_normalization AS (
    SELECT 
        transaction_id,
        customer_id,
        raw_amount,
        currency,
        transaction_timestamp,
        CASE 
            WHEN currency = 'USD' THEN raw_amount
            WHEN currency = 'EUR' THEN raw_amount * 1.08
            WHEN currency = 'PHP' THEN raw_amount * 0.018
            ELSE raw_amount
        END AS amount_usd
    FROM silver_clean
)

SELECT 
    customer_id,
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount_usd), 2) AS lifetime_value_usd,
    MAX(transaction_timestamp) AS last_active_stream_timestamp
FROM currency_normalization
GROUP BY customer_id
