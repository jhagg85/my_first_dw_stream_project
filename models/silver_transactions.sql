{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    file_format='delta'
) }}

WITH raw_bronze AS (
    SELECT 
        UPPER(TRIM(transaction_id)) AS transaction_id,
        UPPER(TRIM(customer_id)) AS customer_id,
        CAST(amount AS DECIMAL(18, 4)) AS raw_amount,
        UPPER(TRIM(currency)) AS currency,
        CAST(event_time AS TIMESTAMP) AS transaction_timestamp,
        ingested_at
    FROM {{ source('streaming_source', 'bronze_transactions') }}
    WHERE transaction_id IS NOT NULL
)

{% if is_incremental() %}
, watermark AS (
    -- Safely isolate the max watermark threshold in its own staging block
    SELECT COALESCE(MAX(ingested_at), CAST('1900-01-01' AS TIMESTAMP)) AS max_ingested
    FROM {{ this }}
)

SELECT 
    b.transaction_id,
    b.customer_id,
    b.raw_amount,
    b.currency,
    b.transaction_timestamp,
    b.ingested_at
FROM raw_bronze b
INNER JOIN watermark w
    ON b.ingested_at > w.max_ingested

{% else %}

-- Initial run setup path bypassing watermark checks completely to prevent compilation exceptions
SELECT * FROM raw_bronze

{% endif %}
