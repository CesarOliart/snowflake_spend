{{
  config(
    materialized = 'incremental',
    unique_key = ['warehouse_id','start_time'],
    )
}}
WITH base AS (
	SELECT
  *
	FROM {{ source('snowflake','warehouse_metering_history') }}
  {% if is_incremental() %}
  WHERE start_time >= (SELECT MAX(start_time)  FROM {{ this }})
  {% endif %}
)

SELECT
  warehouse_id,
  warehouse_name,
  start_time,
  end_time,
  credits_used
FROM base
