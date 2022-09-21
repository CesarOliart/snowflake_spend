WITH base AS (

	SELECT
  warehouse_id,
  warehouse_name,
  start_time,
  end_time,
  credits_used
	FROM {{ source('snowflake','warehouse_metering_history') }}

  UNION ALL

	SELECT
  pipe_id,
  pipe_env,
  start_time,
  end_time,
  credits_used
	FROM {{ source('snowflake','pipe_usage_history') }}

)

SELECT
  warehouse_id,
  warehouse_name,
  start_time,
  end_time,
  credits_used
FROM base
