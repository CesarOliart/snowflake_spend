{{
  config(
    materialized = 'incremental',
    unique_key = ['pipe_id','start_time'],
    )
}}
WITH base AS (
  SELECT
    99999 AS pipe_id,
    'SNOWPIPE' as pipe_name,
    date_trunc(minute,start_time) AS START_TIME,
    date_trunc(minute,end_time) as END_TIME,
    sum(credits_used) AS CREDITS_USED
	FROM {{ source('snowflake','pipe_usage_history') }}
  {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  WHERE START_TIME >= (SELECT MAX(start_time)  FROM {{ this }})
  {% endif %}
  {{dbt_utils.group_by(4)}}
)

SELECT
  pipe_id,
  pipe_name,
  start_time,
  end_time,
  credits_used
FROM base
