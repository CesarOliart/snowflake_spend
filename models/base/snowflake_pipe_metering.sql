{{
  config(
    materialized = 'incremental',
    unique_key = ['pipe_id','start_time'],
    )
}}
WITH base AS (
  SELECT
    env_id AS pipe_id,
    env_name as pipe_env,
    --pipe_name as pipe_name,
    date_trunc(hour,start_time) AS START_TIME,
    date_trunc(hour,start_time) +  INTERVAL '1 hour' - INTERVAL '1 second' as END_TIME,
    sum(credits_used) AS CREDITS_USED,
    sum(BYTES_INSERTED/1e+6) AS MB_INSERTED,
    sum(FILES_INSERTED) AS FILES_INSERTED
	FROM  {{ ref('snowflake_pipe_usage') }} --{{ source('snowflake','pipe_usage_history') }}
  {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  WHERE START_TIME >= (SELECT MAX(start_time)  FROM {{ this }})
  {% endif %}
  {{dbt_utils.group_by(4)}}
)

SELECT
  pipe_id,
  pipe_env,
  start_time,
  end_time,
  credits_used,
  MB_INSERTED,
  FILES_INSERTED
FROM base