{{
  config(
    materialized='incremental',
    unique_key=['pipe_id','start_time']
  )
}}

WITH base AS (
  SELECT 
      pu.pipe_id,
      CASE
          when PIPE_CATALOG ilike '%_prod%' then 99999
          when PIPE_CATALOG ilike '%_stage%' then 99998
          when PIPE_CATALOG ilike '%_dev%' then 99997
          when PIPE_CATALOG ilike '%_test%' then 99996
          else 99995
      END as env_id,
      CASE
          when PIPE_CATALOG ilike '%_stage%' then 'SNOWPIPE_STAGE'
          when PIPE_CATALOG ilike '%_dev%' then 'SNOWPIPE_DEV'
          when PIPE_CATALOG ilike '%_prod%' then 'SNOWPIPE_PROD'
          when PIPE_CATALOG ilike '%_test%' then 'SNOWPIPE_TEST'
          else PIPE_CATALOG
      END as env_name,
      pu.pipe_name,
      pu.start_time,
      pu.end_time,
      pu.CREDITS_USED as CREDITS_USED,
      pu.bytes_inserted as bytes_inserted,
      pu.FILES_INSERTED as FILES_INSERTED
      FROM {{ source('snowflake','pipe_usage_history') }} pu
      left join {{ source('snowflake','pipes') }}  p
      on pu.pipe_id = p.pipe_id
      {% if is_incremental() %}
      -- this filter will only be applied on an incremental run
      WHERE start_time > (SELECT MAX(start_time)  FROM {{ this }})
      {% endif %}

)

SELECT
  *
FROM base
