{{
    config(
        materialized='incremental',
        alias='all_sessions'
    )
}}

select
    userid,
    time_started as session_start_time,
    is_paid as is_paid_session,
    medium as channel,
    __insert_timestamp as __insert_timestamp_source,
    CURRENT_TIMESTAMP as __insert_timestamp
from
    { { source('raw', 'sessions') } }
{% if is_incremental() %}
  WHERE __insert_timestamp > (SELECT MAX(__insert_timestamp_source) FROM {{ this }})
{% endif %}