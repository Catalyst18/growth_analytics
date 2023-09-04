{{
    config(
        materialized='incremental',
        alias='fact_sessions'
    )
}}

select 
    session_sk,
    md5(userid||'-') as user_sk,
    session_start_time,
    is_paid_session,
    md5(channel) as channel_sk,
    __insert_timestamp
from 
{{ ref('staging_all_sessions') }}
{% if is_incremental() %}
  WHERE __insert_timestamp > (SELECT MAX(__insert_timestamp) FROM {{ this }})
{% endif %}