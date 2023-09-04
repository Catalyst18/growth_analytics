{{
    config(
        materialized='incremental',
        alias='all_registrations'
    )
}}

select
    md5(userid||'-'||registration_time) as registration_sk,
    userid,
    registration_time,
    __insert_timestamp as __insert_timestamp_source,
    CURRENT_TIMESTAMP as __insert_timestamp
from
    {{ source('raw', 'conversions') }}
{% if is_incremental() %}
  WHERE __insert_timestamp > (SELECT MAX(__insert_timestamp_source) FROM {{ this }})
{% endif %}