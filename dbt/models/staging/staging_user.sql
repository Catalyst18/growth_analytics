{{
    config(
        materialized='incremental',
        unique_key='userid',
        alias='user'
    )
}}

SELECT
    distinct userid,
    __insert_timestamp as __insert_timestamp_source,
    now()::TIMESTAMP as __insert_timestamp 
FROM
    {{ source('raw', 'sessions') }}
{% if is_incremental() %}
  WHERE __insert_timestamp > (SELECT MAX(__insert_timestamp_source) FROM {{ this }})
{% endif %}
UNION ALL
SELECT
    distinct userid,
    __insert_timestamp as __insert_timestamp_source,
    now()::TIMESTAMP as __insert_timestamp 
FROM
    {{ source('raw', 'conversions') }}
{% if is_incremental() %}
  WHERE __insert_timestamp > (SELECT MAX(__insert_timestamp_source) FROM {{ this }})
{% endif %}

