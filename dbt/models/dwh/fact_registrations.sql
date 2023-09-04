{{
    config(
        materialized='incremental',
        alias='fact_registrations'
    )
}}

select 
    md5(userid||'-') as user_sk,
    registration_time,
    __insert_timestamp
from {{ ref('staging_all_registrations') }}
{% if is_incremental() %}
  WHERE __insert_timestamp > (SELECT MAX(__insert_timestamp) FROM {{ this }})
{% endif %}