{{
    config(
        materialized='incremental',
        alias='dim_user'
    )
}}

select
    user_sk,
    userid,
    __insert_timestamp
from
    {{ ref('staging_user') }}
{% if is_incremental() %}
  WHERE __insert_timestamp > (SELECT MAX(__insert_timestamp) FROM {{ this }})
{% endif %}