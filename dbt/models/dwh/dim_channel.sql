{{
    config(
        materialized='table',
        alias='dim_channel'
    )
}}

select 
    md5(channel) as channel_sk,
    channel
from
{{ ref('staging_all_sessions') }}
group by 
channel
