/*
    Identifies there are conversions without any session from the session tables
*/

{{ config(store_failures = true) }}

select distinct userid from (
select userid from {{ source('raw','conversions') }} c 
except
select userid from {{ source('raw','sessions') }} s
) as t -- 96785