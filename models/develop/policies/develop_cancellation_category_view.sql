{{ config(materialized='view') }}

select cancellation_category, count(*) as count
from  {{ ref('develop_policies_cancellation')}}
group by cancellation_category