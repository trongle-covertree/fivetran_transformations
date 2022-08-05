{{ config(materialized='table') }}

select
  pk,
  sk,
  write_off,
  financial_transactions,
  {{ dbt_date.from_unixtimestamp("start_timestamp", format="milliseconds") }} as start_timestamp,
  {{ dbt_date.from_unixtimestamp("end_timestamp", format="milliseconds") }} as end_timestamp,
  {{ dbt_date.from_unixtimestamp("issue_timestamp", format="milliseconds") }} as issue_timestamp,
  {{ dbt_date.from_unixtimestamp("due_timestamp", format="milliseconds") }} as due_timestamp
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'FUTUREINVOICE#%') and _fivetran_deleted='FALSE'
