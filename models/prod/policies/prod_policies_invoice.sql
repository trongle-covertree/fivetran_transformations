{{ config(materialized='table') }}

select
  pk,
  sk,
  documents,
  {{ dbt_date.from_unixtimestamp("created_timestamp", format="milliseconds") }} as created_timestamp,
  policy_modification_locator,
  policy_locator,
  {{ dbt_date.from_unixtimestamp("updated_timestamp", format="milliseconds") }} as updated_timestamp,
  display_id,
  settlement_status,
  total_due_currency,
  {{ dbt_date.from_unixtimestamp("end_timestamp", format="milliseconds") }} as end_timestamp,
  {{ dbt_date.from_unixtimestamp("start_timestamp", format="milliseconds") }} as start_timestamp,
  total_due,
  transaction_issued,
  payments,
  {{ dbt_date.from_unixtimestamp("due_timestamp", format="milliseconds") }} as due_timestamp,
  statuses,
  invoice_type,
  locator,
  settlement_type,
  financial_transactions,
  future_invoices,
  generated_invoices,
  invoice,
  status
from dynamodb.prod_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'INVOICE#%') and _fivetran_deleted='FALSE'
