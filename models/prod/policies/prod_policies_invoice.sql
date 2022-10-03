{{ config(materialized='table') }}

select
  pk,
  sk,
  documents,
  to_timestamp_ltz(to_varchar(created_timestamp)) as created_timestamp,
  policy_modification_locator,
  policy_locator,
  to_timestamp_ltz(to_varchar(updated_timestamp)) as updated_timestamp,
  display_id,
  settlement_status,
  total_due_currency,
  to_timestamp_ltz(to_varchar(end_timestamp)) as end_timestamp,
  to_timestamp_ltz(to_varchar(start_timestamp)) as start_timestamp,
  total_due,
  transaction_issued,
  payments,
  to_timestamp_ltz(to_varchar(due_timestamp)) as due_timestamp,
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
