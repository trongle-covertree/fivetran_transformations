{{ config(materialized='table') }}

select
  pk,
  sk,
  documents,
  created_timestamp,
  policy_modification_locator,
  policy_locator,
  updated_timestamp,
  display_id,
  settlement_status,
  total_due_currency,
  end_timestamp,
  start_timestamp,
  total_due,
  transaction_issued,
  payments,
  due_timestamp,
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
