{{ config(materialized='table') }}

select pk, sk, write_off, financial_transactions, start_timestamp, end_timestamp, issue_timestamp, due_timestamp from dynamodb_develop.deve_socotra_policy_table
where (pk like 'POLICY#%' and sk like 'FUTUREINVOICE#%') and _fivetran_deleted='FALSE'
