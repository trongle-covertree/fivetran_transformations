{{ config(materialized='incremental', unique_key='id') }}

{{ run_socotra_policy_invoice_schedule('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}