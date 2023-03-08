{{ config(materialized='incremental') }}

{{ run_socotra_policy_underwriting_notes('mysql_data_mart_10001', 'transformations_prod_socotra')  }}