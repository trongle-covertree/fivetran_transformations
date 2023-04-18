{{ config(materialized='incremental') }}

{{ run_socotra_policy_underwriting_notes('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}