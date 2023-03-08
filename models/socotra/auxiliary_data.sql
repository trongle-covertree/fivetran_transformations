{{ config(materialized='incremental') }}

{{ run_socotra_policy_auxiliary_data('mysql_data_mart_10001', 'transformations_prod_socotra')  }}