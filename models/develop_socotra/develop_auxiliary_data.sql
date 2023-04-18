{{ config(materialized='incremental', unique_key=['locator', 'key_name']) }}

{{ run_socotra_policy_auxiliary_data('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}