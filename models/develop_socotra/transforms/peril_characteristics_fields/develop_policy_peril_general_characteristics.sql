{{ config(materialized='incremental', unique_key=['exposure_locator', 'policy_locator'], incremental_strategy='delete+insert') }}

{{ run_socotra_peril_general_chars('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}
