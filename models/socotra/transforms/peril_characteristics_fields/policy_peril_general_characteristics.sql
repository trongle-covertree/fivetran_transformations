{{ config(materialized='incremental', unique_key=['exposure_locator', 'policy_locator']) }}

{{ run_socotra_peril_general_chars('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
