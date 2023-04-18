{{ config(materialized='incremental', unique_key=['exposure_locator', 'policy_locator']) }}

{{ run_socotra_peril_sched_personals('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}