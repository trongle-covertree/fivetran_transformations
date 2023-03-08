{{ config(materialized='incremental') }}

{{ run_socotra_peril_sched_personals('mysql_data_mart_10001', 'transformations_prod_socotra')  }}