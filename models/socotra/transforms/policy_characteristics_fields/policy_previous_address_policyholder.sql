{{ config(materialized='table') }}

{{ run_socotra_policy_prev_addr_ph('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
