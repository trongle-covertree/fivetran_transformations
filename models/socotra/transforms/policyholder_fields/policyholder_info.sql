{{ config(materialized='table') }}

{{ run_socotra_policyholder_info('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
