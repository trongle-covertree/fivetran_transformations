{{ config(materialized='incremental',
    unique_key=['policyholder_locator']) }}

{{ run_socotra_policyholder_info('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
