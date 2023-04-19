{{ config(materialized='incremental',
    unique_key=['policyholder_locator'],
    incremental_strategy='delete+insert') }}

{{ run_socotra_policyholder_info('mysql_non_prod_data_mart_10003', 'transformations_develop_socotra')  }}
