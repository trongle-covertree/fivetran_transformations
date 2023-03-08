{{ config(materialized='incremental', unique_key='locator') }}

{{ run_socotra_policy_modification('mysql_data_mart_10001', 'transformations_prod_socotra')  }}