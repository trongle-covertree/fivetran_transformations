{{ config(materialized='incremental', unique_key='id') }}

{{ run_socotra_policy_characteristics_fields('mysql_data_mart_10001', 'transformations_prod_socotra')  }}