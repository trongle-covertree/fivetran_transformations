{{ config(materialized='incremental', unique_key='locator') }}

{{ run_socotra_peril('mysql_data_mart_10001', 'transformations_prod_socotra')  }}