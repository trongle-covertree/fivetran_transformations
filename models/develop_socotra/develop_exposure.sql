{{ config(materialized='incremental', unique_key='locator') }}

{{ run_socotra_exposure('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}