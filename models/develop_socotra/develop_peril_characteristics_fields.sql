{{ config(materialized='incremental', unique_key='id') }}

{{ run_socotra_peril_characteristics_fields('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}