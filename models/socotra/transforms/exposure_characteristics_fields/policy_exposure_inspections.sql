{{ config(materialized='incremental') }}

{{ run_socotra_exposure_inspections('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
