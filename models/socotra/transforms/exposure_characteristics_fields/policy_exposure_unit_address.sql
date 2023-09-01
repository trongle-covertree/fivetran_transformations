{{ config(materialized='table') }}

{{ run_socotra_exposure_unit_address('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
