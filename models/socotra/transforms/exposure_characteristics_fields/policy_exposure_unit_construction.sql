{{ config(materialized='incremental') }}

{{ run_socotra_exposure_unit_construction('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
