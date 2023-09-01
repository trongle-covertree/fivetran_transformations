{{ config(materialized='table') }}

{{ run_socotra_exposure_gen_chars('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
