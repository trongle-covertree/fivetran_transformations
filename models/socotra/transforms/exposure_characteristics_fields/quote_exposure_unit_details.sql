{{ config(materialized='table') }}

{{ run_socotra_quote_exposure_unit_details('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
