{{ config(materialized='incremental') }}

{{ run_socotra_quote_exposure_usage('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
