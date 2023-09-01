{{ config(materialized='table') }}

{{ run_socotra_quote_adverse_action('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
