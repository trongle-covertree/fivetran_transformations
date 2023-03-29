{{ config(materialized='incremental') }}

{{ run_socotra_quote_basic_info('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
