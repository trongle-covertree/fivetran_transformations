{{ config(materialized='incremental') }}

{{ run_socotra_quote_basic_info('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}
