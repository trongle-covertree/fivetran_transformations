{{ config(materialized='incremental') }}

{{ run_socotra_quote_prev_addr_ph('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
