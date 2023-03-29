{{ config(materialized='incremental') }}

{{ run_socotra_quote_peril_general_chars('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
