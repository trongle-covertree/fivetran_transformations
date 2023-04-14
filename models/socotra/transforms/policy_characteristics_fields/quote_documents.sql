{{ config(materialized='incremental') }}

{{ run_socotra_quote_documents('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
