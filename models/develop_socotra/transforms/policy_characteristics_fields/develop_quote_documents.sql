{{ config(materialized='incremental') }}

{{ run_socotra_quote_documents('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}
