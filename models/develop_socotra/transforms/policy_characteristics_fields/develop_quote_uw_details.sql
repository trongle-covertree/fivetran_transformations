{{ config(materialized='incremental') }}

{{ run_socotra_quote_uw_details('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}
