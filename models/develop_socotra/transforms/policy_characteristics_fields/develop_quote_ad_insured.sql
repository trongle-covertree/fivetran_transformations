{{ config(materialized='incremental') }}

{{ run_socotra_quote_ad_insured('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}
