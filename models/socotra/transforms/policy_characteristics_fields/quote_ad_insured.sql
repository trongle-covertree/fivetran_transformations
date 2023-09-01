{{ config(materialized='table') }}

{{ run_socotra_quote_ad_insured('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
