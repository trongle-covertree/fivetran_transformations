{{ config(materialized='incremental') }}

{{ run_socotra_ad_insured('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
