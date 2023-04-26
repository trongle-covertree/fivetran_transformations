{{ config(materialized='incremental', unique_key=['id']) }}

{{ run_socotra_claim_version('mysql_data_mart_10001', 'transformations_prod_socotra')  }}