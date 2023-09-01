{{ config(materialized='table') }}

{{ run_socotra_quote_prior_claims('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
