{{ config(materialized='table') }}

{{ run_socotra_policy_prior_insurance('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
