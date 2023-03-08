{{ config(materialized='incremental') }}

{{ run_socotra_policy_prior_claims('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
