{{ config(materialized='incremental', unique_key=['policy_characteristics_locator', 'policy_locator']) }}

{{ run_socotra_policy_prior_insurance('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
