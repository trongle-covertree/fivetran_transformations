{{ config(materialized='table') }}

{{ run_socotra_policy_uw_details('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
