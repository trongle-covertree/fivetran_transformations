{{ config(materialized='table') }}

{{ run_socotra_policy_level_suppl_uw('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
