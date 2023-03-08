{{ config(materialized='incremental') }}

{{ run_socotra_policy_agency_information('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
