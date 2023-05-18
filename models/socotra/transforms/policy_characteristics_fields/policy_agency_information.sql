{{ config(materialized='incremental',
    unique_key=['policy_characteristics_locator', 'policy_locator', 'policy_modification_locator']) }}

{{ run_socotra_policy_agency_information('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
