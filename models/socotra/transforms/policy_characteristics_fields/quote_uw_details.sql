{{ config(materialized='incremental',
    unique_key=['quote_policy_characteristics_locator', 'quote_policy_locator'],
    incremental_strategy='delete+insert') }}

{{ run_socotra_quote_uw_details('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
