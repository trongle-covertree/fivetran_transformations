{{ config(materialized='incremental',
    unique_key=['claim_locator'],
    incremental_strategy='delete+insert') }}

{{ run_socotra_claim_injured_party('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
