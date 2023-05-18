{{ config(materialized='incremental', unique_key=['quote_exposure_locator', 'quote_policy_locator'], incremental_strategy='delete+insert') }}

{{ run_socotra_quote_peril_general_chars('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
