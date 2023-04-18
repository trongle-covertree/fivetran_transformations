{{ config(materialized='incremental', unique_key=['quote_exposure_characteristics_locator','quote_policy_locator']) }}

{{ run_socotra_quote_exposure_territory('mysql_non_prod_data_mart_10003', 'transformations_non_prod_socotra')  }}
