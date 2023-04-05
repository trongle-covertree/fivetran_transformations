{{ config(materialized='incremental', unique_key=['quote_exposure_characteristics_locator','quote_policy_locator']) }}

{{ run_socotra_quote_exposure_unit_level_suppl_uw('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
