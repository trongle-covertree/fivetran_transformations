{{ config(materialized='incremental', unique_key=['quote_policy_characteristics_locator', 'quote_policy_locator']) }}

{{ run_socotra_quote_basic_info('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
