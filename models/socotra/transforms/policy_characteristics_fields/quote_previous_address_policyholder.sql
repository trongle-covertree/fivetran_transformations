{{ config(materialized='incremental', unique_key=['quote_policy_characteristics_locator', 'quote_policy_locator']) }}

{{ run_socotra_quote_prev_addr_ph('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
