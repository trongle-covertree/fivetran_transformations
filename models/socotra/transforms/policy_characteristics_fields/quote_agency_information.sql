{{ config(materialized='incremental') }}

{{ run_socotra_quote_agency_information('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
