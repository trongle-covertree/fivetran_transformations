{{ config(materialized='table') }}

{{ run_socotra_commissionable_partners('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
