{{ config(materialized='incremental') }}

{{ run_socotra_exposure_unit_level_suppl_uw('mysql_data_mart_10001', 'transformations_prod_socotra')  }}
