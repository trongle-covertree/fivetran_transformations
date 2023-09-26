{{ config(materialized='table') }}

{{ run_referral_socotra_info_grouping() }}
