{{ config(materialized='table') }}

select deal_id, property_amount, property_cancellation_reason, property_carrier, property_closedate, property_createdate, property_dealname, property_hs_is_closed,
    property_hs_is_closed_won, property_policy_effective_date, property_policy_end_date, property_policy_number, '028221809082235148' as ct_mhcid
from fivetran_covertree.hubspot.deal where lower(property_carrier) in ('amie', 'citizens')