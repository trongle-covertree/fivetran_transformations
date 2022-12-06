{{ config(materialized='table') }}

select id, property_lead_source as lead_source, property_email as customer_email, parse_url(property_hs_analytics_last_referrer):host::varchar as source_hostname,
    parse_url(property_hs_analytics_last_url):host::varchar as referred_hostname, property_usa_state as state
from fivetran_covertree.hubspot.contact where property_email != 'test-dont-pay-me-for-this@gmail.com';