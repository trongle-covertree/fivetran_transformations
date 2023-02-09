{{ config(materialized='table') }}

select d.deal_id, d.property_policy_number, d.property_community_deal, c.property_communityname, c.property_lead_source, c.property_email,
    c.property_community_referrer, c.property_address, c.property_city,
    case
        when contains(lower(c.property_state), 'alaska') then 'AK'
        when contains(lower(c.property_state), 'arizona') then 'AZ'
        when contains(lower(c.property_state), 'connecticut') then 'CT'
        when contains(lower(c.property_state), 'georgia') then 'GA'
        when contains(lower(c.property_state), 'hawaii') then 'AK'
        when contains(lower(c.property_state), 'iowa') then 'IA'
        when contains(lower(c.property_state), 'kansas') then 'KS'
        when contains(lower(c.property_state), 'kentucky') then 'KY'
        when contains(lower(c.property_state), 'louisiana') then 'LA'
        when contains(lower(c.property_state), 'maine') then 'ME'
        when contains(lower(c.property_state), 'maryland') then 'MD'
        when contains(lower(c.property_state), 'minnesota') then 'MN'
        when contains(lower(c.property_state), 'mississippi') then 'MS'
        when contains(lower(c.property_state), 'missouri') then 'MO'
        when contains(lower(c.property_state), 'montana') then 'MT'
        when contains(lower(c.property_state), 'nevada') then 'NV'
        when contains(lower(c.property_state), 'new hampshire') then 'NH'
        when contains(lower(c.property_state), 'new jersey') then 'NJ'
        when contains(lower(c.property_state), 'new mexico') then 'NM'
        when contains(lower(c.property_state), 'new york') then 'NY'
        when contains(lower(c.property_state), 'north carolina') then 'NC'
        when contains(lower(c.property_state), 'north dakota') then 'ND'
        when contains(lower(c.property_state), 'pennsylvania') then 'PA'
        when contains(lower(c.property_state), 'south carolina') then 'SC'
        when contains(lower(c.property_state), 'south dakota') then 'SD'
        when contains(lower(c.property_state), 'tennessee') then 'TN'
        when contains(lower(c.property_state), 'texas') then 'TX'
        when contains(lower(c.property_state), 'vermont') then 'VT'
        when contains(lower(c.property_state), 'virgina') then 'VA'
        when contains(lower(c.property_state), 'west virginia') then 'WV'
        else upper(left(c.property_state, 2))
    end as property_state, c.property_zip, d.property_policy_effective_date::varchar::timestamp as property_policy_effective_date,
    d.property_policy_end_date::varchar::timestamp as property_policy_end_date, d.property_amount, d.property_cancellation_reason, d.property_carrier, d.property_closedate,
    d.property_createdate, d.property_dealname, d.property_hs_is_closed, d.property_hs_is_closed_won
from fivetran_covertree.hubspot.deal as d inner join fivetran_covertree.hubspot.deal_contact as dc on dc.deal_id = d.deal_id
    inner join fivetran_covertree.hubspot.contact as c on c.id = dc.contact_id
where property_hs_is_closed_won = true and d.is_deleted = false and lower(d.property_carrier) != 'covertree'