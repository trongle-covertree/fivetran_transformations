{% macro run_referral_socotra_info_grouping() %}

select
    pr.community_manager_name,
    pr.community_name,
    pr.partner_name,
    ua.policy_locator,
    pi.first_name,
    pi.last_name,
    to_date(convert_timezone('America/New_York', to_timestamp_tz(qp.issued_timestamp/1000))) as issued_timestamp,
    to_date(convert_timezone('America/New_York', to_timestamp_tz(qp.created_timestamp/1000))) as created_timestamp,
    street_address,
    lot_unit,
    city,
    ua.state,
    zip_code,
    county,
    country,
    gross_premium,
    payment_schedule_name,
    model_year,
    home_type,
    iff(issued_timestamp is null, 'Initial-Quote', 'Policy-Activated') as status,
    case
            when right(pr.friendbuy_pk, 5) = 't6lOh' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGj' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGk' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGl' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGm' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGm' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGn' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGo' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGm' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGp' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGq' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGr' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGk' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't9qJP' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't9qJP' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGt' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGu' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGv' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGx' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't6lOi' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGz' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGl' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGA' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGl' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGn' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGn' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGB' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGC' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGD' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGn' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGn' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGF' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGE' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGD' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGG' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGE' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGE' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGE' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGF' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGF' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGn' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGD' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 'ukbX9' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't-Z9f' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 't4OGB' then 'AZ'
            when right(pr.friendbuy_pk, 5) = 'unt1r' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1s' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1v' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1w' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1x' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1z' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt18' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1_' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2b' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2d' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2e' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2h' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2d' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2l' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2m' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1w' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1u' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2n' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2p' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2r' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1t' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1u' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1y' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1-' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2a' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2c' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2a' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unuEh' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2g' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2j' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2k' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt1u' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'unt2g' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'ubZ-l' then 'OH'
            when right(pr.friendbuy_pk, 5) = 'ubZ-X' then 'OH'
            when right(pr.friendbuy_pk, 5) = 'ullNx' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ubZ-m' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ullNy' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'untWb' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'ullNA' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ubZ-D' then 'IL'
            when right(pr.friendbuy_pk, 5) = 'ullNB' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ullNC' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'untWc' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'ubZ-Z' then 'OH'
            when right(pr.friendbuy_pk, 5) = 'ullND' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ullNE' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ullNI' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'ullNA' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ullNF' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'ullNH' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ubZ-E' then 'IL'
            when right(pr.friendbuy_pk, 5) = 'ullNI' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'ubZ-q' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ubZ-G' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'ullNJ' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ubZ-J' then 'IL'
            when right(pr.friendbuy_pk, 5) = 'ullNM' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ubZ-r' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ullNF' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'ubZ-s' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ubZ-t' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ullNN' then 'TN'
            when right(pr.friendbuy_pk, 5) = 'ubZ-1' then 'OH'
            when right(pr.friendbuy_pk, 5) = 'ullNO' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ubZ-v' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ullNN' then 'TN'
            when right(pr.friendbuy_pk, 5) = 'ullNP' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ubZ-y' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ubZ-z' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ubZ-K' then 'IL'
            when right(pr.friendbuy_pk, 5) = 'ubZ-6' then 'OH'
            when right(pr.friendbuy_pk, 5) = 'ubZ-A' then 'MI'
            when right(pr.friendbuy_pk, 5) = 'ullNQ' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ullNS' then 'TN'
            when right(pr.friendbuy_pk, 5) = 'ullNE' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'ullNF' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'ullNU' then 'IN'
            when right(pr.friendbuy_pk, 5) = 'ubZ-O' then 'IL'
            when right(pr.friendbuy_pk, 5) = 'ullNV' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'ullNX' then 'TX'
            when right(pr.friendbuy_pk, 5) = 'untWd' then 'FL'
            when right(pr.friendbuy_pk, 5) = 'ukbX_' then 'IL'
            when right(pr.friendbuy_pk, 5) = 'ubZ-W' then 'IL'
            when right(pr.friendbuy_pk, 5) = 'ullNY' then 'TN'
            when right(pr.friendbuy_pk, 5) = 'ubZ--' then 'OH'
            else null
        end as community_state
from transformations_prod_socotra.quote_exposure_unit_address as ua
    left join (select sum(premium) as gross_premium, policy_locator from (select *, row_number() over (partition by policy_locator, peril_locator order by end_timestamp desc, start_timestamp desc) num from mysql_data_mart_10001.peril_characteristics) where num = 1 group by policy_locator) as pc on ua.policy_locator = pc.policy_locator
    join mysql_data_mart_10001.quote_policy as qp on ua.quote_policy_locator = qp.locator
    join transformations_dynamodb.prod_policies_referrals as pr on qp.policy_locator = pr.policy_locator
    left join transformations_prod_socotra.quote_exposure_unit_construction as uc on qp.locator = uc.quote_policy_locator
    left join transformations_prod_socotra.policyholder_info as pi on qp.policyholder_locator = pi.policyholder_locator
where qp.selected = true

{% endmacro %}
