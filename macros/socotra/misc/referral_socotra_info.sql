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
    home_type 
from transformations_prod_socotra.quote_exposure_unit_address as ua
    left join (select sum(premium) as gross_premium, policy_locator from mysql_data_mart_10001.peril_characteristics group by policy_locator) as pc on ua.policy_locator = pc.policy_locator
    join mysql_data_mart_10001.quote_policy as qp on ua.quote_policy_locator = qp.locator
    join transformations_dynamodb.prod_policies_referrals as pr on qp.policy_locator = pr.policy_locator
    left join transformations_prod_socotra.quote_exposure_unit_construction as uc on qp.locator = uc.quote_policy_locator
    left join transformations_prod_socotra.policyholder_info as pi on qp.policyholder_locator = pi.policyholder_locator
where qp.selected = true

{% endmacro %}
