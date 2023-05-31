{% macro run_socotra_quote_peril_policy_level_chars(socotra_db, sf_schema) %}

select
	max(Case when field_name = 'spp_value' then regexp_replace(field_value, '[$,]')::int end) spp_value,
	max(Case when field_name = 'spp_type' then field_value end) spp_type,
	max(Case when field_name = 'spp_desc' then field_value End) spp_desc,
	max(Case when field_name = 'identity_fraud_limit' then regexp_replace(field_value, '[$,]')::int end) identity_fraud_limit,
	quote_exposure_locator,
	pc.quote_policy_locator,
	pc.policy_locator::varchar as policy_locator,
	to_timestamp_tz(pc.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(pc.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from {{ socotra_db }}.quote_peril_characteristics_fields as pcf
	inner join {{ socotra_db }}.quote_peril_characteristics as pc
		on pc.locator = pcf.quote_peril_characteristics_locator
	inner join {{ socotra_db }}.quote_peril as p
		on p.locator = pc.quote_peril_locator
where p.name in ('Scheduled Personal Property', 'Policy Minimum Premium Coverage', 'Identity Fraud Expense')
{% if is_incremental() %}
    and (to_timestamp_tz(pc.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_peril_general_characteristics order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(pc.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_peril_general_characteristics order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by quote_exposure_locator, pc.datamart_created_timestamp, pc.datamart_updated_timestamp, pc.quote_policy_locator, pc.policy_locator
{% endmacro %}
