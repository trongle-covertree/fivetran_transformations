{% macro run_socotra_claim_injured_party(socotra_db, sf_schema) %}

select
	max(case when field_name = 'injured_first_name' then field_value end) as injured_first_name,
	max(case when field_name = 'injured_last_name' then field_value end) as injured_last_name,
	max(case when field_name = 'injured_phonenumber' then field_value end) as injured_phonenumber,
	max(case when field_name = 'injured_street_address' then field_value end) as injured_street_address,
	max(case when field_name = 'injured_lot_unit#' then field_value end) as injured_lot_unit,
	max(case when field_name = 'injured_city' then field_value end) as injured_city,
	max(case when field_name = 'injured_state' then field_value end) as injured_state,
	max(case when field_name = 'injured_zip_code' then field_value end) as injured_zip_code,
	max(case when field_name = 'injured_county' then field_value end) as injured_county,
	max(case when field_name = 'injured_country' then field_value end) as injured_country,
	claim_locator,
    c.policy_locator::varchar as policy_locator,
	discarded,
	to_timestamp_tz(c.created_timestamp/1000) as created_timestamp,
	to_timestamp_tz(c.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(c.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.claim_fields as cf
	inner join {{ socotra_db }}.claim as c
		on cf.claim_locator = c.locator
	where parent_name = 'injured_party_info'
{% if is_incremental() %}
    and (to_timestamp_tz(c.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_claim_injured_party_info order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(c.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_claim_injured_party_info order by datamart_updated_timestamp desc limit 1)
	  or to_timestamp_tz(c.created_timestamp/1000) > (select created_timestamp from {{ sf_schema }}.policy_claim_injured_party_info order by created_timestamp desc limit 1))
{% endif %}
group by claim_locator, c.policy_locator, discarded, c.created_timestamp, c.datamart_created_timestamp, c.datamart_updated_timestamp
{% endmacro %}
