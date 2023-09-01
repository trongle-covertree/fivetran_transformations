{% macro run_socotra_quote_exposure_usage(socotra_db, sf_schema) %}

select
	max(case when field_name = 'policy_usage' then field_value end) as policy_usage,
	max(case when field_name = 'vacancy_reason' then field_value end) as vacancy_reason,
	max(case when field_name = 'short_term_rental_surcharge' then field_value::boolean end) as short_term_rental_surcharge,
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
	quote_policy_locator,
    ec.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name = 'usage'
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.quote_policy_locator, ec.policy_locator
{% endmacro %}
