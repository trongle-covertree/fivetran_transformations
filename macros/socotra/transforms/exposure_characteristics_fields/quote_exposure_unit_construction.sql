{% macro run_socotra_quote_exposure_unit_construction(socotra_db, sf_schema) %}

select
	max(case when field_name = 'roof_year_yyyy' then field_value::int end) as roof_year_yyyy,
	max(case when field_name = 'roof_shape' then field_value end) as roof_shape,
	max(case when field_name = 'roof_material' then field_value end) as roof_material,
	max(case when field_name = 'roof_rating_image_link' then field_value end) as roof_rating_image_link,
	max(case when field_name = 'total_square_footage' then field_value::int end) as total_square_footage,
	max(case when field_name = 'home_type' then field_value end) as home_type,
	max(case when field_name = 'model_year' then field_value::int end) as model_year,
	max(case when field_name = 'manufacturer_name' then field_value end) as manufacturer_name,
	max(case when field_name = 'roof_condition' then field_value end) as roof_condition,
	max(case when field_name = 'home_fixtures' then field_value end) as home_fixtures,
	max(case when field_name = 'home_hud_number' then field_value end) as home_hud_number,
	max(case when field_name = 'skirting_type' then field_value end) as skirting_type,
	max(case when field_name = 'storm_mitigation_shutters' then field_value::boolean end) as storm_mitigation_shutters,
	max(case when field_name = 'storm_mitigation_impactglass' then field_value::boolean end) as storm_mitigation_impactglass,
	max(case when field_name = 'storm_mitigation_fortified' then field_value end) as storm_mitigation_fortified,
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
	quote_policy_locator,
    ec.policy_locator::varchar as policy_locator,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_created_timestamp/1000)) as datamart_created_timestamp,
	convert_timezone('America/New_York', to_timestamp_tz(ec.datamart_updated_timestamp/1000)) as datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name = 'unit_construction'
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.quote_policy_locator, ec.policy_locator
{% endmacro %}
