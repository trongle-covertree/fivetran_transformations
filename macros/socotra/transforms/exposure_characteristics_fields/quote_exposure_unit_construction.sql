{% macro run_socotra_quote_exposure_unit_construction(socotra_db, sf_schema) %}

select
	min(case when field_name = 'roof_year_yyyy' then field_value::int end) as roof_year_yyyy,
	min(case when field_name = 'roof_shape' then field_value end) as roof_shape,
	min(case when field_name = 'roof_material' then field_value end) as roof_material,
	min(case when field_name = 'roof_rating_image_link' then field_value end) as roof_rating_image_link,
	min(case when field_name = 'total_square_footage' then field_value::int end) as total_square_footage,
	min(case when field_name = 'home_type' then field_value end) as home_type,
	min(case when field_name = 'model_year' then field_value::int end) as model_year,
	min(case when field_name = 'manufacturer_name' then field_value end) as manufacturer_name,
	min(case when field_name = 'roof_condition' then field_value end) as roof_condition,
	min(case when field_name = 'home_fixtures' then field_value end) as home_fixtures,
	min(case when field_name = 'home_hud_number' then field_value end) as home_hud_number,
	min(case when field_name = 'skirting_type' then field_value end) as skirting_type,
	min(case when field_name = 'storm_mitigation_shutters' then field_value::boolean end) as storm_mitigation_shutters,
	min(case when field_name = 'storm_mitigation_impactglass' then field_value::boolean end) as storm_mitigation_impactglass,
	quote_exposure_locator,
	quote_exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	to_timestamp_tz(ecf.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ecf.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.quote_exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.quote_exposure_characteristics as ec
		on ec.locator = ecf.quote_exposure_characteristics_locator
	where parent_name = 'unit_construction'
{% if is_incremental() %}
    and (to_timestamp_tz(ecf.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.quote_policy_exposure_unit_construction order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ecf.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.quote_policy_exposure_unit_construction order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by quote_exposure_locator, ecf.quote_exposure_characteristics_locator, ecf.datamart_created_timestamp, ecf.datamart_updated_timestamp, ec.policy_locator
{% endmacro %}
