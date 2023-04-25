{% macro run_socotra_exposure_unit_construction(socotra_db, sf_schema) %}

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
	exposure_locator,
	ecf.exposure_characteristics_locator,
    ec.policy_locator::varchar as policy_locator,
	to_timestamp_tz(ec.start_timestamp/1000) as start_timestamp,
    to_timestamp_tz(ec.end_timestamp/1000) as end_timestamp,
	to_timestamp_tz(ec.datamart_created_timestamp/1000) as datamart_created_timestamp,
	to_timestamp_tz(ec.datamart_updated_timestamp/1000) as datamart_updated_timestamp
from  {{ socotra_db }}.exposure_characteristics_fields as ecf
	inner join {{ socotra_db }}.exposure_characteristics as ec
		on ec.locator = ecf.exposure_characteristics_locator
	where parent_name = 'unit_construction'
{% if is_incremental() %}
    and (to_timestamp_tz(ec.datamart_created_timestamp/1000) > (select datamart_created_timestamp from {{ sf_schema }}.policy_exposure_unit_construction order by datamart_created_timestamp desc limit 1)
      or to_timestamp_tz(ec.datamart_updated_timestamp/1000) > (select datamart_updated_timestamp from {{ sf_schema }}.policy_exposure_unit_construction order by datamart_updated_timestamp desc limit 1))
{% endif %}
group by exposure_locator, ecf.exposure_characteristics_locator, ec.datamart_created_timestamp, ec.datamart_updated_timestamp, ec.policy_locator, ec.start_timestamp, ec.end_timestamp
{% endmacro %}
