{% macro run_exposures_home_info(env, prefix) %}

{% set exposures_home_info_query %}
select exposures, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
  WHERE created_timestamp > (select policy_created_timestamp from {{ env }}.{{ prefix }}_policy_exposures_home_info order by created_timestamp desc limit 1)
      or updated_timestamp > (select policy_updated_timestamp from {{ env }}.{{ prefix }}_policy_exposures_home_info order by updated_timestamp desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(exposures_home_info_query) %}

{% if execute %}
    {% set exposures = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
    {% set created_timestamps = results.columns[2].values() %}
    {% set updated_timestamps = results.columns[3].values() %}
{% endif %}

{% if exposures|length > 0 %}
    {% if is_incremental() %}
        {% if pk|length == 1 %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_home_info where PK in {{ pk|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_home_info where PK in {{ pk }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}
SELECT Column1 AS ID, Column2 AS PK, Column3 AS ROOF_YEAR_YYYY, Column4 AS SKIRTING_TYPE, Column5 AS ROOF_SHAPE, Column6 AS ROOF_MATERIAL, Column7 AS HOME_HUD_NUMBER,
    Column8 AS TOTAL_SQUARE_FOOTAGE, Column9 AS HOME_TYPE, Column10 AS MODEL_YEAR, Column11 AS MANUFACTURER_NAME, Column12 AS ROOF_CONDITION, Column13 AS HOME_FIXTURES,
    Column14 AS INSPECTION_INTERIOR, Column15 AS INSPECTION_EXTERIOR, Column16 AS INSPECTION_AERIAL, to_timestamp(Column17) AS CREATED_TIMESTAMP,
    to_timestamp(Column18) AS UPDATED_TIMESTAMP, to_timestamp(Column19) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column20) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    {% for exposure in exposures %}
        {% set outer_loop = loop %}
        {% if exposure %}
            {% set exposure_arr = fromjson(exposure) %}
            {% for exposure_json in exposure_arr %}
                {% set exposure_json_loop = loop %}
                {% for char in exposure_json.characteristics if exposure_json.name != 'Policy Level Coverages' %}
                    {% set home_info_keys = { id: none, visitors_in_a_month: none, diving_board: none, please_describe: none, business_on_premises: none, business_employees_on_premises: none, trampoline_liability: none, source_of_heat_installation: none, property_with_fire_protection: none, thermo_static_control: none, type_of_fuel: none, unit_is_tied: none, source_of_heat: none, unrepaired_damages: none, four_feet_fence: none, daycare_on_premises: none, utility_services: none, wrought_iron: none, mortgage: none, secure_rails: none, business_description: none, trampoline_safety_net: none, swimming_pool: none, burglar_alarm: none, created_timestamp: none, updated_timestamp: none } %}
                    {% do home_info_keys.update({ 'created_timestamp': char.createdTimestamp }) %}
                    {% do home_info_keys.update({ 'updated_timestamp': char.updatedTimestamp }) %}
                    {% do home_info_keys.update({ 'id': char.locator })%}
                    {% for current_char_key in char.fieldGroupsByLocator.keys() %}
                        {% if 'roof_year_yyyy' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'roof_year_yyyy': char.fieldGroupsByLocator[current_char_key].roof_year_yyyy[0] }) %}
                        {% endif %}
                        {% if 'skirting_type' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'skirting_type': char.fieldGroupsByLocator[current_char_key].skirting_type[0] }) %}
                        {% endif %}
                        {% if 'roof_shape' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'roof_shape': char.fieldGroupsByLocator[current_char_key].roof_shape[0] }) %}
                        {% endif %}
                        {% if 'roof_material' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'roof_material': char.fieldGroupsByLocator[current_char_key].roof_material[0] }) %}
                        {% endif %}
                        {% if 'home_hud_number' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'home_hud_number': char.fieldGroupsByLocator[current_char_key].home_hud_number[0] }) %}
                        {% endif %}
                        {% if 'total_square_footage' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'total_square_footage': char.fieldGroupsByLocator[current_char_key].total_square_footage[0] }) %}
                        {% endif %}
                        {% if 'home_type' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'home_type': char.fieldGroupsByLocator[current_char_key].home_type[0] }) %}
                        {% endif %}
                        {% if 'model_year' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'model_year': char.fieldGroupsByLocator[current_char_key].model_year[0] }) %}
                        {% endif %}
                        {% if 'manufacturer_name' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'manufacturer_name': char.fieldGroupsByLocator[current_char_key].manufacturer_name[0] }) %}
                        {% endif %}
                        {% if 'roof_condition' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'roof_condition': char.fieldGroupsByLocator[current_char_key].roof_condition[0] }) %}
                        {% endif %}
                        {% if 'home_fixtures' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'home_fixtures': char.fieldGroupsByLocator[current_char_key].home_fixtures[0] }) %}
                        {% endif %}
                        {% if 'interior' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'interior': char.fieldGroupsByLocator[current_char_key].interior[0] }) %}
                        {% endif %}
                        {% if 'exterior' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'exterior': char.fieldGroupsByLocator[current_char_key].exterior[0] }) %}
                        {% endif %}
                        {% if 'aerial' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do home_info_keys.update({ 'aerial': char.fieldGroupsByLocator[current_char_key].aerial[0] }) %}
                        {% endif %}
                        {% if loop.last %}
    (
        {% if home_info_keys.id|length > 0 or home_info_keys is not none %}'{{ home_info_keys.id }}'{% else %}null{% endif %},
        '{{ pk[outer_loop.index0] }}',
        {% if home_info_keys.roof_year_yyyy|length > 0 %}'{{ home_info_keys.roof_year_yyyy }}'{% else %}null{% endif %},
        {% if home_info_keys.skirting_type|length > 0 %}'{{ home_info_keys.skirting_type }}'{% else %}null{% endif %},
        {% if home_info_keys.roof_shape|length > 0 %}'{{ home_info_keys.roof_shape }}'{% else %}null{% endif %},
        {% if home_info_keys.roof_material|length > 0 %}'{{ home_info_keys.roof_material }}'{% else %}null{% endif %},
        {% if home_info_keys.home_hud_number|length > 0 %}'{{ home_info_keys.home_hud_number }}'{% else %}null{% endif %},
        {% if home_info_keys.total_square_footage|length > 0 %}'{{ home_info_keys.total_square_footage }}'{% else %}null{% endif %},
        {% if home_info_keys.home_type|length > 0 %}'{{ home_info_keys.home_type }}'{% else %}null{% endif %},
        {% if home_info_keys.model_year|length > 0 %}'{{ home_info_keys.model_year }}'{% else %}null{% endif %},
        {% if home_info_keys.manufacturer_name|length > 0 %}'{{ home_info_keys.manufacturer_name }}'{% else %}null{% endif %},
        {% if home_info_keys.roof_condition|length > 0 %}'{{ home_info_keys.roof_condition }}'{% else %}null{% endif %},
        {% if home_info_keys.home_fixtures|length > 0 %}'{{ home_info_keys.home_fixtures }}'{% else %}null{% endif %},
        {% if home_info_keys.interior|length > 0 %}'{{ home_info_keys.interior }}'{% else %}null{% endif %},
        {% if home_info_keys.exterior|length > 0 %}'{{ home_info_keys.exterior }}'{% else %}null{% endif %},
        {% if home_info_keys.aerial|length > 0 %}'{{ home_info_keys.aerial }}'{% else %}null{% endif %},
        {% if home_info_keys.created_timestamp|length > 0 %}'{{ home_info_keys.created_timestamp }}'{% else %}null{% endif %},
        {% if home_info_keys.updated_timestamp|length > 0 %}'{{ home_info_keys.updated_timestamp }}'{% else %}null{% endif %},
        '{{ created_timestamps[outer_loop.index0] }}',
        '{{ updated_timestamps[outer_loop.index0] }}'
    ){% if not outer_loop.last %},{% endif %}
                        {% endif %}
                    {% endfor %}
                {% endfor %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% else %}
SELECT Column1 AS ID, Column2 AS PK, Column3 AS ROOF_YEAR_YYYY, Column4 AS SKIRTING_TYPE, Column5 AS ROOF_SHAPE, Column6 AS ROOF_MATERIAL, Column7 AS HOME_HUD_NUMBER,
    Column8 AS TOTAL_SQUARE_FOOTAGE, Column9 AS HOME_TYPE, Column10 AS MODEL_YEAR, Column11 AS MANUFACTURER_NAME, Column12 AS ROOF_CONDITION, Column13 AS HOME_FIXTURES,
    Column14 AS INSPECTION_INTERIOR, Column15 AS INSPECTION_EXTERIOR, Column16 AS INSPECTION_AERIAL, to_timestamp(Column17) AS CREATED_TIMESTAMP,
    to_timestamp(Column18) AS UPDATED_TIMESTAMP, to_timestamp(Column19) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column20) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    ('NO FIELDS', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
{% endif %}
{% endmacro %}