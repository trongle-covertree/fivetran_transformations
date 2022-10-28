{% macro run_exposures_extra_coverages(env, prefix) %}

{% set exposures_extra_coverages_query %}
select exposures, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
  WHERE created_timestamp > (select policy_created_timestamp from {{ env }}.{{ prefix }}_policy_exposures_extra_coverages order by created_timestamp desc limit 1)
      or updated_timestamp > (select policy_updated_timestamp from {{ env }}.{{ prefix }}_policy_exposures_extra_coverages order by updated_timestamp desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(exposures_extra_coverages_query) %}

{% if execute %}
    {% set exposures = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
    {% set created_timestamps = results.columns[2].values() %}
    {% set updated_timestamps = results.columns[3].values() %}
{% endif %}

{% if exposures|length > 0 %}
    {% if is_incremental() %}
        {% set delete_query %}
        DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_extra_coverages where PK in {{ pk }}
        {% endset %}
        {% do run_query(delete_query) %}
    {% endif %}
SELECT Column1 AS ID, Column2 AS PK, Column3 AS VISITORS_IN_A_MONTH, Column4 AS DIVING_BOARD, Column5 AS PLEASE_DESCRIBE, Column6 AS BUSINESS_ON_PREMISES, Column7 AS BUSINESS_EMPLOYEES_ON_PREMISES,
    Column8 AS TRAMPOLINE_LIABILITY, Column9 AS SOURCE_OF_HEAT_INSTALLATION, Column10 AS PROPERTY_WITH_FIRE_PROTECTION, Column11 AS THERMO_STATIC_CONTROL, Column12 AS TYPE_OF_FUEL, Column13 AS UNIT_IS_TIED,
    Column14 AS SOURCE_OF_HEAT, Column15 AS UNREPAIRED_DAMAGES, Column16 AS FOUR_FEET_FENCE, Column17 AS DAYCARE_ON_PREMISES, Column18 AS UTILITY_SERVICES, Column19 AS WROUGHT_IRON, Column20 AS MORTGAGE,
    Column21 AS SECURE_RAILS, Column22 AS BUSINESS_DESCRIPTION, Column23 AS TRAMPOLINE_SAFETY_NET, Column24 AS SWIMMING_POOL, Column25 AS BURGLAR_ALARM, to_timestamp(Column26) AS CREATED_TIMESTAMP,
    to_timestamp(Column27) AS UPDATED_TIMESTAMP, to_timestamp(Column28) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column29) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    {% for exposure in exposures %}
        {% set outer_loop = loop %}
        {% if exposure %}
            {% set exposure_arr = fromjson(exposure) %}
            {% for exposure_json in exposure_arr %}
                {% set exposure_json_loop = loop %}
                {% for char in exposure_json.characteristics if exposure_json.name != 'Policy Level Coverages' %}
                    {% set extra_cov_keys = { id: none, visitors_in_a_month: none, diving_board: none, please_describe: none, business_on_premises: none, business_employees_on_premises: none, trampoline_liability: none, source_of_heat_installation: none, property_with_fire_protection: none, thermo_static_control: none, type_of_fuel: none, unit_is_tied: none, source_of_heat: none, unrepaired_damages: none, four_feet_fence: none, daycare_on_premises: none, utility_services: none, wrought_iron: none, mortgage: none, secure_rails: none, business_description: none, trampoline_safety_net: none, swimming_pool: none, burglar_alarm: none, created_timestamp: none, updated_timestamp: none } %}
                    {% do extra_cov_keys.update({ 'created_timestamp': char.createdTimestamp }) %}
                    {% do extra_cov_keys.update({ 'updated_timestamp': char.updatedTimestamp }) %}
                    {% do extra_cov_keys.update({ 'id': char.locator })%}
                    {% for current_char_key in char.fieldGroupsByLocator.keys() %}
                        {% if 'visitors_in_a_month' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'visitors_in_a_month': char.fieldGroupsByLocator[current_char_key].visitors_in_a_month[0] }) %}
                        {% endif %}
                        {% if 'diving_board' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'diving_board': char.fieldGroupsByLocator[current_char_key].diving_board[0] }) %}
                        {% endif %}
                        {% if 'please_describe' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'please_describe': char.fieldGroupsByLocator[current_char_key].please_describe[0] }) %}
                        {% endif %}
                        {% if 'business_on_premises' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'business_on_premises': char.fieldGroupsByLocator[current_char_key].business_on_premises[0] }) %}
                        {% endif %}
                        {% if 'business_employees_on_premises' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'business_employees_on_premises': char.fieldGroupsByLocator[current_char_key].business_employees_on_premises[0] }) %}
                        {% endif %}
                        {% if 'trampoline_liability' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'trampoline_liability': char.fieldGroupsByLocator[current_char_key].trampoline_liability[0] }) %}
                        {% endif %}
                        {% if 'source_of_heat_installation' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'source_of_heat_installation': char.fieldGroupsByLocator[current_char_key].source_of_heat_installation[0] }) %}
                        {% endif %}
                        {% if 'property_with_fire_protection' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'property_with_fire_protection': char.fieldGroupsByLocator[current_char_key].property_with_fire_protection[0] }) %}
                        {% endif %}
                        {% if 'thermo_static_control' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'thermo_static_control': char.fieldGroupsByLocator[current_char_key].thermo_static_control[0] }) %}
                        {% endif %}
                        {% if 'type_of_fuel' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'type_of_fuel': char.fieldGroupsByLocator[current_char_key].type_of_fuel[0] }) %}
                        {% endif %}
                        {% if 'unit_is_tied' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'unit_is_tied': char.fieldGroupsByLocator[current_char_key].unit_is_tied[0] }) %}
                        {% endif %}
                        {% if 'source_of_heat' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'source_of_heat': char.fieldGroupsByLocator[current_char_key].source_of_heat[0] }) %}
                        {% endif %}
                        {% if 'unrepaired_damages' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'unrepaired_damages': char.fieldGroupsByLocator[current_char_key].unrepaired_damages[0] }) %}
                        {% endif %}
                        {% if 'four_feet_fence' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'four_feet_fence': char.fieldGroupsByLocator[current_char_key].four_feet_fence[0] }) %}
                        {% endif %}
                        {% if 'daycare_on_premises' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'daycare_on_premises': char.fieldGroupsByLocator[current_char_key].daycare_on_premises[0] }) %}
                        {% endif %}
                        {% if 'utility_services' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'utility_services': char.fieldGroupsByLocator[current_char_key].utility_services[0] }) %}
                        {% endif %}
                        {% if 'wrought_iron' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'wrought_iron': char.fieldGroupsByLocator[current_char_key].wrought_iron[0] }) %}
                        {% endif %}
                        {% if 'mortgage' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'mortgage': char.fieldGroupsByLocator[current_char_key].mortgage[0] }) %}
                        {% endif %}
                        {% if 'secure_rails' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'secure_rails': char.fieldGroupsByLocator[current_char_key].secure_rails[0] }) %}
                        {% endif %}
                        {% if 'business_description' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'business_description': char.fieldGroupsByLocator[current_char_key].business_description[0] }) %}
                        {% endif %}
                        {% if 'trampoline_safety_net' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'trampoline_safety_net': char.fieldGroupsByLocator[current_char_key].trampoline_safety_net[0] }) %}
                        {% endif %}
                        {% if 'swimming_pool' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'swimming_pool': char.fieldGroupsByLocator[current_char_key].swimming_pool[0] }) %}
                        {% endif %}
                        {% if 'burglar_alarm' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do extra_cov_keys.update({ 'burglar_alarm': char.fieldGroupsByLocator[current_char_key].burglar_alarm[0] }) %}
                        {% endif %}
                        {% if extra_cov_keys.business_on_premises|length > 0 %}
    (
        {% if extra_cov_keys.id|length > 0 or extra_cov_keys is not none %}'{{ extra_cov_keys.id }}'{% else %}null{% endif %},
        '{{ pk[outer_loop.index0] }}',
        {% if extra_cov_keys.visitors_in_a_month|length > 0 %}'{{ extra_cov_keys.visitors_in_a_month }}'{% else %}null{% endif %},
        {% if extra_cov_keys.diving_board|length > 0 %}'{{ extra_cov_keys.diving_board }}'{% else %}null{% endif %},
        {% if extra_cov_keys.please_describe|length > 0 %}'{{ extra_cov_keys.please_describe }}'{% else %}null{% endif %},
        {% if extra_cov_keys.business_on_premises|length > 0 %}'{{ extra_cov_keys.business_on_premises }}'{% else %}null{% endif %},
        {% if extra_cov_keys.business_employees_on_premises|length > 0 %}'{{ extra_cov_keys.business_employees_on_premises }}'{% else %}null{% endif %},
        {% if extra_cov_keys.trampoline_liability|length > 0 %}'{{ extra_cov_keys.trampoline_liability }}'{% else %}null{% endif %},
        {% if extra_cov_keys.source_of_heat_installation|length > 0 %}'{{ extra_cov_keys.source_of_heat_installation }}'{% else %}null{% endif %},
        {% if extra_cov_keys.property_with_fire_protection|length > 0 %}'{{ extra_cov_keys.property_with_fire_protection }}'{% else %}null{% endif %},
        {% if extra_cov_keys.thermo_static_control|length > 0 %}'{{ extra_cov_keys.thermo_static_control }}'{% else %}null{% endif %},
        {% if extra_cov_keys.type_of_fuel|length > 0 %}'{{ extra_cov_keys.type_of_fuel }}'{% else %}null{% endif %},
        {% if extra_cov_keys.unit_is_tied|length > 0 %}'{{ extra_cov_keys.unit_is_tied }}'{% else %}null{% endif %},
        {% if extra_cov_keys.source_of_heat|length > 0 %}'{{ extra_cov_keys.source_of_heat }}'{% else %}null{% endif %},
        {% if extra_cov_keys.unrepaired_damages|length > 0 %}'{{ extra_cov_keys.unrepaired_damages }}'{% else %}null{% endif %},
        {% if extra_cov_keys.four_feet_fence|length > 0 %}'{{ extra_cov_keys.four_feet_fence }}'{% else %}null{% endif %},
        {% if extra_cov_keys.daycare_on_premises|length > 0 %}'{{ extra_cov_keys.daycare_on_premises }}'{% else %}null{% endif %},
        {% if extra_cov_keys.utility_services|length > 0 %}'{{ extra_cov_keys.utility_services }}'{% else %}null{% endif %},
        {% if extra_cov_keys.wrought_iron|length > 0 %}'{{ extra_cov_keys.wrought_iron }}'{% else %}null{% endif %},
        {% if extra_cov_keys.mortgage|length > 0 %}'{{ extra_cov_keys.mortgage }}'{% else %}null{% endif %},
        {% if extra_cov_keys.secure_rails|length > 0 %}'{{ extra_cov_keys.secure_rails }}'{% else %}null{% endif %},
        {% if extra_cov_keys.business_description|length > 0 %}'{{ extra_cov_keys.business_description }}'{% else %}null{% endif %},
        {% if extra_cov_keys.trampoline_safety_net|length > 0 %}'{{ extra_cov_keys.trampoline_safety_net }}'{% else %}null{% endif %},
        {% if extra_cov_keys.burglar_alarm|length > 0 %}'{{ extra_cov_keys.burglar_alarm }}'{% else %}null{% endif %},
        {% if extra_cov_keys.unrepaired_damages|length > 0 %}'{{ extra_cov_keys.unrepaired_damages }}'{% else %}null{% endif %},
        {% if extra_cov_keys.created_timestamp|length > 0 %}'{{ extra_cov_keys.created_timestamp }}'{% else %}null{% endif %},
        {% if extra_cov_keys.updated_timestamp|length > 0 %}'{{ extra_cov_keys.updated_timestamp }}'{% else %}null{% endif %},
        '{{ created_timestamps[outer_loop.index0] }}',
        '{{ updated_timestamps[outer_loop.index0] }}'
    ){% if not outer_loop.last or (outer_loop.last and exposure_json_loop.index0 != exposure_arr|length - 2) %},{% endif %}
                        {% do extra_cov_keys.update({ 'business_on_premises': '' }) %}
                        {% endif %}
                    {% endfor %}
                {% endfor %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endif %}
{% endmacro %}