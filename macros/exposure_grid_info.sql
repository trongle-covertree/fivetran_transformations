{% macro run_exposures_grid_info(env, prefix) %}

{% set exposures_grid_info_query %}
select exposures, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
  WHERE (created_timestamp > (select policy_created_timestamp from {{ env }}.{{ prefix }}_policy_exposures_grid_info order by created_timestamp desc limit 1)
      or updated_timestamp > (select policy_updated_timestamp from {{ env }}.{{ prefix }}_policy_exposures_grid_info order by updated_timestamp desc limit 1))
      or pk not in (select pk from {{ env }}.{{ prefix }}_policy_exposures_grid_info) and array_size(exposures) != 0
{% endif %}
{% endset %}

{% set results = run_query(exposures_grid_info_query) %}

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
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_grid_info where PK in {{ pk|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_grid_info where PK in {{ pk }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}
SELECT Column1 AS ID, Column2 AS PK, Column3 AS LOC_CODE, Column4 AS GRID_ID, Column5 AS TERRITORY_CODE_FLOODS, Column6 AS TERRITORY_CODE_AOP, Column7 AS UW_ADMITTED,
    Column8 AS LAT, Column9 AS LONG, Column10 AS TERRITORY_CODE_HURRICANE, Column11 AS TERRITORY_CODE_WILD_FIRE, Column12 AS TERRITORY_CODE_WINDHAIL, Column13 AS TERRITORY_CODE_EARTHQUAKE,
    to_timestamp(Column14) AS CREATED_TIMESTAMP, to_timestamp(Column15) AS UPDATED_TIMESTAMP, to_timestamp(Column16) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column17) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    {% for exposure in exposures %}
        {% set outer_loop = loop %}
        {% if exposure %}
            {% set exposure_arr = fromjson(exposure) %}
            {% for exposure_json in exposure_arr %}
                {% set exposure_json_loop = loop %}
                {% for char in exposure_json.characteristics if exposure_json.name != 'Policy Level Coverages' %}
                    {% set grid_info_keys = { id: none, loc_code: none, grid_id: none, please_describe: none, territory_code_floods: none, territory_code_aop: none, uw_admitted: none, lat: none, long: none, territory_code_hurricane: none, territory_code_wild_fire: none, territory_code_windhail: none, territory_code_earthquake: none, created_timestamp: none, updated_timestamp: none } %}
                    {% do grid_info_keys.update({ 'created_timestamp': char.createdTimestamp }) %}
                    {% do grid_info_keys.update({ 'updated_timestamp': char.updatedTimestamp }) %}
                    {% do grid_info_keys.update({ 'id': char.locator })%}
                    {% for current_char_key in char.fieldGroupsByLocator.keys() %}
                        {% if 'loc_code' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'loc_code': char.fieldGroupsByLocator[current_char_key].loc_code[0] }) %}
                        {% endif %}
                        {% if 'grid_id' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'grid_id': char.fieldGroupsByLocator[current_char_key].grid_id[0] }) %}
                        {% endif %}
                        {% if 'territory_code_floods' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'territory_code_floods': char.fieldGroupsByLocator[current_char_key].territory_code_floods[0] }) %}
                        {% endif %}
                        {% if 'territory_code_aop' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'territory_code_aop': char.fieldGroupsByLocator[current_char_key].territory_code_aop[0] }) %}
                        {% endif %}
                        {% if 'uw_admitted' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'uw_admitted': char.fieldGroupsByLocator[current_char_key].uw_admitted[0] }) %}
                        {% endif %}
                        {% if 'lat' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'lat': char.fieldGroupsByLocator[current_char_key].lat[0] }) %}
                        {% endif %}
                        {% if 'long' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'long': char.fieldGroupsByLocator[current_char_key].long[0] }) %}
                        {% endif %}
                        {% if 'territory_code_hurricane' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'territory_code_hurricane': char.fieldGroupsByLocator[current_char_key].territory_code_hurricane[0] }) %}
                        {% endif %}
                        {% if 'territory_code_wild_fire' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'territory_code_wild_fire': char.fieldGroupsByLocator[current_char_key].territory_code_wild_fire[0] }) %}
                        {% endif %}
                        {% if 'territory_code_windhail' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'territory_code_windhail': char.fieldGroupsByLocator[current_char_key].territory_code_windhail[0] }) %}
                        {% endif %}
                        {% if 'territory_code_earthquake' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do grid_info_keys.update({ 'territory_code_earthquake': char.fieldGroupsByLocator[current_char_key].territory_code_earthquake[0] }) %}
                        {% endif %}
                        {% if loop.last %}
    (
        {% if grid_info_keys.id|length > 0 or grid_info_keys is not none %}'{{ grid_info_keys.id }}'{% else %}null{% endif %},
        '{{ pk[outer_loop.index0] }}',
        {% if grid_info_keys.loc_code|length > 0 %}'{{ grid_info_keys.loc_code }}'{% else %}null{% endif %},
        {% if grid_info_keys.grid_id|length > 0 %}'{{ grid_info_keys.grid_id }}'{% else %}null{% endif %},
        {% if grid_info_keys.territory_code_floods|length > 0 %}'{{ grid_info_keys.territory_code_floods }}'{% else %}null{% endif %},
        {% if grid_info_keys.territory_code_aop|length > 0 %}'{{ grid_info_keys.territory_code_aop }}'{% else %}null{% endif %},
        {% if grid_info_keys.uw_admitted|length > 0 %}'{{ grid_info_keys.uw_admitted }}'{% else %}null{% endif %},
        {% if grid_info_keys.lat|length > 0 %}'{{ grid_info_keys.lat }}'{% else %}null{% endif %},
        {% if grid_info_keys.long|length > 0 %}'{{ grid_info_keys.long }}'{% else %}null{% endif %},
        {% if grid_info_keys.territory_code_hurricane|length > 0 %}'{{ grid_info_keys.territory_code_hurricane }}'{% else %}null{% endif %},
        {% if grid_info_keys.territory_code_wild_fire|length > 0 %}'{{ grid_info_keys.territory_code_wild_fire }}'{% else %}null{% endif %},
        {% if grid_info_keys.territory_code_windhail|length > 0 %}'{{ grid_info_keys.territory_code_windhail }}'{% else %}null{% endif %},
        {% if grid_info_keys.territory_code_earthquake|length > 0 %}'{{ grid_info_keys.territory_code_earthquake }}'{% else %}null{% endif %},
        {% if grid_info_keys.created_timestamp|length > 0 %}'{{ grid_info_keys.created_timestamp }}'{% else %}null{% endif %},
        {% if grid_info_keys.updated_timestamp|length > 0 %}'{{ grid_info_keys.updated_timestamp }}'{% else %}null{% endif %},
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
SELECT Column1 AS ID, Column2 AS PK, Column3 AS LOC_CODE, Column4 AS GRID_ID, Column5 AS TERRITORY_CODE_FLOODS, Column6 AS TERRITORY_CODE_AOP, Column7 AS UW_ADMITTED,
    Column8 AS LAT, Column9 AS LONG, Column10 AS TERRITORY_CODE_HURRICANE, Column11 AS TERRITORY_CODE_WILD_FIRE, Column12 AS TERRITORY_CODE_WINDHAIL, Column13 AS TERRITORY_CODE_EARTHQUAKE,
    to_timestamp(Column14) AS CREATED_TIMESTAMP, to_timestamp(Column15) AS UPDATED_TIMESTAMP, to_timestamp(Column16) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column17) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    ('NO FIELDS', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null) limit 0
{% endif %}
{% endmacro %}