{% macro run_peril_modifications(dynamodb_env, dynamodb_prefix, socotra_env ) %}

{% set modification_query %}
select exposure_modifications, right(pk, 9) as policy_locator, locator, policy_created_timestamp, policy_updated_timestamp, name, new_policy_characteristics_locator, new_policy_characteristics_locators
from {{ dynamodb_env }}.{{ dynamodb_prefix }}_policy_modifications
{% if is_incremental() %}
    WHERE (policy_created_timestamp > (select POLICY_CREATED_TIMESTAMP from {{ socotra_env }}.peril_modifications order by POLICY_CREATED_TIMESTAMP desc limit 1)
      or policy_updated_timestamp > (select POLICY_UPDATED_TIMESTAMP from {{ socotra_env }}.peril_modifications order by POLICY_UPDATED_TIMESTAMP desc limit 1))
    or locator not in (select distinct modification_locator from {{ socotra_env }}.peril_modifications) and array_size(exposure_modifications) != 0
{% endif %}
{% endset %}

{% set results = run_query(modification_query) %}

{% if execute %}
    {% set modifications = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
    {% set mod_locators = results.columns[2].values() %}
    {% set created_timestamps = results.columns[3].values() %}
    {% set updated_timestamps = results.columns[4].values() %}
    {% set names = results.columns[5].values() %}
    {% set new_policy_characteristics_locator = results.columns[6].values() %}
    {% set new_policy_characteristics_locators = results.columns[7].values() %}
{% endif %}

{% if modifications|length > 0 %}
    {% if is_incremental() %}
        {% if pk|length == 1 %}
            {% set delete_query %}
            DELETE FROM {{ socotra_env }}.peril_modifications where locator in {{ mod_locators|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ socotra_env }}.peril_modifications where locator in {{ mod_locators }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}
    select Column1 as policy_locator, Column2 as modification_locator, Column3 as modification_name, Column4 as new_policy_characteristics_locator,
        parse_json(Column5) as new_policy_characteristics_locators, Column6 as exposure_modification_locator, Column7 as exposure_characteristic_locator,
        Column8 as peril_modification_locator, Column9 as peril_locator, Column10 as peril_characteristic_locator, Column11 as peril_premium_change,
        Column12 as peril_premium_currency, to_timestamp(Column13) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column14) AS POLICY_UPDATED_TIMESTAMP FROM VALUES
    {% for modification in modifications %}
        {% set outer_loop = loop %}
        {% if modification %}
            {% set mod_arr = fromjson(modification) %}
            {% for mod_json in mod_arr %}
                {% for exposure_mod in mod_json.perilModifications %}
    {# {{ log(modification, info=True) }} #}
    (
        '{{ pk[outer_loop.index0] }}',
        '{{ mod_locators[outer_loop.index0]}}',
        '{{ names[outer_loop.index0] }}',
        '{{ new_policy_characteristics_locator[outer_loop.index0] }}',
        '{{ new_policy_characteristics_locators[outer_loop.index0] }}',
        {% if 'locator' in mod_json  %}'{{ mod_json['locator'] }}'{% else %}null{% endif %},
        {% if 'newExposureCharacteristicsLocator' in mod_json  %}'{{ mod_json['newExposureCharacteristicsLocator'] }}'{% else %}null{% endif %},
        {% if 'locator' in exposure_mod %}'{{ exposure_mod['locator'] }}'{% else %}null{% endif %},
        {% if 'perilLocator' in exposure_mod %}'{{ exposure_mod['perilLocator'] }}'{% else %}null{% endif %},
        {% if 'newPerilCharacteristicsLocator' in exposure_mod %}'{{ exposure_mod['newPerilCharacteristicsLocator'] }}'{% else %}null{% endif %},
        {% if 'premiumChange' in exposure_mod %}'{{ exposure_mod['premiumChange'] }}'{% else %}null{% endif %},
        {% if 'premiumChangeCurrency' in exposure_mod %}'{{ exposure_mod['premiumChangeCurrency'] }}'{% else %}null{% endif %},
        '{{ created_timestamps[outer_loop.index0] }}',
        '{{ updated_timestamps[outer_loop.index0] }}'
    ){% if not outer_loop.last or not loop.last %},{% endif %}
            {% endfor %}
        {% endfor %}
        {% endif %}
    {% endfor %}
{% else %}
    SELECT Column1 as policy_locator, Column2 as modification_locator, Column3 as modification_name, Column4 as new_policy_characteristics_locator,
        parse_json(Column5) as new_policy_characteristics_locators, Column6 as exposure_modification_locator, Column7 as exposure_characteristic_locator,
        Column8 as peril_modification_locator, Column9 as peril_locator, Column10 as peril_characteristic_locator, Column11 as peril_premium_change,
        Column12 as peril_premium_currency, to_timestamp(Column13) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column14) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    (null, null, null, null, null, null, null, null, null, null, null, null, null, null) limit 0
{% endif %}
{% endmacro %}