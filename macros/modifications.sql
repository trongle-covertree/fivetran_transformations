{% macro run_modifications(env, prefix ) %}

{% set modification_query %}
select modifications, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
    WHERE created_timestamp > (select POLICY_CREATED_TIMESTAMP from {{ env }}.{{ prefix }}_policy_modifications order by POLICY_CREATED_TIMESTAMP desc limit 1)
      or updated_timestamp > (select POLICY_UPDATED_TIMESTAMP from {{ env }}.{{ prefix }}_policy_modifications order by POLICY_UPDATED_TIMESTAMP desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(modification_query) %}

{% if execute %}
    {% set modifications = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
    {% set created_timestamps = results.columns[2].values() %}
    {% set updated_timestamps = results.columns[3].values() %}
{% endif %}

{% if modifications|length > 0 %}
    {% if is_incremental() %}
        {% set delete_query %}
        DELETE FROM {{ env }}.{{ prefix }}_policy_modifications where PK in {{ pk }}
        {% endset %}
        {% do run_query(delete_query) %}
    {% endif %}
    SELECT Column1 as ID, Column2 as PK, Column3 as AUTOMATED_UNDERWRITING_RESULT_DECISION, to_timestamp(Column4) as AUTOMATED_UNDERWRITING_RESULT_DECISION_TIMESTAMP,
            parse_json(Column5) as AUTOMATED_UNDERWRITING_RESULT_NOTES, Column6 as CONFIG_VERSION, to_timestamp(Column7) as CREATED_TIMESTAMP, Column8 as DISPLAY_ID, to_timestamp(Column9) as EFFECTIVE_TIMESTAMP,
            parse_json(Column10) as EXPOSURE_MODIFICATIONS, parse_json(Column11) as FIELD_GROUPS_BY_LOCATOR, parse_json(Column12) as FIELD_VALUES, to_timestamp(Column13) as ISSUED_TIMESTAMP, Column14 as LOCATOR,
            parse_json(Column15) as MEDIA_BY_LOCATOR, Column16 as NAME, Column17 as NEW_POLICY_CHARACTERISTICS_LOCATOR, parse_json(Column18) as NEW_POLICY_CHARACTERISTICS_LOCATORS, Column19 as NUMBER,
            to_timestamp(Column20) as POLICY_END_TIMESTAMP, Column21 as POLICY_LOCATOR, Column22 as POLICYHOLDER_LOCATOR, Column23 as PREMIUM_CHANGE, Column24 as PREMIUM_CHANGE_CURRENCY,
            Column25 as PRODUCT_LOCATOR, to_timestamp(Column26) as UPDATED_TIMESTAMP, to_timestamp(Column27) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column28) AS POLICY_UPDATED_TIMESTAMP FROM VALUES
    {% for modification in modifications %}
        {% set outer_loop = loop %}
        {% if modification %}
            {% for mod_json in fromjson(modification) %}
    {# {{ log(mod, info=True) }} #}
    (
        '{{ pk[outer_loop.index0] + '-' + mod_json.displayId }}',
        '{{ pk[outer_loop.index0] }}',
        {% if 'automatedUnderwritingResult' in mod_json and 'decision' in mod_json['automatedUnderwritingResult'] %}'{{ mod_json['automatedUnderwritingResult']['decision'] }}'{% else %}null{% endif %},
        {% if 'automatedUnderwritingResult' in mod_json and 'decisionTimestamp' in mod_json['automatedUnderwritingResult'] %}'{{ mod_json['automatedUnderwritingResult']['decisionTimestamp'] }}'{% else %}null{% endif %},
        {% if 'automatedUnderwritingResult' in mod_json and 'notes' in mod_json['automatedUnderwritingResult'] %}'{{ tojson(mod_json['automatedUnderwritingResult']['notes']) | replace("\\n", " ") }}'{% else %}null{% endif %},
        '{{ mod_json.configVersion or null }}',
        '{{ mod_json.createdTimestamp or null }}',
        '{{ mod_json.displayId or null }}',
        '{{ mod_json.effectiveTimestamp or null }}',
        '{{ tojson(mod_json.exposureModifications) or null }}',
        '{{ tojson(mod_json.fieldGroupsByLocator) or null }}',
        '{{ tojson(mod_json.fieldValues) or null }}',
        {% if mod_json.issuedTimestamp|length > 0%}'{{ mod_json.issuedTimestamp }}'{% else %}null{% endif %},
        '{{ mod_json.locator or null }}',
        '{{ tojson(mod_json.mediaByLocator) or null }}',
        '{{ mod_json.name or null }}',
        '{{ mod_json.newPolicyCharacteristicsLocator or null }}',
        '{{ tojson(mod_json.newPolicyCharacteristicsLocators) or null }}',
        '{{ mod_json.number or null }}',
        {% if mod_json.policyEndTimestamp|length > 0%}'{{ mod_json.policyEndTimestamp }}'{% else %}null{% endif %},
        '{{ mod_json.policyLocator or null }}',
        '{{ mod_json.policyholderLocator or null }}',
        '{{ mod_json.premiumChange or null }}',
        '{{ mod_json.premiumChangeCurrency or null }}',
        '{{ mod_json.productLocator or null }}',
        '{{ mod_json.updatedTimestamp or null }}',
        '{{ created_timestamps[outer_loop.index0] }}',
        '{{ updated_timestamps[outer_loop.index0] }}'
    ){% if not outer_loop.last or not loop.last %},{% endif %}
        {% endfor %}
        {% endif %}
    {% endfor %}
{% else %}
    SELECT Column1 as ID, Column2 as PK, Column3 as AUTOMATED_UNDERWRITING_RESULT_DECISION, to_timestamp(Column4) as AUTOMATED_UNDERWRITING_RESULT_DECISION_TIMESTAMP,
            parse_json(Column5) as AUTOMATED_UNDERWRITING_RESULT_NOTES, Column6 as CONFIG_VERSION, to_timestamp(Column7) as CREATED_TIMESTAMP, Column8 as DISPLAY_ID, to_timestamp(Column9) as EFFECTIVE_TIMESTAMP,
            parse_json(Column10) as EXPOSURE_MODIFICATIONS, parse_json(Column11) as FIELD_GROUPS_BY_LOCATOR, parse_json(Column12) as FIELD_VALUES, to_timestamp(Column13) as ISSUED_TIMESTAMP, Column14 as LOCATOR,
            parse_json(Column15) as MEDIA_BY_LOCATOR, Column16 as NAME, Column17 as NEW_POLICY_CHARACTERISTICS_LOCATOR, parse_json(Column18) as NEW_POLICY_CHARACTERISTICS_LOCATORS, Column19 as NUMBER,
            to_timestamp(Column20) as POLICY_END_TIMESTAMP, Column21 as POLICY_LOCATOR, Column22 as POLICYHOLDER_LOCATOR, Column23 as PREMIUM_CHANGE, Column24 as PREMIUM_CHANGE_CURRENCY,
            Column25 as PRODUCT_LOCATOR, to_timestamp(Column26) as UPDATED_TIMESTAMP, to_timestamp(Column27) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column28) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    ('NO FIELDS', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
{% endif %}
{% endmacro %}