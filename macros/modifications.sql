{% macro run_modifications(env, prefix ) %}

{% set modification_query %}
select modifications, pk
from {{ env }}.{{ prefix }}_policies_policy
{% endset %}

{% set results = run_query(modification_query) %}

{% if execute %}
    {% set modifications = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
{% endif %}


SELECT Column1 as ID, Column2 as PK, Column3 as AUTOMATED_UNDERWRITING_RESULT_DECISION, to_timestamp(Column4) as AUTOMATED_UNDERWRITING_RESULT_DECISION_TIMESTAMP,
        parse_json(Column5) as AUTOMATED_UNDERWRITING_RESULT_NOTES, Column6 as CONFIG_VERSION, to_timestamp(Column7) as CREATED_TIMESTAMP, Column8 as DISPLAY_ID, to_timestamp(Column9) as EFFECTIVE_TIMESTAMP,
        parse_json(Column10) as EXPOSURE_MODIFICATIONS, parse_json(Column11) as FIELD_GROUPS_BY_LOCATOR, parse_json(Column12) as FIELD_VALUES, to_timestamp(Column13) as ISSUED_TIMESTAMP, Column14 as LOCATOR,
        parse_json(Column15) as MEDIA_BY_LOCATOR, Column16 as NAME, Column17 as NEW_POLICY_CHARACTERISTICS_LOCATOR, parse_json(Column18) as NEW_POLICY_CHARACTERISTICS_LOCATORS, Column19 as NUMBER,
        to_timestamp(Column20) as POLICY_END_TIMESTAMP, Column21 as POLICY_LOCATOR, Column22 as POLICYHOLDER_LOCATOR, Column23 as PREMIUM_CHANGE, Column24 as PREMIUM_CHANGE_CURRENCY,
        Column25 as PRODUCT_LOCATOR, to_timestamp(Column26) as UPDATED_TIMESTAMP FROM VALUES
{% for modification in modifications %}
    {% if modification %}
        {% for mod_json in fromjson(modification) %}
{# {{ log(mod, info=True) }} #}
(
    '{{ pk[loop.index0] + '-' + mod_json.displayId }}',
    '{{ pk[loop.index0] }}',
    {% if 'automatedUnderwritingResult' in mod_json and 'decision' in mod_json['automatedUnderwritingResult'] %}'{{ mod_json['automatedUnderwritingResult']['decision'] }}',{% else %}null,{% endif %},
    {% if 'automatedUnderwritingResult' in mod_json and 'decisionTimestamp' in mod_json['automatedUnderwritingResult'] %}'{{ mod_json['automatedUnderwritingResult']['decisionTimestamp'] }}',{% else %}null,{% endif %},
    {% if 'automatedUnderwritingResult' in mod_json and 'notes' in mod_json['automatedUnderwritingResult'] %}'{{ tojson(mod_json['automatedUnderwritingResult']['notes']) }}',{% else %}null,{% endif %},
    '{{ mod_json.configVersion or null }}',
    '{{ mod_json.createdTimestamp or null }}',
    '{{ mod_json.displayId or null }}',
    '{{ mod_json.effectiveTimestamp or null }}',
    '{{ tojson(mod_json.exposureModifications) or null }}',
    '{{ tojson(mod_json.fieldGroupsByLocator) or null }}',
    '{{ tojson(mod_json.fieldValues) or null }}',
    '{{ mod_json.issuedTimestamp or null }}',
    '{{ mod_json.locator or null }}',
    '{{ tojson(mod_json.mediaByLocator) or null }}',
    '{{ mod_json.name or null }}',
    '{{ mod_json.newPolicyCharacteristicsLocator or null }}',
    '{{ tojson(mod_json.newPolicyCharacteristicsLocators) or null }}',
    '{{ mod_json.number or null }}',
    '{{ mod_json.policyEndTimestamp or null }}',
    '{{ mod_json.policyLocator or null }}',
    '{{ mod_json.policyholderLocator or null }}',
    '{{ mod_json.premiumChange or null }}',
    '{{ mod_json.premiumChangeCurrency or null }}',
    '{{ mod_json.productLocator or null }}',
    '{{ mod_json.updatedTimestamp or null }}'
){% if not loop.last %},{% endif %}
    {% endfor %}
    {% endif %}
{% endfor %}
{% endmacro %}