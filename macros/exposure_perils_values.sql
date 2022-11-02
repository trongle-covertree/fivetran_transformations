{% macro run_exposures_perils_values(env, prefix) %}

{% set exposures_perils_values_query %}
select exposures, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
  WHERE created_timestamp > (select policy_created_timestamp from {{ env }}.{{ prefix }}_policy_exposures_perils_values order by created_timestamp desc limit 1)
      or updated_timestamp > (select policy_updated_timestamp from {{ env }}.{{ prefix }}_policy_exposures_perils_values order by updated_timestamp desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(exposures_perils_values_query) %}

{% if execute %}
    {% set exposures = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
    {% set created_timestamps = results.columns[2].values() %}
    {% set updated_timestamps = results.columns[3].values() %}
{% endif %}

{% if exposures|length > 0%}
    {% if is_incremental() %}
        {% if pk|length == 1 %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_perils_values where PK in {{ pk|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_perils_values where PK in {{ pk }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}
    SELECT Column1 AS PK, Column2 AS PERIL_LOCATOR, Column3 AS LOCATOR, Column4 AS COVERAGE, Column5 AS NAME, Column6 AS VALUE, Column7 AS SPP_DESC, Column8 AS SPP_TYPE,
        Column9 AS SPP_VALUE, to_timestamp(Column10) AS CREATED_TIMESTAMP, to_timestamp(Column11) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column12) AS POLICY_UPDATED_TIMESTAMP
        FROM VALUES
    {% for exposure in exposures %}
        {% set outer_loop = loop %}
        {% if exposure %}
            {% for exposure_json in fromjson(exposure) %}
                {% for peril in exposure_json.perils if exposure_json.name != 'Policy Level Coverages' %}
                    {% set peril_loop = loop %}
                    {% for peril_char in peril.characteristics %}
                        {% set char_loop = loop %}
                        {% set peril_char_keys = { coverageEndTimestamp: none, coverageStartTimestamp: none, createdTimestamp: none, deductible: none, deductibleCurrency: none, exposureCharacteristicsLocator: none, fieldGroupsByLocator: none, fieldValues: none, indemnityInAggregate: none, indemnityInAggregateCurrency: none, indemnityPerEvent: none, indemnityPerEventCurrency: none, indemnityPerItem: none, indemnityPerItemCurrency: none, issuedTimestamp: none, locator: none, lumpSumPayment: none, lumpSumPaymentCurrency: none, mediaByLocator: none, monthPremium: none, perilLocator: none, policyCharacteristicsLocator: none, policyLocator: none, policyModificationLocator: none, policyholderLocator: none, premium: none, premiumCurrency: none, productLocator: none, replacedTimestamp: none, updatedTimestamp: none } %}
                        {% for current_peril_char_key in peril_char.keys() %}
                            {% if 'fieldValues' == current_peril_char_key and peril.name != 'Scheduled Personal Property' %}
                                {% for key in peril_char[current_peril_char_key].keys() %}
                                    (
                                        '{{ pk[outer_loop.index0] }}',
                                        '{{ peril_char.perilLocator }}', 
                                        '{{ peril_char.locator }}',
                                        '{{ peril.name| replace("'", "\\'")}}',
                                        '{{key}}',
                                        '{{ peril_char[current_peril_char_key][key][0]|replace("/$|,/g", "") }}',
                                        null,
                                        null,
                                        null,
                                        '{{ peril_char.createdTimestamp}}',
                                        '{{ created_timestamps[outer_loop.index0] }}',
                                        '{{ updated_timestamps[outer_loop.index0] }}'
                                    ){% if not outer_loop.last or not peril_loop.last or not char_loop.last or not loop.last %},{% endif %}
                                {% endfor %}
                            {% endif %}
                        {% endfor %}
                    {% endfor %}
                {% endfor %}
                {% for peril in exposure_json.perils if exposure_json.name == 'Policy Level Coverages' %}
                    {% set peril_loop = loop %}
                    {% for peril_char in peril.characteristics %}
                        {% if peril.name == 'Scheduled Personal Property' %}
                            {% for sppKey in peril_char.fieldGroupsByLocator.keys() %}
                                    (
                                        '{{ pk[outer_loop.index0] }}',
                                        '{{ peril_char.perilLocator }}', 
                                        '{{ peril_char.locator }}',
                                        '{{ peril.name| replace("'", "\\'")}}',
                                        null,
                                        null,
                                        '{{ peril_char.fieldGroupsByLocator[sppKey].spp_desc[0] }}',
                                        '{{ peril_char.fieldGroupsByLocator[sppKey].spp_type[0] }}',
                                        '{{ peril_char.fieldGroupsByLocator[sppKey].spp_value[0] }}',
                                        '{{ peril_char.createdTimestamp}}',
                                        '{{ created_timestamps[outer_loop.index0] }}',
                                        '{{ updated_timestamps[outer_loop.index0] }}'
                                    ){% if not outer_loop.last or not peril_loop.last or not char_loop.last or not loop.last %},{% endif %}
                            {% endfor %}
                        {% else %}
                            {% if 'fieldValues' == current_peril_char_key and peril.name != 'Scheduled Personal Property' %}
                                {% for key in peril_char[current_peril_char_key].keys() %}
                                    (
                                        '{{ pk[outer_loop.index0] }}',
                                        '{{ peril_char.perilLocator }}', 
                                        '{{ peril_char.locator }}',
                                        '{{ peril.name| replace("'", "\\'")}}',
                                        '{{key}}',
                                        '{{ peril_char[current_peril_char_key][key][0]|replace("/$|,/g", "") }}',
                                        null,
                                        null,
                                        null,
                                        '{{ peril_char.createdTimestamp}}',
                                        '{{ created_timestamps[outer_loop.index0] }}',
                                        '{{ updated_timestamps[outer_loop.index0] }}'
                                    ){% if not outer_loop.last or not peril_loop.last or not char_loop.last or not loop.last %},{% endif %}
                                {% endfor %}
                            {% endif %}
                        {% endif %}
                    {% endfor %}
                {% endfor %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% else %}
    SELECT Column1 AS PK, Column2 AS PERIL_LOCATOR, Column3 AS LOCATOR, Column4 AS COVERAGE, Column5 AS NAME, Column6 AS VALUE, Column7 AS SPP_DESC, Column8 AS SPP_TYPE,
        Column9 AS SPP_VALUE, to_timestamp(Column10) AS CREATED_TIMESTAMP, to_timestamp(Column11) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column12) AS POLICY_UPDATED_TIMESTAMP
        FROM VALUES
    ('NO FIELDS', null, null, null, null, null, null, null, null, null, null, null) limit 0
{% endif %}
{% endmacro %}