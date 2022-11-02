{% macro run_exposures_perils(env, prefix) %}

{% set exposures_perils_query %}
select exposures, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
  WHERE created_timestamp > (select policy_created_timestamp from {{ env }}.{{ prefix }}_policy_exposures_perils order by created_timestamp desc limit 1)
      or updated_timestamp > (select policy_updated_timestamp from {{ env }}.{{ prefix }}_policy_exposures_perils order by updated_timestamp desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(exposures_perils_query) %}

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
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_perils where PK in {{ pk|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_perils where PK in {{ pk }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}
    SELECT Column1 AS PK, Column2 AS PERIL_LEVEL, Column3 AS PERIL_NAME, to_timestamp(Column4) AS COVERAGE_END_TIMESTAMP, to_timestamp(Column5) AS COVERAGE_START_TIMESTAMP,
        to_timestamp(Column6) AS CREATED_TIMESTAMP, Column7 AS DEDUCTIBLE, Column8 AS DEDUCTIBLE_CURRENCY, Column9 AS EXPOSURE_CHARACTERISTICS_LOCATOR,
        parse_json(Column10) AS FIELD_GROUPS_BY_LOCATOR, parse_json(Column11) AS FIELD_VALUES, Column12 AS INDEMNITY_IN_AGGREGATE, Column13 AS INDEMNITY_IN_AGGREGATE_CURRENCY,
        Column14 AS INDEMNITY_PER_EVENT, Column15 AS INDEMNITY_PER_EVENT_CURRENCY, Column16 AS INDEMNITY_PER_ITEM, Column17 AS INDEMNITY_PER_ITEM_CURRENCY,
        to_timestamp(Column18) AS ISSUED_TIMESTAMP, Column19 AS LOCATOR, Column20 AS LUMP_SUM_PAYMENT, Column21 AS LUMP_SUM_PAYMENT_CURRENCY, parse_json(Column22) AS MEDIA_BY_LOCATOR,
        Column23 AS MONTH_PREMIUM, Column24 AS PERIL_LOCATOR, Column25 AS POLICY_CHARACTERISTICS_LOCATOR, Column26 AS POLICY_LOCATOR,
        Column27 AS POLICY_MODIFICATION_LOCATOR, Column28 AS POLICYHOLDER_LOCATOR, Column29 AS PREMIUM, Column30 AS PREMIUM_CURRENCY,
        Column31 AS PRODUCT_LOCATOR, to_timestamp(Column32) AS REPLACED_TIMESTAMP, to_timestamp(Column33) AS UPDATED_TIMESTAMP, to_date(Column34) AS COVERAGE_END_DATE,
        to_date(Column35) AS COVERAGE_START_DATE, to_timestamp(Column36) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column37) AS POLICY_UPDATED_TIMESTAMP FROM VALUES
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
        {% if 'coverageEndTimestamp' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'coverageEndTimestamp': peril_char[current_peril_char_key] }) %}
        {% elif 'coverageStartTimestamp' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'coverageStartTimestamp': peril_char[current_peril_char_key] }) %}
        {% elif 'createdTimestamp' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'createdTimestamp': peril_char[current_peril_char_key] }) %}
        {% elif 'deductible' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'deductible': peril_char[current_peril_char_key] }) %}
        {% elif 'deductibleCurrency' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'deductibleCurrency': peril_char[current_peril_char_key] }) %}
        {% elif 'exposureCharacteristicsLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'exposureCharacteristicsLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'fieldGroupsByLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'fieldGroupsByLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'fieldValues' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'fieldValues': peril_char[current_peril_char_key] }) %}
        {% elif 'indemnityInAggregate' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'indemnityInAggregate': peril_char[current_peril_char_key] }) %}
        {% elif 'indemnityInAggregateCurrency' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'indemnityInAggregateCurrency': peril_char[current_peril_char_key] }) %}
        {% elif 'indemnityPerEvent' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'indemnityPerEvent': peril_char[current_peril_char_key] }) %}
        {% elif 'indemnityPerEventCurrency' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'indemnityPerEventCurrency': peril_char[current_peril_char_key] }) %}
        {% elif 'indemnityPerItem' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'indemnityPerItem': peril_char[current_peril_char_key] }) %}
        {% elif 'indemnityPerItemCurrency' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'indemnityPerItemCurrency': peril_char[current_peril_char_key] }) %}
        {% elif 'issuedTimestamp' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'issuedTimestamp': peril_char[current_peril_char_key] }) %}
        {% elif 'locator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'locator': peril_char[current_peril_char_key] }) %}
        {% elif 'lumpSumPayment' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'lumpSumPayment': peril_char[current_peril_char_key] }) %}
        {% elif 'lumpSumPaymentCurrency' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'lumpSumPaymentCurrency': peril_char[current_peril_char_key] }) %}
        {% elif 'mediaByLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'mediaByLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'monthPremium' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'monthPremium': peril_char[current_peril_char_key] }) %}
        {% elif 'perilLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'perilLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'policyCharacteristicsLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'policyCharacteristicsLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'policyLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'policyLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'policyModificationLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'policyModificationLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'policyholderLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'policyholderLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'premium' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'premium': peril_char[current_peril_char_key] }) %}
        {% elif 'premiumCurrency' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'premiumCurrency': peril_char[current_peril_char_key] }) %}
        {% elif 'productLocator' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'productLocator': peril_char[current_peril_char_key] }) %}
        {% elif 'replacedTimestamp' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'replacedTimestamp': peril_char[current_peril_char_key] }) %}
        {% elif 'updatedTimestamp' == current_peril_char_key %}
            {% do peril_char_keys.update({ 'updatedTimestamp': peril_char[current_peril_char_key] }) %}
        {% endif %}
                        {% endfor %}
    (
        '{{ pk[outer_loop.index0] }}',
        '{{ exposure_json.name }}',
        '{{ peril.name }}',
        {% if peril_char_keys['coverageEndTimestamp']|length > 0%}'{{ peril_char_keys['coverageEndTimestamp'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['coverageStartTimestamp']|length > 0%}'{{ peril_char_keys['coverageStartTimestamp'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['createdTimestamp']|length > 0%}'{{ peril_char_keys['createdTimestamp'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['deductible']|length > 0%}'{{ peril_char_keys['deductible'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['deductibleCurrency']|length > 0%}'{{ peril_char_keys['deductibleCurrency'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['exposureCharacteristicsLocator']|length > 0%}'{{ peril_char_keys['exposureCharacteristicsLocator'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['fieldGroupsByLocator']|length > 0%}'{{ tojson(peril_char_keys['fieldGroupsByLocator']) }}'{% else %}null{% endif %},
        {% if peril_char_keys['fieldValues']|length > 0%}'{{ tojson(peril_char_keys['fieldValues']) }}'{% else %}null{% endif %},
        {% if peril_char_keys['indemnityInAggregate']|length > 0%}'{{ peril_char_keys['indemnityInAggregate'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['indemnityInAggregateCurrency']|length > 0%}'{{ peril_char_keys['indemnityInAggregateCurrency'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['indemnityPerEvent']|length > 0%}'{{ peril_char_keys['indemnityPerEvent'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['indemnityPerEventCurrency']|length > 0%}'{{ peril_char_keys['indemnityPerEventCurrency'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['indemnityPerItem']|length > 0%}'{{ peril_char_keys['indemnityPerItem'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['indemnityPerItemCurrency']|length > 0%}'{{ peril_char_keys['indemnityPerItemCurrency'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['issuedTimestamp']|length > 0%}'{{ peril_char_keys['issuedTimestamp'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['locator']|length > 0%}'{{ peril_char_keys['locator'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['lumpSumPayment']|length > 0%}'{{ peril_char_keys['lumpSumPayment'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['lumpSumPaymentCurrency']|length > 0%}'{{ peril_char_keys['lumpSumPaymentCurrency'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['mediaByLocator']|length > 0%}'{{ tojson(peril_char_keys['mediaByLocator']) }}'{% else %}null{% endif %},
        {% if peril_char_keys['monthPremium']|length > 0%}'{{ peril_char_keys['monthPremium'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['perilLocator']|length > 0%}'{{ peril_char_keys['perilLocator'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['policyCharacteristicsLocator']|length > 0%}'{{ peril_char_keys['policyCharacteristicsLocator'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['policyLocator']|length > 0%}'{{ peril_char_keys['policyLocator'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['policyModificationLocator']|length > 0%}'{{ peril_char_keys['policyModificationLocator'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['policyholderLocator']|length > 0%}'{{ peril_char_keys['policyholderLocator'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['premium']|length > 0%}'{{ peril_char_keys['premium'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['premiumCurrency']|length > 0%}'{{ peril_char_keys['premiumCurrency'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['productLocator']|length > 0%}'{{ peril_char_keys['productLocator'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['replacedTimestamp']|length > 0%}'{{ peril_char_keys['replacedTimestamp'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['updatedTimestamp']|length > 0%}'{{ peril_char_keys['updatedTimestamp'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['coverageEndTimestamp']|length > 0%}'{{ peril_char_keys['coverageEndTimestamp'] }}'{% else %}null{% endif %},
        {% if peril_char_keys['coverageStartTimestamp']|length > 0%}'{{ peril_char_keys['coverageStartTimestamp'] }}'{% else %}null{% endif %},
        '{{ created_timestamps[outer_loop.index0] }}',
        '{{ updated_timestamps[outer_loop.index0] }}'
    ){% if not outer_loop.last or not peril_loop.last or not char_loop.last or not loop.last %},{% endif %}
                    {% endfor %}
                {% endfor %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% else %}
SELECT Column1 AS PK, Column2 AS PERIL_LEVEL, Column3 AS PERIL_NAME, to_timestamp(Column4) AS COVERAGE_END_TIMESTAMP, to_timestamp(Column5) AS COVERAGE_START_TIMESTAMP,
    to_timestamp(Column6) AS CREATED_TIMESTAMP, Column7 AS DEDUCTIBLE, Column8 AS DEDUCTIBLE_CURRENCY, Column9 AS EXPOSURE_CHARACTERISTICS_LOCATOR,
    parse_json(Column10) AS FIELD_GROUPS_BY_LOCATOR, parse_json(Column11) AS FIELD_VALUES, Column12 AS INDEMNITY_IN_AGGREGATE, Column13 AS INDEMNITY_IN_AGGREGATE_CURRENCY,
    Column14 AS INDEMNITY_PER_EVENT, Column15 AS INDEMNITY_PER_EVENT_CURRENCY, Column16 AS INDEMNITY_PER_ITEM, Column17 AS INDEMNITY_PER_ITEM_CURRENCY,
    to_timestamp(Column18) AS ISSUED_TIMESTAMP, Column19 AS LOCATOR, Column20 AS LUMP_SUM_PAYMENT, Column21 AS LUMP_SUM_PAYMENT_CURRENCY, parse_json(Column22) AS MEDIA_BY_LOCATOR,
    Column23 AS MONTH_PREMIUM, Column24 AS PERIL_LOCATOR, Column25 AS POLICY_CHARACTERISTICS_LOCATOR, Column26 AS POLICY_LOCATOR,
    Column27 AS POLICY_MODIFICATION_LOCATOR, Column28 AS POLICYHOLDER_LOCATOR, Column29 AS PREMIUM, Column30 AS PREMIUM_CURRENCY,
    Column31 AS PRODUCT_LOCATOR, to_timestamp(Column32) AS REPLACED_TIMESTAMP, to_timestamp(Column33) AS UPDATED_TIMESTAMP, to_date(Column34) AS COVERAGE_END_DATE,
    to_date(Column35) AS COVERAGE_START_DATE, to_timestamp(Column36) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column37) AS POLICY_UPDATED_TIMESTAMP FROM VALUES
    ('NO FIELDS', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
{% endif %}
{% endmacro %}