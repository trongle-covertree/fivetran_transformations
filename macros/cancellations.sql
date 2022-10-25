{% macro run_cancellations(env, prefix) %}

{% set cancellations_query %}
select price, pk, sk, POLICY_MODIFICATION_LOCATOR, CREATED_TIMESTAMP, ISSUED_TIMESTAMP, EFFECTIVE_TIMESTAMP
{# {{ log(pk[loop.index0], info=True) }} #}
from {{ env }}.{{ prefix }}_policies_cancellation
{% if is_incremental() %}
   where CREATED_TIMESTAMP > (select CREATED_TIMESTAMP from {{ env }}.{{ prefix }}_policies_cancellations_prices order by CREATED_TIMESTAMP desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(cancellations_query) %}

{% if execute %}
    {% set prices = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
    {% set sk = results.columns[2].values() %}
    {% set pol_mod_locator = results.columns[3].values() %}
    {% set created_timestamps = results.columns[4].values() %}
    {% set issued_timestamps = results.columns[5].values() %}
    {% set effective_timestamps = results.columns[6].values() %}
{% endif %}

{% if is_incremental() %}
SELECT * FROM {{ env }}.{{ prefix }}_policies_cancellations_prices
{% endif %}
{% if prices|length > 0 %}
{% if is_incremental() %}
UNION
    (
{% endif %}
    SELECT  Column1 AS ID, Column2 AS PK, Column3 AS SK, Column4 AS POLICY_MODIFICATION_LOCATOR, parse_json(Column5) AS COMMISSIONS, parse_json(Column6) AS EXPOSURE_PRICES,
        parse_json(Column7) AS FEES, Column8 AS GROSS_COMMISSIONS_CHANGE, Column9 AS GROSS_FEES_CHANGE, Column10 AS GROSS_PREMIUM_CHANGE, Column11 AS GROSS_TAXES_CHANGE,
        parse_json(Column12) AS HOLDBACKS, to_double(Column13) AS NEW_GROSS_COMMISSIONS, Column14 AS NEW_GROSS_FEES, Column15 AS NEW_GROSS_PREMIUM,
        Column16 AS NEW_GROSS_TAXES, Column17 AS NEW_TOTAL, parse_json(Column18) AS TAX_GROUPS, Column19 AS TOTAL_CHANGE, to_timestamp(Column20) as CREATED_TIMESTAMP,
        to_timestamp(Column21) AS ISSUED_TIMESTAMP, to_timestamp(Column22) as EFFECTIVE_TIMESTAMP
    FROM VALUES 
    {% for price in prices %}
        {% if price %}
            {% set price_json = fromjson(price) %}
    (
        '{{ pk[loop.index0] + '-' + sk[loop.index0] }}',
        '{{ pk[loop.index0] }}',
        '{{ sk[loop.index0] }}',
        '{{ pol_mod_locator[loop.index0] }}',
        '{{ tojson(price_json.commissions) }}',
        '{{ tojson(price_json.exposurePrices) | replace("'", "\\'") }}',
        '{{ tojson(price_json.fees) }}',
        '{{ price_json.grossCommissionsChange or null }}',
        {{ price_json.grossFeesChange or 'null' }},
        {{ price_json.grossPremiumChange or 'null' }},
        {{ price_json.grossTaxesChange or 'null' }},
        '{{ tojson(price_json.holdbacks) }}',
        {{ price_json.newGrossCommissions or 'null' }},
        {{ price_json.newGrossFees or 'null' }},
        {{ price_json.newGrossPremium or 'null' }},
        {{ price_json.newGrossTaxes or 'null' }},
        {{ price_json.newTotal or 'null' }},
        '{{ tojson(price_json.taxGroups) }}',
        {{ price_json.totalChange or 'null' }},
        '{{ created_timestamps[loop.index0] }}',
        '{{ issued_timestamps[loop.index0] }}',
        '{{ effective_timestamps[loop.index0] }}'
    ){% if not loop.last %},{% endif %}
        {% endif %}
    {% endfor %}
{% if is_incremental() %}
    )
{% endif %}
{% endif %}
{% endmacro %}