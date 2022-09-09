{% macro run_cancellations(env) %}

{% set cancellations_query %}
select price, pk, sk, POLICY_MODIFICATION_LOCATOR
{# {{ log(pk[loop.index0], info=True) }} #}
from {{ env }}.policies_cancellation

{% endset %}

{% set results = run_query(cancellations_query) %}

{% if execute %}
    {% set prices = results.columns[0].values() %}
    {% set pk = results.columns[1].values() %}
    {% set sk = results.columns[2].values() %}
    {% set pol_mod_locator = results.columns[3].values() %}
{% endif %}

SELECT  Column1 AS ID, Column2 AS PK, Column3 AS SK, Column4 AS POLICY_MODIFICATION_LOCATOR, parse_json(Column5) AS COMMISSIONS, parse_json(Column6) AS EXPOSURE_PRICES,
    parse_json(Column7) AS FEES, Column8 AS GROSS_COMMISSIONS_CHANGE, Column9 AS GROSS_FEES_CHANGE, Column10 AS GROSS_PREMIUM_CHANGE, Column11 AS GROSS_TAXES_CHANGE,
    parse_json(Column12) AS HOLDBACKS, Column13 AS NEW_GROSS_COMMISSIONS, Column14 AS NEW_GROSS_FEES, Column15 AS NEW_GROSS_PREMIUM,
    Column16 AS NEW_GROSS_TAXES, Column17 AS NEW_TOTAL, parse_json(Column18) AS TAX_GROUPS, Column19 AS TOTAL_CHANGE
FROM VALUES 
{% for price in prices %}
    {% if price %}
        {% set price_json = fromjson(price) %}
(
    '{{ pk[loop.index0] + '-' + sk[loop.index0] }}',
    '{{ pk[loop.index0] }}',
    '{{ sk[loop.index0] }}',
    '{{ pol_mod_locator[loop.index0] }}',
    '{{ price_json.commissions }}',
    '{{ price_json.exposurePrices }}',
    '{{ price_json.fees }}',
    {{ price_json.grossCommissionsChange or 'null' }},
    {{ price_json.grossFeesChange or 'null' }},
    {{ price_json.grossPremiumChange or 'null' }},
    {{ price_json.grossTaxesChange or 'null' }},
    {{ price_json.holdbacks or 'null' }},
    {{ price_json.newGrossCommissions or 'null' }},
    {{ price_json.newGrossFees or 'null' }},
    {{ price_json.newGrossPremium or 'null' }},
    {{ price_json.newGrossTaxes or 'null' }},
    {{ price_json.newTotal or 'null' }},
    '{{ price_json.taxGroups }}',
    {{ price_json.totalChange or 'null' }}
){% if not loop.last %},{% endif %}
    {% endif %}
{% endfor %}
{% endmacro %}