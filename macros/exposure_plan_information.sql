{% macro run_exposures_plan_information(env, prefix) %}

{% set exposures_plan_information_query %}
select exposures, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
  WHERE created_timestamp > (select policy_created_timestamp from {{ env }}.{{ prefix }}_policy_exposures_plan_information order by created_timestamp desc limit 1)
      or updated_timestamp > (select policy_updated_timestamp from {{ env }}.{{ prefix }}_policy_exposures_plan_information order by updated_timestamp desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(exposures_plan_information_query) %}

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
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_plan_information where PK in {{ pk|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_plan_information where PK in {{ pk }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}
SELECT Column1 AS ID, Column2 AS PK, Column3 AS COMMUNITY_POLICY_DISCOUNT, Column4 AS PERSONALIZED_PLAN_TYPE, Column5 AS ACV, Column6 AS PARK_NAME, Column7 AS FORM,
    Column8 AS RCV, Column9 AS VALUATION_ID, Column10 AS PURCHASE_DATE, Column11 AS UNIT_ID, Column12 AS UNIT_LOCATION, Column13 AS POLICY_USAGE,
    Column14 AS SHORT_TERM_RENTAL_SURCHARGE, Column15 AS UNUSUAL_RISK, to_timestamp(Column16) AS CREATED_TIMESTAMP, to_timestamp(Column17) AS UPDATED_TIMESTAMP,
    to_timestamp(Column18) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column19) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    {% for exposure in exposures %}
        {% set outer_loop = loop %}
        {% if exposure %}
            {% set exposure_arr = fromjson(exposure) %}
            {% for exposure_json in exposure_arr %}
                {% set exposure_json_loop = loop %}
                {% for char in exposure_json.characteristics if exposure_json.name != 'Policy Level Coverages' %}
                    {% set char_loop = loop %}
                    {% set plan_info_keys = { id: none, community_policy_discount: none, personalized_plan_type: none, acv: none, park_name: none, form: none, rcv: none, valuation_id: none, purchase_date: none, unit_id: none, unit_location: none, policy_usage: none, short_term_rental_surcharge: none, unusual_risk: none, created_timestamp: none, updated_timestamp: none } %}
                    {% do plan_info_keys.update({ 'created_timestamp': char.createdTimestamp }) %}
                    {% do plan_info_keys.update({ 'updated_timestamp': char.updatedTimestamp }) %}
                    {% do plan_info_keys.update({ 'id': char.locator })%}
                    {% for current_char_key in char.fieldGroupsByLocator.keys() %}
                        {% if 'community_policy_discount' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'community_policy_discount': char.fieldGroupsByLocator[current_char_key].community_policy_discount[0] }) %}
                        {% endif %}
                        {% if 'personalized_plan_type' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'personalized_plan_type': char.fieldGroupsByLocator[current_char_key].personalized_plan_type[0] }) %}
                        {% endif %}
                        {% if 'acv' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'acv': char.fieldGroupsByLocator[current_char_key].acv[0] }) %}
                        {% endif %}
                        {% if 'park_name' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'park_name': char.fieldGroupsByLocator[current_char_key].park_name[0] }) %}
                        {% endif %}
                        {% if 'form' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'form': char.fieldGroupsByLocator[current_char_key].form[0] }) %}
                        {% endif %}
                        {% if 'rcv' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'rcv': char.fieldGroupsByLocator[current_char_key].rcv[0] }) %}
                        {% endif %}
                        {% if 'valuation_id' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'valuation_id': char.fieldGroupsByLocator[current_char_key].valuation_id[0] }) %}
                        {% endif %}
                        {% if 'purchase_date' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'purchase_date': char.fieldGroupsByLocator[current_char_key].purchase_date[0] }) %}
                        {% endif %}
                        {% if 'unit_id' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'unit_id': char.fieldGroupsByLocator[current_char_key].unit_id[0] }) %}
                        {% endif %}
                        {% if 'unit_location' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'unit_location': char.fieldGroupsByLocator[current_char_key].unit_location[0] }) %}
                        {% endif %}
                        {% if 'policy_usage' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'policy_usage': char.fieldGroupsByLocator[current_char_key].policy_usage[0] }) %}
                        {% endif %}
                        {% if 'short_term_rental_surcharge' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'short_term_rental_surcharge': char.fieldGroupsByLocator[current_char_key].short_term_rental_surcharge[0] }) %}
                        {% endif %}
                        {% if 'unusual_risk' in char.fieldGroupsByLocator[current_char_key] %}
                            {% do plan_info_keys.update({ 'unusual_risk': char.fieldGroupsByLocator[current_char_key].unusual_risk[0] }) %}
                        {% endif %}
                        {% if loop.last %}
    (
        {% if plan_info_keys.id|length > 0 or plan_info_keys is not none %}'{{ plan_info_keys.id }}'{% else %}null{% endif %},
        '{{ pk[outer_loop.index0] }}',
        {% if plan_info_keys.community_policy_discount|length > 0 %}'{{ plan_info_keys.community_policy_discount }}'{% else %}null{% endif %},
        {% if plan_info_keys.personalized_plan_type|length > 0 %}'{{ plan_info_keys.personalized_plan_type }}'{% else %}null{% endif %},
        {% if plan_info_keys.acv|length > 0 %}'{{ plan_info_keys.acv | replace(",", "") }}'{% else %}null{% endif %},
        {% if plan_info_keys.park_name|length > 0 %}'{{ plan_info_keys.park_name | replace("'", "\\'") }}'{% else %}null{% endif %},
        {% if plan_info_keys.form|length > 0 %}'{{ plan_info_keys.form }}'{% else %}null{% endif %},
        {% if plan_info_keys.rcv|length > 0 %}'{{ plan_info_keys.rcv | replace(",", "") }}'{% else %}null{% endif %},
        {% if plan_info_keys.valuation_id|length > 0 %}'{{ plan_info_keys.valuation_id }}'{% else %}null{% endif %},
        {% if plan_info_keys.purchase_date|length > 0 %}'{{ plan_info_keys.purchase_date }}'{% else %}null{% endif %},
        {% if plan_info_keys.unit_id|length > 0 %}'{{ plan_info_keys.unit_id }}'{% else %}null{% endif %},
        {% if plan_info_keys.unit_location|length > 0 %}'{{ plan_info_keys.unit_location }}'{% else %}null{% endif %},
        {% if plan_info_keys.policy_usage|length > 0 %}'{{ plan_info_keys.policy_usage }}'{% else %}null{% endif %},
        {% if plan_info_keys.short_term_rental_surcharge|length > 0 %}'{{ plan_info_keys.short_term_rental_surcharge }}'{% else %}null{% endif %},
        {% if plan_info_keys.unusual_risk|length > 0 %}'{{ plan_info_keys.unusual_risk }}'{% else %}null{% endif %},
        {% if plan_info_keys.created_timestamp|length > 0 %}'{{ plan_info_keys.created_timestamp }}'{% else %}null{% endif %},
        {% if plan_info_keys.updated_timestamp|length > 0 %}'{{ plan_info_keys.updated_timestamp }}'{% else %}null{% endif %},
        '{{ created_timestamps[outer_loop.index0] }}',
        '{{ updated_timestamps[outer_loop.index0] }}'
    ){% if not outer_loop.last or (outer_loop.last and not char_loop.last ) %},{% endif %}
                        {% endif %}
                    {% endfor %}
                {% endfor %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% else %}
SELECT Column1 AS ID, Column2 AS PK, Column3 AS COMMUNITY_POLICY_DISCOUNT, Column4 AS PERSONALIZED_PLAN_TYPE, Column5 AS ACV, Column6 AS PARK_NAME, Column7 AS FORM,
    Column8 AS RCV, Column9 AS VALUATION_ID, Column10 AS PURCHASE_DATE, Column11 AS UNIT_ID, Column12 AS UNIT_LOCATION, Column13 AS POLICY_USAGE,
    Column14 AS SHORT_TERM_RENTAL_SURCHARGE, Column15 AS UNUSUAL_RISK, to_timestamp(Column16) AS CREATED_TIMESTAMP, to_timestamp(Column17) AS UPDATED_TIMESTAMP,
    to_timestamp(Column18) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column19) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    ('NO FIELDS', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
{% endif %}
{% endmacro %}