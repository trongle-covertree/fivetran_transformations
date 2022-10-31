{% macro run_exposures_lender_info(env, prefix) %}

{% set exposures_lender_info_query %}
select exposures, pk, created_timestamp, updated_timestamp
from {{ env }}.{{ prefix }}_policies_policy
{% if is_incremental() %}
  WHERE created_timestamp > (select policy_created_timestamp from {{ env }}.{{ prefix }}_policy_exposures_lender_info order by created_timestamp desc limit 1)
      or updated_timestamp > (select policy_updated_timestamp from {{ env }}.{{ prefix }}_policy_exposures_lender_info order by updated_timestamp desc limit 1)
{% endif %}
{% endset %}

{% set results = run_query(exposures_lender_info_query) %}

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
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_lender_info where PK in {{ pk|replace(",", "") }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% else %}
            {% set delete_query %}
            DELETE FROM {{ env }}.{{ prefix }}_policy_exposures_lender_info where PK in {{ pk }}
            {% endset %}
            {% do run_query(delete_query) %}
        {% endif %}
    {% endif %}

SELECT Column1 AS ID, Column2 AS PK, Column3 AS ACCOUNT_NUMBER, Column4 AS NAME, Column5 AS OFFICER_FIRST_NAME, Column6 AS OFFICER_LAST_NAME, Column7 AS OFFICER_MAIL_ADDRESS,
    Column8 AS STREET_ADDRESS, Column9 AS LOT_UNIT, Column10 AS STATE, Column11 AS ZIP_CODE, Column12 AS CITY, Column13 AS COUNTRY, Column14 AS TYPE, Column15 AS ADDITIONAL_INTEREST_ID,
    to_timestamp(Column16) AS CREATED_TIMESTAMP, to_timestamp(Column17) AS UPDATED_TIMESTAMP, to_timestamp(Column18) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column19) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    {% for exposure in exposures %}
        {% set outer_loop = loop %}
        {% if exposure %}
            {% set exposure_arr = fromjson(exposure) %}
            {% for exposure_json in exposure_arr %}
                {% set exposure_json_loop = loop %}
                {% for char in exposure_json.characteristics if exposure_json.name != 'Policy Level Coverages' %}
                    {% set char_loop = loop %}
                    {% set lender_info_keys = { id: none, account_number: none, name: none, officer_first_name: none, officer_last_name: none, officer_mail_address: none, type: none, additional_interest_id: none, country: none, city: none, lot_unit: none, state: none, county: none, street_address: none, zip_code: none, created_timestamp: none, updated_timestamp: none } %}
                    {% for current_char_key in char.fieldGroupsByLocator.keys() %}
                        {% if 'officer_first_name' in char.fieldGroupsByLocator[current_char_key] or 'officer_last_name' in char.fieldGroupsByLocator[current_char_key] or 'officer_mail_address' in char.fieldGroupsByLocator[current_char_key]
                            or 'account_number' in char.fieldGroupsByLocator[current_char_key] or 'additional_interest_id' in char.fieldGroupsByLocator[current_char_key] %}
                           {% if 'account_number' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'account_number': char.fieldGroupsByLocator[current_char_key].account_number[0] }) %}
                            {% endif %} 
                            {% if 'name' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'name': char.fieldGroupsByLocator[current_char_key].name[0] }) %}
                            {% endif %}
                            {% if 'officer_first_name' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'officer_first_name': char.fieldGroupsByLocator[current_char_key].officer_first_name[0] }) %}
                            {% endif %}
                            {% if 'officer_last_name' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'officer_last_name': char.fieldGroupsByLocator[current_char_key].officer_last_name[0] }) %}
                            {% endif %}
                            {% if 'officer_mail_address' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'officer_mail_address': char.fieldGroupsByLocator[current_char_key].officer_mail_address[0] }) %}
                            {% endif %}
                            {% if 'type' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'type': char.fieldGroupsByLocator[current_char_key].type[0] }) %}
                            {% endif %}
                            {% if 'additional_interest_id' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'additional_interest_id': char.fieldGroupsByLocator[current_char_key].additional_interest_id[0] }) %}
                            {% endif %}
                            {% if 'country' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'country': char.fieldGroupsByLocator[current_char_key].country[0] }) %}
                            {% endif %}
                            {% if 'city' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'city': char.fieldGroupsByLocator[current_char_key].city[0] }) %}
                            {% endif %}
                            {% if 'lot_unit' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'lot_unit': char.fieldGroupsByLocator[current_char_key].lot_unit[0] }) %}
                            {% endif %}
                            {% if 'state' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'state': char.fieldGroupsByLocator[current_char_key].state[0] }) %}
                            {% endif %}
                            {% if 'street_address' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'street_address': char.fieldGroupsByLocator[current_char_key].street_address[0] }) %}
                            {% endif %}
                            {% if 'zip_code' in char.fieldGroupsByLocator[current_char_key] %}
                                {% do lender_info_keys.update({ 'zip_code': char.fieldGroupsByLocator[current_char_key].zip_code[0] }) %}
                            {% endif %}
                        {% endif %}
                        {% if loop.last %}
                            {% do lender_info_keys.update({ 'id': char.locator }) %}
                            {% do lender_info_keys.update({ 'created_timestamp': char.createdTimestamp }) %}
                            {% do lender_info_keys.update({ 'updated_timestamp': char.updatedTimestamp }) %}
    (
        {% if lender_info_keys.id|length > 0 %}'{{ lender_info_keys.id }}'{% else %}null{% endif %},
        '{{ pk[outer_loop.index0] }}',
        {% if lender_info_keys.account_number|length > 0 %}'{{ lender_info_keys.account_number }}'{% else %}null{% endif %},
        {% if lender_info_keys.name|length > 0 %}'{{ lender_info_keys.name | replace("'", "\\'") }}'{% else %}null{% endif %},
        {% if lender_info_keys.officer_first_name|length > 0 %}'{{ lender_info_keys.officer_first_name }}'{% else %}null{% endif %},
        {% if lender_info_keys.officer_last_name|length > 0 %}'{{ lender_info_keys.officer_last_name }}'{% else %}null{% endif %},
        {% if lender_info_keys.officer_mail_address|length > 0 %}'{{ lender_info_keys.officer_mail_address }}'{% else %}null{% endif %},
        {% if lender_info_keys.street_address|length > 0 %}'{{ lender_info_keys.street_address | replace("'", "\\'") }}'{% else %}null{% endif %},
        {% if lender_info_keys.lot_unit|length > 0 %}'{{ lender_info_keys.lot_unit }}'{% else %}null{% endif %},
        {% if lender_info_keys.state|length > 0 %}'{{ lender_info_keys.state }}'{% else %}null{% endif %},
        {% if lender_info_keys.zip_code|length > 0 %}'{{ lender_info_keys.zip_code }}'{% else %}null{% endif %},
        {% if lender_info_keys.city|length > 0 %}'{{ lender_info_keys.city | replace("'", "\\'") }}'{% else %}null{% endif %},
        {% if lender_info_keys.country|length > 0 %}'{{ lender_info_keys.country }}'{% else %}null{% endif %},
        {% if lender_info_keys.type|length > 0 %}'{{ lender_info_keys.city | replace("'", "\\'") }}'{% else %}null{% endif %},
        {% if lender_info_keys.additional_interest_id|length > 0 %}'{{ lender_info_keys.country }}'{% else %}null{% endif %},
        {% if lender_info_keys.created_timestamp|length > 0 %}'{{ lender_info_keys.created_timestamp }}'{% else %}null{% endif %},
        {% if lender_info_keys.updated_timestamp|length > 0 %}'{{ lender_info_keys.updated_timestamp }}'{% else %}null{% endif %},
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
SELECT Column1 AS ID, Column2 AS PK, Column3 AS ACCOUNT_NUMBER, Column4 AS NAME, Column5 AS OFFICER_FIRST_NAME, Column6 AS OFFICER_LAST_NAME, Column7 AS OFFICER_MAIL_ADDRESS,
    Column8 AS STREET_ADDRESS, Column9 AS LOT_UNIT, Column10 AS STATE, Column11 AS ZIP_CODE, Column12 AS CITY, Column13 AS COUNTRY, Column14 AS TYPE, Column15 AS ADDITIONAL_INTEREST_ID,
    to_timestamp(Column16) AS CREATED_TIMESTAMP, to_timestamp(Column17) AS UPDATED_TIMESTAMP, to_timestamp(Column18) AS POLICY_CREATED_TIMESTAMP, to_timestamp(Column19) AS POLICY_UPDATED_TIMESTAMP
    FROM VALUES
    ('NO FIELDS', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
{% endif %}
{% endmacro %}